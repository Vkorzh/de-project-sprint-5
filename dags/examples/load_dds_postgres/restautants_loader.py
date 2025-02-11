from logging import Logger
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import json

class RestaurantObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime

class RestaurantDestObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurants_threshold: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": restaurants_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class RestaurantsDestRepository:
    def insert_restaurants(self, conn: Connection, restaurant: RestaurantDestObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "id": restaurant.id,
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

class RestaurantsLoader:
    WF_KEY = "example_restaurants_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.stg = RestaurantsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        self.log.info("Starting restaurants loading process...")
        with self.pg_dest.connection() as conn:
            try:
                # Прочитываем состояние загрузки
                self.log.info("Fetching workflow settings...")
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, self.schema)
                if not wf_setting:
                    self.log.info("No workflow settings found. Creating new settings...")
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

                # Вычитываем очередную пачку объектов.
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                self.log.info(f"Last loaded ID: {last_loaded}")
                self.log.info(f"Fetching restaurants with ID > {last_loaded}...")
                load_queue = self.origin.list_restaurants(last_loaded)
                self.log.info(f"Found {len(load_queue)} restaurants to load.")

                if not load_queue:
                    self.log.info("No new restaurants found. Exiting...")
                    return

                # Сохраняем объекты в базу dwh.
                self.log.info("Inserting restaurants into the destination table...")
                for restaurant in load_queue:
                    self.log.info(f"Processing restaurant ID: {restaurant.id}")
                    restaurant_data = json.loads(restaurant.object_value)
                    restaurant_dest = RestaurantDestObj(
                        id=restaurant.id,
                        restaurant_id=restaurant.object_id,
                        restaurant_name=restaurant_data.get("name"),
                        active_from=restaurant.update_ts,
                        active_to=datetime(2099, 12, 31)  # Устанавливаем active_to в максимальную дату для текущей записи
                    )
                    self.stg.insert_restaurants(conn, restaurant_dest)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.schema)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise