from logging import Logger
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from datetime import datetime,date,time
import json
from pydantic import ValidationError

class ProductObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime

class ProductDestObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int

class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, restaurants_threshold: int=-1) -> List[ProductDestObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
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
        products = []
        for restaurant in objs:
            restaurant_data = json.loads(restaurant.object_value)
            for menu_item in restaurant_data.get("menu", []):
                product = ProductDestObj(
                    id=restaurant.id,  # Используем _id из меню как product_id
                    product_id=menu_item["_id"],
                    product_name=menu_item["name"],
                    product_price=float(menu_item["price"]),
                    active_from=restaurant.update_ts,
                    active_to=datetime(2099, 12, 31),  # Устанавливаем active_to в максимальную дату
                    restaurant_id=restaurant.id  # Связь с рестораном
                )
                products.append(product)
        return products

class ProductsDestRepository:
    def insert_products(self, conn: Connection, product: ProductDestObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to,
                        restaurant_id = EXCLUDED.restaurant_id;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to,
                    "restaurant_id": product.restaurant_id
                },
            )


class ProductsLoader:
    WF_KEY = "example_products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.stg = ProductsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
        self.log.info("Starting products loading process...")
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
                self.log.info(f"Fetching products with ID > {last_loaded}...")
                load_queue = self.origin.list_products(last_loaded)
                self.log.info(f"Found {len(load_queue)} products to load.")

                if not load_queue:
                    self.log.info("No new products found. Exiting...")
                    return

                # Сохраняем объекты в базу dwh.
                self.log.info("Inserting products into the destination table...")
                for product in load_queue:
                    self.log.info(f"Processing product ID: {product.product_id}")
                    self.stg.insert_products(conn, product)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.schema)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise