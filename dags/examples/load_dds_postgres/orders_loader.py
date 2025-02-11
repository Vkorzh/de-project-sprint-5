from logging import Logger
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from datetime import datetime
import json
from psycopg import Connection
from psycopg.rows import dict_row
from pydantic import BaseModel

class OrderObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime

class OrderDestObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, orders_threshold: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": orders_threshold
                }
            )
            return cur.fetchall()

class OrdersDestRepository:
    def insert_order(self, conn: Connection, order: OrderDestObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                """, {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "user_id": order.user_id
                }
            )

class OrdersLoader:
    WF_KEY = "example_orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.stg = OrdersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        self.log.info("Starting orders loading process...")

        with self.pg_dest.connection() as conn:
            try:
                # Получаем настройки рабочего процесса
                self.log.info("Fetching workflow settings...")
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, self.schema)
                if not wf_setting:
                    self.log.info("No workflow settings found. Creating new settings...")
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

                # Вычитываем очередную пачку заказов
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                self.log.info(f"Last loaded ID: {last_loaded}")
                self.log.info(f"Fetching orders with ID > {last_loaded}...")
                load_queue = self.origin.list_orders(last_loaded)
                self.log.info(f"Found {len(load_queue)} orders to load.")

                if not load_queue:
                    self.log.info("No new orders found. Exiting...")
                    return

                # Сохраняем данные в таблицу dm_orders
                self.log.info("Inserting orders into the destination table...")
                for order in load_queue:
                    self.log.info(f"Processing order ID: {order['object_id']}")

                    # Парсим заказ из object_value
                    order_data = json.loads(order['object_value'])
                    order_key = order_data["_id"]
                    order_status = order_data["final_status"]
                    order_timestamp = datetime.strptime(order_data["date"], "%Y-%m-%d %H:%M:%S")

                    # Поиск нужных значений для ресторанов, пользователей и временной метки
                    self.log.info("Processing restaurant_id: %s",order_data["restaurant"]["id"])
                    restaurant_id = self._get_restaurant_id(order_data["restaurant"]["id"])
                    self.log.info("Processing user_id: %s",order_data["user"]["id"])
                    user_id = self._get_user_id(order_data["user"]["id"])
                    self.log.info("Processing timestamp: %s",order_timestamp)
                    timestamp_id = self._get_timestamp_id(order_timestamp)

                    order_dest = OrderDestObj(
                        order_key=order_key,
                        order_status=order_status,
                        restaurant_id=restaurant_id,
                        timestamp_id=timestamp_id,
                        user_id=user_id
                    )

                    # Вставка записи в таблицу dm_orders
                    self.stg.insert_order(conn, order_dest)
                    self.log.info(f"inserted order ID: {order['object_id']}")

                # Сохраняем прогресс
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([order['id'] for order in load_queue])
                wf_setting_json = json.dumps(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.schema)

                self.log.info(f"Load finished. Last loaded order ID: {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise

    def _get_restaurant_id(self, restaurant_object_id: str) -> int:
        # Поиск id ресторана по object_id
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM dds.dm_restaurants WHERE restaurant_id = %s LIMIT 1", (restaurant_object_id,)
                )
                result = cur.fetchone()
                return result[0] if result else None

    def _get_user_id(self, user_object_id: str) -> int:
        # Поиск id пользователя по object_id
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM dds.dm_users WHERE user_id = %s LIMIT 1", (user_object_id,)
                )
                result = cur.fetchone()
                return result[0] if result else None

    def _get_timestamp_id(self, timestamp: datetime) -> int:
        # Поиск id временной метки
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM dds.dm_timestamps WHERE ts = %s LIMIT 1", (timestamp,)
                )
                result = cur.fetchone()
                return result[0] if result else None
