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


class SalesObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class ProductSaleObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class ProductSalesOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log

    def list_product_sales(self, orders_threshold: int) -> List[SalesObj]:
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
            objs = cur.fetchall()
        product_sales = []
        for order in objs:
            try:
                order_data = json.loads(order['object_value'])
                order_id = self._get_order_id(order['object_id'])  # Получаем ID заказа из dm_orders
                bonus_payment = order_data.get("bonus_payment", 0)
                bonus_grant = order_data.get("bonus_grant", 0)

                for item in order_data.get("order_items", []):
                    product_id = self._get_product_id(item["id"])  # Получаем ID продукта из dm_products
                    if not product_id:
                        self.log.warning(f"Product {item['id']} not found in dm_products")
                        continue

                    product_sale = ProductSaleObj(
                        id = item['id'],
                        product_id=product_id,
                        order_id=order_id,
                        count=item["quantity"],
                        price=item["price"],
                        total_sum=item["price"] * item["quantity"],
                        bonus_payment=bonus_payment,
                        bonus_grant=bonus_grant
                    )
                    product_sales.append(product_sale)
            except json.JSONDecodeError as e:
                self.log.error(f"JSON decode error for order {order['object_id']}: {e}")
                continue
        return product_sales

    def _get_order_id(self, order_object_id: str) -> int:
        # Поиск ID заказа по object_id
        with self._db.client().cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_orders WHERE order_key = %s LIMIT 1", (order_object_id,)
            )
            result = cur.fetchone()
            return result[0] if result else None

    def _get_product_id(self, product_object_id: str) -> int:
        # Поиск ID продукта по object_id
        with self._db.client().cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_products WHERE product_id = %s LIMIT 1", (product_object_id,)
            )
            result = cur.fetchone()
            return result[0] if result else None

class ProductSalesDestRepository:
    def insert_product_sale(self, conn: Connection, product_sale: ProductSaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                {
                    "product_id": product_sale.product_id,
                    "order_id": product_sale.order_id,
                    "count": product_sale.count,
                    "price": product_sale.price,
                    "total_sum": product_sale.total_sum,
                    "bonus_payment": product_sale.bonus_payment,
                    "bonus_grant": product_sale.bonus_grant
                },
            )

class ProductSalesLoader:
    WF_KEY = "example_product_sales_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductSalesOriginRepository(pg_origin, log)
        self.stg = ProductSalesDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_product_sales(self):
        self.log.info("Starting product sales loading process...")
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
                self.log.info(f"Fetching product sales with ID > {last_loaded}...")
                load_queue = self.origin.list_product_sales(last_loaded)
                self.log.info(f"Found {len(load_queue)} product sales to load.")

                if not load_queue:
                    self.log.info("No new product sales found. Exiting...")
                    return

                # Сохраняем объекты в базу dwh.
                self.log.info("Inserting product sales into the destination table...")
                for product_sale in load_queue:
                    self.stg.insert_product_sale(conn, product_sale)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([product_sale.order_id for product_sale in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.schema)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise
