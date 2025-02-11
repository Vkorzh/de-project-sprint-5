import pendulum
from lib import ConnectionBuilder
import logging 
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from datetime import date
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from psycopg import Connection
from psycopg.rows import dict_row
from pydantic import BaseModel
import json

log = logging.getLogger(__name__)


# Модель данных для отчета
class SettlementReportObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


# Репозиторий для получения данных из источника
class SettlementReportOriginRepository:
    def __init__(self, pg: PgConnect, log: log) -> None:
        self._db = pg
        self.log = log

    def list_settlement(self, orders_threshold: int) -> List[SettlementReportObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT  o.restaurant_id,
                            dr.restaurant_name,
                            date as settlement_date,
                            count(distinct pr.order_id) as orders_count,
                            sum(pr.total_sum) as orders_total_sum,
                            sum(pr.bonus_payment) as orders_bonus_payment_sum,
                            sum(pr.bonus_grant) as orders_bonus_granted_sum,
                            sum(pr.total_sum) * 0.25 as order_processing_fee,
                            abs(sum(pr.total_sum) - sum(pr.bonus_payment) - sum(pr.total_sum * 0.25)) as restaurant_reward_sum
                    FROM dds.fct_product_sales AS pr
                    INNER JOIN dds.dm_orders AS o ON pr.order_id = o.id
                    INNER JOIN dds.dm_restaurants AS dr ON o.restaurant_id = dr.id
                    INNER JOIN dds.dm_timestamps AS t ON o.timestamp_id = t.id
                    INNER JOIN dds.dm_products AS dp ON pr.product_id = dp.id
                    WHERE (date::date >= (now()at time zone 'utc')::date - 3
                        AND
                        date::date <= (now()at time zone 'utc')::date - 1) AND o.order_status = 'CLOSED'
                    GROUP BY o.restaurant_id, dr.restaurant_name,date
                    """
                )
                objs = cur.fetchall()

            
        settlement_reports = []
        for settlement in objs:
            self.log.info(f"СТРОКА:{settlement['restaurant_id']},{settlement['restaurant_name']},{settlement['settlement_date']}")

            settlement_sale = SettlementReportObj(
                restaurant_id = settlement['restaurant_id'],
                restaurant_name = settlement['restaurant_name'],
                settlement_date = settlement['settlement_date'],
                orders_count = settlement['orders_count'],
                orders_total_sum = settlement['orders_total_sum'],
                orders_bonus_payment_sum = settlement['orders_bonus_payment_sum'],
                orders_bonus_granted_sum = settlement['orders_bonus_granted_sum'],
                order_processing_fee = settlement['order_processing_fee'],
                restaurant_reward_sum = settlement['restaurant_reward_sum']
            )
            settlement_reports.append(settlement_sale)
            
        return settlement_reports

# Репозиторий для вставки данных в целевую таблицу
class SettlementReportDestRepository:
     def insert_settlement_report(self, conn: Connection, settlement_report: SettlementReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                    """
                    INSERT INTO cdm.dm_settlement_report (
                        restaurant_id, restaurant_name, settlement_date, orders_count,
                        orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
                        order_processing_fee, restaurant_reward_sum
                    )
                    VALUES (
                        %(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s,
                        %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s,
                        %(order_processing_fee)s, %(restaurant_reward_sum)s
                    )
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                    """,
                    {
                        "restaurant_id": settlement_report.restaurant_id,
                        "restaurant_name": settlement_report.restaurant_name,
                        "settlement_date": settlement_report.settlement_date,
                        "orders_count": settlement_report.orders_count,
                        "orders_total_sum": settlement_report.orders_total_sum,
                        "orders_bonus_payment_sum": settlement_report.orders_bonus_payment_sum,
                        "orders_bonus_granted_sum": settlement_report.orders_bonus_granted_sum,
                        "order_processing_fee": settlement_report.order_processing_fee,
                        "restaurant_reward_sum": settlement_report.restaurant_reward_sum
                    },
                )

# Класс для загрузки данных
class SettlementReportLoader:
    WF_KEY = "settlement_report_origin_to_cdm_workflow"
    LAST_LOADED_DATE_KEY = "last_loaded_date"
    SCHEMA = 'dds'

    def __init__(self, pg_origin: PgConnect, log: log) -> None:
        self.pg_dest = pg_origin
        self.origin = SettlementReportOriginRepository(pg_origin, log)
        self.stg = SettlementReportDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_settlement_reports(self):
        self.log.info("Starting settlement report loading process...")
        with self.pg_dest.connection() as conn:
            try:
                # Прочитываем состояние загрузки
                self.log.info("Fetching workflow settings...")
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
                if not wf_setting:
                    self.log.info("No workflow settings found. Creating new settings...")
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_DATE_KEY: '2022-01-01'})

                # Вычитываем очередную пачку объектов.
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY]
                self.log.info(f"Last loaded ID: {last_loaded}")
                self.log.info(f"Fetching product sales with ID > {last_loaded}...")
                load_queue = self.origin.list_settlement(last_loaded)
                self.log.info(f"Found {len(load_queue)} product sales to load.")

                if not load_queue:
                    self.log.info("No new settlement reports found. Exiting...")
                    return

                # Сохраняем объекты в базу cdm.
                self.log.info("Inserting settlement reports into the destination table...")
                for settlement_report in load_queue:
                    self.stg.insert_settlement_report(conn,settlement_report)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY] = max([settlement_report.settlement_date for settlement_report in load_queue])
                
                # Преобразуем дату в строку перед сериализацией
                wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY] = wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY].isoformat()
                wf_setting_json = json2str(wf_setting.workflow_settings)
                
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.SCHEMA)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise


@dag(
    schedule_interval='0/20 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dmc', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_load_settlement_reports_dag():

    # Создаем подключение к базе
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="cmd_settlement_reports_load")
    def load_settlement_reports():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SettlementReportLoader(origin_pg_connect,  log)
        rest_loader.load_settlement_reports()  # Вызываем функцию, которая перельет данные.

    load_settlement_reports = load_settlement_reports()

    load_settlement_reports

load_settlement_reports_dag=sprint5_load_settlement_reports_dag()