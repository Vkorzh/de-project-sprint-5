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
class Couriers_Reward_ReportObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float
    mnt_date: date


# Репозиторий для получения данных из источника
class Couriers_Reward_ReportOriginRepository:
    def __init__(self, pg: PgConnect, log: log) -> None:
        self._db = pg
        self.log = log

    def list_couriers_reward(self, orders_threshold: int) -> List[Couriers_Reward_ReportObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    select  dd.courier_id as courier_id,
                            dc.full_name as courier_name,
                            dt2."year" as settlement_year,
                            dt2."month" as settlement_month,
                            count(fps.order_id) as orders_count,
                            sum(fps.total_sum) as orders_total_sum,
                            avg(dd.rate) as rate_avg,
                            sum(fps.total_sum * 0.25) order_processing_fee,
                            case 
                                when avg(dd.rate) < 4 
                                        then case 
                                                when 0.05*sum(fps.total_sum * 0.25)<=100
                                                    then 100
                                                else 0.05*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) < 4.5 
                                        then case 
                                                when 0.07*sum(fps.total_sum * 0.25)<=150
                                                    then 150
                                                else 0.07*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) < 4.9 
                                        then case 
                                                when 0.08*sum(fps.total_sum * 0.25)<=175
                                                    then 175
                                                else 0.08*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) >= 4.9 
                                        then case 
                                                when 0.1*sum(fps.total_sum * 0.25)<=200
                                                    then 200
                                                else 0.1*sum(fps.total_sum * 0.25)
                                            end
                                    else 0.05 
                                end as courier_order_sum,
                            sum(fps.tip_sum) as courier_tips_sum,
                            case 
                                when avg(dd.rate) < 4 
                                        then case 
                                                when 0.05*sum(fps.total_sum * 0.25)<=100
                                                    then 100
                                                else 0.05*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) < 4.5 
                                        then case 
                                                when 0.07*sum(fps.total_sum * 0.25)<=150
                                                    then 150
                                                else 0.07*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) < 4.9 
                                        then case 
                                                when 0.08*sum(fps.total_sum * 0.25)<=175
                                                    then 175
                                                else 0.08*sum(fps.total_sum * 0.25)
                                            end
                                    when avg(dd.rate) >= 4.9 
                                        then case 
                                                when 0.1*sum(fps.total_sum * 0.25)<=200
                                                    then 200
                                                else 0.1*sum(fps.total_sum * 0.25)
                                            end
                                    else 0.05 
                                end + sum(fps.tip_sum)*0.95 as courier_reward_sum,
                               date_trunc('month',(now() at time zone 'utc')+'1 month'::interval) as mnt_date
                        from dds.fct_product_sales fps 
                            join dds.dm_orders do2 
                                on fps.order_id = do2.id 
                            join dds.dm_deliveries dd 
                                on fps.delivery_id = dd.id  
                            join dds.dm_timestamps dt2 
                                on dd.timestamp_id = dt2.id 
                            join dds.dm_adress da 
                                on dd.adress_id = da.id 
                            join dds.dm_couriers dc 
                                on dd.courier_id = dc.id
                        WHERE (dt2."date"::date >= date_trunc('month',(now() at time zone 'utc')-'1 month'::interval)::date
                              AND
                             dt2."date"::date <= ((now() at time zone 'utc')::date -'1 day'::interval)::date) 
                              AND 
                            do2.order_status = 'CLOSED'
                        group by dd.courier_id,
                            dc.full_name,
                            dt2."year",
                            dt2."month",date_trunc('month',dt2.date)
                    """
                )
                objs = cur.fetchall()

            
        couriers_reward_reports = []
        for couriers_reward in objs:
            self.log.info(f"СТРОКА:{couriers_reward['courier_id']},{couriers_reward['courier_name']},{couriers_reward['settlement_year']},{couriers_reward['settlement_month']}")

            couriers_reward_sale = Couriers_Reward_ReportObj(
                courier_id = couriers_reward['courier_id'],
                courier_name = couriers_reward['courier_name'],
                settlement_year = couriers_reward['settlement_year'],
                settlement_month = couriers_reward['settlement_month'],
                orders_count = couriers_reward['orders_count'],
                orders_total_sum = couriers_reward['orders_total_sum'],
                rate_avg = couriers_reward['rate_avg'],
                order_processing_fee = couriers_reward['order_processing_fee'],
                courier_order_sum = couriers_reward['courier_order_sum'],
                courier_tips_sum = couriers_reward['courier_tips_sum'],
                courier_reward_sum = couriers_reward['courier_reward_sum'],
                mnt_date = couriers_reward['mnt_date']
            )
            couriers_reward_reports.append(couriers_reward_sale)

        return couriers_reward_reports

# Репозиторий для вставки данных в целевую таблицу
class Couriers_Reward_ReportDestRepository:
    def insert_couriers_reward_report(self, conn: Connection, couriers_reward_report: Couriers_Reward_ReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_couriers_reward_report
                (
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_reward_sum
                )
                VALUES (
                    %(courier_id)s, 
                    %(courier_name)s, 
                    %(settlement_year)s,
                    %(settlement_month)s, 
                    %(orders_count)s,
                    %(orders_total_sum)s, 
                    %(rate_avg)s, 
                    %(order_processing_fee)s, 
                    %(courier_order_sum)s,
                    %(courier_tips_sum)s, 
                    %(courier_reward_sum)s
                )
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                SET
                    courier_name = EXCLUDED.courier_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": couriers_reward_report.courier_id,
                    "courier_name": couriers_reward_report.courier_name,
                    "settlement_year": couriers_reward_report.settlement_year,
                    "settlement_month": couriers_reward_report.settlement_month,
                    "orders_count": couriers_reward_report.orders_count,
                    "orders_total_sum": couriers_reward_report.orders_total_sum,
                    "rate_avg": couriers_reward_report.rate_avg,
                    "order_processing_fee": couriers_reward_report.order_processing_fee,
                    "courier_order_sum": couriers_reward_report.courier_order_sum,
                    "courier_tips_sum": couriers_reward_report.courier_tips_sum,
                    "courier_reward_sum": couriers_reward_report.courier_reward_sum
                },
            )

# Класс для загрузки данных
class Couriers_Reward_ReportLoader:
    WF_KEY = "couriers_reward_report_origin_to_cdm_workflow"
    LAST_LOADED_DATE_KEY = "last_loaded_date"
    SCHEMA = 'dds'

    def __init__(self, pg_origin: PgConnect, log: log) -> None:
        self.pg_dest = pg_origin
        self.origin = Couriers_Reward_ReportOriginRepository(pg_origin, log)
        self.stg = Couriers_Reward_ReportDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers_reward_reports(self):
        self.log.info("Starting couriers_reward report loading process...")
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
                load_queue = self.origin.list_couriers_reward(last_loaded)
                self.log.info(f"Found {len(load_queue)} product sales to load.")

                if not load_queue:
                    self.log.info("No new couriers_reward reports found. Exiting...")
                    return

                # Сохраняем объекты в базу cdm.
                self.log.info("Inserting couriers_reward reports into the destination table...")
                for couriers_reward_report in load_queue:
                    self.stg.insert_couriers_reward_report(conn,couriers_reward_report)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY] = max([couriers_reward_report.mnt_date for couriers_reward_report in load_queue])
                
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
def sprint5_load_couriers_reward_reports_dag():

    # Создаем подключение к базе
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="cmd_couriers_reward_reports_load")
    def load_couriers_reward_reports():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = Couriers_Reward_ReportLoader(origin_pg_connect,  log)
        rest_loader.load_couriers_reward_reports()  # Вызываем функцию, которая перельет данные.

    load_couriers_reward_reports = load_couriers_reward_reports()

    load_couriers_reward_reports

load_settlement_reports_dag=sprint5_load_couriers_reward_reports_dag()