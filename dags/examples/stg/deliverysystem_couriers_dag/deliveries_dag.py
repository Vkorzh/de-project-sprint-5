import logging
from examples.stg.deliverysystem_couriers_dag.pg_saver import PgSaver
from examples.stg.deliverysystem_couriers_dag.couriers_reader import CouriersReader
from examples.stg.deliverysystem_couriers_dag.deliveries_reader import DeliveryReader
from examples.stg.deliverysystem_couriers_dag.deliveries_loader import DeliveryLoader
import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)
# Добавляем путь к текущей директории в PYTHONPATH
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Импортируем модули
from deliveries_reader import DeliveryReader
from deliveries_loader import DeliveryLoader
from couriers_loader import CouriersLoader
from lib import ConnectionBuilder


@dag(
    schedule_interval='0/60 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_delivery_system_events_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="Couriers_load")
    def load_couriers():

        nickname = 'Vkorzh'
        cohort = 3
        api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        pg_saver = PgSaver()
        collection_loader=CouriersReader(nickname,cohort,api_key)

        # создаем экземпляр класса, в котором реализована логика.
        load_couriers = CouriersLoader(collection_loader,dwh_pg_connect,pg_saver,log)
        load_couriers.run_copy()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    load_couriers = load_couriers()

    # Объявляем таск, который загружает данные.
    @task(task_id="Deliveries_load")
    def load_deliveries():

        nickname = 'Vkorzh'
        cohort = 3
        api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        pg_saver = PgSaver()
        collection_loader=DeliveryReader(nickname,cohort,api_key,log)

        # создаем экземпляр класса, в котором реализована логика.
        load_couriers = DeliveryLoader(collection_loader,dwh_pg_connect,pg_saver,log)
        load_couriers.run_copy()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    load_deliveries = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    load_couriers >> load_deliveries  # type: ignore


stg_bonus_system_events_dag = sprint5_stg_delivery_system_events_dag()
