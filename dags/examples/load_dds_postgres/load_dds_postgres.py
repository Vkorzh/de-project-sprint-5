import logging
import pendulum
from airflow.decorators import dag, task
from examples.load_dds_postgres.users_loader import UsersLoader
from examples.load_dds_postgres.restautants_loader import RestaurantsLoader
from examples.load_dds_postgres.timestamps_loader import TimestampsLoader
from examples.load_dds_postgres.products_loader import ProductsLoader
from examples.load_dds_postgres.orders_loader import OrdersLoader
from examples.load_dds_postgres.fct_product_sales_loader import ProductSalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_bonus_system_users_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dds_users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    load_users = load_users()

    @task(task_id="dds_restautants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    load_restaurants = load_restaurants()

    @task(task_id="dds_timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    load_timestamps = load_timestamps()

    @task(task_id="dds_products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    load_products = load_products()

    @task(task_id="dds_orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrdersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    load_orders = load_orders()


    @task(task_id="dds_product_sales_load")
    def load_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductSalesLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_product_sales()  # Вызываем функцию, которая перельет данные.

    load_product_sales = load_product_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    load_users >> load_restaurants >> load_products  >> load_timestamps >> load_orders >>  load_product_sales# type: ignore


dds_bonus_system_users_dag = sprint5_example_dds_bonus_system_users_dag()
