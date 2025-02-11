from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Функция для переноса данных
def transfer_data_ranks():
    # Подключение к исходной базе данных
    src_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Подключение к целевой базе данных
    dst_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    dst_conn = dst_hook.get_conn()
    dst_cursor = dst_conn.cursor()

    # Чтение данных из исходной таблицы
    src_cursor.execute("SELECT id, name, bonus_percent, min_payment_threshold FROM public.ranks")
    rows = src_cursor.fetchall()

    # Запись данных в целевую таблицу
    for row in rows:
        dst_cursor.execute("""
            INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                bonus_percent = EXCLUDED.bonus_percent,
                min_payment_threshold = EXCLUDED.min_payment_threshold
        """, row)

    # Фиксация изменений и закрытие соединений
    dst_conn.commit()
    src_cursor.close()
    src_conn.close()
    dst_cursor.close()
    dst_conn.close()

def transfer_data_users():
    # Подключение к исходной базе данных
    src_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Подключение к целевой базе данных
    dst_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    dst_conn = dst_hook.get_conn()
    dst_cursor = dst_conn.cursor()

    # Чтение данных из исходной таблицы
    src_cursor.execute("SELECT id, order_user_id FROM public.users;")
    rows = src_cursor.fetchall()

    # Запись данных в целевую таблицу
    for row in rows:
        dst_cursor.execute("""
            INSERT INTO stg.bonussystem_users (id, order_user_id)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET
                order_user_id = EXCLUDED.order_user_id
        """, row)

    # Фиксация изменений и закрытие соединений
    dst_conn.commit()
    src_cursor.close()
    src_conn.close()
    dst_cursor.close()
    dst_conn.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'transfer_bonus_system_ranks',
    default_args=default_args,
    description='DAG для переноса данных из одной таблицы в другую',
    schedule_interval = '0/15 * * * *',
    catchup=False,
)

# Задача для переноса данных
transfer_task_ranks = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data_ranks,
    dag=dag,
)

# Задача для переноса данных
transfer_data_users = PythonOperator(
    task_id='transfer_data_users',
    python_callable=transfer_data_users,
    dag=dag,
)

transfer_task_ranks >> transfer_data_users