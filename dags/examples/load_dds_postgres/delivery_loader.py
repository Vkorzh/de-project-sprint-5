from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


# Определяем DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='load_dds_dm_deliveries',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    
    insert_data_dm_couriers = PostgresOperator(
        task_id='insert_into_dm_couriers_task',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
                INSERT INTO dds.dm_couriers(courier_key, first_name, second_name, full_name)
                SELECT object_value->>'_id' as courier_key,
                    (string_to_array(object_value->>'name', ' '))[1] AS first_name,
                    (string_to_array(object_value->>'name', ' '))[2] AS second_name,
                    object_value->>'name' full_name
                FROM stg.deliverysystem_couriers c
                WHERE c.id >
                (
                    SELECT (workflow_settings ->> 'last_loaded_id')::int
                    FROM dds.srv_wf_settings
                    WHERE workflow_key='couriers_to_dds_workflow'
                );
                {% raw %}
                UPDATE dds.srv_wf_settings 
                SET workflow_settings = jsonb_set(
                    workflow_settings::jsonb,         -- Преобразование к типу jsonb
                    '{last_loaded_id}',               -- Путь к JSON ключу
                    to_jsonb(COALESCE((SELECT MAX(id) FROM stg.deliverysystem_couriers), 0)::text)
                )
                WHERE workflow_key='couriers_to_dds_workflow';
                {% endraw %}
            """
    )

    insert_data_dm_adress = PostgresOperator(
        task_id='insert_into_dm_adress_task',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
            INSERT INTO dds.dm_adress(city, street, house_number, apartment_number, full_adress)
            SELECT 'Москва',
                    TRIM(split_part(object_value->>'address', ',', 1)) AS street,
                    TRIM(split_part(object_value->>'address', ',', 2)) AS house,
                    TRIM(split_part(object_value->>'address', 'кв.', 2)) AS apartment,
                    object_value->>'address' full_adress
            FROM stg.deliverysystem_deliveries oo  
            WHERE  oo.id >
            (
                SELECT (workflow_settings ->> 'last_loaded_id')::int
                FROM dds.srv_wf_settings
                WHERE workflow_key='deliveries_to_dds_workflow'
            );
        """
    )

    insert_data_dm_timestamps = PostgresOperator(
        task_id='insert_into_dm_timestamps_task',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
            INSERT INTO dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
            SELECT     (object_value->>'delivery_ts')::timestamp as ts,
                    EXTRACT(YEAR FROM  (object_value->>'delivery_ts')::timestamp) as "year",
                    EXTRACT(MONTH FROM  (object_value->>'delivery_ts')::timestamp) as "month",
                    EXTRACT(DAY FROM  (object_value->>'delivery_ts')::timestamp) as "day",
                    ((object_value->>'delivery_ts')::timestamp)::time as "time",
                    ((object_value->>'delivery_ts')::timestamp)::date as "date"
            FROM  stg.deliverysystem_deliveries oo 
            WHERE  oo.id >
            (
                SELECT (workflow_settings ->> 'last_loaded_id')::int
                FROM dds.srv_wf_settings
                WHERE workflow_key='deliveries_to_dds_workflow'
            )
            ON CONFLICT (ts) DO NOTHING;
            """
    )

    insert_data_dm_deliveries = PostgresOperator(
        task_id='insert_into_dm_deliveries_task',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
            INSERT INTO dds.dm_deliveries(delivery_key, adress_id, courier_id, timestamp_id, rate)
            SELECT	object_value->>'delivery_id' as delivery_key,
                    ad.id as adress_id,
                    dc.id as courier_id,
                    dt.id as timestamp_id,
                    (object_value->>'rate')::smallint as rate
            FROM  stg.deliverysystem_deliveries oo 
                LEFT JOIN dds.dm_adress as ad
                    on ad.full_adress = object_value->>'address'
                LEFT JOIN dds.dm_couriers dc 
                    on dc.courier_key = object_value->>'courier_id'
                LEFT JOIN dds.dm_timestamps dt 
                    on dt.ts = (object_value->>'delivery_ts')::timestamp
            WHERE  oo.id >
            (
                select(workflow_settings ->> 'last_loaded_id')::int
                FROM dds.srv_wf_settings
                WHERE workflow_key='deliveries_to_dds_workflow'
            );
            """
    )

    update_data_dm_deliveries = PostgresOperator(
        task_id='update_dm_deliveries_task',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
            UPDATE dds.fct_product_sales SET delivery_id = dmd.id,  tip_sum = (object_value->>'tip_sum')::numeric(14,2)
            FROM stg.deliverysystem_deliveries as  dd 
            LEFT JOIN dds.dm_orders as ddo
                on object_value->>'order_id' = ddo.order_key
            LEFT JOIN dds.dm_deliveries dmd
                on object_value->>'delivery_id' = dmd.delivery_key 
            WHERE ddo.id = order_id
                 AND dd.id >
                            (
                                SELECT (workflow_settings ->> 'last_loaded_id')::int
                                FROM dds.srv_wf_settings
                                WHERE workflow_key='deliveries_to_dds_workflow'
                            );
            {% raw %}
            UPDATE dds.srv_wf_settings 
            SET workflow_settings = jsonb_set(
                workflow_settings::jsonb,         -- Преобразование к типу jsonb
                '{last_loaded_id}',               -- Путь к JSON ключу
                to_jsonb(COALESCE((SELECT MAX(id) FROM stg.deliverysystem_couriers), 0)::text)
            )
            WHERE workflow_key='deliveries_to_dds_workflow';
            {% endraw %}
            """
    )

    insert_data_dm_couriers >> insert_data_dm_adress >> insert_data_dm_timestamps  >> insert_data_dm_deliveries >> update_data_dm_deliveries
