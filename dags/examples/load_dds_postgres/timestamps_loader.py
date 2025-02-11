from logging import Logger
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from psycopg.rows import dict_row
from pydantic import BaseModel
from datetime import datetime,date,time
import json

class TimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time

class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, orders_threshold: int) -> List[dict]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, object_id, object_value, update_ts
                FROM stg.ordersystem_orders
                WHERE id > %(threshold)s
                ORDER BY id ASC
                """,
                {"threshold": orders_threshold}
            )
            objs = cur.fetchall()
        return objs

class TimestampsDestRepository:
    def insert_timestamp(self, conn: Connection, ts: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                VALUES (
                    %(ts)s,
                    EXTRACT(YEAR FROM %(ts)s),
                    EXTRACT(MONTH FROM %(ts)s),
                    EXTRACT(DAY FROM %(ts)s),
                    %(ts)s::date,
                    %(ts)s::time
                )
                ON CONFLICT (ts) DO NOTHING;
                """,
                {
                    "ts": ts
                }
            )


class TimestampsLoader:
    WF_KEY = "example_timestamps_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampsOriginRepository(pg_origin)
        self.stg = TimestampsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        self.log.info("Starting timestamps loading process...")
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
                self.log.info(f"Fetching timestamps with ID > {last_loaded}...")
                load_queue = self.origin.list_timestamps(last_loaded)
                self.log.info(f"Found {len(load_queue)} timestamps to load.")

                if not load_queue:
                    self.log.info("No new timestamps found. Exiting...")
                    return

                self.log.info("Inserting timestamps into the destination table...")
                for order in load_queue:
                    self.log.info(f"Processing order ID: {order['id']}")
                    order_data_str = order['object_value'].strip()

                    try:
                        order_data = json.loads(order_data_str)
                    except json.JSONDecodeError as e:
                        self.log.error(f"Invalid JSON in object_value for order {order['id']}: {e}")
                        continue

                    # Извлечение значения из ключа 'date'
                    date_value_str = order_data.get("date")
                    if not date_value_str:
                        self.log.warning(f"No 'date' key for order {order['id']}")
                        continue

                    try:
                        # Преобразование строки даты в datetime объект
                        date_value = datetime.strptime(date_value_str, "%Y-%m-%d %H:%M:%S")

                        # Вставка временной метки через функцию insert_timestamp
                        self.stg.insert_timestamp(conn, date_value)
                        self.log.info(f"Inserted timestamp for order {order['id']} with date {date_value_str}")
                    except ValueError:
                        self.log.warning(f"Invalid date format for order {order['id']}: {date_value_str}")

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t['id'] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.schema)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise