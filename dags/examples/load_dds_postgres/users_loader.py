from logging import Logger
from typing import List
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import json

class UserObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime

class UserDestObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str

class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, users_threshold: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": users_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class UsersDestRepository:
    def insert_users(self, conn: Connection, user: UserDestObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(id, user_id, user_name, user_login)
                    VALUES (%(id)s, %(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "id": user.id,
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )

class UsersLoader:
    WF_KEY = "example_users_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    schema = 'dds'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin)
        self.stg = UsersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_users(self):
        self.log.info("Starting users loading process...")
        with self.pg_dest.connection() as conn:
            try:
                # Прочитываем состояние загрузки
                self.log.info("Fetching workflow settings...")
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY,self.schema)
                if not wf_setting:
                    self.log.info("No workflow settings found. Creating new settings...")
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

                # Вычитываем очередную пачку объектов.
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                self.log.info(f"Last loaded ID: {last_loaded}")
                self.log.info(f"Fetching users with ID > {last_loaded}...")
                load_queue = self.origin.list_users(last_loaded)
                self.log.info(f"Found {len(load_queue)} users to load.")

                if not load_queue:
                    self.log.info("No new users found. Exiting...")
                    return

                # Сохраняем объекты в базу dwh.
                self.log.info("Inserting users into the destination table...")
                for user in load_queue:
                    self.log.info(f"Processing user ID: {user.id}")
                    user_data = json.loads(user.object_value)
                    user_dest = UserDestObj(
                        id=user.id,
                        user_id=user.object_id,
                        user_name=user_data.get("name"),
                        user_login=user_data.get("login")
                    )
                    self.stg.insert_users(conn, user_dest)

                # Сохраняем прогресс.
                self.log.info("Saving progress...")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json,self.schema)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                raise