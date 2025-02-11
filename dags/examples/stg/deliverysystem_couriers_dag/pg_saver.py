from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, id: str, val: Any, table: str = 'restaurants'):
        str_val = json2str(val)
        with conn.cursor() as cur:
            query = f"""
                INSERT INTO stg.deliverysystem_{table}(object_id, object_value, update_ts)
                VALUES (%(id)s, %(val)s, now())
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
            """
            cur.execute(
                query,
                {
                    "id": id,
                    "val": str_val
                }
            )