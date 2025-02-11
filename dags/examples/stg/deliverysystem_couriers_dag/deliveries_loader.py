from logging import Logger
from datetime import datetime
from lib import PgConnect
from lib.dict_util import json2str
from examples.stg import EtlSetting, StgEtlSettingsRepository
from examples.stg.deliverysystem_couriers_dag.pg_saver import PgSaver
from examples.stg.deliverysystem_couriers_dag.deliveries_reader import DeliveryReader



class DeliveryLoader:
    _LOG_THRESHOLD = 2

    WF_KEY = "deliveriesystem_deliveries_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            cursor = conn.cursor()

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key = self.WF_KEY,
                    workflow_settings =
                    {
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str).strftime('%Y-%m-%d %H:%M:%S')
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            # Получаем данные из API
            load_queue = self.collection_loader.get_all_deliveries(From=last_loaded_ts)
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")

            if not load_queue:
                self.log.info("No data to load. Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["order_id"]), d,'deliveries')

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["order_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
