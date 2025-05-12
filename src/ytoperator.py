from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from state import TaskRunState

if TYPE_CHECKING:
    from dag import DAG


class BaseOperator:
    task_id: str
    dag_id: str

    spec: dict
    spec_path: str

    def __init__(self, task_id: str, dag_id: str, spec: dict):
        self.task_id = task_id
        self.dag_id = dag_id
        self.spec = spec
        self.spec_path = f"//home/specs/{dag_id}_{task_id}_{int(time.time())}.json"

    def run_task(self, yt_client: yt.YtClient, result_queue, key):
        raise NotImplementedError


class MapOperator(BaseOperator):
    def __init__(self, task_id: str, dag_id: str, spec: dict):
        super().__init__(task_id, dag_id, spec)
        self.binary = spec["mapper"]["command"]
        self.input_tables  = spec["input_table_paths"]
        self.output_tables = spec["output_table_paths"]


    def run_task(self, yt_client: yt.YtClient, result_queue, key):
        yt_client.create("file", self.spec_path, force=True)
        yt_client.write_file(self.spec_path, json.dumps(self.spec))

        import multiprocessing

        def _run():
            try:
                yt_client.run_map(
                    binary=self.binary,
                    source_table=self.input_tables,
                    destination_table=self.output_tables[0],
                    spec=self.spec
                )
                result_queue.put((key, TaskRunState.SUCCESS))
            except Exception:
                result_queue.put((key, TaskRunState.FAILED))

        p = multiprocessing.Process(target=_run, daemon=True)
        p.start()

operators = {"map": MapOperator}