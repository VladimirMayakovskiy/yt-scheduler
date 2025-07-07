from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from state import TaskRunState

if TYPE_CHECKING:
    from dag import DAG

from dag_node import DAGNode


class BaseOperator(DAGNode):
    dag_id: str

    spec: dict
    spec_path: str

    def __init__(self, task_id: str, dag_id: str, spec: dict):
        print("INIT")
        super().__init__(task_id)
        self.task_id = task_id
        self.dag_id = dag_id
        self.spec = spec
        self.spec_path = f"//home/specs/{dag_id}_{task_id}_{int(time.time())}.json"

    def run_operation(self, yt_client: yt.YtClient) -> str:
        raise NotImplementedError


class MapOperator(BaseOperator):
    def __init__(self, task_id: str, dag_id: str, spec: dict):
        print("MAPOPERATOR")
        super().__init__(task_id, dag_id, spec)
        # self.binary = spec["mapper"]["command"]
        # self.input_tables  = spec["input_table_paths"]
        # self.output_tables = spec["output_table_paths"]


    def run_operation(self, yt_client: yt.YtClient) -> str:
        yt_client.create("file", self.spec_path, force=True)
        yt_client.write_file(self.spec_path, json.dumps(self.spec))

        mapper = self.spec["mapper"]
        input_tables = self.spec.get("input_table_paths", [])
        output_tables = self.spec.get("output_table_paths", [])

        operation_id = yt_client.run_map(
            mapper=mapper,
            source_table=input_tables,
            destination_table=output_tables[0],
            spec=self.spec
        )
        return operation_id

operators = {"map": MapOperator}