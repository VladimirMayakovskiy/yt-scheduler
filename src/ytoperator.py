from __future__ import annotations

import os.path
from copy import deepcopy
from typing import TYPE_CHECKING
import yt.wrapper as yt

if TYPE_CHECKING:
    from dag import DAG

from dag_node import DAGNode
from logging_mixin import LoggingMixin


def _process_table_paths(spec, work_dir, yt_client):
    spec = deepcopy(spec)
    if work_dir is None:
        return spec

    processed_keys = [
        "input_table_paths",
        "output_table_paths",
        "output_table_path",
        "table_path",
    ]

    for key in processed_keys:
        if key in spec:
            prefix = yt.ypath.to_ypath(work_dir, client=yt_client)
            result = [prefix.join(path).to_yson_type() for path in yt.common.flatten(spec[key])]
            if isinstance(spec[key], list):
                spec[key] = result
            elif isinstance(spec[key], str):
                spec[key] = result[0] # yt.ypath.ypath_join(work_dir, spec[key])
    return spec

def make_operator(*, task_id: str, dag_id: str, spec: dict, yt_client, work_dir: str = None):
    spec_builder_cls = dict({
        builder_cls().operation_type: builder_cls
        for builder_cls in yt.spec_builders.SpecBuilder.__subclasses__()
    }).get(spec["operation_type"])

    if spec_builder_cls is None:
        return None

    processed_keys = [
        "operation_type",
    ]

    for key in processed_keys:
        if key in spec:
            spec.pop(key)

    spec_builder = spec_builder_cls()
    spec_builder.spec(_process_table_paths(spec, work_dir, yt_client))

    return Operator(spec_builder, task_id, dag_id, work_dir)

class Operator(DAGNode, LoggingMixin):
    def __init__(self, spec_builder, task_id: str, dag_id: str, work_dir: str = None):
        super().__init__(task_id, dag_id)

        self.spec_builder = spec_builder
        self.work_dir = work_dir

    def prepare_tables(self, yt_client: yt.YtClient):
        self.spec_builder._prepare_tables(client=yt_client)

    def get_input_table_paths(self):
        return self.spec_builder.get_input_table_paths()

    def get_output_table_paths(self):
        return self.spec_builder.get_output_table_paths()

    def run_operation(self, yt_client: yt.YtClient) -> str:
        return yt_client.run_operation(self.spec_builder, sync=False, enable_optimizations=True)
