from __future__ import annotations

from typing import Callable, Any

from yt_wrapper import with_yt_client
from dagnode import DagNode

import yt.wrapper as yt

class Operator(DagNode):
    def __init__(self, task_id: str, dag_id: str, spec_builder: yt.spec_builders.SpecBuilder, context: Callable):
        super().__init__(task_id, dag_id)

        self.spec_builder = spec_builder
        self._contextualize = context

    @property
    def operation_type(self) -> str:
        return self.spec_builder.operation_type
    @property
    def spec(self) -> dict[str, Any]:
        return self.spec_builder._user_spec

    def get_input_table_paths(self):
        return self.spec_builder.get_input_table_paths()

    def get_output_table_paths(self):
        return self.spec_builder.get_output_table_paths()

    @with_yt_client
    def _prepare_user_spec(self, yt_client: yt.YtClient):
        self.spec_builder._prepare_tables(spec=self.spec_builder._user_spec, client=yt_client)
    def prepare_user_spec(self):
        return self._contextualize(self._prepare_user_spec)

    @with_yt_client
    def _run_operation(self, yt_client: yt.YtClient) -> str:
        return yt_client.run_operation(self.spec_builder, sync=False, enable_optimizations=True)
    def run_operation(self) -> str:
        return self._contextualize(self._run_operation)