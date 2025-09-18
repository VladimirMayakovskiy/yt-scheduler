from __future__ import annotations

from typing import Callable

from yt_wrapper import with_yt_client, with_context
from dagnode import DagNode, Dependency
from errors import DagInitializationError
import hashlib

import yt.wrapper as yt

class Task(DagNode):
    task_id: str
    dag_id: str
    spec_builder: yt.spec_builders.SpecBuilder
    _context: Callable

    dependency_rules = [
        Dependency(
            producer_accessor="get_output_table_paths",
            consumer_accessor="get_input_table_paths",
        )
    ]

    def __init__(self, task_id: str, dag_id: str, spec_builder: yt.spec_builders.SpecBuilder, context: Callable):
        super().__init__(task_id, dag_id)

        self.spec_builder = spec_builder
        self._context = context

    @classmethod
    def from_spec_conf(cls, spec: dict, task_id: str) -> Task | None:
        spec_builder_cls = None
        operation_type = spec.get("operation_type", None)
        if operation_type is not None:
            spec_builder_cls = dict({
                builder_cls().operation_type: builder_cls
                for builder_cls in yt.spec_builders.SpecBuilder.__subclasses__()
            }).get(operation_type)

        if spec_builder_cls is None:
            cls.logger.exception("Unknown operation type: %s", operation_type)
            return None

        processed_keys = [
            "operation_type",
        ]

        for key in processed_keys:
            if key in spec:
                spec.pop(key)

        spec_builder = spec_builder_cls()
        spec_builder.spec(spec)

        task = cls.__new__(cls)
        task.task_id = task_id
        task.dag_id = None

        task.preceding_task_ids = set()
        task.succeeding_task_ids = set()

        task.spec_builder = spec_builder

        return task

    @property
    def operation_type(self) -> str:
        return self.spec_builder.operation_type

    def get_input_table_paths(self):
        return self.spec_builder.get_input_table_paths()

    def get_output_table_paths(self):
        return self.spec_builder.get_output_table_paths()

    @with_context
    def prepare_user_spec(self, yt_client: yt.YtClient):
        self.spec_builder._prepare_tables(spec=self.spec_builder._user_spec, client=yt_client)

    @with_context
    def run_operation(self, mutation_id, yt_client: yt.YtClient) -> str:
        return yt_client.run_operation(self.spec_builder, sync=False, run_operation_mutation_id=mutation_id, enable_optimizations=True)

    @staticmethod
    @with_yt_client
    def abort_operation(operation, reason=None, yt_client: yt.YtClient=None):
        return yt_client.abort_operation(operation, reason)

    @classmethod
    @with_yt_client
    def from_serialized_repr(cls, ref: "TaskRef.row_type") -> Task:
        from serialized import SerializedTask
        try:
            task = SerializedTask.deserialize_operator(SerializedTask.from_json(ref.serialized_repr))
            return task
        except Exception as e:
            raise DagInitializationError(f"Failed to deserialize Task {ref.task_id} from dag {ref.dag_id}: {e}") from e

    def to_serialized_repr(self) -> tuple[str, str]:
        from serialized import SerializedTask
        serialized = SerializedTask.to_json(SerializedTask.serialize_operator(self))
        payload_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        return serialized, payload_hash