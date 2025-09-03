from __future__ import annotations

import typing
from typing import Any
import yaml

if typing.TYPE_CHECKING:
    from dagref import DagRef
from logging_mixin import LoggingMixin
from yt_operator import Operator
from yt_wrapper import with_yt_client, ContextWrapper

import yt.wrapper as yt

class DAG(LoggingMixin):
    dag_id: str

    default_args: dict[str, Any]

    work_dir: str

    task_dict: dict[str, Operator]
    upstream: dict[str, list[str]]
    downstream: dict[str, list[str]]

    @classmethod
    def from_spec_conf(cls, spec: dict, work_dir: str, context_wrapper: ContextWrapper) -> DAG:
        # spec = yaml.safe_load(yt_client.read_file(spec_path))
        context = context_wrapper.bind(work_dir=work_dir)

        dag = cls.__new__(cls)
        dag.dag_id = yt.common.generate_uuid()
        dag.work_dir = work_dir
        dag.task_dict = {}

        def make_operator(*, task_id: str, _spec: dict):
            spec_builder_cls = dict({
                builder_cls().operation_type: builder_cls
                for builder_cls in yt.spec_builders.SpecBuilder.__subclasses__()
            }).get(_spec["operation_type"])

            if spec_builder_cls is None:
                return None

            processed_keys = [
                "operation_type",
            ]

            for key in processed_keys:
                if key in spec:
                    spec.pop(key)

            spec_builder = spec_builder_cls()
            spec_builder.spec(_spec)

            return Operator(task_id=task_id, dag_id=dag.dag_id, spec_builder=spec_builder, context=context)

        for task, params in spec.get("steps", {}).items():
            cfg = dict(params)
            operator = make_operator(task_id=task, _spec=cfg)
            if operator is None:
                raise ValueError(f"Unknown operator type: {cfg.get('operation_type', '')}")
            operator.prepare_user_spec()
            dag.task_dict[task] = operator

        DAG.resolve_tasks_dependencies(dag)
        return dag

    @classmethod
    @with_yt_client
    def from_serialized_dag(cls, ref: "DagRef.row_type", context_wrapper: ContextWrapper, yt_client: yt.YtClient):
        cls.logger.info(f"Creating DAG for dag_entity: {ref}, prefix: {yt.ypath.get_config(yt_client)['prefix']}")
        from serialized import SerializedDag
        dag = SerializedDag.deserialize_dag(
            encoded_dag=SerializedDag.from_json(ref.serialized_dag),
            dag_id=ref.dag_id,
            context_wrapper=context_wrapper)
        DAG.resolve_tasks_dependencies(dag)
        return dag

    @staticmethod
    def resolve_tasks_dependencies(dag: DAG):
        outlets_producers: dict[str, list[Operator]] = {}
        for operator in dag.tasks:
            for outlet in operator.get_output_table_paths():
                outlets_producers.setdefault(outlet, []).append(operator)
        for operator in dag.tasks:
            for inlet in operator.get_input_table_paths():
                if producers := outlets_producers.get(inlet):
                    operator.set_upstream(producers)

        dag.upstream = {tid: list(op.upstream_task_ids) for tid, op in dag.task_dict.items()}
        dag.downstream = {tid: list(op.downstream_task_ids) for tid, op in dag.task_dict.items()}

    @property
    def tasks(self) -> list[Operator]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict.keys())

    @property
    def roots(self) -> list[Operator]:
        return [self.task_dict.get(tid, None) for tid, ups in self.upstream.items() if not ups]

    @property
    def leaves(self) -> list[Operator]:
        return [self.task_dict.get(tid, None) for tid, downs in self.downstream.items() if not downs]