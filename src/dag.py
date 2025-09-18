from __future__ import annotations

import hashlib
from typing import Any

from task import Task
from dagref import DagRef
from errors import DagInitializationError
from logging_mixin import LoggingMixin
from yt_wrapper import context_wrapper

class DAG(LoggingMixin):
    dag_id: str

    default_args: dict[str, Any]

    work_dir: str

    task_dict: dict[str, Task]

    @classmethod
    def from_spec_conf(cls, spec: dict, work_dir: str) -> DAG:
        context = context_wrapper(prefix=work_dir)

        task_dict: dict[str, Task] = {}
        for task_id, params in spec.get("steps", {}).items():
            cfg = dict(params)

            task = Task.from_spec_conf(cfg, task_id)

            if task is None:
                cls.logger.exception("Cannot build task from spec: %s", cfg)
                raise DagInitializationError("Task not created %s for %s", task_id, cfg)

            task._context = context
            task.prepare_user_spec()
            task_dict[task_id] = task

        dag = cls.__new__(cls)
        dag.dag_id = None
        dag.work_dir = work_dir
        dag.task_dict = task_dict
        dag.resolve_tasks_dependencies()
        return dag

    def resolve_tasks_dependencies(self):
        from dagnode import Dependency
        dependency_rules: list[Dependency] = []
        for task in self.tasks:
            dependency_rules.extend(getattr(task.__class__, "dependency_rules", []))

        for dep_rule in dependency_rules:
            producers_map: dict[str, list[Task]] = {}

            for task in self.tasks:
                for dep_key in dep_rule.downstream_deps(task):
                    producers_map.setdefault(dep_key, []).append(task)

            for task in self.tasks:
                for dep_key in dep_rule.upstream_deps(task):
                    if producers := producers_map.get(dep_key):
                        task.set_upstream(producers)

    @classmethod
    def from_serialized_repr(cls, ref: "DagRef.row_type") -> DAG:
        from serialized import SerializedDag
        try:
            dag = SerializedDag.deserialize_dag(
                encoded_dag=SerializedDag.from_json(ref.serialized_repr),
                dag_id=ref.dag_id)
        except Exception as e:
            raise DagInitializationError(f"Failed to deserialize DAG {ref.dag_id}: {e}") from e

        try:
            context = context_wrapper(prefix=dag.work_dir)
            for task in dag.task_dict.values():
                task._context = context
                task.prepare_user_spec()

            dag.resolve_tasks_dependencies()
        except Exception as e:
            raise DagInitializationError(f"Failed to initialize DAG {dag.dag_id}: {e}") from e

        return dag

    def to_serialized_repr(self) -> tuple[str, str]:
        from serialized import SerializedDag
        serialized = SerializedDag.to_json(SerializedDag.serialize_dag(self))
        payload_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        return serialized, payload_hash

    @property
    def tasks(self) -> list[Task]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict.keys())

    @property
    def roots(self) -> list[Task]:
        return [self.task_dict.get(t.task_id, None) for t in self.tasks if not t.upstream_task_ids]

    @property
    def leaves(self) -> list[Task]:
        return [self.task_dict.get(t.task_id, None) for t in self.tasks if not t.downstream_task_ids]