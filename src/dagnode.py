from __future__ import annotations

from typing import Union, Callable
from dataclasses import dataclass

from logging_mixin import LoggingMixin

Accessor = Union[str, Callable[["DagNode"], list[str]]]

@dataclass(frozen=True)
class Dependency:
    producer_accessor: Accessor
    consumer_accessor: Accessor

    @staticmethod
    def _access(node: "DagNode", accessor) -> list[str]:
        if callable(accessor):
            return list(accessor(node))

        attr = getattr(node, accessor, None)
        if attr is None:
            node.log.error("Accessor %s returned None for node %s, %s", accessor, node.task_id, node.__class__)
            return []
        if callable(attr):
            return list(attr())
        return list(attr)

    def upstream_deps(self, node: "DagNode") -> list[str]:
        return self._access(node, self.consumer_accessor)

    def downstream_deps(self, node: "DagNode") -> list[str]:
        return self._access(node, self.producer_accessor)


class DagNode(LoggingMixin):
    task_id: str
    dag_id: str
    preceding_task_ids: set[str]
    succeeding_task_ids: set[str]

    dependency_rules: list[Dependency] = []

    def __init__(self, task_id: str, dag_id: str):
        self.task_id = task_id
        self.dag_id = dag_id
        self.preceding_task_ids = set()
        self.succeeding_task_ids = set()

    @property
    def upstream_task_ids(self) -> list[str]:
        return list(self.preceding_task_ids)

    @property
    def downstream_task_ids(self) -> list[str]:
        return list(self.succeeding_task_ids)

    def set_upstream(self, tasks: "DagNode" | list["DagNode"]) -> None:
        self._set_relatives(tasks, upstream=True)

    def set_downstream(self, tasks: "DagNode" | list["DagNode"]) -> None:
        self._set_relatives(tasks, upstream=False)

    def _set_relatives(self, tasks: "DagNode" | list["DagNode"], upstream: bool = False) -> None:
        if not isinstance(tasks, list):
            tasks = [tasks]

        for task in tasks:
            if task.dag_id != self.dag_id:
                self.log.warning("Cannot set relation to task %s in DAG %s: settable task %s is in a different DAG (%s)",
                                 task.task_id, self.dag_id, task.task_id, task.dag_id)
                continue
            if upstream:
                task.succeeding_task_ids.add(self.task_id)
                self.preceding_task_ids.add(task.task_id)
            else:
                task.preceding_task_ids.add(self.task_id)
                self.succeeding_task_ids.add(task.task_id)