from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from src.dag import DAG


class DAGNode:
    dag_id: str
    id: str
    preceding_task_ids: set[str]
    succeeding_task_ids: set[str]

    def __init__(self, id: str):
        self.id = id
        self.succeeding_task_ids = set()
        self.preceding_task_ids = set()

    @property
    def upstream_task_ids(self) -> list[str]:
        return list(self.preceding_task_ids)

    @property
    def downstream_task_ids(self) -> list[str]:
        return list(self.succeeding_task_ids)

    @property
    def task_id(self) -> str:
        return self.id

    def set_upstream(self, tasks: DAGNode | list[DAGNode]) -> None:
        self._set_relatives(tasks, upstream=True)

    def set_downstream(self, tasks: DAGNode | list[DAGNode]) -> None:
        self._set_relatives(tasks, upstream=False)

    def _set_relatives(self, tasks: DAGNode | list[DAGNode], upstream: bool = False) -> None:
        if not isinstance(tasks, list):
            tasks = [tasks]

        # TODO check dag

        for task in tasks:
            if upstream:
                task.succeeding_task_ids.add(self.id)
                self.preceding_task_ids.add(task.id)
            else:
                task.preceding_task_ids.add(self.id)
                self.succeeding_task_ids.add(task.id)