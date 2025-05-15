from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from src.dag import DAG


class DAGNode:
    dag: DAG | None
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
    def task_id(self) -> str:
        return self.id