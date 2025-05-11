from __future__ import annotations

from dag import DAG


class BaseOperator:
    task_id: str

    inlets: list[str]
    outlets: list[str]

    def __init__(self, task_id: str, dag: DAG, inlets: list[str] | None = None, outlets: list[str] | None = None):
        self.dag = dag
        if inlets:
            self.inlets = inlets
        else:
            self.inlets = []

        if outlets:
            self.outlets = outlets
        else:
            self.outlets = []


operators = {}