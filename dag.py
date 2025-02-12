from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from graphlib import TopologicalSorter
from typing import Any, Iterable, Sequence
from dataclasses import field, dataclass
from enum import Enum

import attrs
import os


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


def _build_dag_from_spec(spec: dict, work_dir: str = None) -> DAG:
    dag = DAG(dag_id=f'dag_{datetime.utcnow().isoformat()}', spec=spec, work_dir=work_dir)
    tasks: dict[str, Task] = {}

    steps = spec.get('steps', {})
    table_producers: dict[str, list[Task]] = {}
    for step_id, step_spec in steps.items():
        params = dict(step_spec)
        inlets = params.pop('input_table_paths', [])
        outlets = params.pop('output_table_paths', [])

        if work_dir is not None:
            inlets = [os.path.join(work_dir, path) for path in inlets]
            outlets = [os.path.join(work_dir, path) for path in outlets]

        task = Task(
            task_id=step_id,
            task_name=step_id,
            dag=dag,
            default_args=params,
            inlets=inlets,
            outlets=outlets,
        )
        dag.add_task(task)
        tasks[step_id] = task

        for table in task.outlets:
            table_producers.setdefault(table, []).append(task)

    for task_id, task in tasks.items():
        for table in task.inlets:
            if producers := table_producers.get(table):
                task.set_upstream(producers)

    return dag


@attrs.define(repr=False, kw_only=True)
class DAG:
    dag_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    default_args: dict[str, Any] = attrs.field(factory=dict, validator=attrs.validators.instance_of(dict))
    start_date: datetime | None = attrs.field(default=None)
    end_date: datetime | None = attrs.field(default=None)

    spec: dict = attrs.field(validator=attrs.validators.instance_of(dict))
    work_dir: str = attrs.field(validator=attrs.validators.instance_of(str))

    task_dict: dict[str, DAGNode] = attrs.field(factory=dict, init=False)

    Graph: dict[str, set[str]] = attrs.field(factory=dict, init=False)
    ts: TopologicalSorter = attrs.field(init=False)

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def tasks(self) -> list[DAGNode]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict)

    def add_dependency(self, predecessor_task_id: str, successor_task_id: str):
        self.get_task(predecessor_task_id).set_downstream(self.get_task(successor_task_id))

    def get_task(self, task_id: str) -> DAGNode:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise RuntimeError()

    def add_task(self, task: DAGNode) -> None:
        if not task.start_date:
            task.start_date = self.start_date
        elif self.start_date:
            task.start_date = max(self.start_date, task.start_date)

        if not task.end_date:
            task.end_date = self.end_date
        elif self.end_date:
            task.end_date = min(self.end_date, task.end_date)

        if task.node_id in self.task_dict and self.task_dict[task.node_id] is not task:
            raise RuntimeError()
        else:
            self.task_dict[task.node_id] = task
            task.dag = self

    def add_tasks(self, tasks: Iterable[DAGNode]) -> None:
        for task in tasks:
            self.add_task(task)

    def build(self) -> None:
        graph: dict[str, set[str]] = {}
        for task_id, task in self.task_dict.items():
            graph[task_id] = task.preceding_task_ids
        self.ts = TopologicalSorter(graph)
        self.ts.prepare()
        self.Graph = graph

    def complete_task(self, task_id: str) -> None:
        if task_id in self.task_dict:
            self.ts.done(task_id)


class DAGNode:
    dag: DAG | None
    start_date: datetime | None
    end_date: datetime | None
    preceding_task_ids: set[str]
    succeeding_task_ids: set[str]

    def __init__(self):
        self.succeeding_task_ids = set()
        self.preceding_task_ids = set()

    @property
    @abstractmethod
    def node_id(self) -> str:
        raise NotImplementedError()

    @property
    def dag_id(self) -> str:
        if self.dag:
            return self.dag.dag_id
        return ""

    def _set_relatives(self, tasks: DAGNode | Sequence[DAGNode], upstream: bool = False) -> None:
        if not isinstance(tasks, Sequence):
            tasks = [tasks]

        dags: set[DAG] = {task.dag_id for task in [self, *tasks] if task.dag}

        if len(dags) > 1:
            raise RuntimeError()
        elif len(dags) == 1:
            dag = self.dag
        else:
            raise RuntimeError()

        if self.dag is None:
            self.dag = dag

        for task in tasks:
            if dag and task.dag is None:
                dag.add_task(task)
            if upstream:
                task.succeeding_task_ids.add(self.node_id)
                self.preceding_task_ids.add(task.node_id)
            else:
                task.preceding_task_ids.add(self.node_id)
                self.succeeding_task_ids.add(task.node_id)

    def set_downstream(self, tasks: DAGNode | Sequence[DAGNode]) -> None:
        self._set_relatives(tasks, upstream=False)

    def set_upstream(self, tasks: DAGNode | Sequence[DAGNode]) -> None:
        self._set_relatives(tasks, upstream=True)

    @property
    def successor_list(self) -> Iterable[DAGNode]:
        return [self.dag.get_task(tid) for tid in self.succeeding_task_ids]

    @property
    def predecessor_list(self) -> Iterable[DAGNode]:
        return [self.dag.get_task(tid) for tid in self.preceding_task_ids]


@dataclass(kw_only=True)
class Task(DAGNode):
    task_id: str
    task_name: str

    inlets: list[Any] = field(default_factory=list)
    outlets: list[Any] = field(default_factory=list)

    default_args: dict[str, Any] = field(default_factory=dict)

    def __init__(
            self,
            *,
            task_id: str,
            task_name: str,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
            dag: DAG | None = None,
            default_args: dict | None = None,
            inlets: Any | None = None,
            outlets: Any | None = None
    ):
        self.task_id = task_id
        self.task_name = task_name
        super().__init__()

        self.start_date = start_date
        self.end_date = end_date
        self.default_args = default_args
        self.dag = dag

        if inlets:
            self.inlets = (inlets if isinstance(inlets, list) else [inlets])
        else:
            self.inlets = []
        if outlets:
            self.outlets = (outlets if isinstance(outlets, list) else [outlets])
        else:
            self.outlets = []

    @property
    def node_id(self):
        return self.task_id

    def add_inlets(self, inlets: Iterable[Any]):
        self.inlets.extend(inlets)

    def add_outlets(self, outlets: Iterable[Any]):
        self.outlets.extend(outlets)

    def __repr__(self):
        return f"<Task: {self.task_id}>"