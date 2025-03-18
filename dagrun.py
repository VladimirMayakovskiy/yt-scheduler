from __future__ import annotations
from datetime import datetime
from enum import Enum
from graphlib import TopologicalSorter

import attrs
import yt.wrapper as yt

from dag import DAG, Task, DAGNode
from taskrun import TaskRun

class RunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class DagRun:
    __tablename__ = "dag_runs"

    id: str
    dag_id: str
    run_id: str
    queued_at: datetime
    start_date: datetime
    end_date: datetime

    spec_path: str
    workdir: str

    _state: RunState

    updated_at: datetime

##########
    dag: DAG
    yt_client: yt.YtClient

    Graph: dict[str, set[str]] = attrs.field(factory=dict, init=False)
    ts: TopologicalSorter = attrs.field(init=False)

    def __init__(
            self,
            *,
            spec_path: str,
            work_dir: str,
            dag_id: str,
            run_id: str,
            queued_at: datetime,
            start_date: datetime,
            state: RunState,
            yt_client: yt.YtClient
    ):
        self.spec_path = spec_path
        self.workdir = work_dir
        self.dag_id = dag_id
        self.run_id = run_id
        self.start_date = start_date
        if state is not None:
            self._state = state
        if not queued_at:
            self.queued_at = datetime.utcnow() if state == RunState.QUEUED else None
        else:
            self.queued_at = queued_at

        self.yt_client = yt_client

        self.taskruns: dict[str, TaskRun] = {}

    def _prepare(self):
        graph: dict[str, set[str]] = {}
        for task_id, task in self.dag.task_dict.items():
            graph[task_id] = task.preceding_task_ids
        self.ts = TopologicalSorter(graph)
        self.ts.prepare()
        self.Graph = graph

    def get_state(self) -> RunState:
        return self._state

    def set_state(self, state: RunState) -> None:
        if self._state == state:
            if state == RunState.QUEUED:
                self.queued_at = datetime.utcnow()
        else:
            if state == RunState.QUEUED:
                self.queued_at = datetime.utcnow()
                self.start_date = None
                self.end_date = None
            if state == RunState.RUNNING:
                if self._state in [RunState.SUCCESS, RunState.FAILED]:
                    self.start_date = datetime.utcnow()
                else:
                    self.start_date = self.start_date or datetime.utcnow()
                self.end_date = None
            if self._state in [RunState.RUNNING, RunState.QUEUED, None] and state in [RunState.SUCCESS, RunState.FAILED]:
                self.end_date = datetime.utcnow()
            self._state = state


    # def refresh_from_db(self) -> None:
    #     pass # TODO

    # active runs of dag ?

    @classmethod
    def get_running_dag_runs_to_examine(cls, yt_client):
        # выбрать DagRun, находящиеся в RunState.Running, чтобы можно было шедулить задачи
        pass # TODO

    @classmethod
    def get_queued_dag_runs_to_set_running(cls, yt_client):
        # выбрать DagRun, находящиеся в RunState.PENDING
        pass # TODO

    def get_taskruns(self, state: RunState | None = None) -> list[TaskRun]:
        return list(self.taskruns.values())

    def get_taskrun(self, task_id: str) -> TaskRun | None:
        return self.taskruns.get(task_id, None)

    def get_dag(self) -> DAG:
        return self.dag

    @staticmethod
    def get_latest_dagrun(dag_id: str, yt_client: yt.YtClient, state: RunState | None = None) -> DagRun | None:
        query = f"""
            SELECT run_id, status, queued_at, start_date, end_date FROM GRAPH_TABLE
            WHERE dag_id = '{dag_id}'
            ORDER BY start_date DESC LIMIT 1
            """
        result = yt_client.select_rows(query)

        if result:
            row = result[0]
            return DagRun(
                dag_id=dag_id,
                run_id=row['run_id'],
                state=RunState(row['status']),
                queued_at=row['queued_at'],
                start_date=row['start_date'],
                end_date=row['end_date'],
                yt_client=yt_client
            )
        return None

    def update_state(self, yt_client: yt.YtClient) -> list[TaskRun]:
        # определить общий state запуска, на основе taskrun

        taskruns, schedulable, finished, unfinished = self.taskrun_scheduling_check()

        if not unfinished and any(x.state in RunState.FAILED for x in taskruns):
            self.set_state(RunState.FAILED)
            # dagrun state changed, FAILED <- task_failed

            # handle some callbacks
        elif not unfinished and all(x.state in RunState.SUCCESS for x in taskruns):
            self.set_state(RunState.SUCCESS)
            # dagrun state changed
        else:
            self.set_state(RunState.RUNNING)

        return schedulable

    def taskrun_scheduling_check(self) -> tuple[list[TaskRun], list[TaskRun], list[TaskRun], list[TaskRun]]:
        taskruns = self.get_taskruns()

        unfinished = [t for t in taskruns if t.state in [RunState.RUNNING, RunState.QUEUED]]
        finished = [t for t in taskruns if t.state in [RunState.SUCCESS, RunState.FAILED]]

        if unfinished:
            schedulable = [ut for ut in unfinished if ut.state in RunState.RETRY]
            schedulable = self._get_ready_tis(schedulable, finished)

        else:
            schedulable = []

        return taskruns, schedulable, unfinished, finished

    def _get_ready_tis(self, schedulable_tis:  list[TaskRun], finished_tis: list[TaskRun]) -> list[TaskRun]:
        ready_tis: list[TaskRun] = []

        if not schedulable_tis:
            return ready_tis

        for tis in finished_tis:
            self.ts.done(tis.task_id)

        ready_tis_ids = self.ts.get_ready()

        for tis in schedulable_tis:
            if tis.task_id in ready_tis_ids:
                ready_tis.append(tis)

        return ready_tis

    # mb retry tasks

    def verify(self, yt_client: yt.YtClient) -> None:
        # Проверяем DagRun, создаем TaskRun
        dag = self.dag
        task_ids = set(x.task_id for x in self.get_taskruns())

        def create_taskrun(task: Task) -> TaskRun:
            ti = TaskRun(task=task, run_id=self.run_id)
            yield ti

        tasks_to_create = [task for task in dag.task_dict.values() if task.task_id not in task_ids]
        # if existed
        taskruns_to_create = [create_taskrun(task) for task in tasks_to_create]
        self._create_task_instances(self.dag_id, taskruns_to_create)

    def _create_task_instances(self, dag_id: str, tasks: list[TaskRun]) -> None:
        for task in tasks:
            TaskRun.save_to_db(task, self.yt_client)

    def schedule_taskruns(self, schedulable: list[TaskRun]) -> int:
        for ti in schedulable:
            ti.set_state(RunState.QUEUED)
