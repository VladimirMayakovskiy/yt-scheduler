from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from graphlib import TopologicalSorter

import attrs
import yt.wrapper as yt

from dag import DAG, Task, DAGNode
from dag_run import DagRun

import uuid


class RunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


def _run_task(
    taskrun: TaskRun,
    yt_client
) -> None:
    taskrun.refresh_from_db() # проверяем актуальное состояние

    # run

    TaskRun.save_to_db(ti=taskrun, yt_client=yt_client)

    taskrun.end_date = datetime.utcnow()
    taskrun.set_duration()

    if taskrun.state == RunState.SUCCESS:
        pass
        # обработчики ?
        # _run_finished_callback(callbacks=taskrun.task.on_success_callback)


def _refresh_from_task(
    *, task_run: TaskRun, task: Task) -> None:
    task_run.task = task

# def get_previous_dagrun


def _run_finished_callback(
    *,
    callbacks: None | Callback | list[Callback]
):
    if callbacks:
        callbacks = callbacks if isinstance(callbacks, list) else list[callbacks]

        for callback in callbacks:
            try:
                callback()
            except Exception:
                pass # TODO


#TODO Context

class TaskRun:
    __tablename__ = "task_runs"

    id: str
    task_id: str
    dag_id: str
    run_id: str

    start_date: datetime
    end_date: datetime
    updated_at: datetime
    state: RunState

    try_number: int

    # task: Task
    #
    # yt_client: yt.YtClient

    def __init__(
        self,
        *,
        task: Task,
        run_id: str,
        state: RunState,
        yt_client
    ):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task

        # self.init() initialize the attributes that arent stored in db

        self.run_id = run_id
        self.try_number = 0

        self.id = str(uuid.UUID) # TODO

        if state:
            self.state = state

        self.yt_client = yt_client

    def current_state(self):
        # Get the very latest state from the database.
        pass

    # def error(self):??

    @classmethod
    def get_task_run(
        cls,
        dag_id: str,
        run_id: str,
        task_id: str,
        yt_client
    ) -> TaskRun | None:
        # смотрим в yt
        return None

    def refresh_from_db(self, yt_client) -> None:
        taskrun = self.get_task_run(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            yt_client=yt_client
        )
        if taskrun:
            self.id = taskrun.id
            self.start_date = taskrun.start_date
            self.end_date = taskrun.end_date
            self.duration = taskrun.duration
            self.try_number = taskrun.try_number
            self.state = taskrun.state
        else:
            self.state = None

    @staticmethod
    def _set_state(taskrun: TaskRun, state, yt_client) -> bool:
        if taskrun.state == state:
            return False
        current_time = datetime.utcnow()
        taskrun.state = state
        taskrun.start_date = taskrun.start_date or current_time
        if taskrun.state in [RunState.FAILED, RunState.SUCCESS]:
            taskrun.end_date = taskrun.end_date or current_time
            #ti.duration

      # do bd
#        table_path = yt.TablePath("//path/to/task_instance_table", append=True)

        yt_client.insert_rows(TaskRun.__tablename__, [taskrun])
        return True

    def set_state(self, state: RunState) -> bool:
        return self._set_state(taskrun=self, state=state, yt_client=self.yt_client)

    def get_previous_taskrun(self):
        pass

    def _execute_task(self):
        pass

    def run(self):
        self._prepare()
        _run_task()

    def handle_failure(self):
        pass

    @staticmethod
    def save_to_db(taskrun: TaskRun, yt_client):
        taskrun.updated_at = datetime.utcnow()
        yt_client.insert_rows(TaskRun.__tablename__, [taskrun])

    def validate_inlets_outlets(self):
        pass

