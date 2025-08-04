from __future__ import annotations

import itertools
import multiprocessing
import time
from datetime import datetime
from queue import Empty, Queue
from typing import Any

import yt.wrapper as yt

from state import TaskRunState
from task_run import TaskRun, TaskRunRow
from ytoperator import Operator
from logging_mixin import LoggingMixin


class Executor(LoggingMixin):
    def __init__(self, job_id: str, parallelism: int = 4):
        self.job_id = job_id
        self.parallelism = parallelism

        self._running_ops: dict = {} #TaskRun.id -> operation_id
        self._pending: list[TaskRunRow] = []

    def start(self) -> None:
        self.log.info("Start Executor")
        #TODO

    def end(self) -> None:
        self.log.info("Shutting down Executor")
        #TODO

    @property
    def slots_occupied(self):
        return len(self._running_ops) + len(self._pending)

    def queue_task_run(
            self,
            task_run: TaskRunRow,
            operator: Operator,
            yt_client: yt.YtClient,
    ):
        self.log.info(f"QUEUE taskrun: {task_run}")
        if self.slots_occupied >= self.parallelism:
            return

        operation_id = operator.run_operation(yt_client).id
        TaskRun.update_rows(yt_client, TaskRun.TaskRunUpdateRow(tr=task_run, state=TaskRunState.RUNNING, operation_id=operation_id))

        self._running_ops[task_run.id] = operation_id
        self._pending.append(task_run)

    def heartbeat(self, yt_client: yt.YtClient) -> None:
        self.log.info("Heartbeat")
        update_rows = []
        for task_run in self._pending.copy():
            try:
                operation_id = task_run.operation_id
                status = yt_client.get_operation_state(operation_id)
            except Exception as e:
                self.log.exception(f"Can not get operation state taskrun={task_run.task_id},{task_run.run_id}:")
                raise

            self.log.info(f"Taskrun={task_run.task_id},{task_run.run_id} operation_id={operation_id}, STATUS={status}, is_running={status.is_running()}")
            if not status.is_finished():
                print(dir(status))
                continue

            new_state = TaskRunState.FAILED if status.is_unsuccessfully_finished() else TaskRunState.SUCCESS
            update_rows.append(TaskRun.TaskRunUpdateRow(tr=task_run, state=new_state))
            self._pending.remove(task_run)
            del self._running_ops[task_run.id]

        TaskRun.update_rows(yt_client, update_rows) #, state=TaskRunState.SUCCESS)
