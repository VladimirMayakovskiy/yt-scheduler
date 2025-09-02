from __future__ import annotations

import time
from typing import Callable

from job import JobBase, classproperty, JobContext
from taskrun import TaskRunnable, TaskRun

from yt_wrapper import with_yt_client
import yt.wrapper as yt

class TaskRunner(JobBase):
    def __init__(self, context: JobContext, job_id: str = None, task_runner_idle_sleep_time: float = 1.0):
        super().__init__(context)

        self.job_id = job_id
        self._task_runner_idle_sleep_time = task_runner_idle_sleep_time
        self._pending: list[TaskRun] = []

    @classproperty
    def name(self) -> str:
        return "task_runner"

    @property
    def _entry(self) -> Callable[[], int | None]:
        return self._execute

    def queue_task_run(self, runnable: TaskRunnable):
        op = runnable()
        operation_id = op.id if hasattr(op, "id") else op

        self._pending.append(runnable.row) # todo exception
        self.context.pool_executor.submit(
            TaskRun.update_rows, trs=[runnable.row.update_row(state=TaskRun.state_type.RUNNING, operation_id=operation_id)],
            context=self.client_context,
            block=False
        )

    def _execute(self) -> int | None:
        self.log.info("Starting the task runner")
        try:
            self._initialize_pending_tasks()
            self._runner_loop()
        except Exception as e:
            print(e)
            raise
        finally:
            self.log.info("Exited execute loop")
            return None

    @with_yt_client
    def _initialize_pending_tasks(self):
        self.log.info("Initializing pending tasks")

        trs = self.context.pool_executor.submit(
            TaskRun.get_running_task_runs_to_poll,
            context=self.client_context,
            block=False
        )

        @trs.on_complete
        def _():
            self._pending.extend(trs.result)


    def _runner_loop(self):
        while not self._runner.stop_event.is_set():
            self._polling()
            time.sleep(self._task_runner_idle_sleep_time)

    @with_yt_client
    def _polling(self, yt_client: yt.YtClient):
        update_rows = []
        for tr in self._pending.copy():
            op_id = tr.operation_id
            try:
                status = yt_client.get_operation_state(op_id)

                if not status.is_finished():
                    return

                state = TaskRun.state_type.FAILED if status.is_unsuccessfully_finished() else TaskRun.state_type.SUCCESS # todo

                update_rows.append(tr.update_row(state=state))
                self._pending.remove(tr)
            except Exception as e:
                self.log.exception("Failed to poll operation state for op %s: %s", op_id, e)
                raise

        self.context.pool_executor.submit(TaskRun.update_rows, update_rows, context=self.client_context, block=False)

    # todo add heartbeat and to executor same