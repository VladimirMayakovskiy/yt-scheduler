from __future__ import annotations

import time

from job import JobBase, classproperty, JobContext
from taskrun import TaskRun
from pool import Pool

from yt_wrapper import with_yt_client
import yt.wrapper as yt

class Executor(JobBase):
    def __init__(
        self,
        context: JobContext,
        pool: Pool = None,
        task_runner_idle_sleep_time: float = 1.0,
        task_start_interval: float = 5.0,
    ):
        super().__init__(context)

        self.pool = pool

        self._task_runner_idle_sleep_time = task_runner_idle_sleep_time
        self._pending_task_runs: dict[str, TaskRun] = {}
        self._pending_operations: dict[str, str] = {}

        self._last_task_start_attempt: dict[str, float] = {}
        self._task_start_interval = task_start_interval

    @classproperty
    def name(self) -> str:
        return "task_runner"

    def _start_task_run(self, operator):
        try:
            operation = operator.run_operation()
            if hasattr(operation, "id"):
                operation = operation.id

            self._pending_operations[operator.row.run_id] = operation
            self._pending_task_runs[operator.row.run_id] = operator.row
            TaskRun.update_row(row=operator.row, state=TaskRun.state_type.RUNNING, operation_id=operation)
        except Exception as e:
            self.log.exception("Run operation failed for task %s: set state to failed: %s", operator.row.task_id, e) # todo yt.YtError
            TaskRun.update_row(row=operator.row, state=TaskRun.state_type.FAILED)

    def _start_task_runs(self):
        promise = self.pool.submit_or_execute(TaskRun.get_executable_task_runs_to_queue, context=self.client_context)
        def _on_complete():
            trs = promise.result
            for tr in trs:
                last_attempt = self._last_task_start_attempt.get(tr.run_id, 0)
                if time.time() - last_attempt < self._task_start_interval:
                    self.log.info(f"Skipping task {tr.task_id} due to start attempt interval")
                    continue

                try:
                    operator = tr.as_operator()
                    if not operator:
                        self.pool.submit_or_execute(TaskRun.update_row, row=tr, state=TaskRun.state_type.FAILED, context=self.client_context)
                        continue

                    self.pool.submit_or_execute(self._start_task_run, operator=operator, context=self.client_context)
                    self._last_task_start_attempt[tr.run_id] = time.time()
                except Exception as e:
                    self.log.exception("Failed to enqueue task %s: %s, skipping",tr.task_id, e)
                    continue
        promise.on_complete(_on_complete)

    def _execute(self) -> int | None:
        self.log.info("Starting the executor")
        try:
            assert self.pool is not None, "Pool executor not found in context"
            self._update_pending_tasks()
            self._run_executor_loop()
        except Exception as e:
            print(e)
            raise
        finally:
            self.log.info("Exited execute loop")
            return None

    def _update_pending_tasks(self):
        trs = TaskRun.get_running_task_runs_to_poll()
        for tr in trs:
            self._pending_task_runs[tr.run_id] = tr
            self._pending_operations[tr.run_id] = tr.operation_id

        self.log.info(f"UPDATED PENDING TASKS: {trs}")

    def _run_executor_loop(self):
        while not self._stop_event.is_set():
            self._loop_iter()
            time.sleep(self._task_runner_idle_sleep_time)

    def _loop_iter(self):
        self._start_task_runs()
        self._polling()

    @with_yt_client
    def _polling(self, yt_client: yt.YtClient):
        for run_id, operation in list(self._pending_operations.items()):
            try:
                status = yt_client.get_operation_state(operation)
            except Exception as e:
                self.log.exception("Failed to get operation (%s) state for tr %s: %s", operation, run_id, e)
                continue

            if not status.is_finished():
                continue

            self.log.info(f"STATUS FINISHED: {status}")
            state = TaskRun.state_type.FAILED if status.is_unsuccessfully_finished() else TaskRun.state_type.SUCCESS # todo

            tr = self._pending_task_runs.get(run_id)
            if not tr:
                promise = self.pool.submit_or_execute(TaskRun.update_row, row=run_id, state=state, context=self.client_context)
            else:
                promise = self.pool.submit_or_execute(TaskRun.update_row, row=tr, state=state, context=self.client_context)


            def _on_complete():
                self.log.info(f"FINISHED: {promise.result}")
                self._pending_operations.pop(run_id, None)
                self._pending_task_runs.pop(run_id, None)
            promise.on_complete(_on_complete)

    # todo add heartbeat and to executor same