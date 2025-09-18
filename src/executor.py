from __future__ import annotations

import threading
import time
from logging_mixin import logger
from job import JobBase, classproperty, JobContext
from dagrun import DagRun
from taskrun import TaskRun
from pool import Pool
from callback import CallbackMixin
from rows_clients import TaskRunClient

from yt_wrapper import with_yt_client
import yt.wrapper as yt
from scheduler import ShardingOptions
from task import Task, DagInitializationError
from retrier import Retrier

def _get_task_run_manifest(tr: TaskRun):
    from rows_clients import DagRunClient, TaskRefClient

    dagrun = DagRunClient.get(run_id=tr.dagrun_id)
    try:
        ref = TaskRefClient.get(dag_id=tr.dag_id, task_id=tr.task_id)
        if ref is None:
            return None, dagrun
        task = Task.from_serialized_repr(ref=ref)
    except DagInitializationError as e:
        logger.warning("Failed to initialize task for TaskRun %s (dag_id=%s, task_id=%s): %s",
                       tr.run_id, tr.dag_id, tr.task_id, e, exc_info=True)
        if dagrun is not None:
            try:
                DagRunClient.set_state(self=dagrun, state=dagrun.state_type.FAILED)
            except Exception:
                pass
        return None, dagrun

    return task, dagrun


class Executor(JobBase, CallbackMixin):
    def __init__(
        self,
        context: JobContext,
        pool: Pool = None,
        task_runner_idle_sleep_time: float = 1.0,
        task_start_interval: float = 5.0,
        reconcile_limit: int = 20,
    ):
        JobBase.__init__(self, context)
        CallbackMixin.__init__(self, self._job_id)
        self.pool = pool
        self._task_runner_idle_sleep_time = task_runner_idle_sleep_time

        self._pending_task_runs: dict[str, TaskRun] = {}
        self._pending_operations: dict[str, str] = {}
        self._lock = threading.Lock()

        self._last_task_start_attempt: dict[str, float] = {}
        self._task_start_interval = task_start_interval

        self.num_virtual_shards = 500

        self._reconcile_limit = reconcile_limit

    @classproperty
    def name(self) -> str:
        return "task_runner"

    def determine_shard_options(self) -> ShardingOptions:
        try:
            index, num_shards = self.context.compute_shard_index(self)
        except Exception as e:
            index, num_shards = 0, 1
            self.log.warning(f"Failed to compute shard index: {e}")
        return ShardingOptions(shard_index=index, num_shards=num_shards, num_virtual_shards=self.num_virtual_shards)

    def _execute(self) -> int | None:
        self.log.info("Starting the executor")
        try:
            assert self.pool is not None, "Pool executor not found in context"
            self._update_pending_tasks()
            self._run_executor_loop()
        except Exception as e:
            self.log.exception("Executor crashed: %s", e)
            raise
        finally:
            self.log.info("Exited executor execute loop")
            return None

    def _update_pending_tasks(self):
        trs = TaskRunClient.get_running_task_runs_to_poll()
        with self._lock:
            for tr in trs:
                self._pending_task_runs[tr.run_id] = tr
                self._pending_operations[tr.run_id] = tr.operation_id

    def _run_executor_loop(self):
        while not self._stop_event.is_set():
            self._heartbeat_tick()
            self._loop_iter()
            time.sleep(self._task_runner_idle_sleep_time)

    def _loop_iter(self):
        self._start_taskruns()
        self._poll_pending_operations()
        self.pool.submit_or_execute(self._orphan_cleanup, context=self.client_context)
        self._drain_callbacks()

    def _start_taskrun(self, tr: TaskRun):
        task, dagrun = _get_task_run_manifest(tr)
        if not task or not dagrun:
            self.logger.warning(
                "TaskRun %s is not runnable (state=%s, dag_id=%s): task is null (%s) or dagrun is null (%s)",
                tr.run_id, tr.state, tr.dag_id, task is None, dagrun is None
            )
        if dagrun and dagrun.state == DagRun.state_type.FAILED:
            self.logger.warning(
                "DagRun %s is in failed state (%s), skipping task run %s",
                dagrun.run_id, dagrun.state, tr.run_id
            )
            try:
                TaskRunClient.set_state(rows=TaskRun.update_row(row=tr, state=TaskRun.state_type.SKIPPED,
                                                                required_state=TaskRun.state_type.QUEUED))
            except Exception:
                pass
            return

        if not task or not dagrun:
            return

        try:
            operation = task.run_operation(mutation_id=tr.run_id)
            operation = operation.id if hasattr(operation, "id") else operation
            dagrun.set_state(state=DagRun.state_type.RUNNING)
        except Exception as e:
            self.log.exception("Run operation failed for task %s: set state to failed: %s", tr.task_id, e)
            try:
                TaskRunClient.set_state(rows=TaskRun.update_row(row=tr, state=TaskRun.state_type.FAILED))
            except Exception as e:
                self.log.exception("Failed to mark failed for task %s after run operation failure: %s", tr.run_id, e)
            return

        with self._lock:
            self._pending_operations[tr.run_id] = operation
            self._pending_task_runs[tr.run_id] = tr

        try:
            TaskRunClient.set_state(rows=TaskRun.update_row(row=tr, state=TaskRun.state_type.RUNNING,
                                                            operation_id=operation, required_state=TaskRun.state_type.QUEUED))
        except Exception as e:
            self.log.exception("Failed to mark running for run %s after run operation success: %s", tr.run_id, e)
        return

    def _start_taskruns(self):
        shard = self.determine_shard_options()
        promise = self.pool.submit_or_execute(TaskRunClient.get_executable_task_runs_to_queue,
                                              shard=shard, context=self.client_context)
        def _on_complete():
            trs = promise.result
            for tr in trs:
                last_attempt = self._last_task_start_attempt.get(tr.run_id, 0)
                if time.time() - last_attempt < self._task_start_interval:
                    self.log.info(f"Skipping task {tr.task_id} due to start attempt interval")
                    continue

                self.pool.submit_or_execute(self._start_taskrun, tr=tr, context=self.client_context)
                self._last_task_start_attempt[tr.run_id] = time.time()
        promise.on_complete(_on_complete, owner_enqueue=self.add_callback)

    @with_yt_client
    def _poll_pending_operations(self, yt_client: yt.YtClient):
        with self._lock:
            pending_operations_items = list(self._pending_operations.items())
        for run_id, operation in pending_operations_items:
            try:
                status = yt_client.get_operation_state(operation)
            except Exception as e:
                self.log.exception("Failed to get operation (%s) state for tr %s: %s", operation, run_id, e)
                continue

            if status.is_finished():
                final_state = TaskRun.state_type.FAILED if status.is_unsuccessfully_finished() else TaskRun.state_type.SUCCESS
                with self._lock:
                    tr = self._pending_task_runs.get(run_id)
                if not tr:
                    promise = self.pool.submit_or_execute(
                        TaskRunClient.set_state, rows=TaskRun.update_row(run_id, state=final_state),
                        context=self.client_context
                    )
                else:
                    promise = self.pool.submit_or_execute(
                        TaskRunClient.set_state, rows=TaskRun.update_row(tr, state=final_state),
                        context=self.client_context
                    )
                def _on_complete():
                    with self._lock:
                        self._pending_operations.pop(run_id, None)
                        self._pending_task_runs.pop(run_id, None)
                    self.log.info("Finalized run %s as %s", run_id, final_state)
                promise.on_complete(_on_complete, owner_enqueue=self.add_callback)
            else:
                self.pool.submit_or_execute(
                    TaskRunClient.set_state, rows=TaskRun.update_row(run_id, state=TaskRun.state_type.RUNNING,
                                                                     operation_id=operation,
                                                                     required_state=TaskRun.state_type.QUEUED),
                    context=self.client_context
                )

    def _orphan_cleanup(self):
        trs = TaskRunClient.get_orphaned_task_runs(shard=self.determine_shard_options())
        if not trs:
            return

        skippable_trs = []
        for tr in trs:
            if tr.state == TaskRun.state_type.QUEUED:
                skippable_trs.append(tr)
            elif tr.state == TaskRun.state_type.RUNNING:
                if tr.operation_id:
                    def _try_abort_run():
                        abort_op_retrier = Retrier(logger=self.logger,
                                                   retry_options={"retries": 2, "raise_on_exhaust": False})
                        abort_op_retrier.run(
                            lambda: Task.abort_operation(operation=tr.operation_id, reason="DAG failed, aborting running tasks.")
                        )

                        TaskRunClient.set_state(
                            rows=TaskRun.update_row(row=tr, state=TaskRun.state_type.FAILED,
                            required_state=[TaskRun.state_type.RUNNING])
                        )

                    self.pool.submit_or_execute(_try_abort_run, context=self.client_context)
                else:
                    skippable_trs.append(tr)

        if skippable_trs:
            try:
                TaskRunClient.set_state(
                    rows=[TaskRun.update_row(row=tr, state=TaskRun.state_type.SKIPPED,
                                             required_state=[TaskRun.state_type.RUNNING, TaskRun.state_type.QUEUED]) for tr in skippable_trs])
            except Exception as e:
                self.log.exception("Failed to mark skipped orphan task rows: %s", e)
                return