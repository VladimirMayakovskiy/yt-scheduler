from __future__ import annotations
import time
from dataclasses import asdict
from typing import Callable

from dag import DAG
from dagrun import DagRun
from state import DagRunState
from taskrun import TaskRun
from task_runner import TaskRunner
from dagref import DagRef
from job import JobBase, classproperty, JobContext
from yt_wrapper import with_yt_client

import yt.wrapper as yt

class Scheduler(JobBase):
    def __init__(
        self,
        context: JobContext,
        scheduler_idle_sleep_time: float = 5.0,
    ):
        super().__init__(context)

        self._executor: TaskRunner = self.context.find(TaskRunner)

        assert self._executor is not None, "Executor not found in context"
        assert self.context.pool_executor is not None, "Pool executor not found in context"

        if self._executor is None:
            raise RuntimeError("Executor not found in context")

        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        self._dag_cache: dict[str, DAG] = {}

        self._created_dagruns_since_last_tick = 0

    @classproperty
    def name(self) -> str:
        return "scheduler"

    @property
    def _entry(self) -> Callable[[], int | None]:
        return self._execute

    def _get_dag(self, ref: DagRef.row_type) -> DAG:
        if ref.dag_id not in self._dag_cache:
            self._dag_cache[ref.dag_id] = DAG.from_serialized_dag(ref, context_wrapper=self.client_context)
        return self._dag_cache[ref.dag_id]

    def _execute(self) -> int | None:
        self.log.info("Starting the scheduler")
        try:
            self._run_scheduler_loop()
        except Exception as e:
            print(e)
            raise
        finally:
            self.log.info("Exited execute loop")
            return None

    def _run_scheduler_loop(self):
        try:
            while not self._runner.stop_event.is_set():
                self.log.info("\n\nONE STEP OF SCHEDULER LOOP")

                num_scheduled = self._do_scheduling() # один шаг планирования # todo return num_queued

                self.context.pool_executor.process_callbacks() # обрабатываем все завершившиеся колбеки todo

                # self._executor.heartbeat_async() # уведомляем executor todo check

                if not num_scheduled:
                    time.sleep(self._scheduler_idle_sleep_time)
        except Exception as e:
            print(e)
            raise

    def _do_scheduling(self) -> int:
        self._create_dagruns_for_dags()
        self._queue_scheduled_dagruns()
        self._start_queued_dagruns()
        self._schedule_running_dagruns()
        self._enqueue_task_runs()
        return self._created_dagruns_since_last_tick

    def _create_dagruns_for_dags(self):
        num_created = self.context.pool_executor.submit(
            self._try_claim_dagruns, context=self.client_context, block=False
        )
        @num_created.on_complete
        def _():
            self.log.info(f"CREATED DAGRUNS: {num_created.result}")
            self._created_dagruns_since_last_tick = num_created.result

    @with_yt_client
    def _try_claim_dagruns(self, yt_client: yt.YtClient) -> int:
        try:
            with yt_client.Transaction(type="tablet"):
                rows = DagRef.dags_needing_dagruns()

                metas = []
                runs = []
                for row in rows:
                    run_id = yt.common.generate_uuid()
                    metas.append(DagRef.meta_row_type(id=row["id"], dag_id=row["dag_id"], created_at=row["created_at"], run_id=run_id))

                    ref = DagRef.row_type(
                        dag_id=row["dag_id"],
                        serialized_dag=row["serialized_dag"],
                        payload_hash=row["payload_hash"],
                        load_at=row["load_at"]
                    )
                    dag = self._get_dag(ref=ref)

                    run = DagRun(run_id=run_id, dag_id=dag.dag_id, creating_job_id=self._runner.job_id).set_state(state=DagRunState.SCHEDULED)
                    runs.append(run)

                yt_client.insert_rows(DagRun.table_path, [asdict(r) for r in runs])
                yt_client.insert_rows(DagRef.meta_row_type.table_path, [asdict(m) for m in metas], update=True)
            return len(runs)
        except Exception as e:
            self.log.warning("Failed to claim dagruns: %s", e)
            raise

    def _queue_scheduled_dagruns(self):
        dag_runs = self.context.pool_executor.submit(DagRun.get_scheduled_dag_runs_to_queue, context=self.client_context, block=False)
        @dag_runs.on_complete
        def _():
            for run in dag_runs.result:
                dag = self._get_dag(ref=DagRef.get(run.dag_id))
                if not dag:
                    self.log.warning(f"Can not find ref to dag or dag of dag_id={dag.dag_id},"
                                     f" skipping queue dagrun {run.run_id}")
                    continue

                run.queue_dag_run(dag)

    def _start_queued_dagruns(self):
        dag_runs = self.context.pool_executor.submit(DagRun.get_queued_dag_runs_to_set_running, context=self.client_context, block=False)
        @dag_runs.on_complete
        def _():
            self._schedule_dag_runs(dag_runs.result)

    def _schedule_running_dagruns(self):
        dag_runs = self.context.pool_executor.submit(DagRun.get_running_dag_runs_to_examine, context=self.client_context, block=False)
        @dag_runs.on_complete
        def _():
            self._schedule_dag_runs(dag_runs.result)

    def _schedule_dag_runs(
        self,
        dag_runs: list[DagRun],
    ) -> None:
        for run in dag_runs:
            dag = self._get_dag(DagRef.get(run.dag_id))
            if not dag:
                self.log.warning(f"Can not find ref to dag or dag of dag_id={dag.dag_id},"
                                 f" skipping scheduling dagrun {run.run_id}")
                continue

            schedulable_trs = run.update_state(dag)

            scheduled = run.schedule_trs(schedulable_trs)

            self.log.info(f"SCHEDULED trs COUNT: {scheduled} of {len(schedulable_trs)}, run_id={run.run_id}")

    def _enqueue_task_runs(self):
        trs = self.context.pool_executor.submit(TaskRun.get_executable_task_runs_to_queue, context=self.client_context, block=False)
        @trs.on_complete
        def _():
            for tr in trs.result:
                try:
                    tr_runnable = tr.make_runnable(dag_loader=lambda dag_id: self._get_dag(DagRef.get(dag_id)))
                    if not tr_runnable:
                        self.log.warning(
                            "TaskRun %s is not runnable (state=%s, dag_id=%s)",
                            tr.task_id, tr.state, tr.dag_id
                        )
                        continue
                    self._executor.queue_task_run(tr_runnable)
                except Exception as e:
                    self.log.exception("Failed to enqueue task %s: %s, skipping",tr.task_id, e)
                    continue