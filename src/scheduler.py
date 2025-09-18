from __future__ import annotations
import time
from dataclasses import asdict

from dag import DAG
from dagrun import DagRun
from state import DagRunState
from dagref import DagRef
from common import classproperty
from job import JobBase, JobContext
from yt_wrapper import with_yt_client
from pool import Pool

import yt.wrapper as yt

class Scheduler(JobBase):
    def __init__(
        self,
        context: JobContext,
        pool: Pool = None,
        scheduler_idle_sleep_time: float = 5.0,
    ):
        super().__init__(context)

        self.pool = pool

        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        self._dag_cache: dict[str, DAG] = {}

        self._created_dagruns_since_last_tick = 0

    @classproperty
    def name(self) -> str:
        return "scheduler"

    def _get_dag(self, ref: DagRef.row_type) -> DAG | None:
        try:
            if ref.dag_id not in self._dag_cache:
                self._dag_cache[ref.dag_id] = DAG.from_serialized_repr(ref)
            return self._dag_cache[ref.dag_id]
        except Exception as e:
            self.log.exception("Failed to get dag %s: %s", ref.dag_id, e)
            return None

    def _execute(self) -> int | None:
        self.log.info("Starting the scheduler")
        try:
            assert self.pool is not None, "Pool executor not found in context"
            self._run_scheduler_loop()
            self.log.info("Scheduler loop finished normally")
        except Exception as e:
            self.log.exception("Scheduler crashed: %s", e)
            raise
        finally:
            self.log.info("Exited execute loop")
            return None

    def _run_scheduler_loop(self):
        while not self._stop_event.is_set():
            num_scheduled = self._loop_iter()
            if not num_scheduled:
                time.sleep(self._scheduler_idle_sleep_time)

    def _loop_iter(self):
        self.log.info("\n\nONE STEP OF SCHEDULER LOOP")
        num_scheduled = self._do_scheduling()
        self.log.info(num_scheduled)
        self.pool.process_callbacks()
        # self._executor.heartbeat_async() # уведомляем executor todo check
        return num_scheduled


    def _do_scheduling(self) -> int:
        self._create_dagruns_for_dags()
        self._queue_scheduled_dagruns()
        self._start_queued_dagruns()
        self._schedule_running_dagruns()
        return self._created_dagruns_since_last_tick

    def _create_dagruns_for_dags(self):
        promise = self.pool.submit_or_execute(self._try_claim_dagruns, context=self.client_context)
        promise.on_complete(lambda: setattr(self, "_created_dagruns_since_last_tick", promise.result))

    @with_yt_client
    def _try_claim_dagruns(self, yt_client: yt.YtClient) -> int:
        try:
            with yt_client.Transaction(type="tablet"):
                rows = DagRef.dags_needing_dagruns()

                metas = []
                runs = []
                for row in rows:
                    run_id = yt.common.generate_uuid()
                    metas.append(DagRef.meta_row_type(
                        id=row["id"],
                        dag_id=row["dag_id"],
                        created_at=row["created_at"],
                        run_id=run_id)
                    )

                    ref = DagRef.row_type(
                        dag_id=row["dag_id"],
                        serialized_repr=row["serialized_repr"],
                        payload_hash=row["payload_hash"],
                        created_at=row["created_at"]
                    )
                    dag = self._get_dag(ref=ref)

                    run = DagRun(
                        run_id=run_id,
                        dag_id=dag.dag_id, creating_job_id=self._job_id
                    ).set_state(state=DagRunState.SCHEDULED)
                    runs.append(run)

                yt_client.insert_rows(DagRun.table_path, [asdict(r) for r in runs])
                yt_client.insert_rows(DagRef.meta_row_type.table_path, [asdict(m) for m in metas], update=True)
            return len(runs)
        except Exception as e:
            self.log.warning("Failed to claim dagruns: %s", e)
            raise

    def _queue_scheduled_dagruns(self):
        promise = self.pool.submit_or_execute(DagRun.get_scheduled_dag_runs_to_queue, context=self.client_context)

        def _on_complete():
            dag_runs = promise.result
            for run in dag_runs:
                dag = self._get_dag(ref=DagRef.get(run.dag_id))
                if not dag:
                    self.log.warning(f"Can not find ref to dag or dag of dag_id={dag.dag_id},"
                                     f" skipping queue dagrun {run.run_id}")
                    continue

                run.queue_dag_run(dag)

        promise.on_complete(_on_complete)

    def _start_queued_dagruns(self):
        promise = self.pool.submit_or_execute(DagRun.get_queued_dag_runs_to_set_running, context=self.client_context)
        promise.on_complete(lambda: self._schedule_dag_runs(promise.result))

    def _schedule_running_dagruns(self):
        promise = self.pool.submit_or_execute(DagRun.get_running_dag_runs_to_examine, context=self.client_context)
        promise.on_complete(lambda: self._schedule_dag_runs(promise.result))

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
            if schedulable_trs:
                scheduled = run.schedule_trs(schedulable_trs)
                self.log.info(f"SCHEDULED trs COUNT: {scheduled} of {len(schedulable_trs)}, run_id={run.run_id}")