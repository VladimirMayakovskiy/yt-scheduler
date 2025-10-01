from __future__ import annotations

import threading
from typing import TypedDict
from datetime import datetime, timezone as tz

from dag import DAG
from dagrun import DagRun
from state import DagRunState
from dagref import DagRef, DagMeta
from common import classproperty
from job import JobBase, JobContext
from yt_wrapper import with_yt_client
from pool import Pool
from callback import CallbackMixin
from handle_exceptions import handle_exceptions
from rows_clients import DagRunClient, DagRefClient, _READ_COUNTER_TRANSIENT_RETRY_OPTIONS, _WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS # todo
from errors import DagInitializationError

import yt.wrapper as yt

class ShardingOptions(TypedDict):
    shard_index: int
    num_shards: int
    num_virtual_shards: int | None

@with_yt_client
def _set_invalid_meta(invalid_metas, yt_client: yt.YtClient):
    try:
        with yt_client.Transaction(type="tablet"):
            batched_metas = []
            rows = DagRef.dags_needing_dagruns_of_metas(metas=invalid_metas)
            for ref, meta in rows:
                meta.run_id = f"ERR:PARSING_FAILED:{datetime.now(tz.utc).isoformat()}"
                batched_metas.append(meta)
            if batched_metas:
                DagMeta.upsert_rows(batched_metas)
    except Exception as e:
        print("EXCEPTION", threading.get_ident(), invalid_metas, e) # todo
        raise

@with_yt_client
def _create_dag_runs_atomic(metas: list[str], yt_client: yt.YtClient):
    try:
        with (yt_client.Transaction(type="tablet")):
            rows: list[tuple[DagRef.row_type, DagRef.meta_row_type]] = \
                DagRef.dags_needing_dagruns_of_metas(metas=metas, yt_client=yt_client)
            batched_metas = []
            batched_runs = []
            for ref, meta in rows:
                run_id = yt.common.generate_uuid() # todo
                meta.run_id = run_id
                batched_metas.append(meta)

                batched_runs.append(DagRun(run_id=run_id,
                                           dag_id=meta.dag_id,
                                           state=DagRunState.SCHEDULED,
                                           ))
            if batched_runs:
                DagRun.upsert_rows(rows=batched_runs)
                DagMeta.upsert_rows(rows=batched_metas)
            return len(batched_runs)
    except Exception as e:
        print("EXCEPTION", threading.get_ident(), metas, e) # todo
        raise

class Scheduler(JobBase, CallbackMixin):
    def __init__(
        self,
        context: JobContext,
        pool: Pool = None,
        scheduler_idle_sleep_time: float = 5.0,
        num_virtual_shards: int = 500,
    ):
        JobBase.__init__(self, context)
        CallbackMixin.__init__(self, self._job_id)

        self.pool = pool

        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        self._dag_cache: dict[str, DAG] = {}

        self._created_dagruns_since_last_tick = 0
        self._created_dagruns_lock = threading.Lock()

        self.num_virtual_shards = num_virtual_shards # todo from config

        self._create_dag_runs_atomic = handle_exceptions(_create_dag_runs_atomic,
                                                         default_retry_options=_READ_COUNTER_TRANSIENT_RETRY_OPTIONS)

        self._set_invalid_meta = handle_exceptions(_set_invalid_meta,
                                                   default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS)

    @classproperty
    def name(self) -> str:
        return "scheduler"

    def determine_shard_options(self) -> ShardingOptions:
        try:
            index, num_shards = self.context.compute_shard_index(self)
        except Exception as e:
            index, num_shards = 0, 1
            self.log.warning(f"Failed to compute shard index: {e}")
        return ShardingOptions(shard_index=index, num_shards=num_shards, num_virtual_shards=self.num_virtual_shards)

    def _get_dag(self, ref: DagRef.row_type) -> DAG | None:
        if not ref:
            return None
        if ref.dag_id not in self._dag_cache:
            try:
                self._dag_cache[ref.dag_id] = DAG.from_serialized_repr(ref) # todo lock?
            except DagInitializationError as e:
                self.log.exception("Failed to get dag %s: %s", ref.dag_id, e)
                return None
        return self._dag_cache[ref.dag_id]

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
            self.log.info("Exited scheduler execute loop")
            return None

    def _run_scheduler_loop(self):
        while not self._stop_event.is_set():
            self._heartbeat_tick()
            num_scheduled = self._loop_iter()
            if not num_scheduled:
                self._stop_event.wait(self._scheduler_idle_sleep_time)

    def _loop_iter(self):
        self._do_scheduling()
        self._drain_callbacks()
        with self._created_dagruns_lock:
            num_scheduled = self._created_dagruns_since_last_tick
            self._created_dagruns_since_last_tick = 0
        return num_scheduled

    def _do_scheduling(self):
        self._create_dagruns_for_dags()
        self._queue_scheduled_dagruns()
        self._start_queued_dagruns()
        self._schedule_running_dagruns()

    def _create_dagruns_for_dags(self):
        promise = self.pool.submit_or_execute(
            self._try_claim_dagruns,
            context=self.client_context,
        )
        def _on_complete_inc():
            try:
                res = int(promise.result)
            except Exception as e:
                self.log.exception("Failed to claim dagruns: %s", e)
                res = 0
            with self._created_dagruns_lock:
                self._created_dagruns_since_last_tick += res
        promise.on_complete(_on_complete_inc, owner_enqueue=self.add_callback)

    @with_yt_client
    def _try_claim_dagruns(self, yt_client: yt.YtClient) -> int:
        rows = DagRefClient.dags_needing_dagruns(shard=self.determine_shard_options())
        target_metas: list[str] = []
        invalid_rows = []
        for ref, meta in rows:
            dag = self._get_dag(ref=ref)
            if not dag:
                self.log.warning(f"Can not find ref to dag or dag of dag_id={ref.dag_id}, will mark failed")
                invalid_rows.append((ref, meta))
                continue
            target_metas.append(meta.id)

        def _try_set_metas_invalid():
            from retrier import Retrier
            def _try_get_dag(try_ref): # noqa
                self._dag_cache[try_ref.dag_id] = DAG.from_serialized_repr(try_ref)
                return True
            get_dag_retrier = Retrier(logger=self.logger, retry_options={"exceptions": (DagInitializationError,),
                                                                         "retries": 1,
                                                                         "raise_on_exhaust": False,
                                                                         "on_exhaust": lambda e: False})
            invalid_metas: list[str] = []
            for try_ref, try_meta in invalid_rows:
                dag_loaded = get_dag_retrier.run(lambda: _try_get_dag(try_ref=try_ref))
                if not dag_loaded:
                    invalid_metas.append(try_meta.id)
            return invalid_metas

        def _on_complete_set_invalid():
            invalid_metas = promise.result
            self._set_invalid_meta(invalid_metas, yt_client=yt_client)

        promise = self.pool.submit_or_execute(_try_set_metas_invalid, context=self.client_context)
        promise.on_complete(_on_complete_set_invalid, owner_enqueue=self.add_callback)

        if target_metas:
            return self._create_dag_runs_atomic(metas=target_metas, yt_client=yt_client)
        return 0

    def _queue_scheduled_dagruns(self):
        promise = self.pool.submit_or_execute(
            DagRunClient.get_scheduled_dag_runs_to_queue, shard=self.determine_shard_options(),
            context=self.client_context)

        def _on_complete():
            dag_runs = promise.result
            for run in dag_runs:
                dag = self._get_dag(ref=DagRefClient.get(run.dag_id))
                if not dag:
                    self.log.warning(f"Can not find ref to dag or dag of dag_id={dag.dag_id},"
                                     f" skipping queue dagrun {run.run_id}")
                    continue
                self.pool.submit_or_execute(DagRunClient.queue_run_atomic, run, dag, context=self.client_context)

        promise.on_complete(_on_complete, owner_enqueue=self.add_callback)

    def _start_queued_dagruns(self):
        promise = self.pool.submit_or_execute(
            DagRunClient.get_queued_dag_runs_to_set_running, shard=self.determine_shard_options(),
            context=self.client_context)
        promise.on_complete(lambda: self._schedule_dag_runs(promise.result), owner_enqueue=self.add_callback)

    def _schedule_running_dagruns(self):
        promise = self.pool.submit_or_execute(
            DagRunClient.get_running_dag_runs_to_examine, shard=self.determine_shard_options(),
            context=self.client_context)
        promise.on_complete(lambda: self._schedule_dag_runs(promise.result), owner_enqueue=self.add_callback)

    def _schedule_dag_runs(
        self,
        dag_runs: list[DagRun],
    ) -> None:
        for run in dag_runs:
            dag = self._get_dag(ref=DagRefClient.get(run.dag_id))
            if not dag:
                self.log.warning(f"Can not find ref to dag or dag of dag_id={dag.dag_id},"
                                 f" skipping scheduling dagrun {run.run_id}")
                continue

            schedulable_tids = DagRunClient.update_state(self=run, dag=dag)
            if not schedulable_tids:
                continue

            num_scheduled = DagRunClient.schedule_trs(run.run_id, schedulable_tids)
            self.log.info(f"For run_id=%s scheduled trs count: %s of all schedulable %s",
                          run.run_id, num_scheduled, len(schedulable_tids))