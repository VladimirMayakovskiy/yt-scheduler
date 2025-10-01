from __future__ import annotations

import typing
from datetime import datetime, timezone as tz
from typing import Optional, ClassVar
from dataclasses import field, asdict, KW_ONLY

from logging_mixin import LoggingMixin

if typing.TYPE_CHECKING:
    from dag import DAG
    from scheduler import ShardingOptions
from state import DagRunState
from taskrun import TaskRun
from base_row import YtRow, TablePath
from rows_helpers import make_formatted_select, copy_fields
from yt_wrapper import with_yt_client
import yt.wrapper as yt

@yt.yt_dataclass
class DagRunRow(YtRow):
    table_path:  ClassVar[str] = TablePath("dag_run")
    key_columns: ClassVar[str] = ["run_id"]
    alias: ClassVar[list[str]] = "dagrun"

    run_id: str = field(default_factory=lambda: yt.common.generate_uuid())

    _: KW_ONLY

    dag_id: str
    state: str

    scheduled_at: Optional[str] = field(default_factory=lambda: datetime.now(tz.utc).isoformat())
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class DagRun(DagRunRow, LoggingMixin):
    row_type: ClassVar[type[DagRunRow]] = DagRunRow
    state_type: ClassVar[type[DagRunState]] = DagRunState

    def __init__(
            self,
            row: DagRun.row_type | None = None,
            *,
            run_id: str | None = None,
            dag_id: str | None = None,
            state: DagRun.state_type | None = None,
            scheduled_at: datetime | str | None = None,
            queued_at: datetime | str | None = None,
            start_date: datetime | str | None = None,
            end_date: datetime | str | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
            def _to_iso(value: datetime | str | None) -> str | None:
                if isinstance(value, datetime):
                    return value.isoformat()
                return value

            super().__init__(
                dag_id=dag_id,
                state=state,
                queued_at=_to_iso(queued_at),
                start_date=_to_iso(start_date),
                end_date=_to_iso(end_date),
            )
            if run_id is not None:
                self.run_id = run_id
            if scheduled_at is not None:
                self.scheduled_at = _to_iso(scheduled_at)

    @classmethod
    def fetch_rows(
        cls,
        run_id: str | list[str] | tuple[str] = None,
        dag_id: str | list[str] | tuple[str] = None,
        state: DagRun.state_type | list[DagRun.state_type] | tuple[DagRun.state_type] = None,
        limit: int = None,
        shard_key: str = None,
        shard: "ShardingOptions" | None = None,
    ) -> list[DagRun]:
        rows = make_formatted_select(
            cls=cls,
            run_id=run_id,
            dag_id=dag_id,
            state=state,
            limit=limit,
            shard_key=shard_key,
            shard=shard,
        )
        return [cls(cls.row_type(**row)) for row in rows]

    @classmethod
    def get(cls, run_id: Optional[str]=None, dag_id: Optional[str]=None) -> "DagRunRow" | None:
        return super(DagRunRow, cls).get(run_id=run_id, dag_id=dag_id)

    @classmethod
    def get_scheduled_dag_runs_to_queue(cls, shard: "ShardingOptions" | None = None) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.SCHEDULED,
                              shard_key=cls.key_columns[0],
                              shard=shard)

    @classmethod
    def get_queued_dag_runs_to_set_running(cls, shard: "ShardingOptions" | None = None) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.QUEUED,
                              shard_key=cls.key_columns[0],
                              shard=shard)

    @classmethod
    def get_running_dag_runs_to_examine(cls, shard: "ShardingOptions" | None = None) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.RUNNING,
                              shard_key=cls.key_columns[0],
                              shard=shard)

    @with_yt_client
    def queue_run_atomic(self, dag: DAG, yt_client: yt.YtClient):
        try:
            with yt_client.Transaction(type="tablet"):
                run = DagRun.get(run_id=self.run_id)
                if run is None or run.state != DagRun.state_type.SCHEDULED:
                    return
                trs = self._init_task_runs_for_run(dag)
                if trs:
                    TaskRun.upsert_rows(rows=trs, yt_client=yt_client)
                self.set_state(DagRun.state_type.QUEUED)
        except Exception as e:
            self.log.exception("Failed to enqueue dagrun %s for dag, skipping: %s", self.run_id, self.dag_id, e)
            raise

    def _init_task_runs_for_run(self, dag: DAG) -> list[TaskRun]:
        try:
            existing_trs = TaskRun.fetch_rows(run_id=self.run_id, dag_id=self.dag_id, task_id=dag.task_ids)
            existing_task_ids = {t.task_id for t in existing_trs}

            tasks_to_create = [task for task in dag.tasks if task.task_id not in existing_task_ids]

            roots_trs_instant_queue = [
                TaskRun(
                    dagrun_id=self.run_id,
                    task=task,
                    state=TaskRun.state_type.QUEUED,
                )
                for task in tasks_to_create
                if task in dag.roots
            ]
            trs_to_create = [
                TaskRun(
                    dagrun_id=self.run_id,
                    task=task,
                    state=TaskRun.state_type.SCHEDULED,
                )
                for task in tasks_to_create
                if task not in dag.roots
            ]

            self.log.info(f"roots: {roots_trs_instant_queue}")
            self.log.info(f"other: {trs_to_create}")

            return roots_trs_instant_queue + trs_to_create
        except Exception as e:
            self.log.exception(f"Failed to create trs for run %s: %s", self.run_id, e)
            raise

    @with_yt_client
    def update_state(self, dag: DAG, yt_client: yt.YtClient) -> list[str]:
        try:
            with yt_client.Transaction(type="tablet"):
                trs, schedulable_trs, unfinished_trs, finished_trs = self.trs_scheduling_decisions(dag)

                all_finished = (len(unfinished_trs) == 0)
                any_failed = any(tr.state == TaskRun.state_type.FAILED for tr in trs)
                all_success = all(tr.state == TaskRun.state_type.SUCCESS for tr in trs)

                skippable_trs = []
                if any_failed:
                    run_state = DagRun.state_type.FAILED
                    schedulable_trs.clear()
                    skippable_trs = [tr for tr in unfinished_trs if tr.state in (TaskRun.state_type.SCHEDULED, TaskRun.state_type.QUEUED)]
                elif all_finished and all_success:
                    run_state = DagRun.state_type.SUCCESS
                else:
                    run_state = DagRun.state_type.RUNNING

                if skippable_trs:
                    TaskRun.set_state(
                        rows=[TaskRun.update_row(tr, state=TaskRun.state_type.SKIPPED) for tr in skippable_trs]
                    )

                self.set_state(run_state)
            return [tr.run_id for tr in schedulable_trs]
        except Exception as e:
            self.log.exception("Failed to atomic update state for run=%s: %s", self.run_id, e)
            raise

    def trs_scheduling_decisions(self, dag: DAG):
        trs = TaskRun.fetch_rows(dagrun_id=self.run_id, dag_id=self.dag_id, task_id=dag.task_ids)

        unfinished_trs = [tr for tr in trs if tr.state in TaskRun.state_type.unfinished_states]
        finished_trs = [tr for tr in trs if tr.state in TaskRun.state_type.finished_states]
        schedulable_trs = [tr for tr in trs if tr.state == TaskRun.state_type.SCHEDULED]

        if schedulable_trs:
            schedulable_trs = DagRun._get_ready_trs(
                dag,
                schedulable_trs,
                finished_trs,
            )
        else:
            schedulable_trs = []

        self.log.info(f"schedulable_trs for dagrun={self.run_id}: {schedulable_trs}")
        self.log.info(f"unfinished_trs for dagrun={self.run_id}: {unfinished_trs}")
        self.log.info(f"finished_trs for dagrun={self.run_id}: {finished_trs}")
        return trs, schedulable_trs, unfinished_trs, finished_trs

    @staticmethod
    def _get_ready_trs(
        dag: DAG,
        schedulable_trs: list[TaskRun],
        finished_trs: list[TaskRun],
    ):
        ready_trs: list[TaskRun] = []
        finished_trs_ids = {ti.task_id for ti in finished_trs}

        for ti in schedulable_trs:
            upstream = set().union(*(set(task.upstream_task_ids) for task in [dag.task_dict.get(ti.task_id, None)] if task)) or set()
            if upstream.issubset(finished_trs_ids):
                ready_trs.append(ti)
        return ready_trs

    @staticmethod
    @with_yt_client
    def schedule_trs(run_id: str, tids: list[str], yt_client: yt.YtClient) -> int:
        if not tids:
            return 0

        try:
            with yt_client.Transaction(type="tablet"):
                rows = TaskRun.fetch_rows(run_id=tids, dagrun_id=run_id, state=TaskRun.state_type.SCHEDULED)
                if rows:
                    trs = TaskRun.set_state(rows=[TaskRun.update_row(tr, state=TaskRun.state_type.QUEUED) for tr in rows])
                else:
                    trs = []
            return len(trs)
        except Exception as e:
            DagRun.logger.exception("Failed to atomic schedule trs for run=%s: %s", run_id, e)
            raise

    def set_state(self, state: DagRun.state_type) -> DagRun:
        def _build_row_by_state() -> DagRun.row_type:
            base = self.row_type(**asdict(self))
            now = datetime.utcnow().isoformat()
            if base.state != state:
                if state in [DagRun.state_type.SCHEDULED, DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                    base.scheduled_at = base.scheduled_at or now
                    if state == DagRun.state_type.SCHEDULED:
                        base.queued_at = None
                        base.start_date = None
                    else:
                        base.queued_at = base.queued_at or now
                        if state == DagRun.state_type.QUEUED:
                            base.start_date = None
                        else:
                            base.start_date = now
                    base.end_date = None
                elif base.state in [None, DagRun.state_type.SCHEDULED, DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                    base.end_date = now
                    if base.queued_at is None:
                        base.queued_at = base.scheduled_at
                    if base.start_date is None:
                        base.start_date = base.queued_at
                base.state = state
            return base

        row_obj = _build_row_by_state()
        try:
            DagRun.upsert_rows(rows=row_obj)
        except Exception as e:
            self.log.exception("Failed update state for run_id=%s: %s", self.run_id, e)
            raise

        copy_fields(self, row_obj, cls=DagRun)
        return self