from __future__ import annotations

import typing
from datetime import datetime
from typing import Optional, ClassVar
from dataclasses import field, asdict, KW_ONLY

from logging_mixin import LoggingMixin
if typing.TYPE_CHECKING:
    from dag import DAG
from state import DagRunState
from taskrun import TaskRun
from base_row import YtRow, TablePath
from rows_helpers import make_formatted_select
from yt_wrapper import with_yt_client
import yt.wrapper as yt

@yt.yt_dataclass
class DagRunRow(YtRow):
    table_path:  ClassVar[str] = TablePath("dag_run")
    key_columns: ClassVar[list[str]] = ["run_id"]
    alias: ClassVar[list[str]] = "dagrun"

    run_id: str = field(default_factory=lambda: yt.common.generate_uuid())

    _: KW_ONLY

    dag_id: str
    state: str

    scheduled_at: Optional[str] = None
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class DagRun(DagRunRow, LoggingMixin):
    row_type: ClassVar[type[DagRunRow]] = DagRunRow
    state_type: ClassVar[type[DagRunState]] = DagRunState

    def __init__( # todo add scheduled_at, queued_at, start_date, end_date correct
            self,
            row: DagRun.row_type | None = None,
            *,
            run_id: str | None = None,
            dag_id: str | None = None,
            state: DagRun.state_type | None = None,
            start_date: datetime | str | None = None,
            end_date: datetime | str | None = None,
            creating_job_id: str | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
            super().__init__(
                dag_id=dag_id,
                state=state,
                start_date=(start_date.isoformat() if isinstance(start_date, datetime) else start_date) or datetime.utcnow().isoformat(),
                end_date=end_date.isoformat() if isinstance(end_date, datetime) else end_date,
            )
        if run_id is not None:
            self.run_id = run_id
        self.creating_job_id = creating_job_id

    @classmethod
    def fetch_rows(
        cls,
        run_id: str | list[str] | tuple[str] = None,
        dag_id: str | list[str] | tuple[str] = None,
        state: DagRun.state_type | list[DagRun.state_type] | tuple[DagRun.state_type] = None,
        limit: int = None,
    ) -> list[DagRun]:
        rows = make_formatted_select(
            cls=cls,
            run_id=run_id,
            dag_id=dag_id,
            state=state,
            limit=limit,
        )
        return [cls(cls.row_type(**row)) for row in rows]

    @classmethod
    def get(cls, run_id: Optional[str], dag_id: Optional[str]) -> "DagRunRow" | None:
        return YtRow.get(cls=cls, run_id=run_id, dag_id=dag_id)

    @classmethod
    def get_dag_runs_to_examine(cls) -> list["DagRun"]:
        return cls.fetch_rows(state=[cls.state_type.QUEUED, cls.state_type.RUNNING])

    @classmethod
    def get_scheduled_dag_runs_to_queue(cls) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.SCHEDULED)

    @classmethod
    def get_queued_dag_runs_to_set_running(cls) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.QUEUED) # todo add priopity queue и тд

    @classmethod
    def get_running_dag_runs_to_examine(cls) -> list["DagRun"]:
        return cls.fetch_rows(state=cls.state_type.RUNNING)

    @with_yt_client
    def queue_dag_run(self, dag: DAG, yt_client: yt.YtClient):
        try:
            trs = self._create_task_runs(dag)
            with yt_client.Transaction(type="tablet"):
                yt_client.insert_rows(TaskRun.table_path, [asdict(t) for t in trs])
                self.set_state(DagRun.state_type.QUEUED)
        except Exception as e:
            self.log.exception("Failed to queue dagrun, SKIPPING: %s", e)

    def _create_task_runs(self, dag: DAG) -> list[TaskRun]:
        try:
            existing_trs = TaskRun.fetch_rows(run_id=self.run_id, dag_id=self.dag_id, task_id=dag.task_ids)
            existing_task_ids = {t.task_id for t in existing_trs}

            tasks_to_create = [task for task in dag.tasks if task.task_id not in existing_task_ids]

            roots_trs_instant_queue = [
                TaskRun(
                    dag_run_id=self.run_id,
                    operator=task,
                    state=TaskRun.state_type.QUEUED,
                )
                for task in tasks_to_create
                if task in dag.roots
            ]
            trs_to_create = [
                TaskRun(
                    dag_run_id=self.run_id,
                    operator=task,
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

    def update_state(
            self, dag: DAG
    ) -> list[TaskRun]:
        try:
            trs, schedulable_trs, unfinished_trs, finished_trs = self.trs_scheduling_decisions(dag)

            all_finished = (len(unfinished_trs) == 0)
            any_failed = any(t.state == TaskRun.state_type.FAILED for t in trs)
            all_success = all(t.state == TaskRun.state_type.SUCCESS for t in trs)

            if all_finished and any_failed:
                self.set_state(DagRun.state_type.FAILED) # todo set all unfinished not running tasks failed
            elif all_finished and all_success:
                self.set_state(DagRun.state_type.SUCCESS)
            else:
                self.set_state(DagRun.state_type.RUNNING) # todo
            return schedulable_trs
        except Exception as e:
            self.log.exception(f"Failed to update state for run={self.run_id}, SKIPPING: %s", e)
            return []

    def trs_scheduling_decisions(self, dag: DAG):
        trs = TaskRun.fetch_rows(dag_run_id=self.run_id, dag_id=self.dag_id, task_id=dag.task_ids)

        unfinished_trs = [t for t in trs if t.state in TaskRun.state_type.unfinished_states]
        finished_trs = [t for t in trs if t.state in TaskRun.state_type.finished_states]
        schedulable_trs = [t for t in trs if t.state == TaskRun.state_type.SCHEDULED]

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
        finished_trs_ids = {ti.run_id for ti in finished_trs}

        for ti in schedulable_trs:
            upstream = (set(task.upstream_task_ids) for task in [dag.task_dict.get(ti.step, None)] if task) or set()
            if upstream.issubset(finished_trs_ids):
                ready_trs.append(ti)
        return ready_trs

    @staticmethod
    @with_yt_client
    def schedule_trs(schedulable_trs: list[TaskRun], yt_client: yt.YtClient) -> int:
        try:
            with yt_client.Transaction(type="tablet"):
                trs = [TaskRun.update_row(row=tr, state=TaskRun.state_type.QUEUED) for tr in schedulable_trs]
                yt_client.insert_rows(TaskRun.table_path, [asdict(row) for row in trs])
                return len(trs)
        except Exception as e:
            DagRun.logger.exception("Failed to update rows, skipping: %s", e)
            return 0

    @with_yt_client
    def set_state(self, state: DagRun.state_type, yt_client: yt.YtClient) -> DagRun: # todo
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
                            base.start_date = base.queued_at
                    base.end_date = None
                elif base.state in [None, DagRun.state_type.SCHEDULED, DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                    base.end_date = now
                base.state = state
            return base

        row_obj = _build_row_by_state()
        try:
            yt_client.insert_rows(DagRun.table_path, [asdict(row_obj)])
        except Exception as e:
            self.log.exception("Failed update state for run_id=%s: %s", self.run_id, e)
            raise

        self.state = row_obj.state
        self.scheduled_at = row_obj.scheduled_at
        self.queued_at = row_obj.queued_at
        self.start_date = row_obj.start_date
        self.end_date = row_obj.end_date
        return self