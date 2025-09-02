from __future__ import annotations

import typing
import uuid
from dataclasses import field, asdict
from datetime import datetime
from typing import Optional, ClassVar

from logging_mixin import LoggingMixin
if typing.TYPE_CHECKING:
    from dag import DAG
from state import DagRunState
from taskrun import TaskRun
from base import _fetch_rows, get_all_row_fields, BaseRow
from yt_wrapper import with_yt_client

import yt.wrapper as yt
from yt.wrapper.schema import TableSchema

@yt.yt_dataclass
class DagRunRow(BaseRow):
    table_path:  ClassVar[str] = "//tmp/dag_run"
    key_columns: ClassVar[list[str]] = ["run_id"]

    dag_id: str

    state: str # DagRunState

    scheduled_at: Optional[str] = None
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)

# @yt.yt_dataclass
class DagRun(DagRunRow, LoggingMixin):
    row_type: ClassVar[type[DagRunRow]] = DagRunRow
    state_type: ClassVar[type[DagRunState]] = DagRunState

    def __init__(
            self,
            row: DagRun.row_type | None = None,
            *,
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

        self.creating_job_id = creating_job_id


    @classmethod
    @with_yt_client
    def get_dag_runs_by_state(cls, state: DagRun.state_type, yt_client: yt.YtClient) -> list["DagRun"]:
        try:
            rows = list(yt_client.select_rows(
                f"""
                {get_all_row_fields(cls, "dr")} 
                FROM [{cls.table_path}] AS dr
                WHERE dr.state = "{state}"
                """
            ))
        except Exception as e:
            cls.logger.exception("Failed to select rows: %s", e)
            raise
        cls.logger.info(f"selected rows for state={state}: {rows}")
        return [cls(cls.row_type(**row)) for row in rows]

    @classmethod
    def get_scheduled_dag_runs_to_queue(cls) -> list["DagRun"]:
        return cls.get_dag_runs_by_state(cls.state_type.SCHEDULED)

    @classmethod
    def get_queued_dag_runs_to_set_running(cls) -> list["DagRun"]:
        return cls.get_dag_runs_by_state(cls.state_type.QUEUED)

    @classmethod
    def get_running_dag_runs_to_examine(cls) -> list["DagRun"]:
        return cls.get_dag_runs_by_state(cls.state_type.RUNNING)

    def queue_dag_run(self, dag: DAG):
        try:
            self._create_task_runs(dag)
            self.set_state(DagRun.state_type.QUEUED)
        except Exception as e:
            self.log.exception("Failed to queue dagrun, SKIPPING: %s", e)

    def update_state(
            self, dag: DAG
    ) -> list[TaskRun]:
        try:
            trs, schedulable_trs, unfinished_trs, finished_trs = self.trs_scheduling_decisions(dag)

            # TODO self._trs_for_dagrun_state(dag=dag, trs=trs) # берем только листья таски чтоб судить про failed/running dags
            if not unfinished_trs and any(x.state in [TaskRun.state_type.FAILED] for x in trs):
                self.set_state(DagRun.state_type.FAILED)
            elif not finished_trs and all(x.state in [TaskRun.state_type.SUCCESS] for x in trs):
                self.set_state(DagRun.state_type.SUCCESS)
            else:
                self.set_state(DagRun.state_type.RUNNING)
            return schedulable_trs
        except Exception as e:
            self.log.exception(f"Failed to update state for run={self.run_id}, SKIPPING: %s", e)
            return []

    def trs_scheduling_decisions(self, dag: DAG):
        trs = DagRun.fetch_task_runs(run_id=self.run_id, dag_id=self.dag_id, task_ids=dag.task_ids)

        unfinished_trs = [t for t in trs if t.state in [TaskRun.state_type.RUNNING, TaskRun.state_type.QUEUED, TaskRun.state_type.SCHEDULED]] # TaskRunState.READY,
        finished_trs = [t for t in trs if t.state in [TaskRun.state_type.FAILED, TaskRun.state_type.SUCCESS]]
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
        return [trs, schedulable_trs, unfinished_trs, finished_trs]

    @staticmethod
    def _get_ready_trs(
        dag: DAG,
        schedulable_trs: list[TaskRun],
        finished_trs: list[TaskRun],
    ):
        ready_trs: list[TaskRun] = []
        finished_trs_ids = {ti.task_id for ti in finished_trs}

        for ti in schedulable_trs:
            upstream = set(dag.upstream.get(ti.task_id, []))

            if upstream.issubset(finished_trs_ids):
                ready_trs.append(ti)
        return ready_trs

    @staticmethod
    def schedule_trs(schedulable_trs: list[TaskRun]) -> int:
        try:
            if schedulable_trs:
                return len(
                    TaskRun.update_rows(trs=[tr.update_row(state=TaskRun.state_type.QUEUED) for tr in schedulable_trs])
                )
            return 0
        except Exception as e:
            DagRun.logger.exception("Failed to update rows, skipping: %s", e)
            return 0

    @with_yt_client
    def set_state(self, state: DagRun.state_type, yt_client: yt.YtClient) -> None: # todo
        self.log.info(f"UPDATE state of run_id={self.run_id} FROM {self.state} TO {state}")
        if self.state != state:
            if state in [DagRun.state_type.SCHEDULED, DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                self.scheduled_at = self.scheduled_at or datetime.utcnow().isoformat()
                if state == DagRun.state_type.SCHEDULED:
                    self.queued_at = None
                    self.start_date = None
                elif state in [DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                    self.queued_at = self.queued_at or self.scheduled_at

                    if state == DagRun.state_type.QUEUED:
                        self.start_date = None
                    else:
                        self.start_date = self.queued_at
                self.end_date = None
            elif self.state in [None, DagRun.state_type.SCHEDULED, DagRun.state_type.QUEUED, DagRun.state_type.RUNNING]:
                self.end_date = datetime.utcnow().isoformat()
            self.state = state
            try:
                yt_client.insert_rows(DagRun.table_path, [asdict(self)])
            except Exception as e:
                self.log.exception("Failed update state for run_id=%s: %s", self.run_id, e)
                raise

    @staticmethod
    def fetch_task_runs(
            run_id: str | None = None,
            dag_id: str | None = None,
            task_ids: list[str] | None = None
    )  -> list[TaskRun] :
        conditions: list[str] = []
        if run_id is not None:
            conditions.append(f"tr.run_id = '{run_id}'")
        if dag_id is not None:
            conditions.append(f"tr.dag_id = '{dag_id}'")
        if task_ids is not None and task_ids:
            ids_list = ", ".join(f"'{tid}'" for tid in task_ids)
            conditions.append(f"tr.task_id in ({ids_list})")

        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)
        try:
            rows = _fetch_rows(cls=DagRun, rows=f"""
                {get_all_row_fields(TaskRun, "tr")}
                FROM [{TaskRun.table_path}] AS tr
                {where_clause}
                """
            )
            return [TaskRun(TaskRun.row_type(**row)) for row in rows]
        except Exception:
            raise

    @with_yt_client
    def _create_task_runs(self, dag: DAG, yt_client: yt.YtClient) -> None:
        try:
            existing_task_ids = {t.task_id for t in DagRun.fetch_task_runs(run_id=self.run_id, dag_id=self.dag_id, task_ids=dag.task_ids)}

            tasks_to_create = [task for task in dag.tasks if task.task_id not in existing_task_ids]

            roots_trs_instant_ready = [
                TaskRun(
                    run_id=self.run_id,
                    operator=task,
                    state=TaskRun.state_type.SCHEDULED, #TODO
                )
                for task in tasks_to_create
                if task in dag.roots
            ]
            trs_to_create = [
                TaskRun(
                    run_id=self.run_id,
                    operator=task,
                    state=TaskRun.state_type.SCHEDULED,
                )
                for task in tasks_to_create
                if task not in dag.roots
            ]

            self.log.info(f"roots: {roots_trs_instant_ready}")
            self.log.info(f"other: {trs_to_create}")

            tasks = roots_trs_instant_ready + trs_to_create
            yt_client.insert_rows(TaskRun.table_path, [asdict(task) for task in tasks])
        except Exception as e:
            self.log.exception(f"Failed to create trs for run %s: %s", self.run_id, e)
            raise