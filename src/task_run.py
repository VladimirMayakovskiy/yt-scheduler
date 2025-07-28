from __future__ import annotations

import itertools
from dataclasses import field, asdict
from datetime import datetime
from typing import TYPE_CHECKING, Optional, ClassVar

import uuid

if TYPE_CHECKING:
    from dag_run import DagRun


from state import TaskRunState
from logging_mixin import LoggingMixin

import yt.wrapper as yt
@yt.yt_dataclass
class TaskRunRow:
    table_path:  ClassVar[str] = "//tmp/task_run"
    key_columns: ClassVar[list[str]] = ["id"]
    unique_keys: ClassVar[bool] = True

    task_id: str
    run_id: str
    dag_id: str

    state: str # TaskRunState

    scheduled_at: str
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    #update_at: Optional[str] = None

    operation_id: Optional[str] = None

    id: str = field(default_factory=lambda: uuid.uuid4().hex)

# @yt.yt_dataclass
class TaskRun(TaskRunRow, LoggingMixin):

    def __init__(
            self,
            row: TaskRunRow | None = None,
            *,
            task_id : str | None = None,
            run_id: str | None = None,
            dag_id: str | None = None,
            state: TaskRunState | None = None,
            scheduled_at: str | None = None,
            queued_at: str | None = None,
            start_date: str | None = None,
            end_date: str | None = None,
            operation_id: str | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
            state = state if state else TaskRunState.SCHEDULED

            scheduled_at = scheduled_at or datetime.utcnow().isoformat()

            super().__init__(
                task_id=task_id,
                run_id=run_id,
                dag_id=dag_id,
                state=state,
                scheduled_at=scheduled_at,
                queued_at=queued_at,
                start_date=start_date,
                end_date=end_date,
                operation_id=operation_id,
            )

    @classmethod
    def get_executable_task_runs_to_queued(cls, yt_client: yt.YtClient) -> list["TaskRun"]:
        from dag_run import get_all_table_fields
        try:
            if yt_client.exists(TaskRun.table_path):
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_table_fields(TaskRunRow, "tr")}
                    FROM [{TaskRun.table_path}] AS tr
                    WHERE tr.state = '{TaskRunState.QUEUED}'
                    LIMIT 1
                    """
                ))
            else:
                rows = []
            cls.logger.info(f"READY TASKRUNS TO QUEUE: {rows}")
            return [cls(TaskRunRow(**row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select_rows SKIPPING:")
            return []


    def to_row(self) -> dict: #todo rename to serialize
        return {
            "id":           self.id,
            "task_id":      self.task_id,
            "dag_id":       self.dag_id,
            "run_id":       self.run_id,
            "scheduled_at": self.scheduled_at,
            "queued_at": self.queued_at,
            "start_date":   self.start_date,
            "end_date":     self.end_date,
            "state":        self.state,
            "operation_id": self.operation_id,
        }

    @staticmethod
    def filter_for_trs(trs):
        if not trs:
            return ""

        first = trs[0]

        dag_id = first.dag_id
        run_id = first.run_id
        first_task_id = first.task_id

        dag_ids, run_ids, task_ids = set(), set(), set()
        for t in trs:
            dag_ids.add(t.dag_id)
            run_ids.add(t.run_id)
            task_ids.add(t.task_id)

        if len(dag_ids) == 1 and len(run_ids) == 1:
            ids = ", ".join(f"'{t}" for t in task_ids)
            return (
                f"ti.dag_id = '{dag_id}' and "
                f"ti.run_id = '{run_id}' and "
                f"ti.task_id in ({ids})"
            )
        if len(dag_ids) == 1 and len(task_ids) == 1:
            ids = ", ".join(f"'{t}" for t in run_ids)
            return (
                f"ti.dag_id = '{dag_id}' and "
                f"ti.task_id = '{first_task_id}' and "
                f"ti.run_id in ({ids})"
            )

        filter_condition = []

        for dag, run in itertools.product(dag_ids, run_ids):
            tids = {t.task_id for t in trs if t.dag_id == dag and t.run_id == run}
            ids = ", ".join(f"'{tid}'" for tid in tids)
            filter_condition.append(
                f"(ti.dag_id = '{dag}' and ti.run_id = '{run}' and ti.task_id in ({ids}))"
            )

        return " or ".join(filter_condition)

    class TaskRunUpdateRow:
        def __init__(self, tr: TaskRunRow, state: TaskRunState | None = None, operation_id: str | None = None):
            self.tr = tr
            if state:
                self.state = state
            else:
                self.state = tr.state
            if operation_id:
                self.operation_id = operation_id
            else:
                self.operation_id = tr.operation_id

    @staticmethod
    def update_rows(
            yt_client: yt.YtClient,
            trus : TaskRunUpdateRow | list[TaskRunUpdateRow],
    ) -> list[TaskRunRow]:
        if not isinstance(trus, list):
            trus = [trus]

        def _set_state(tru: TaskRun.TaskRunUpdateRow):
            if tru.tr.state != tru.state:
                if tru.state in [TaskRunState.SCHEDULED, TaskRunState.QUEUED, TaskRunState.RUNNING]:
                    if tru.tr.state in [TaskRunState.SUCCESS, TaskRunState.FAILED]:
                        tru.tr.scheduled_at, tru.tr.queued_at, tru.tr.start_date, tru.tr.end_date = None, None, None, None

                    if tru.state == TaskRunState.RUNNING:
                        tru.tr.start_date = datetime.utcnow().isoformat() # TODO get from op

                    if tru.tr.state in [TaskRunState.RUNNING, TaskRunState.QUEUED]:
                        tru.tr.queued_at = tru.tr.queued_at or datetime.utcnow().isoformat()

                    tru.tr.scheduled_at = tru.tr.scheduled_at or datetime.utcnow().isoformat()
                    tru.tr.end_date = None
                elif tru.state in [TaskRunState.SUCCESS, TaskRunState.FAILED] and tru.tr.state in [TaskRunState.SCHEDULED, TaskRunState.QUEUED, TaskRunState.RUNNING]:
                    tru.tr.end_date = datetime.utcnow().isoformat()

                tru.tr.state = tru.state
            else:
                # TODO
                pass

        update = []
        for tru in trus:
            TaskRun.logger.info(f"UPDATE taskrun={tru.tr.task_id},{tru.tr.id} row: "
                                   f"{'state FROM ' + tru.tr.state + ' TO ' + tru.state if tru.state else ''} "
                                   f"{'operation_id FROM ' + (tru.tr.operation_id or 'None') + ' TO ' + tru.operation_id if tru.operation_id else ''}")
            if tru.state is not None:
                _set_state(tru)

            if tru.operation_id is not None:
                tru.tr.operation_id = tru.operation_id

            update.append(tru.tr)
        yt_client.insert_rows(TaskRunRow.table_path, [asdict(row) for row in update])
        return update