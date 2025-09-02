from __future__ import annotations

import uuid
from dataclasses import field, asdict
from datetime import datetime
from typing import TYPE_CHECKING, Optional, ClassVar, Any, Callable

if TYPE_CHECKING:
    from dag import DAG

from base import BaseRow, get_all_row_fields
from state import TaskRunState
from logging_mixin import LoggingMixin
from yt_operator import Operator
from yt_wrapper import with_yt_client
import yt.wrapper as yt

@yt.yt_dataclass
class TaskRunRow(BaseRow):
    table_path:  ClassVar[str] = "//tmp/task_run"
    key_columns: ClassVar[list[str]] = ["id"]

    task_id: str
    run_id: str
    dag_id: str

    state: str # TaskRunState

    scheduled_at: Optional[str] = None
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    operation_id: Optional[str] = None

    id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def make_runnable(self, dag_loader: Callable[[str], "DAG"]) -> Optional[TaskRunnable]:
        try:
            dag = dag_loader(self.dag_id)
        except Exception as e:
            return None
        operator: Operator = dag.task_dict[self.task_id]
        return TaskRunnable(task_run=self, execute_callable=operator.run_operation)


class TaskRun(TaskRunRow, LoggingMixin):
    row_type: ClassVar[type[TaskRunRow]] = TaskRunRow
    state_type: ClassVar[type[TaskRunState]] = TaskRunState

    def __init__(
        self,
        row: TaskRun.row_type | None = None,
        *,
        run_id: str | None = None,
        operator: Operator | None = None,
        state: TaskRun.state_type | None = None,
        queued_at: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        scheduled_at: str | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
            state = state if state else self.state_type.SCHEDULED
            scheduled_at = scheduled_at or datetime.utcnow().isoformat()

            super().__init__(
                run_id=run_id,
                task_id=operator.task_id,
                dag_id=operator.dag_id,
                state=state,
                scheduled_at=scheduled_at,
                queued_at=queued_at,
                start_date=start_date,
                end_date=end_date,
            )

    @classmethod
    @with_yt_client
    def get_executable_task_runs_to_queue(cls, yt_client: yt.YtClient) -> list["TaskRun"]:
        try:
            if yt_client.exists(TaskRun.table_path):
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_row_fields(cls, "tr")}
                    FROM [{cls.table_path}] AS tr
                    WHERE tr.state = '{cls.state_type.QUEUED}'
                    LIMIT 1
                    """
                ))
            else: # todo limit
                rows = []
            cls.logger.info(f"READY TASKRUNS TO QUEUE: {rows}")
            return [cls(cls.row_type(**row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select_rows SKIPPING:")
            return []

    @classmethod
    @with_yt_client
    def get_running_task_runs_to_poll(cls, yt_client: yt.YtClient) -> list["TaskRun"]:
        try:
            if yt_client.exists(TaskRun.table_path):
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_row_fields(cls, "tr")}
                    FROM [{cls.table_path}] AS tr
                    WHERE tr.state = '{cls.state_type.RUNNING}'
                    """
                ))
            else:
                rows = []
            cls.logger.info(f"READY TASKRUNS TO QUEUE: {rows}")
            return [cls(cls.row_type(**row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select_rows SKIPPING:")
            return []

    @with_yt_client
    def update_row(self, state: TaskRun.state_type | None = None, operation_id: str | None = None) -> TaskRun.row_type:
        if state is not None:
            if self.state != state:
                if state in [self.state_type.SCHEDULED, self.state_type.QUEUED, self.state_type.RUNNING]:
                    if self.state in [self.state_type.SUCCESS, self.state_type.FAILED]:
                        self.scheduled_at, self.queued_at, self.start_date, self.end_date = None, None, None, None

                    if state == self.state_type.RUNNING:
                        self.start_date = datetime.utcnow().isoformat() # TODO get from op

                    if self.state in [self.state_type.RUNNING, self.state_type.QUEUED]:
                        self.queued_at = self.queued_at or datetime.utcnow().isoformat()

                    self.scheduled_at = self.scheduled_at or datetime.utcnow().isoformat()
                    self.end_date = None
                elif state in [self.state_type.SUCCESS, self.state_type.FAILED] and self.state in [self.state_type.SCHEDULED, self.state_type.QUEUED, self.state_type.RUNNING]:
                    self.end_date = datetime.utcnow().isoformat()

                self.state = state
        if operation_id is not None:
            self.operation_id = operation_id
        return self

    @staticmethod
    @with_yt_client
    def update_rows(
            trs : TaskRun.row_type | list[TaskRun.row_type],
            yt_client: yt.YtClient,
    ) -> list[TaskRun.row_type]:
        if not isinstance(trs, list):
            trs = [trs]
        yt_client.insert_rows(TaskRun.table_path, [asdict(row) for row in trs])
        return trs

class TaskRunnable:
    def __init__(self, task_run: TaskRun.row_type, execute_callable: Callable[[], Any]):
        self._task_run = task_run
        self._execute_callable = execute_callable

    def __call__(self):
        return self._execute_callable()

    def __getattr__(self, attr):
        return getattr(self._task_run, attr)

    @property
    def row(self):
        return self._task_run