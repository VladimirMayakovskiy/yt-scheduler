from __future__ import annotations

from dataclasses import field, asdict, KW_ONLY
from datetime import datetime
from types import SimpleNamespace
from typing import Optional, ClassVar

from rows_helpers import make_formatted_select
from base_row import YtRow, TablePath
from state import TaskRunState
from logging_mixin import LoggingMixin
from task import Task
from yt_wrapper import with_yt_client

import yt.wrapper as yt

@yt.yt_dataclass
class TaskRunRow(YtRow):
    table_path:  ClassVar[str] = TablePath("task_run")
    key_columns: ClassVar[list[str]] = ["run_id"]
    alias: ClassVar[str] = "taskrun"

    run_id: str = field(default_factory=lambda: yt.common.generate_uuid())

    _: KW_ONLY

    task_id: str
    dag_id: str
    dag_run_id: str
    state: str

    operation_id: Optional[str] = None

    scheduled_at: Optional[str] = None
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class TaskRun(TaskRunRow, LoggingMixin):
    row_type: ClassVar[type[TaskRunRow]] = TaskRunRow
    state_type: ClassVar[type[TaskRunState]] = TaskRunState

    def __init__(
        self,
        row: TaskRun.row_type | None = None,
        *,
        dag_run_id: str | None = None,
        operator: Task | None = None,
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
                dag_run_id=dag_run_id,
                task_id=operator.task_id,
                dag_id=operator.dag_id,
                state=state,
                scheduled_at=scheduled_at,
                queued_at=queued_at,
                start_date=start_date,
                end_date=end_date,
            )

    @classmethod
    def fetch_rows(
        cls,
        run_id: str | list[str] | tuple[str] = None,
        task_id: str | list[str] | tuple[str] = None,
        dag_id: str | list[str] | tuple[str] = None,
        dag_run_id: str | list[str] | tuple[str] = None,
        state: TaskRun.state_type | list[TaskRun.state_type] | tuple[TaskRun.state_type] = None,
        operation_id: str | list[str] | tuple[str] = None,
        limit: int = None,
    ) -> list[TaskRun]:
        rows = make_formatted_select(
            cls=cls,
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            state=state,
            operation_id=operation_id,
            limit=limit,
        )
        return [cls(cls.row_type(**row)) for row in rows]

    @classmethod
    def get(
        cls,
        run_id: str = None,
        task_id: str = None,
        dag_id: str = None,
        dag_run_id: str = None,
        operation_id: str = None
    ) -> "TaskRun" | None:
        return YtRow.get(cls=cls, run_id=run_id, task_id=task_id, dag_id=dag_id,
                         dag_run_id=dag_run_id, operation_id=operation_id)


    def as_operator(self):
        from dagref import TaskRef
        try:
            ref = TaskRef.get(dag_id=self.dag_id, task_id=self.task_id)
            task: Task = Task.from_serialized_repr(ref=ref)

            return SimpleNamespace(run_operation=lambda: task.run_operation(mutation_id=self.run_id), row=self)
        except yt.YtError as e:
            raise e
        except Exception as e:
            TaskRun.logger.warning(
                "TaskRun %s is not runnable (state=%s, dag_id=%s): %s",
                self.run_id, self.state, self.dag_id, e
            )
            return None

    @classmethod
    @with_yt_client
    def get_executable_task_runs_to_queue(cls) -> list["TaskRun"]:
        return cls.fetch_rows(state=cls.state_type.QUEUED)

    @classmethod
    @with_yt_client
    def get_running_task_runs_to_poll(cls) -> list["TaskRun"]:
        res = cls.fetch_rows(state=cls.state_type.RUNNING)
        return res

    @with_yt_client
    def _update_row(
        self,
        yt_client: yt.YtClient,
        state: TaskRun.state_type | None = None,
        operation_id: str | None = None,
    ) -> TaskRun.row_type:
        def _build_row() -> TaskRun.row_type:
            base = TaskRun.row_type(**asdict(self))
            now = datetime.utcnow().isoformat() # todo
            if state is not None and base.state != state:
                if state in TaskRun.state_type.unfinished_states:
                    base.scheduled_at = base.scheduled_at or now
                    if state == TaskRun.state_type.SCHEDULED:
                        base.queued_at = None
                        base.start_date = None
                    else:
                        base.queued_at = base.queued_at or now
                        if state == TaskRun.state_type.RUNNING:
                            base.start_date = now # TODO get from op
                        else:
                            base.start_date = None
                    base.end_date = None
                elif base.state in TaskRun.state_type.unfinished_states:
                    base.end_date = now
                base.state = state

            if operation_id is not None:
                base.operation_id = operation_id
            return base

        row_obj = _build_row()
        try:
            self.log.info(f"Updating task run id={self.run_id} with state={row_obj.state}")
            yt_client.insert_rows(TaskRun.table_path, [asdict(row_obj)]) # change we have create_rows method
        except Exception as e:
            TaskRun.logger.exception("Failed update state for task run id=%s: %s", self.run_id, e)
            raise

        self.state = row_obj.state
        self.scheduled_at = row_obj.scheduled_at
        self.queued_at = row_obj.queued_at
        self.start_date = row_obj.start_date
        self.end_date = row_obj.end_date
        self.operation_id = row_obj.operation_id
        return self

    @staticmethod
    def update_row(
        row: TaskRun | TaskRun.row_type | str,
        state: TaskRun.state_type | None = None,
        operation_id: str | None = None
    ) -> TaskRun.row_type:
        if isinstance(row, str):
           row = TaskRun.get(run_id=row)
        if isinstance(row, TaskRun.row_type):
            row = TaskRun(row=row)
        return row._update_row(state=state, operation_id=operation_id)