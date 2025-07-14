from __future__ import annotations

import itertools
from dataclasses import field, asdict
from datetime import datetime
from typing import TYPE_CHECKING, Optional, ClassVar

import uuid

if TYPE_CHECKING:
    from dag_run import DagRun


from ytoperator import BaseOperator
from state import TaskRunState

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

    operation_id: Optional[str] = None

    id: str = field(default_factory=lambda: uuid.uuid4().hex)

# @yt.yt_dataclass
class TaskRun(TaskRunRow):
    # table_path:  ClassVar[str] = "//tmp/task_run"
    # key_columns: ClassVar[list[str]] = ["id"]
    # unique_keys: ClassVar[bool] = True
    #
    # id: str
    # task_id: str
    # run_id: str
    # dag_id: str
    #
    # scheduled_at: str
    # queued_at: Optional[str]
    # start_date: Optional[str]
    # end_date: Optional[str]
    # # updated_at: datetime
    #
    # state: str # TaskRunState
    #
    # operation_id: Optional[str] = None

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
            # task: BaseOperator | None = None,
            # dag_run: DagRun | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
        # task_id = task_id if task_id else task.task_id
        # run_id = run_id if run_id else dag_run.run_id
        # dag_id = dag_id if dag_id else dag_run.dag_id
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

        # self.task = task
        # self.dag_id = dag_id if dag_id else (task.dag_id if task else None)
        # self.task_id = task_id if task_id else (task.task_id if task else None)
        # self.dag_run = dag_run
        # self.id = id or uuid.uuid4().hex
        # self.scheduled_at = scheduled_at or datetime.utcnow().isoformat()
        # self.queued_at = queued_at
        # if state:
        #     self.state = state
        # else:
        #     self.state = ""
        #
        # self.start_date = start_date
        # self.end_date = end_date
        #
        # self.operation_id = operation_id
    @classmethod
    def get_executable_task_runs_to_queued(cls, yt_client: yt.YtClient) -> list["TaskRun"]:
        print("TaskRun.get_executable_task_runs_to_queued")
        from dag_run import get_all_table_fields
        try:
            if yt_client.exists(TaskRun.table_path):
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_table_fields(TaskRunRow, "tr")}
                    FROM [{TaskRun.table_path}] AS tr
                    WHERE tr.state = '{TaskRunState.READY}'
                    LIMIT 1
                    """
                ))
            else:
                rows = []
            print("ROWS TO QUEUE: ", rows)

            return [cls(TaskRunRow(**row)) for row in rows]
        except Exception as e:
            print(e)
            raise


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

    # def set_state(self, state: TaskRunState, yt_client: yt.YtClient) -> None:
    #     print("TASKRUN.set_state, cur: ", self.state, "new: ", state)
    #
    #     if self.state != state:
    #         if state == TaskRunState.SCHEDULED:
    #             self.scheduled_at = datetime.utcnow().isoformat()
    #             self.queued_at = None
    #             self.start_date = None
    #             self.end_date = None
    #         elif state == TaskRunState.READY:
    #             self.scheduled_at = self.scheduled_at or datetime.utcnow().isoformat()
    #             self.queued_at = None
    #             self.start_date = None
    #             self.end_date = None
    #         elif state == TaskRunState.QUEUED:
    #             self.scheduled_at = self.scheduled_at or datetime.utcnow().isoformat()
    #             self.queued_at = datetime.utcnow().isoformat()
    #             self.start_date = None
    #             self.end_date = None
    #         elif state == TaskRunState.RUNNING:
    #             self.scheduled_at = self.scheduled_at or datetime.utcnow().isoformat()
    #             self.queued_at = self.queued_at or datetime.utcnow().isoformat()
    #             self.start_date = datetime.utcnow().isoformat()
    #             self.end_date = None
    #         else:
    #             if state not in [TaskRunState.SUCCESS, TaskRunState.FAILED]:
    #                 self.end_date = datetime.utcnow().isoformat()
    #         self.state = state
    #     else:
    #         # TODO
    #         pass
    #
    #     yt_client.insert_rows(TaskRunRow.table_path, [asdict(self)])
    @staticmethod
    def update_rows(
            yt_client: yt.YtClient,
            trs: TaskRunRow | list[TaskRunRow],
            state: TaskRunState | None = None,
            operation_id: str | None = None,
    ) -> int:
        print("TaskRun.update_state")
        if not isinstance(trs, list):
            trs = [trs]

        def _set_state(tr: TaskRunRow):
            if tr.state != state:
                if state == TaskRunState.SCHEDULED:
                    tr.scheduled_at = datetime.utcnow().isoformat()
                    tr.queued_at = None
                    tr.start_date = None
                    tr.end_date = None
                elif state == TaskRunState.READY:
                    tr.scheduled_at = tr.scheduled_at or datetime.utcnow().isoformat()
                    tr.queued_at = None
                    tr.start_date = None
                    tr.end_date = None
                elif state == TaskRunState.QUEUED:
                    tr.scheduled_at = tr.scheduled_at or datetime.utcnow().isoformat()
                    tr.queued_at = datetime.utcnow().isoformat()
                    tr.start_date = None
                    tr.end_date = None
                elif state == TaskRunState.RUNNING:
                    tr.scheduled_at = tr.scheduled_at or datetime.utcnow().isoformat()
                    tr.queued_at = tr.queued_at or datetime.utcnow().isoformat()
                    tr.start_date = datetime.utcnow().isoformat()
                    tr.end_date = None
                else:
                    if tr.state not in [TaskRunState.SUCCESS, TaskRunState.FAILED]:
                        tr.end_date = datetime.utcnow().isoformat()
                tr.state = state
            else:
                # TODO
                pass

        update = []
        for tr in trs:
            if state is not None:
                _set_state(tr)

            if operation_id is not None:
                tr.operation_id = operation_id

            update.append(asdict(tr))
        yt_client.insert_rows(TaskRunRow.table_path, update)
        return len(update)