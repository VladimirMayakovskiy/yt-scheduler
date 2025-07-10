from __future__ import annotations

import itertools
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import uuid

if TYPE_CHECKING:
    from dag_run import DagRun


from ytoperator import BaseOperator
from state import TaskRunState

import yt.wrapper as yt

class TaskRunKey:
    id: str
    task_id: str
    dag_id: str
    run_id: str

@yt.yt_dataclass
class TaskRun:
    id: str
    task_id: str
    dag_id: str
    run_id: str

    scheduled_at: str
    start_date: Optional[str]
    end_date: Optional[str]
    # updated_at: datetime

    state: str # TaskRunState

    operation_id: str
    # operation_id: Optional[str] = None
    # task: BaseOperator | None = None

    # executor: str

    def __init__(
            self,
            id: str | None = None,
            task_id : str | None = None,
            dag_id: str | None = None,
            run_id: str | None = None,
            scheduled_at: str | None = None,
            start_date: str | None = None,
            end_date: str | None = None,
            state: TaskRunState | None = None,
            operation_id: str | None = None,
            task: BaseOperator | None = None,
            dag_run: DagRun | None = None,
    ):
        self.task = task
        self.dag_id = dag_id if dag_id else (task.dag_id if task else None)
        self.task_id = task_id if task_id else (task.task_id if task else None)
        # self.dag_run = dag_run

        self.run_id = run_id


        self.id = id or str(uuid.uuid4())

        self.scheduled_at = scheduled_at or datetime.utcnow().isoformat()

        if state:
            self.state = state
        else:
            self.state = ""

        self.start_date = start_date
        self.end_date = end_date

        self.operation_id = operation_id

    def to_row(self) -> dict:
        return {
            "id":           self.id,
            "task_id":      self.task_id,
            "dag_id":       self.dag_id,
            "run_id":       self.run_id,
            "scheduled_at": self.scheduled_at,
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

    def set_state(self, state: TaskRunState | None, yt_client: yt.YtClient) -> bool:
        print("TASKRUN.set_state, cur: ", self.state, "new: ", state)
        # self.refresh_from_yt() # TODO
        if self.state == state:
            return False

        current_time = datetime.utcnow().isoformat()
        self.state = state
        self.start_date = self.start_date or current_time
        if self.state in [TaskRunState.FAILED, TaskRunState.SUCCESS]:
            self.end_date = self.end_date or current_time

        # with yt_client.Transaction():
        yt_client.insert_rows("//home/task_run", [self.to_row()])

        return True

    # @property
    # def key(self) -> TaskRunKey:
    #     return TaskRunKey(self.id, self.dag_id, self.task_id, self.run_id)