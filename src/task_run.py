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
    # task: BaseOperator | None = None

    # executor: str

    def __init__(
            self,
            task: BaseOperator,
            dag_run: DagRun,
            run_id: str | None = None,
            state: TaskRunState | None = None,
    ):
        self.task = task
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.dag_run = dag_run

        self.run_id = run_id


        self.id = str(uuid.uuid4())

        self.scheduled_at = datetime.utcnow().isoformat()

        if state:
            self.state = state
        else:
            self.state = ""

        self.start_date = None
        self.end_date = None

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
        # self.refresh_from_yt() # TODO
        if self.state == state:
            return False

        current_time = datetime.utcnow()
        self.state = state
        self.start_date = self.start_date or current_time
        if self.state in [TaskRunState.FAILED, TaskRunState.SUCCESS]:
            self.end_date = self.end_date or current_time

        with yt_client.Transaction():
            yt_client.insert_rows("//home/task_run", self)

        return True

    # @property
    # def key(self) -> TaskRunKey:
    #     return TaskRunKey(self.id, self.dag_id, self.task_id, self.run_id)