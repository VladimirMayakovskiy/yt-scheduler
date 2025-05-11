from __future__ import annotations

import itertools
from datetime import datetime

import uuid

from dagrun import DagRun
from operator import BaseOperator
from state import TaskRunState

import yt.wrapper as yt


@yt.yt_dataclass
class TaskRun:
    id: str
    task_id: str
    dag_id: str
    run_id: str

    start_date: datetime
    end_date: datetime
    updated_at: datetime

    state: TaskRunState

    task: BaseOperator | None = None

    executor: str

    def __init__(
            self,
            task: BaseOperator,
            dag_run: DagRun,
            run_id: str | None = None,
            state: TaskRunState | None = None,
    ):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.dag_run = dag_run

        self.run_id = run_id

        if not self.id:
            self.id = str(uuid.uuid4())

        if state:
            self.state = state

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
        self.refresh_from_yt() # TODO
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
