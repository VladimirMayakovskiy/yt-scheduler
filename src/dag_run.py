from __future__ import annotations

import uuid
from dataclasses import field
from datetime import datetime
from typing import Optional

# from src.dag import DAG
from state import DagRunState, TaskRunState
from task_run import TaskRun

import yt.wrapper as yt


@yt.yt_dataclass
class DagRun:
    dag_id: str
    run_id: str

    queued_at: Optional[datetime]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    updated_at: datetime

    state: str # DagRunState

    id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def __init__(
            self,
            *,
            dag,
            dag_id: str,
            run_id: str,
            queued_at: datetime | None = None,
            start_date: datetime | None = None,
            state: DagRunState,
            creating_job_id: int | None = None,
    ):
        self.dag = dag # TODO
        self.dag_id = dag_id
        self.run_id = run_id
        self.start_date = start_date
        self.state = state
        if queued_at is not None:
            self.queued_at = datetime.utcnow() if state == DagRunState.QUEUED else None
        else:
            self.queued_at = queued_at
        self.creating_job_id = creating_job_id

        self.Graph: dict[str, set[str]] = {
            task.task_id: set(task.upstream_task_ids)
            for task in self.dag.tasks
        }

    @classmethod
    def get_running_dag_runs_to_examine(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        with yt_client.Transaction():
            rows = list(yt_client.select_rows(
                f"""
                dr.id as id,
                dr.dag_id as dag_id,
                dr.run_id as run_id,
                dr.state as state,
                dr.updated_at as updated_at,
                dr.start_date as start_date,
                dr.end_date as end_date,
                dr.queued_at as queued_at,
                from [{"//home/dag_run"}] as dr
                join [{"//home/dag_state"}] as ds
                    on ds.dag_id = dr.dag_id
                    and ds.is_paused = false
                where dr.state = "RUNNING"
                limit 50
                """
            ))
        return [cls(**row) for row in rows]

    @classmethod
    def get_queued_dag_runs_to_set_running(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        with yt_client.Transaction(type="tablet"):
            rows = list(yt_client.select_rows(
                f"""
                dr.id as id,
                dr.dag_id as dag_id,
                dr.run_id as run_id,
                dr.state as state,
                dr.updated_at as updated_at,
                dr.start_date as start_date,
                dr.end_date as end_date,
                dr.queued_at as queued_at,
                from [{"//home/dag_run"}] as dr
                join [{"//home/dag_state"}] as ds
                    on ds.dag_id = dr.dag_id
                    and ds.is_paused = false
                where dr.state = "QUEUED"
                limit 50
                """
            ))
        return [cls(**row) for row in rows]

    def update_state(
            self, yt_client: yt.YtClient
    ) -> list[TaskRun]:
        dag = self.dag
        trs, schedulable_trs, unfinished, finished = self.task_run_scheduling_decisions(yt_client)

        trs_for_dagrun_state = self._trs_for_dagrun_state(dag=dag, trs=trs) # берем только листья таски чтоб судить про failed/running dags

        if not unfinished and any(x.state in [TaskRunState.FAILED] for x in trs_for_dagrun_state):
            self.set_state(DagRunState.FAILED)
        elif not unfinished and all(x.state in [TaskRunState.SUCCESS] for x in trs_for_dagrun_state):
            self.set_state(DagRunState.SUCCESS)
        else:
            self.set_state(DagRunState.RUNNING)

        return schedulable_trs

    def _trs_for_dagrun_state(self, *, dag, trs: list[TaskRun]):
        def is_leaf(task):
            return len(task.downstream_task_ids) == 0


        leaf_task_ids = {t.task_id for t in dag.tasks if is_leaf(t)}

        leaf_trs = {
            ti for ti in trs
            if ti.task_id in leaf_task_ids
        }
        return leaf_trs

    def task_run_scheduling_decisions(self, yt_client: yt.YtClient):
        trs = self.get_task_runs(yt_client)

        unfinished_trs = [t for t in trs if t.state in [TaskRunState.RUNNING, TaskRunState.QUEUED]]
        finished_trs = [t for t in trs if t.state in [TaskRunState.FAILED, TaskRunState.SUCCESS]]

        if unfinished_trs:
            schedulable_trs = [ut for ut in unfinished_trs if ut.state in [TaskRunState.SCHEDULED]]
            schedulable_trs  =  self._get_ready_trs(
                schedulable_trs,
                finished_trs,
            )
        else:
            schedulable_trs = []


        return [trs, schedulable_trs, unfinished_trs, finished_trs]

    def get_task_runs(self, yt_client: yt.YtClient):
        return DagRun.fetch_task_runs(
            dag_id=self.dag_id, run_id=self.run_id, task_ids=self.dag.task_ids, yt_client=yt_client
        )

    def _get_ready_trs(
            self,
            schedulable_trs: list[TaskRun],
            finished_trs: list[TaskRun]
    ):
        # отбирает из переданного списка те таскраны, у которых все зависимости выполнены
        ready_trs: list[TaskRun] = []

        if not schedulable_trs:
            return ready_trs


        finished_trs_ids = {ti.task_id for ti in finished_trs}

        for ti in schedulable_trs:
            upstream = self.Graph.get(ti.task_id, set())

            if upstream.issubset(finished_trs_ids):
                ready_trs.append(ti)

        return ready_trs

    @staticmethod
    def schedule_trs(
            schedulable_trs: list[TaskRun],
            yt_client: yt.YtClient,
    ) -> int:
        updated = []
        if schedulable_trs:
            now_iso = datetime.utcnow().isoformat()
            with yt_client.Transaction() as t:
                rows = t.lookup_rows(
                    "//home/task_run",
                    [{"id": i} for i in schedulable_trs]
                )

                for row in rows:
                    updated.append({
                        "id": row["id"],
                        "state": TaskRunState.SCHEDULED,
                        "scheduled_at": now_iso,
                    })

                if updated:
                    t.insert_rows(
                        "//home/task_run",
                        updated,
                        update=True
                    )
        return len(updated)

    def set_state(self, state: DagRunState) -> None:
        if self.state != state:
            if state == DagRunState.QUEUED:
                self.queued_at = datetime.utcnow()
                self.start_date = None
                self.end_date = None
            if state == DagRunState.RUNNING:
                if self.state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.start_date = datetime.utcnow()
                else:
                    self.start_date = self.start_date or datetime.utcnow()
                self.end_date = None
            if self.state in [DagRunState.RUNNING, DagRunState.QUEUED] or self.state is None:
                if state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.end_date = datetime.utcnow()
            self.state = state
        else:
            if state == DagRunState.QUEUED:
                self.queued_at = datetime.utcnow()

    @staticmethod
    def fetch_task_runs(
            self,
            yt_client: yt.YtClient,
            dag_id: str | None = None,
            run_id: str | None = None,
            task_ids: list[str] | None = None
    )  -> list[TaskRun] :
        cond = []
        if dag_id is not None:
            cond.append(f"ti.dag_id = '{dag_id}'")
        if run_id is not None:
            cond.append(f"ti.run_id = '{run_id}'")
        if task_ids is not None and task_ids:
            ids_list = ", ".join(f"'{tid}'" for tid in task_ids)
            cond.append(f"ti.task_id in ({ids_list})")


        where_clause = ""
        if cond:
            where_clause = "where " + " and ".join(cond)
        rows = list(yt_client.select_rows(
            f"""
            select tr.*
            from [{"//home/task_run"}] as tr
            {where_clause}
            """
        ))

        return [TaskRun(**row) for row in rows]


    def verify_integrity(self, yt_client: yt.YtClient) -> None:
        dag = self.dag
        task_ids = {t.task_id for t in self.get_task_runs(yt_client)}
        tasks_to_create = [task for task in dag.task_dict.values() if task.task_id not in task_ids]
        trs_to_create = [TaskRun(task, run_id=self.run_id) for task in tasks_to_create]
        self._create_task_runs(self.dag_id, trs_to_create, yt_client=yt_client)


    def _create_task_runs(
            self,
            dag_id: str,
            tasks: list[TaskRun],
            yt_client: yt.YtClient,
    ) -> None:
        with yt_client.Transaction():
            yt_client.insert_rows("//home/task_run", tasks)
