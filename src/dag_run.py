from __future__ import annotations

import typing
import uuid
from dataclasses import field, asdict
from datetime import datetime
from typing import Optional

if typing.TYPE_CHECKING:
    from dag import DAG
from state import DagRunState, TaskRunState
from task_run import TaskRun

import yt.wrapper as yt
from yt.wrapper.schema import TableSchema


@yt.yt_dataclass
class DagRun:
    dag_id: str
    run_id: str
    creating_job_id: int

    start_date: Optional[str]
    end_date: Optional[str]
    # updated_at: datetime

    state: str # DagRunState

    def __init__(
            self,
            *,
            dag_id: str,
            run_id: str,
            state: DagRunState,
            dag: DAG | None = None,
            start_date: datetime | str | None = None,
            end_date: datetime | str | None = None,
            creating_job_id: int | None = None,
    ):
        self.dag = dag
        self.dag_id = dag_id
        self.run_id = run_id
        self.start_date = start_date.isoformat() if start_date is not None and isinstance(start_date, datetime) else start_date
        self.end_date = end_date.isoformat() if end_date is not None and isinstance(end_date, datetime) else end_date
        self.state = state
        self.creating_job_id = creating_job_id
        # self.Graph: dict[str, set[str]] = {}


    def dag_run_prepare_for_execution(self, yt_client: yt.YtClient):
        print("dag_run_prepare_for_execution")
        # self.Graph = {
        #     task.task_id: set(task.upstream_task_ids)
        #     for task in self.dag.tasks
        # }
        if not yt_client.exists("//home/dag_run"):
            print("create table DAG_RUN")
            yt_client.create("table", "//home/dag_run", attributes={"schema": TableSchema.from_row_type(DagRun) , "dynamic": True})
            yt_client.mount_table("//home/dag_run", sync=True)
        yt_client.insert_rows("//home/dag_run", [asdict(self)])
        self.create_task_runs_for_dag_run(yt_client) # self.verify_integrity(yt_client)


    @classmethod
    def get_running_dag_runs_to_examine(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        if yt_client.exists("//home/dag_run"): # and yt_client.exists("//home/dag_state"):
            rows = list(yt_client.select_rows(
                f"""
                dr.id as id,
                dr.dag_id as dag_id,
                dr.run_id as run_id,
                dr.state as state,
                dr.start_date as start_date,
                dr.end_date as end_date,
                from [{"//home/dag_run"}] as dr
                where dr.state = "{DagRunState.RUNNING}"
                limit 1
                """
            ))
        else:
            rows = []
        return [cls(**row) for row in rows]

    @classmethod
    def get_queued_dag_runs_to_set_running(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        print("get_queued_dag_runs_to_set_running")
        if yt_client.exists("//home/dag_run") and yt_client.exists("//home/dag_state"):
            try:
                rows = list(yt_client.select_rows(
                    f"""
                    dr.id as id,
                    dr.dag_id as dag_id,
                    dr.run_id as run_id,
                    dr.state as state,
                    dr.start_date as start_date,
                    dr.end_date as end_date
                    from [{"//home/dag_run"}] dr
                    where dr.state = "{DagRunState.QUEUED}"
                    limit 1
                    """
                ))
            except Exception as e:
                print(e)
                raise
        else:
            rows = []
        print(rows)
        try:
            return [cls(**row) for row in rows]
        except Exception as e:
            print(e)
            return []

    def update_state(
            self, yt_client: yt.YtClient
    ) -> list[TaskRun]:
        dag = self.dag
        trs, schedulable_trs, unfinished, finished = self.task_run_scheduling_decisions(yt_client)

        trs_for_dagrun_state = dag.leaves # self._trs_for_dagrun_state(dag=dag, trs=trs) # берем только листья таски чтоб судить про failed/running dags

        if not unfinished and any(x.state in [TaskRunState.FAILED] for x in trs_for_dagrun_state):
            self.set_state(DagRunState.FAILED, yt_client)
        elif not unfinished and all(x.state in [TaskRunState.SUCCESS] for x in trs_for_dagrun_state):
            self.set_state(DagRunState.SUCCESS, yt_client)
        else:
            self.set_state(DagRunState.RUNNING, yt_client)

        return schedulable_trs

    # def _trs_for_dagrun_state(self, *, dag, trs: list[TaskRun]):
    #     def is_leaf(task):
    #         return len(task.downstream_task_ids) == 0
    #
    #
    #     leaf_task_ids = {t.task_id for t in dag.tasks if is_leaf(t)}
    #
    #     leaf_trs = {
    #         ti for ti in trs
    #         if ti.task_id in leaf_task_ids
    #     }
    #     return leaf_trs

    def task_run_scheduling_decisions(self, yt_client: yt.YtClient):
        trs = self.get_task_runs(yt_client)

        schedulable_trs = [t for t in trs if t.state == TaskRunState.SCHEDULED]
        unfinished_trs = [t for t in trs if t.state in [TaskRunState.RUNNING, TaskRunState.QUEUED, TaskRunState.READY]]
        finished_trs = [t for t in trs if t.state in [TaskRunState.FAILED, TaskRunState.SUCCESS]]

        if unfinished_trs:
            schedulable_trs  =  self._get_ready_trs(
                schedulable_trs,
                finished_trs,
            )
        else:
            schedulable_trs = []


        return [trs, schedulable_trs, unfinished_trs, finished_trs]

    def get_task_runs(self, yt_client: yt.YtClient):
        print("get_task_runs")
        try:
            return DagRun.fetch_task_runs(
                dag_id=self.dag_id, run_id=self.run_id, task_ids=self.dag.task_ids, yt_client=yt_client
            )
        except Exception as e:
            print(e)
            raise

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
            upstream = set(self.dag.upstream.get(ti.task_id, []))

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
                        "state": TaskRunState.READY,
                        "scheduled_at": now_iso,
                    })

                if updated:
                    t.insert_rows(
                        "//home/task_run",
                        updated,
                        update=True
                    )
        return len(updated)

    def set_state(self, state: DagRunState, yt_client: yt.YtClient) -> None:
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

        yt_client.insert_rows("//home/dag_run", [asdict(self)], update=True)

    @staticmethod
    def fetch_task_runs(
            yt_client: yt.YtClient,
            dag_id: str | None = None,
            run_id: str | None = None,
            task_ids: list[str] | None = None
    )  -> list[TaskRun] : # TODO
        print("fetch_task_runs")

        table = "//home/task_run"
        if not yt_client.exists(table):
            return []

        conditions: list[str] = []
        if dag_id is not None:
            conditions.append(f"tr.dag_id = '{dag_id}'")
        if run_id is not None:
            conditions.append(f"tr.run_id = '{run_id}'")
        if task_ids is not None and task_ids:
            ids_list = ", ".join(f"'{tid}'" for tid in task_ids)
            print(ids_list)
            conditions.append(f"tr.task_id in ({ids_list})")

        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)
        print(where_clause)

        rows = list(yt_client.select_rows(
            f"""
            tr.id          AS id,
            tr.task_id     AS task_id,
            tr.dag_id      AS dag_id,
            tr.run_id      AS run_id,
            tr.scheduled_at AS scheduled_at,
            tr.start_date  AS start_date,
            tr.end_date    AS end_date,
            tr.state       AS state
            from [{"//home/task_run"}] as tr
            {where_clause}
            """
        ))
        return [TaskRun(**row) for row in rows]


    def create_task_runs_for_dag_run(self, yt_client: yt.YtClient) -> None: #verify_integrity
        # инициализируем taskrun
        print("verify_integrity")
        task_ids = {t.task_id for t in self.get_task_runs(yt_client)}
        tasks_to_create = [task for task in dag.tasks if task.task_id not in task_ids]
        try:
            roots_trs_instant_ready = [TaskRun(task, run_id=self.run_id, dag_run=self, state=TaskRunState.SCHEDULED) for task in tasks_to_create if task.task_id in dag.roots]
            trs_to_create = [TaskRun(task, run_id=self.run_id, dag_run=self, state=TaskRunState.SCHEDULED) for task in tasks_to_create if task.task_id not in dag.roots]
        except Exception as e:
            print(e)
            raise
        DagRun._create_task_runs(tasks=trs_to_create + roots_trs_instant_ready, yt_client=yt_client)


    @staticmethod
    def _create_task_runs(
            tasks: list[TaskRun],
            yt_client: yt.YtClient,
    ) -> None:
        print("_create_task_runs")
        try:
            print(len(tasks))
            if not yt_client.exists("//home/task_run"):
                print("create table")
                yt_client.create("table", "//home/task_run", attributes={"schema": TableSchema.from_row_type(TaskRun) , "dynamic": True})
                yt_client.mount_table("//home/task_run", sync=True)
            yt_client.insert_rows("//home/task_run", [asdict(task) for task in tasks])
        except Exception as e:
            print(e)
            raise
