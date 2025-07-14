from __future__ import annotations

import dataclasses
import typing
import uuid
from dataclasses import field, asdict
from datetime import datetime
from typing import Optional

if typing.TYPE_CHECKING:
    from dag import DAG
from state import DagRunState, TaskRunState
from task_run import TaskRun, TaskRunRow

import yt.wrapper as yt
from yt.wrapper.schema import TableSchema
from typing import ClassVar

def get_all_table_fields(cls: type, alias: str) -> str:
    return ",\n".join(
        f"{alias}.{f.name} AS {f.name}"
        for f in dataclasses.fields(cls) if f.init
    )

@yt.yt_dataclass
class DagRunRow:
    table_path:  ClassVar[str] = "//tmp/dag_run"
    key_columns: ClassVar[list[str]] = ["run_id"]
    unique_keys: ClassVar[bool] = True

    dag_id: str

    state: str # DagRunState

    start_date: str
    end_date: Optional[str] = None

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)

# @yt.yt_dataclass
class DagRun(DagRunRow):
    # table_path:  ClassVar[str] = "//tmp/dag_run"
    # key_columns: ClassVar[list[str]] = ["run_id"]
    # unique_keys: ClassVar[bool] = True
    #
    # run_id: str # TODO field
    # dag_id: str
    # # creating_job_id: str
    #
    # start_date: Optional[str]
    # end_date: Optional[str]
    # # updated_at: datetime
    #
    # state: str # DagRunState

    def __init__(
            self,
            row: DagRunRow | None = None,
            *,
            dag_id: str | None = None,
            state: DagRunState | None = None,
            dag: DAG | None = None,
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

        self.dag = dag
        self.creating_job_id = creating_job_id


    def dag_run_prepare_for_execution(self, yt_client: yt.YtClient):
        print("DAGRUN.dag_run_prepare_for_execution")
        # if not yt_client.exists("//home/dag_run"): todo
        yt_client.insert_rows(DagRun.table_path, [asdict(self)])
        self.create_task_runs_for_dag_run(yt_client)


    @classmethod
    def get_running_dag_runs_to_examine(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        print("DAGRUN.get_running_dag_runs_to_examine")
        try:
            if yt_client.exists(DagRun.table_path): # and yt_client.exists("//home/dag_state"):
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_table_fields(DagRunRow, "dr")} 
                    FROM [{DagRun.table_path}] AS dr
                    WHERE dr.state = "{DagRunState.RUNNING}"
                    """
                ))
            else:
                rows = []
            print("DAGRUNS TO EXAMINE: ", rows)
            return [cls(DagRunRow(**row)) for row in rows]
        except Exception as e:
            print(e)
            return []

    @classmethod
    def get_queued_dag_runs_to_set_running(cls, yt_client: yt.YtClient) -> list["DagRun"]:
        print("DAGRUN.get_queued_dag_runs_to_set_running")
        if yt_client.exists(DagRun.table_path):
            try:
                rows = list(yt_client.select_rows(
                    f"""
                    {get_all_table_fields(DagRunRow, "dr")} 
                    FROM [{DagRun.table_path}] AS dr
                    WHERE dr.state = "{DagRunState.QUEUED}"
                    """
                ))
            except Exception as e:
                print(e)
                raise
        else:
            rows = []
        print("DAGRUNS TO SET RUNNING: ", rows)
        try:
            return [cls(DagRunRow(**row)) for row in rows]
        except Exception as e:
            print("HERE", e)
            return []

    def update_state(
            self, yt_client: yt.YtClient
    ) -> list[TaskRun]:
        print("DAGRUN.update_state")
        # dag = self.dag
        trs, schedulable_trs, unfinished, finished = self.task_run_scheduling_decisions(yt_client)

        print("back in DAGRUN.update_state")

        try:
            trs_for_dagrun_state = trs # TODO self._trs_for_dagrun_state(dag=dag, trs=trs) # берем только листья таски чтоб судить про failed/running dags

            if not unfinished and any(x.state in [TaskRunState.FAILED] for x in trs_for_dagrun_state):
                self.set_state(DagRunState.FAILED, yt_client)
            elif not unfinished and all(x.state in [TaskRunState.SUCCESS] for x in trs_for_dagrun_state):
                self.set_state(DagRunState.SUCCESS, yt_client)
            else:
                print("here")
                self.set_state(DagRunState.RUNNING, yt_client)

            return schedulable_trs
        except Exception as e:
            print(e)
            raise

    def task_run_scheduling_decisions(self, yt_client: yt.YtClient):
        print("DAGRUN.task_run_scheduling_decisions")
        try:
            trs = self.get_task_runs(yt_client)
            print("trs", trs)

            unfinished_trs = [t for t in trs if t.state in
                              [TaskRunState.RUNNING, TaskRunState.QUEUED, TaskRunState.READY, TaskRunState.SCHEDULED]]
            schedulable_trs = [t for t in trs if t.state == TaskRunState.SCHEDULED]
            finished_trs = [t for t in trs if t.state in [TaskRunState.FAILED, TaskRunState.SUCCESS]]
        except Exception as e:
            print(e)
            raise

        if schedulable_trs:
            schedulable_trs  =  self._get_ready_trs(
                schedulable_trs,
                finished_trs,
            )
        else:
            schedulable_trs = []

        print("schedulable_trs: ", schedulable_trs)
        print("unfinished_trs: ", unfinished_trs)
        print("finished_trs: ", finished_trs)
        return [trs, schedulable_trs, unfinished_trs, finished_trs]

    def get_task_runs(self, yt_client: yt.YtClient):
        print("DAGRUN.get_task_runs")
        try:
            return DagRun.fetch_task_runs(
                run_id=self.run_id, dag_id=self.dag_id, task_ids=self.dag.task_ids, yt_client=yt_client
            )
        except Exception as e:
            print(e)
            raise

    def _get_ready_trs(
            self,
            schedulable_trs: list[TaskRun],
            finished_trs: list[TaskRun]
    ):
        print("DAGRUN._get_ready_trs")
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
        print("DAGRUN.schedule_trs")
        updated = []
        try:
            if schedulable_trs:
                print(schedulable_trs)
                # now_iso = datetime.utcnow().isoformat()
                # for tr in schedulable_trs: # TODO написать метод set_state
                #
                #     updated.append({
                #         "id": tr.id,
                #         "task_id": tr.task_id,
                #         "dag_id": tr.dag_id,
                #         "run_id": tr.run_id,
                #         "operation_id": tr.operation_id,
                #         "state": TaskRunState.READY,
                #         "scheduled_at": now_iso,
                #     })
                #
                # if updated:
                #     yt_client.insert_rows(
                #         TaskRun.table_path,
                #         updated,
                #     )
                return TaskRun.update_rows(yt_client, schedulable_trs, state=TaskRunState.READY)

            return 0
        except Exception as e:
            print(e)
            raise

    def set_state(self, state: DagRunState, yt_client: yt.YtClient) -> None: # todo
        print("DAGRUN.set_state, current: ", self.state, "new: ", state)
        if self.state != state:
            if state == DagRunState.QUEUED:
                self.queued_at = datetime.utcnow()
                self.start_date = None
                self.end_date = None
            if state == DagRunState.RUNNING:
                if self.state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.start_date = datetime.utcnow().isoformat()
                else:
                    self.start_date = self.start_date or datetime.utcnow()
                self.end_date = None
            if self.state in [DagRunState.RUNNING, DagRunState.QUEUED] or self.state is None:
                if state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.end_date = datetime.utcnow().isoformat()
            self.state = state
        else:
            if state == DagRunState.QUEUED: # TODO updated
                self.queued_at = datetime.utcnow().isoformat()

        try:
            yt_client.insert_rows(DagRun.table_path, [asdict(self)])
        except Exception as e:
            print(e)
            pass

    @staticmethod
    def fetch_task_runs(
            yt_client: yt.YtClient,
            run_id: str | None = None,
            dag_id: str | None = None,
            task_ids: list[str] | None = None
    )  -> list[TaskRun] : # TODO
        print("DAGRUN.fetch_task_runs")
        if not yt_client.exists(TaskRun.table_path):
            return []

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
        print(where_clause)
        try:
            rows = list(yt_client.select_rows(
                f"""
                {get_all_table_fields(TaskRunRow, "tr")}
                FROM [{TaskRun.table_path}] AS tr
                {where_clause}
                """
            ))
            print("TASKRUNS: ", rows)
            return [TaskRun(TaskRunRow(**row)) for row in rows]
        except Exception as e:
            print(e)
            return []


    def create_task_runs_for_dag_run(self, yt_client: yt.YtClient) -> None: #verify_integrity
        # инициализируем taskrun
        print("DAGRUN.create_task_runs_for_dag_run")
        try:
            existing_task_ids = {t.task_id for t in self.get_task_runs(yt_client)}
            print(existing_task_ids)

            tasks_to_create = [task for task in self.dag.tasks if task.task_id not in existing_task_ids]
            print("TASK TO CREATE ", {t.task_id for t in tasks_to_create})

            # TODO mb мы не должны сразу создавать все таскраны
            roots_trs_instant_ready = [
                TaskRun(
                    task_id=task.task_id,
                    run_id=self.run_id,
                    dag_id=self.dag_id,
                    state=TaskRunState.SCHEDULED,
                )
                for task in tasks_to_create
                if task in self.dag.roots
            ]
            trs_to_create = [
                TaskRun(
                    task_id=task.task_id,
                    run_id=self.run_id,
                    dag_id=self.dag_id,
                    state=TaskRunState.SCHEDULED,
                )
                for task in tasks_to_create
                if task not in self.dag.roots
            ]

            print("ROOTS:", roots_trs_instant_ready)
            print("OTHER: ", trs_to_create)
            DagRun._create_task_runs(tasks=trs_to_create + roots_trs_instant_ready, yt_client=yt_client)
        except Exception as e:
            print(e)
            raise


    @staticmethod
    def _create_task_runs(
            tasks: list[TaskRunRow],
            yt_client: yt.YtClient,
    ) -> None:
        print("DAGRUN._create_task_runs")
        try:
            print(len(tasks), asdict(tasks[0]))
            # if not yt_client.exists("//home/task_run"): todo
            print([asdict(task) for task in tasks])
            yt_client.insert_rows(TaskRunRow.table_path, [asdict(task) for task in tasks])
            print("HERE")
        except Exception as e:
            print(e)
            raise
