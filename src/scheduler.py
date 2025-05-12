from __future__ import annotations

import time
from datetime import datetime
from typing import Callable
import yt.wrapper as yt

from dag import DAG
from dag_run import DagRun
from job import Job
from state import JobState, DagRunState, TaskRunState
from dag_entity import DagEntity
from task_run import TaskRun
from executor import Executor


def run_job(
        job: Job, execute_callable: Callable[[], int | None], yt_client: yt.YtClient
) -> int | None:
    job.prepare_for_execution(yt_client=yt_client)
    try:
        ret = None
        try:
            ret = execute_callable()
            job.state = JobState.SUCCESS
        except SystemExit:
            job.state = JobState.SUCCESS
        except Exception:
            job.state = JobState.FAILED
            raise
        return ret
    finally:
        job.complete_execution(yt_client=yt_client)

def _get_current_dag(dag_id: str, yt_client: yt.YtClient) -> DAG | None:
    de = DagEntity.get(dag_id=dag_id, yt_client=yt_client)
    # создаем dag
    if not de:
        return None
    return DAG.from_dag_entity(de)

class Scheduler:
    def __init__(
            self,
            job : Job,
            yt_client: yt.YtClient,
            scheduler_idle_sleep_time: float = 2,
    ):
        self.job = job
        self.yt_client = yt_client
        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time

    def _execute(self) -> int | None:
        print("Starting the scheduler")
        try:
            self.job.executor.job_id = self.job.id
            self.job.executor.start()

            self._run_scheduler_loop()
        except Exception:
            raise
        finally:
            self.job.executor.end()
            print("Exited execute loop")
            return None

    def _run_scheduler_loop(self):
        print("run_scheduler_loop")
        while True:
            num_queued = self._do_scheduling()

            self.job.executor.heartbeat() # обработка задач

            if not num_queued:
                time.sleep(self._scheduler_idle_sleep_time)

    def _do_scheduling(self) -> int:
        print("_do_scheduling")
        self._create_dagruns_for_dags() # создает даграны

        self._start_queued_dagruns() # запуск и проверка существующих дагран

        dag_runs = DagRun.get_running_dag_runs_to_examine(yt_client=self.yt_client)

        self._schedule_all_dag_runs(dag_runs)

        # total_free_executor_slots = self.job.executor.slots_available # TODO
        # if total_free_executor_slots <= 0:
        #     num_queued = 0
        # else:
        num_queued = self._critical_section_enqueue_task_runs() # переводим задачи в QUEUED; отправляем в executor

        return num_queued

    def _create_dagruns_for_dags(self):
        print("_create_dagruns_for_dags")
        all_dags_needing_dag_runs = DagEntity.dags_needing_dagruns(self.yt_client)
        self._create_dag_runs(all_dags_needing_dag_runs)

    def _create_dag_runs(self, dag_entities: list[DagEntity]) -> None:
        for de in dag_entities:
            dag = _get_current_dag(dag_id=de.dag_id, yt_client=self.yt_client)
            if not dag:
                continue

            try:
                dag.create_dagrun(
                    dag=dag,
                    yt_client=self.yt_client,
                    state=DagRunState.QUEUED,
                    creating_job_id=self.job.id,
                    start_date=datetime.utcnow(),
                )
            except Exception:
                continue

    def _start_queued_dagruns(self) -> None:
        print("_start_queued_dagruns")
        dag_runs: list[DagRun] = DagRun.get_queued_dag_runs_to_set_running(self.yt_client)

        for dag_run in dag_runs:
            dag_run.state = DagRunState.RUNNING
            dag_run.start_date = datetime.utcnow()

    def _schedule_all_dag_runs(
            self,
            dag_runs: list[DagRun],
    ) -> None:
        for dag_run in dag_runs:
            dag_model = DagEntity.get(dag_run.dag_id, yt_client=self.yt_client)

            if not dag_model:
                return

            dag_run.scheduled_by_job_id = self.job.id

            schedulable_trs = dag_run.update_state(yt_client=self.yt_client) #Пересчитывает статус DagRun,находит задачи, которые нужно поставить в SCHEDULED.

            dag_run.schedule_trs(schedulable_trs, yt_client=self.yt_client)

    def _critical_section_enqueue_task_runs(self):
        num_occupied_slots = self.job.executor.slots_occupied
        parallelism = 4
        max_trs = parallelism - num_occupied_slots
        if max_trs <= 0:
            return 0

        queued_trs = self._executable_task_runs_to_queued(max_trs)

        self._enqueue_task_runs_with_queued_state(queued_trs, self.job.executor)

        return len(queued_trs)

    def _executable_task_runs_to_queued(self, max_trs: int) -> list[TaskRun]:
        executable_trs: list[TaskRun] = []

        starved_tasks: set[tuple[str, str]] = set()

        while True:
            num_starved_tasks = len(starved_tasks)

            query = f"""
                select tr.*
                from [{'//home/task_run'}] as tr
                join {'//home/dag_run'} as dr
                    on tr.run_id = dr.run_id
                    and dr.state = {DagRunState.RUNNING}
                join {'//home/dag_model'} as dm
                    on tr.dag_id = dm.dag_id
                    and dm.is_paused = false
                where tr.state = {TaskRunState.SCHEDULED}
                """

            if starved_tasks:
                pairs = ",\n    ".join(
                    f"('{dag_id}', '{task_id}')"
                    for dag_id, task_id in starved_tasks
                )
                query += f"and (tr.dag_id, tr.task_id) NOT IN (\n{pairs}\n)\n"

            query += f"limit {max_trs}"

            with yt.Transaction():
                rows = list(self.yt_client.select_rows(query))
                task_runs_to_examine = [TaskRun(**row) for row in rows]


            if not task_runs_to_examine:
                break


            executor_slots_available = self.job.executor.slots_available
            for tr in task_runs_to_examine:
                if executor_slots_available <= 0:
                    starved_tasks.add((tr.dag_id, tr.task_id))
                    continue
                executor_slots_available -= 1

                executable_trs.append(tr)

            is_done = executable_trs or len(task_runs_to_examine) < max_trs
            found_new_filters = (len(starved_tasks) > num_starved_tasks)

            if is_done or not found_new_filters:
                break

        if executable_trs:
            filter_for_trs = TaskRun.filter_for_trs(executable_trs)

            now_iso = datetime.utcnow().isoformat()
            updated_rows = []

            with yt.Transaction() as tx:
                rows = list(tx.select_rows(
                    f"""
                    select tr.*
                    from `{"//home/task_run"}` as tr
                    where {filter_for_trs}
                    """
                ))

                for row in rows:
                    row["state"] = TaskRunState.QUEUED
                    row["queued_at"] = now_iso
                    row["queued_by_job_id"] = self.job.id
                    updated_rows.append(row)

                if updated_rows:
                    tx.insert_rows("//home/task_run", updated_rows, update=True)
        return executable_trs

    def _enqueue_task_runs_with_queued_state(
            self, task_runs: list[TaskRun], executor: Executor
    ) -> None:
        for tr in task_runs:
            if tr.dag_run.state in [DagRunState.FAILED, DagRunState.FAILED]:
                tr.set_state(None, yt_client=self.yt_client)
                continue

            executor.queue_task_run(tr)
