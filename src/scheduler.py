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
from task_run import TaskRun, TaskRunRow
from executor import Executor
from logging_mixin import LoggingMixin


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
    print(_get_current_dag.__name__)
    de = DagEntity.get(dag_id=dag_id, yt_client=yt_client)
    # создаем dag
    if not de:
        return None
    return DAG.from_dag_entity(de, yt_client)

class Scheduler(LoggingMixin):
    def __init__(
            self,
            job : Job,
            yt_client: yt.YtClient,
            scheduler_idle_sleep_time: float = 10,
    ):
        self.job = job
        self.yt_client = yt_client
        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        self._dag_cache: dict[str, DAG] = {}

    def _get_dag(self, de: DagEntity) -> DAG:
        if de.dag_id not in self._dag_cache:
            self._dag_cache[de.dag_id] = DAG.from_dag_entity(de, self.yt_client)
        return self._dag_cache[de.dag_id]

    def _execute(self) -> int | None:
        self.log.info("Starting the scheduler")
        try:
            self.job.executor.start()
            self._run_scheduler_loop()
        except Exception:
            raise
        finally:
            self.job.executor.end()
            self.log.info("Exited execute loop")
            return None

    def _run_scheduler_loop(self):
        while True:
            self.log.info("\nONE STEP OF SCHEDULER LOOP")
            num_queued = self._do_scheduling()

            self.job.executor.heartbeat(self.yt_client)

            if not num_queued:
                time.sleep(self._scheduler_idle_sleep_time)

    def _do_scheduling(self) -> int:
        created = self._create_dagruns_for_dags()

        self._start_queued_dagruns() # запуск и проверка существующих дагран

        self._schedule_running_dagruns()

        self.log.info("TRANSFERRING TASK TO EXECUTOR")
        num_queued = self._enqueue_task_runs() # переводим задачи в QUEUED; отправляем в executor
        self.log.info(f"NUM_QUEUED: {num_queued}")
        return num_queued

    def _create_dagruns_for_dags(self) -> int:
        all_dags_needing_dag_runs = DagEntity.dags_needing_dagruns(self.yt_client)
        created = 0
        for de in all_dags_needing_dag_runs:
            try:
                dag = self._get_dag(de)
                DAG.create_dagrun(
                    yt_client=self.yt_client,
                    dag=dag,
                    state=DagRunState.QUEUED,
                    creating_job_id=self.job.id,
                )
                created += 1
            except Exception as e:
                self.log.info("Failed to create dagrun SKIPPING:")
                continue
        self.log.info(f"CREATED DAGRUNS: {created}")
        return created

    def _start_queued_dagruns(self) -> None:
        dag_runs: list[DagRun] = DagRun.get_queued_dag_runs_to_set_running(yt_client=self.yt_client)
        self._schedule_dag_runs(dag_runs)

    def _schedule_running_dagruns(self) -> None:
        dag_runs: list[DagRun] = DagRun.get_running_dag_runs_to_examine(yt_client=self.yt_client)
        self._schedule_dag_runs(dag_runs)

    def _schedule_dag_runs(
            self,
            dag_runs: list[DagRun],
    ) -> None:
        # Переводим SCHEDULE DagRun в RUNNING, SCHEDULED TASKRUN в READY
        for dag_run in dag_runs:
            dag = self._get_dag(DagEntity.get(dag_run.dag_id, yt_client=self.yt_client))
            if not dag:
                self.log.warning(f"Skip dagrun run_id={dag_run.run_id}, dag_id={dag_run.dag_id}")
                continue

            dag_run.dag = dag
            schedulable_trs = dag_run.update_state(yt_client=self.yt_client) #Пересчитывает статус DagRun,находит задачи, которые нужно поставить в SCHEDULED.

            scheduled = dag_run.schedule_trs(schedulable_trs, yt_client=self.yt_client)
            self.log.info(f"SCHEDULED COUNT: {scheduled} of {len(schedulable_trs)}")

    def _enqueue_task_runs(self) -> int:
        # max_trs = self.job.executor.parallelism - self.job.executor.slots_occupied
        # if max_trs <= 0:
        #     return 0

        trs = TaskRun.get_executable_task_runs_to_queued(yt_client=self.yt_client)
        updated = TaskRun.update_rows(self.yt_client, trs, state=TaskRunState.QUEUED)
        self.log.info(f"STARTED QUEUE: {len(updated)} of {len(trs)}")

        self._enqueue_task_runs_with_queued_state(updated, self.job.executor)
        return len(updated)

    # def _executable_task_runs_to_queued(self, max_trs: int) -> list[TaskRun]:
    #     print("_executable_task_runs_to_queued")
    #     free_slots = max_trs # TODO self.job.executor.parallelism - self.job.executor.slots_occupied
    #
    #     try:
    #         executable_trs = TaskRun.get_executable_task_runs_to_queued(yt_client=self.yt_client)
    #
    #         print("OK")
    #         print(executable_trs)
    #         try:
    #             for tr in executable_trs:
    #                 tr.set_state(TaskRunState.QUEUED, self.yt_client)
    #         except Exception as e:
    #             print("HERE", e)
    #             raise
    #
    #         # starved_tasks: set[tuple[str, str]] = set()
    #         #
    #         # while True:
    #         #     num_starved_tasks = len(starved_tasks)
    #         #
    #         #     query = f"""
    #         #         select tr.*
    #         #         from [{'//home/task_run'}] as tr
    #         #         join {'//home/dag_run'} as dr
    #         #             on tr.run_id = dr.run_id
    #         #             and dr.state = {DagRunState.RUNNING}
    #         #         join {'//home/dag_model'} as dm
    #         #             on tr.dag_id = dm.dag_id
    #         #             and dm.is_paused = false
    #         #         where tr.state = {TaskRunState.READY}
    #         #         """
    #         #
    #         #     if starved_tasks:
    #         #         pairs = ",\n    ".join(
    #         #             f"('{dag_id}', '{task_id}')"
    #         #             for dag_id, task_id in starved_tasks
    #         #         )
    #         #         query += f"and (tr.dag_id, tr.task_id) NOT IN (\n{pairs}\n)\n"
    #         #
    #         #     query += f"limit {max_trs}"
    #         #
    #         #     if self.yt_client.exists("//home/task_run") and self.yt_client.exists("//home/dag_run") and self.yt_client.exists("//home/dag_model"):
    #         #         rows = list(self.yt_client.select_rows(query))
    #         #     else:
    #         #         rows = []
    #         #     task_runs_to_examine = [TaskRun(**row) for row in rows]
    #         #
    #         #
    #         #     if not task_runs_to_examine:
    #         #         break
    #         #
    #         #
    #         #     executor_slots_available = self.job.executor.slots_available
    #         #     for tr in task_runs_to_examine:
    #         #         if executor_slots_available <= 0:
    #         #             starved_tasks.add((tr.dag_id, tr.task_id))
    #         #             continue
    #         #         executor_slots_available -= 1
    #         #
    #         #         executable_trs.append(tr)
    #         #
    #         #     is_done = executable_trs or len(task_runs_to_examine) < max_trs
    #         #     found_new_filters = (len(starved_tasks) > num_starved_tasks)
    #         #
    #         #     if is_done or not found_new_filters:
    #         #         break
    #         #
    #         # if executable_trs:
    #         #     filter_for_trs = TaskRun.filter_for_trs(executable_trs)
    #         #
    #         #     now_iso = datetime.utcnow().isoformat()
    #         #     updated_rows = []
    #         #
    #         #     with yt.Transaction() as tx:
    #         #         rows = list(tx.select_rows(
    #         #             f"""
    #         #             select tr.*
    #         #             from `{"//home/task_run"}` as tr
    #         #             where {filter_for_trs}
    #         #             """
    #         #         ))
    #         #
    #         #         for row in rows:
    #         #             row["state"] = TaskRunState.QUEUED
    #         #             row["queued_at"] = now_iso
    #         #             row["queued_by_job_id"] = self.job.id
    #         #             updated_rows.append(row)
    #         #
    #         #         if updated_rows:
    #         #             tx.insert_rows("//home/task_run", updated_rows, update=True)
    #         print("OK2")
    #         return executable_trs
    #     except Exception as e:
    #         print(e)
    #         raise

    def _enqueue_task_runs_with_queued_state(
            self, task_runs: list[TaskRunRow], executor: Executor
    ) -> None: # TODO return
        for tr in task_runs:
            try:
                dag = self._get_dag(DagEntity.get(tr.dag_id, yt_client=self.yt_client)) # TODO
                executor.queue_task_run(tr, dag.task_dict[tr.task_id], self.yt_client)
            except Exception as e:
                self.log.exception("Failed to enqueue task:")
                raise
