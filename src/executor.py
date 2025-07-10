from __future__ import annotations

import itertools
import multiprocessing
import time
from datetime import datetime
from queue import Empty, Queue
from typing import Any

import yt.wrapper as yt

from ytoperator import BaseOperator
from state import TaskRunState
from task_run import TaskRun, TaskRunKey


class Executor:
    def __init__(self, job_id: str, parallelism: int = 4):
        self.job_id = job_id
        self.parallelism = parallelism

        self._running_ops: dict = {} #TaskRun.id -> operation_id
        self._pending: list[TaskRun] = []

    def start(self) -> None:
        print("Start Executor")

    def end(self) -> None:
        print("Shutting down Executor")

    @property
    def slots_occupied(self):
        return len(self._running_ops) + len(self._pending)

    def queue_task_run(
            self,
            task_run: TaskRun,
            operator: BaseOperator,
            yt_client: yt.YtClient,
    ):
        print("EXECUTOR.queue_task_run")
        if self.slots_occupied >= self.parallelism:
            return

        task_run.operation_id = operator.run_operation(yt_client).id

        print("back in EXECUTOR.queue_task_run")

        update = task_run.to_row()
        update["state"] = TaskRunState.RUNNING
        update["operation_id"] = task_run.operation_id

        # {
        #     "id": task_run.id,
        #     "state": TaskRunState.RUNNING,
        #     "queued_at": datetime.now().utcnow().isoformat(),
        #     "queued_by_job_id": self.job_id,
        #     "operation_id": task_run.operation_id,
        # }
        yt_client.insert_rows("//home/task_run", [update])

        self._running_ops[task_run.id] = task_run.operation_id
        self._pending.append(task_run)

    def heartbeat(self, yt_client: yt.YtClient) -> None:
        print("Heartbeat")
        finished = []
        try:
            for task_run in self._pending:
                print(task_run.operation_id)
                try:
                    operation_id = task_run.operation_id
                    status = yt_client.get_operation_state(operation_id)
                except Exception as e:
                    print("HERE", e)
                    raise

                print(operation_id, "STATUS: ", status)

                if status.is_running():
                    continue

                new_state = TaskRunState.FAILED if status.is_unsuccessfully_finished() else TaskRunState.SUCCESS

                task_run.set_state(new_state, yt_client)
                # update = {
                #     "id": task_run.id,
                #     "state": new_state,
                #     "started_at": task_run.start_date or datetime.now().utcnow().isoformat(),
                #     "ended_at": datetime.now().utcnow().isoformat(),
                # }
                # yt_client.insert_rows("//home/task_run", [update], update=True)

                finished.append(task_run)
                self._pending.remove(task_run)
                del self._running_ops[task_run.id]
        except Exception as e:
            print(e)
            raise

# class Executor1:
#     job_id: None | int | str = None
#
#     def __init__(self):
#         self._manager = multiprocessing.Manager()
#         self.task_queue = self._manager.Queue()
#         self.result_queue = self._manager.Queue()
#         self.scheduler_job_id: str | None = None
#         # self.workers: list[Worker] = []
#
#         self.yt_client: yt.YtClient | None = None
#
#         self.parallelism: int = 4
#         self.queued_tasks: dict[TaskRunKey, BaseOperator] = {}
#         self.running: set[TaskRunKey] = set()
#         self.event_buffer: dict = {}
#
#
#     def start(self) -> None:
#         print("Start Executor")
#
#         self.yt_client =  yt.YtClient(proxy='localhost:8000')
#         self.scheduler_job_id = str(self.job_id)
#         # self.workers = self._make_workers()
#
#     def end(self) -> None:
#         print("Shutting down Executor")
#         self._flush_queues()
#         self.task_queue.join()
#         self.result_queue.join()
#         self._manager.shutdown()
#
#     def _flush_queues(self) -> None:
#         while True:
#             try:
#                 self.task_queue.get_nowait()
#             except Empty:
#                 break
#             else:
#                 self.task_queue.task_done()
#
#         while True:
#             try:
#                 self.result_queue.get_nowait()
#             except Empty:
#                 break
#             else:
#                 self.result_queue.task_done()
#
#
#     def queue_task_run(
#             self,
#             task_run: TaskRun,
#     ):
#         if task_run.key not in self.queued_tasks:
#             print("Adding to queue: %s", task_run.key)
#             self.queued_tasks[task_run.key] = task_run.task
#
#     def heartbeat(self) -> None:
#         print("Heartbeat")
#         open_slots = self.parallelism - len(self.running)
#         self.trigger_tasks(open_slots)
#         self.sync()
#
#     def trigger_tasks(self, open_slots: int) -> None:
#         print("trigger_tasks")
#         queue = itertools.islice(self.queued_tasks.items(), open_slots)
#
#         for key, op in queue:
#             del self.queued_tasks[key]
#
#             if key not in self.running:
#                 self.task_queue.put((key, op))
#                 self.event_buffer[key] = (TaskRunState.QUEUED, self.scheduler_job_id)
#                 self.running.add(key)
#
#     def sync(self) -> None:
#         print("Syncing, Executor")
#         while True:
#             try:
#                 results = self.result_queue.get_nowait()
#             except Empty:
#                 break
#
#             key, state = results
#             self.event_buffer[key] = (state, self.scheduler_job_id)
#             if key in self.running:
#                 self.running.remove(key)
#             self.result_queue.task_done()
#
#         slots = self.parallelism - len(self.running)
#         for _ in range(slots):
#             try:
#                 key, task = self.task_queue.get_nowait()
#             except Empty:
#                 break
#             try:
#                 self.run_next(key, task)
#             except Exception:
#                 self.event_buffer[key] = (TaskRunState.FAILED, self.scheduler_job_id)
#             finally:
#                 self.task_queue.task_done()
#
#     def run_next(self, task_key: TaskRunKey, op: BaseOperator) -> None:
#         op.run_task(self.yt_client, self.result_queue, task_key)
#         self.running.add(task_key)
#         self.event_buffer[task_key] = ("RUNNING", self.scheduler_job_id)
#
#     # def _make_workers(self) -> list[Worker]:
#     #     print("Make workers")
#     #     workers = []
#     #     for _ in range(4):
#     #         p = Worker(
#     #             worker_queue=self.worker_queue,
#     #             scheduler_job_id=self.scheduler_job_id,
#     #         )
#     #         p.start()
#     #         workers.append(p)
#     #     return workers
#
#     @property
#     def slots_occupied(self):
#         return len(self.running) + len(self.queued_tasks)



# class Worker(multiprocessing.Process):
#     def __init__(
#             self,
#             task_queue: Queue,
#             result_queue: Queue,
#             scheduler_job_id: str,
#             yt_client: yt.YtClient
#     ):
#         super().__init__(daemon=True)
#         self.yt_client = yt_client
#         self.scheduler_job_id = scheduler_job_id
#         self.task_queue = task_queue
#         self.result_queue = result_queue
#
#     def run(self):
#         while True:
#             operator = self.task_queue.get()
#             operator.run_task(self.yt_client)
#             self.task_queue.task_done()
