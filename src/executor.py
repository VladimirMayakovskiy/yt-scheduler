from __future__ import annotations

# import contextlib
import multiprocessing
from queue import Empty

import yt.wrapper as yt

from task_run import TaskRun


class Executor:
    job_id: None | int | str = None

    def __init__(self, parallelism: int = 4):
        self._manager = multiprocessing.Manager()
        self.task_queue = self._manager.Queue()
        self.result_queue = self._manager.Queue()
        self.yt_scheduler: YtScheduler | None = None
        self.yt_client: yt.YtClient | None = None

        self.parallelism: int = parallelism
        self.queued_tasks: dict = {}
        self.running = set()
        self.event_buffer: dict = {}


    def start(self) -> None:
        print("Start Executor")

        self.yt_client =  yt.YtClient(proxy='localhost:8000')
        self.scheduler_job_id = str(self.job_id)
        self.yt_scheduler = YtScheduler(
            scheduler_job_id=self.scheduler_job_id,
            yt_client=self.yt_client,
            result_queue=self.result_queue,
        )


    def end(self) -> None:
        print("Shutting down Kubernetes executor")
        self._flush_task_queue()
        self._flush_result_queue()
        self.task_queue.join()
        self.result_queue.join()
        if self.yt_scheduler:
            self.yt_scheduler.terminate() # TODO
        self._manager.shutdown()

    def queue_command(
            self,
            task_run: TaskRun,
    ):
        if task_run.key not in self.queued_tasks:
            self.queued_tasks[task_run.key] = task_run

    def heartbeat(self) -> None:
        open_slots = self.parallelism - len(self.running)
        self.trigger_tasks(open_slots)

        print("Calling the %s sync method", self.__class__)
        self.sync()

    def trigger_tasks(self, open_slots: int) -> None:
        queue = self.queued_tasks.items()
        tasks = []

        for _ in range(min(open_slots, len(self.queued_tasks))):
            key, item = queue.pop(0)

            if key in self.running:
                del self.queued_tasks[key]
            else:
                tasks.append(item)

        if tasks:
            self._process_tasks(tasks)


    def sync(self) -> None:
        self.yt_scheduler.sync()
        with contextlib.suppress(Empty):
            while True:
                results = self.result_queue.get_nowait()
                try:
                    key, state = results
                    self._change_state(key, state)
                finally:
                    self.result_queue.task_done()

        with contextlib.suppress(Empty):
            task = self.task_queue.get_nowait()
            try:
                self.yt_scheduler.run_next(task)
            finally:
                self.task_queue.task_done()
