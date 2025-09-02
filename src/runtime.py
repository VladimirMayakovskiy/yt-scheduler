import logging
from typing import Optional

from config import Config
from job import JobContext, JobList, JobRunner
from yt_wrapper import  ClientContext

from pool_executor import PoolExecutor
from task_runner import TaskRunner
from scheduler import Scheduler

class Runtime:
    def __init__(self, runners: list[JobRunner], context: JobContext):
        self._runners = runners
        self._context = context

    def _start(self):
        self._context.pool_executor._start_workers()
        for runner in self._runners:
            runner.run_job()

    def _stop(self, timeout: Optional[float] = 1.0): # todo timeout
        for runner in self._runners:
            try:
                runner.stop(timeout=timeout)
            except Exception:
                raise

            if runner.exception:
                logging.exception(runner.exception)

    def run(self):
        self._start()
        for runner in self._runners:
            try:
                runner._join(timeout=None)
            except Exception:
                raise

            if runner.exception:
                logging.exception(runner.exception)

    def shutdown(self, timeout: Optional[float] = 1.0):
        self._stop(timeout=timeout)

def build_runtime(config: Config) -> Runtime:
    job_list = JobList().append(TaskRunner) \
                        .append(Scheduler)
    context = JobContext(config=config, pool_executor=PoolExecutor()).build(registry=job_list)

    client_context = ClientContext(config=config)

    runners: list[JobRunner] = []
    for job in context._instances.values():
        runner = JobRunner(job_id=job.name, context=client_context, execute_callable=job._entry)
        job.set_runner(runner)
        runners.append(runner)

    return Runtime(runners=runners, context=context)