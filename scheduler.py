from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime

from dag import DAG, Task, _build_dag_from_spec, TaskState
from dag_run import RunState # , DagRun
from dagrun import DagRun

import queue
from concurrent.futures import ThreadPoolExecutor
from executor import Executor
from taskrun import TaskRun


class Scheduler:
    def __init__(
            self,
            executor : Executor,
            scheduler_idle_sleep_time,
            yt_client: yt.YtClient
            # log
    ):
        self.executor = executor

        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        self.yt_client = yt_client

        self.dags: dict[str, DAG] = {}


        # self.pipelines = {} # dag_id -> DAG
        #
        # self.active_runs: dict[str, DagRun] = {} # dag_id -> DagRun
        # self.executor = ThreadPoolExecutor(max_workers=max_workers)
        # self.futures = {} # future -> {dag_run, task)
        #
        # self.yt_client = yt_client
        #
        # self.run_queue = queue.Queue()

    def _executable_taskruns_to_queued(self) -> list[TaskRun]:
        executable: list[TaskRun] = []

        active_dagruns = []

        for dagrun in active_dagruns:
            executable.extend(dagrun.get_ready_tasks())

    def _enqueue_taskruns(self) -> int:
        queued_taskruns = self._executable_taskruns_to_queued()

        for t in queued_taskruns:
            if t.state in [RunState.SUCCESS, RunState.FAILED]:
                t.set_state(None)
                continue

            self.executor.submit(t)

        return len(queued_taskruns)

    def _execute(self) -> int | None:
        # Starting the scheduler

        try:
            self._load_executor()
            self.executor.start()

            self._run_scheduler_loop()
        except Exception:
            # Exception
            raise
        finally:
            self.executor.end()
        return None

    # _update_dag_run_state_for_paused_dags

    def _run_scheduler_loop(self) -> None:
        while True:
            self._do_scheduling()

            self._process_executor_events(executor=self.executor)
                # self._process_task_event_logs(executor._task_event_log)

            time.sleep(self._scheduler_idle_sleep_time)

    def _process_executor_events(self, executor: Executor) -> int:
        pass

    def _do_scheduling(self) -> int:

        dags_to_create = list(self.dags.values()) # TODO
        self._create_dagruns(dags_to_create)

        self._to_queued_dagruns()

        dag_runs = DagRun.get_running_dag_runs_to_examine(yt_client=self.yt_client)
        # callback_tuples =
        self._schedule_dagruns(dag_runs)

        # for dag_run, callback in callback_tuples:
        #     self.executor.send_callback(callback)

        num = self._enqueue_taskruns()

        return num


    def _create_dagruns(self, dags: list[DAG]) -> None:
        for dag in dags:
            dagrun: DagRun = DagRun.get_latest_dagrun(dag.dag_id, self.yt_client)

            if not dagrun or dagrun.get_state() not in [RunState.RUNNING, RunState.PENDING]: # TODO какая логика?
                dag.create_dagrun(run_id="gen", yt_client=self.yt_client, start_date=datetime.utcnow()) # сразу queued

    def _to_queued_dagruns(self) -> None: # лучше start
        dag_runs: list[DagRun] = DagRun.get_queued_dag_runs_to_set_running(self.yt_client)

        active_runs_of_dags = [] # TODO

        for dag_run in dag_runs:
            dag_run.state = RunState.RUNNING
            dag_run.start_date = datetime.utcnow()
            # dagrun state changed

        # callbacks
    def _schedule_dagruns(self, dag_runs: list[DagRun]) -> list[tuple[DagRun, Callback]]:
        callback_tuples = [(run, self._schedule_dag_run(run)) for run in dag_runs]
        return callback_tuples

    def _schedule_dag_run(self, dag_run: DagRun) -> None:  # callback:
        dag = dag_run.dag
        if (dag_run.start_date and dag.dagrun_timeout and dag_run.start_date + dag.dagrun_timeout < datetime.utcnow()):
            dag_run.set_state(RunState.FAILED)
            unfinished_taskruns = [] # TODO

            for taskrun in unfinished_taskruns:
                taskrun.set_state(RunState.SKIPPED)

                callback = [] # TODO

                # dagrun state changed
            return callback

        schedulable = dag_run.update_state(yt_client=self.yt_client)
        # schedulable_tis, callback_to_run = dag_run.update_state(yt_client=self.yt_client)

        dag_run.schedule_taskruns(schedulable)
        return None
        # return callback_to_run

    def _load_executor(self):
        pass

    def process_executor_events(self):
        pass
