from __future__ import annotations

import contextvars
import threading
import time
from collections import defaultdict
from itertools import chain
from datetime import datetime, timezone as tz
import socket
from typing import Any, Optional, Callable, ClassVar

from logging_mixin import LoggingMixin
from base_row import YtRow, TablePath
from state import JobState
from config import Config
from common import classproperty
from yt_wrapper import context_wrapper

import yt.wrapper as yt

def job_factory(cls: type, *args, **kwargs) -> Callable:
    def _factory(context: JobContext):
        return cls(context, *args, **kwargs)
    return _factory

class JobList:
    def __init__(self):
        self._job_factories: list[Callable[[JobContext], JobBase]] = []

    def append(self, factory: Callable[[JobContext], JobBase]) -> "JobList":
        self._job_factories.append(factory)
        return self

    def __iter__(self):
        return iter(self._job_factories)

    def __len__(self):
        return len(self._job_factories)

    def __contains__(self, job_cls):
        return job_cls in self._job_factories

class JobContext:
    def __init__(self, config: Config):
        self._config = config
        self._instances: dict[tuple[type[JobBase], str], list[JobBase]] = defaultdict(list)

        self._active_jobs: dict[str, dict[str, _JobRun]] = {} # job_type -> {job_id -> options}
        self._lock = threading.Lock()
        self._heartbeat_ttl = 30.0

    def build(self, registry: JobList) -> "JobContext":
        for factory in registry:
            job = factory(self)
            assert isinstance(job, JobBase)
            self._instances[(type(job), job.name)].append(job)
        return self

    def find(self, job_cls: type[JobBase], job_cls_name: Optional[str] = None) -> Optional:
        if job_cls_name is None:
            return self._instances.get((job_cls, job_cls.name))
        return self._instances.get((job_cls, job_cls_name))

    def __iter__(self):
        return (job for jobs in self._instances.values() for job in jobs)

    def touch_job(self, options: dict):
        ts = time.time()
        job_type = options.pop("job_type")
        with self._lock:
            jobs = self._active_jobs.setdefault(job_type, {})
            job_id = options.get("job_id")
            if not job_id:
                print("Job id not found in options")
                return

            if job_id in jobs:
                row = jobs[job_id]
                for k, v in options.items(): # todo вынести из serialized в common чтоб не дублировать
                    setattr(row, k, v)
            else:
                run_options = {
                    "start_date": None,
                    "last_seen": None,
                    "last_seen_iso": None,
                }
                options = {**run_options, **options}
                row = _JobRun(**options)
            row.last_seen = ts
            row.last_seen_iso = datetime.fromtimestamp(ts, tz=tz.utc).isoformat()
            jobs[job_id] = row

    def get_all_jobs(self, job: JobBase | str | None = None) -> list[_JobRun]:
        with self._lock:
            if job is None:
                return list(chain.from_iterable(jobs.values() for jobs in self._active_jobs.values()))

            if isinstance(job, JobBase):
                job = job.name
            return list(self._active_jobs.get(job, {}).values())

    def get_active_jobs(self, job: str | JobBase | None = None) -> list[str]:
        ts = time.time()
        return [run.job_id for run in self.get_all_jobs(job) if (ts - run.last_seen) <= self._heartbeat_ttl]

    def compute_shard_index(self, job: JobBase) -> tuple[int, int]:
        jobs = self.get_active_jobs(job)
        if not jobs:
            return 0, 0
        if job._job_id not in jobs:
            job._heartbeat_tick()
            jobs.append(job._job_id)
            jobs.sort()
        idx = jobs.index(job._job_id)
        return idx, len(jobs)

    def flush_to_yt(self, context, pool=None, *, batch_size: int = 50, retries: int = 3, backoff: float = 0.5):
        rows = self.get_all_jobs()
        if not rows:
            return

        def _write_rows():
            from common import _chunked
            def _do():
                for batch in _chunked(rows, batch_size):
                    _JobRun.upsert_rows(rows=batch)
            return context(lambda: _do())

        if pool is None:
            _write_rows()
        else:
            _execution_options = {"retries": retries, "backoff": backoff}
            pool.submit_or_execute(_write_rows, context=context, _execution_options=_execution_options)

class JobBase(LoggingMixin):
    state_type: ClassVar[type[JobState]] = JobState
    def __init__(self, context: JobContext):
        self.context = context
        self._job_id = None
        self.client_context = None
        self._stop_event = None

    @classproperty
    def name(self) -> str:
        raise NotImplementedError

    @property
    def _entry(self) -> Callable[[], int | None]:
        return self._execute

    def _execute(self) -> int | None:
        raise NotImplementedError

    def _init_state(self, runner: JobRunner):
        self._stop_event = runner.stop_event
        self._job_id = runner.job_id
        self.client_context = context_wrapper(context=runner)

    def _heartbeat_tick(self, options: dict = None):
        try:
            run_options = { # todo сделать отдельный run_options класс верифайед дикт
                "job_type": self.name,
                "job_id": self._job_id,
                "hostname": socket.gethostname(),
                "thread_id": threading.get_ident(),
            }
            if options:
                options = {**run_options, **options}
            else:
                options = run_options
            self.context.touch_job(options)
        except Exception as e:
            self.log.info("Failed job_id=%s to heartbeat: %s", self._job_id, e)

class LightweightJob(JobBase):
    def __init__(self, context: JobContext, func: Callable):
        super().__init__(context)
        self._func = func

    @classproperty
    def name(self) -> str:
        if isinstance(self, LightweightJob):
            return f"lightweight:{self._func.__name__}"
        return "lightweight"

    def _execute(self) -> int | None:
        return self._func()

@yt.yt_dataclass
class _JobRun(YtRow, LoggingMixin):
    table_path:  ClassVar[str] = TablePath("job_state")
    key_columns: ClassVar[list[str]] = ["job_id"]
    alias: ClassVar[str] = "job_st"

    # job_type: str # todo add
    job_id: str
    thread_id: Optional[int]
    last_seen: Optional[float]
    last_seen_iso: Optional[str]
    hostname: str
    start_date: Optional[str]
    end_date: Optional[str] = None

class ClientContext:
    def __init__(self, agent: "ClientAgent"):
        self.job_id = yt.common.generate_uuid()
        self._agent = agent
        self.client_var: contextvars.ContextVar = self._agent.client_var_template(self.job_id)

class JobRunner(ClientContext, LoggingMixin):
    def __init__(self, job: JobBase, agent: "ClientAgent"):
        super().__init__(agent)

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._finished = threading.Event()
        self.result: Any = None
        self.exception: Optional[BaseException] = None
        self.state: JobBase.state_type = None

        self._job = job
        self._execute_callable = self._job._entry

        self._context = context_wrapper(context=self)

        job._init_state(self)

    def _prepare_for_execution(self):
        options = {
            "start_date": datetime.now(tz.utc).isoformat(),
            "end_date": None,
        }
        self._job._heartbeat_tick(options)

        self._started.set()

    def _complete_execution(self):
        self._job._heartbeat_tick(options={"end_date": datetime.now(tz.utc).isoformat()})

        self._finished.set()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    def run_job(self, *, daemon: bool = False):
        if issubclass(type(self._job), LightweightJob):
            return self._run_sync(self._execute_callable)
        else:
            return self._run_async(self._execute_callable, daemon=daemon)

    def _target_wrapper(self, execute_callable: Callable[[], int | None]):
        def _target():
            try:
                self.state = JobState.RUNNING
                self._prepare_for_execution()
                ret = None
                try:
                    ret = execute_callable()
                    self.state = JobState.SUCCESS
                except SystemExit:
                    self.state = JobState.SUCCESS
                except Exception as e:
                    self.exception = e
                    self.state = JobState.FAILED
                    raise # todo
                self.result = ret
                return ret
            finally:
                self._complete_execution()
        return context_wrapper(context=self, func=_target)

    def _run_async(self, execute_callable: Callable[[], int | None], *, daemon: bool = True):
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Job already running.")

        self._stop_event.clear()

        t = threading.Thread(target=self._target_wrapper(execute_callable), daemon=daemon, name=f"job-{self.job_id}")
        self._thread = t
        t.start()
        return t

    def _run_sync(self, execute_callable: Callable[[], int | None]):
        target = self._target_wrapper(execute_callable)
        return target()

    def stop(self, timeout: Optional[float] = 5.0):
        self._stop_event.set()
        self._join(timeout=timeout)

    def _join(self, timeout: Optional[float] = None):
        if self._thread:
            self._thread.join(timeout=timeout)