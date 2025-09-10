from __future__ import annotations

import contextvars
import threading
import types
from collections import defaultdict
from typing import Any, Optional, Callable, ClassVar

from logging_mixin import LoggingMixin
from state import JobState
from config import Config
from common import classproperty
from yt_wrapper import context_wrapper

import yt.wrapper as yt

def job_factory(cls: type[JobBase], factory: Callable[[JobContext], JobBase]):
    return types.SimpleNamespace(factory=factory, cls=cls)

class JobList:
    def __init__(self):
        self._job_factories: list = []
        # self._job_cls_list: list[type[JobBase]] = []
        # self._job_factories: list[Callable[[JobContext], JobBase]] = []

    def append(self, job_cls: type[JobBase], factory: Callable[[JobContext], JobBase]) -> "JobList":
        self._job_factories.append(job_factory(job_cls, factory))
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

    def build(self, registry: JobList) -> "JobContext":
        for desc in registry:
            job = desc.factory(self)
            assert isinstance(job, desc.cls)
            self._instances[(desc.cls, job.name)].append(job)
        return self

    def find(self, job_cls: type[JobBase], job_cls_name: Optional[str] = None) -> Optional: # todo убрать классы
        if job_cls_name is None:
            return self._instances.get((job_cls, job_cls.name))
        return self._instances.get((job_cls, job_cls_name))

    def __iter__(self):
        return (job for jobs in self._instances.values() for job in jobs)


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
        self.client_context = context_wrapper(client_state=runner)

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

class ClientState:
    def __init__(self, agent: "ClientAgent"):
        self.job_id = yt.common.generate_uuid()
        self._agent = agent
        self.client_var: contextvars.ContextVar = self._agent.client_var_template(self.job_id)

class JobRunner(ClientState):
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

        job._init_state(self)

    def _prepare_for_execution(self, yt_client: yt.YtClient): # todo
        self._started.set()
    def _complete_execution(self, yt_client: yt.YtClient): # todo
        self._finished.set()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    def run_job(self, *, daemon: bool = True):
        if issubclass(type(self._job), LightweightJob):
            return self._run_sync(self._execute_callable)
        else:
            return self._run_async(self._execute_callable, daemon=daemon)

    def _target_wrapper(self, execute_callable: Callable[[], int | None]):
        def _target():
            try:
                self._prepare_for_execution(self.client_var.get())
                ret = None
                try:
                    ret = execute_callable()
                    self.state = JobBase.state_type.SUCCESS
                except SystemExit:
                    self.state = JobBase.state_type.SUCCESS
                except Exception as e:
                    self.exception = e
                    self.state = JobBase.state_type.FAILED
                    raise # todo
                self.result = ret
                return ret
            finally:
                self._complete_execution(self.client_var.get())
        return context_wrapper(client_state=self, func=_target)

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