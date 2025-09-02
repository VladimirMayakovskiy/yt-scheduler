from __future__ import annotations

import contextvars
import threading
import types
from typing import Any, Optional, Callable, ClassVar

from logging_mixin import LoggingMixin
from state import JobState
from config import Config
from pool_executor import PoolExecutor
from yt_wrapper import ClientContext, context_wrapper

import yt.wrapper as yt

class classproperty(property):
    def __get__(self, obj: Any, objtype: Optional[type] = None) -> Any:
        return self.fget(objtype)


class JobList:
    def __init__(self):
        self._job_cls_list: list[type[JobBase]] = []

    def append(self, job_cls: type[JobBase]) -> "JobList":
        if job_cls in self._job_cls_list:
            raise RuntimeError(f"Component {job_cls.name} already registered.")
        self._job_cls_list.append(job_cls)
        return self

    def __iter__(self):
        return iter(self._job_cls_list)

    def __len__(self):
        return len(self._job_cls_list)

    def __contains__(self, job_cls):
        return job_cls in self._job_cls_list

class JobContext:
    def __init__(self, config: Config, pool_executor: PoolExecutor):
        self._config = config
        self._instances: dict[tuple[type[JobBase], str], JobBase] = {}
        self.pool_executor = pool_executor

    def build(self, registry: JobList) -> "JobContext":
        for cls in registry:
            print(cls, cls.name)
            self._instances[(cls, cls.name)] = cls(self)
        return self

    def find(self, job_cls: type[JobBase], job_cls_name: Optional[str] = None) -> Optional:
        print("IN FIND", job_cls, job_cls.name)
        if job_cls_name is None:
            return self._instances.get((job_cls, job_cls.name))
        return self._instances.get((job_cls, job_cls_name))

class JobBase(LoggingMixin):
    state_type: ClassVar[type[JobState]] = JobState

    @classproperty
    def name(self) -> str:
        raise NotImplementedError

    @property
    def _entry(self) -> Callable[[], int | None]:
        raise NotImplementedError

    def set_runner(self, runner: JobRunner):
        self._runner = runner
        self.client_context = context_wrapper(self._runner.dag_context, env=self._runner)

    def __init__(self, context: JobContext):
        self.context = context
        self._runner = None
        self.client_context = None


class RunnerEnv:
    def __init__(self, job_id: str, context: ClientContext):
        self.job_id = job_id
        self.dag_context = context
        self.client_var: contextvars.ContextVar = self.dag_context.client_var_template(self.job_id)

class JobRunner(RunnerEnv):
    def __init__(self, job_id: str, context: ClientContext, execute_callable: Callable[[], int | None]):
        super().__init__(job_id, context)

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._finished = threading.Event()
        self.result: Any = None
        self.exception: Optional[BaseException] = None
        self.state: JobBase.state_type = None

        self._execute_callable = execute_callable

    def _prepare_for_execution(self, yt_client: yt.YtClient): # todo
        self._started.set()
    def _complete_execution(self, yt_client: yt.YtClient): # todo
        self._finished.set()

    @property
    def stop_event(self):
        return types.SimpleNamespace(
            is_set=self._stop_event.is_set,
            wait=self._stop_event.wait,
        )

    def run_job(self):
        self._run_job(self._execute_callable)

    def _thread_target(self, execute_callable: Callable[[], int | None]):
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
        return self.dag_context.run_in_context(env=self, func=_target)

    def _run_job(self, execute_callable: Callable[[], int | None], *, daemon: bool = True):
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Job already running.")

        self._stop_event.clear()

        t = threading.Thread(target=self._thread_target, args=(execute_callable,), daemon=daemon, name=f"job-{self.job_id}")
        self._thread = t
        t.start()

    def stop(self, timeout: Optional[float] = 5.0):
        self._stop_event.set()
        self._join(timeout=timeout)

    def _join(self, timeout: Optional[float] = None):
        if self._thread:
            self._thread.join(timeout=timeout)