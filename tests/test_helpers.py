import pytest

from functools import partial
from unittest.mock import Mock

from job import ClientState
from scheduler import Scheduler
from executor import Executor
from pool import Pool, ExecutionOptions

import yt.wrapper as yt

@pytest.fixture
def gen_id() -> str:
    return yt.common.generate_uuid()

@pytest.fixture
def gen_id_f():
    def _f():
        return yt.common.generate_uuid()
    return _f

def is_valid_yt_uid(uid: str) -> bool:
    parts = uid.split("-")
    return len(parts) == 4 and all(p.isalnum() for p in parts)

class SchedulerStub(Scheduler):
    def __init__(self, client_state: ClientState, pool=None):
        if pool is None:
            pool = Pool(max_workers=1)
        super().__init__(context=Mock(), pool=pool)

        job_runner = Mock(name="FakeSchedulerJobRunner")
        job_runner.__dict__.update(client_state.__dict__)
        self._init_state(job_runner)

@pytest.fixture
def scheduler_stub(env_client_state):
    return SchedulerStub(env_client_state)

@pytest.fixture
def scheduler_stub_f(env_client_state):
    def _f(pool=None):
        return SchedulerStub(env_client_state, pool=pool)
    return _f

class ExecutorStub(Executor):
    def __init__(self, client_state: ClientState, pool=None, task_start_interval=None):
        if pool is None:
            pool = Pool(max_workers=1)

        if task_start_interval is None:
            task_start_interval = 5.0

        super().__init__(context=Mock(), pool=pool, task_start_interval=task_start_interval)

        job_runner = Mock(name="FakeExecutorJobRunner")
        job_runner.__dict__.update(client_state.__dict__)
        self._init_state(job_runner)

@pytest.fixture
def executor_stub(env_client_state):
    return ExecutorStub(env_client_state)

@pytest.fixture
def executor_stub_f(env_client_state):
    def _f(pool=None, task_start_interval=None):
        return ExecutorStub(env_client_state, pool=pool, task_start_interval=task_start_interval)
    return _f

class ImmediatePromise:
    def __init__(self, result):
        self.result = result
        self._completed = True
        self._callback = None

    def on_complete(self, func):
        try:
            self._callback = func
            self._callback()
        except Exception:
            raise
        return func

class DelayedPromise:
    def __init__(self, fn):
        self.result = None
        self._completed = False
        self._callback = None

        self._fn = fn

    def on_complete(self, func):
        self._callback = func
        return func

    def complete(self):
        self.result = self._fn()
        self._completed = True
        self._callback()


class ControlledPool:
    def __init__(self):
        self.delayed_promises = []
        self._deferred_fns = set()

    def _mark_as_deferred_fn(self, fn):
        self._deferred_fns.add(getattr(fn, "__name__", None) or str(fn))

    def submit(self, fn, *args, context, _execution_options: ExecutionOptions = None, **kwargs):
        _execution_options = _execution_options or {}
        force_sync = _execution_options.get("force_sync", True) # todo add test options
        fn_name = getattr(fn, "__name__", None) or str(fn)
        fn = partial(fn, *args, **kwargs)
        if fn_name in self._deferred_fns:
            promise = DelayedPromise(fn=lambda: context(fn))
            self.delayed_promises.append(promise)
            return promise
        else:
            return ImmediatePromise(result=context(fn))

    def submit_or_execute(self, fn, *args, context, _execution_options: ExecutionOptions = None, **kwargs):
        return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)

    def complete_next(self):
        if not self.delayed_promises:
            raise RuntimeError("No delayed promises to complete")

        promise = self.delayed_promises.pop(0)
        promise.complete()
        return promise