import pytest

from functools import partial
from unittest.mock import Mock

from job import ClientContext
from scheduler import Scheduler
from executor import Executor
from pool import Pool, ExecutionOptions
from taskrun import TaskRun
from dagrun import DagRun
from dagref import DagRef, TaskRef
from errors import DagInitializationError

import yt.wrapper as yt

@pytest.fixture
def gen_id() -> str:
    return yt.common.generate_uuid()

@pytest.fixture
def gen_id_f():
    def _f():
        return yt.common.generate_uuid()
    return _f

@pytest.fixture
def tr_factory(gen_id_f):
    def _f(run_id=None, task_id=None, dag_id=None, dagrun_id=None, state=None, operation_id=None,
           scheduled_at=None, queued_at=None, start_date=None, end_date=None,
           upsert_rows: bool = False,
    ):
        tr = TaskRun(row=TaskRun.row_type(run_id=run_id or gen_id_f(),
                                            task_id=task_id or f"task_{gen_id_f()}",
                                            dag_id=dag_id or gen_id_f(),
                                            dagrun_id=dagrun_id or gen_id_f(),
                                            state=state or TaskRun.state_type.QUEUED,
                                            operation_id=operation_id,
                                            scheduled_at=scheduled_at, queued_at=queued_at,
                                            start_date=start_date, end_date=end_date,
                                            ))
        if upsert_rows:
            TaskRun.upsert_rows(rows=tr)
        return tr
    return _f

@pytest.fixture
def dr_factory(gen_id_f):
    def _f(run_id=None, dag_id=None, state=None, scheduled_at=None, queued_at=None,
           start_date=None, end_date=None,
           upsert_rows: bool = False):
        dr = DagRun(run_id=run_id or gen_id_f(),
                       dag_id=dag_id or gen_id_f(),
                       state=state or TaskRun.state_type.SCHEDULED,
                       scheduled_at=scheduled_at, queued_at=queued_at,
                       start_date=start_date, end_date=end_date)
        if upsert_rows:
            DagRun.upsert_rows(rows=[dr])
        return dr
    return _f

@pytest.fixture
def dagref_factory(gen_id_f):
    def _f(dag_id=None, serialized_repr=None, payload_hash=None, created_at=None, upsert_rows: bool = False):
        ref = DagRef(dag_id=dag_id or gen_id_f(),
                     serialized_repr=serialized_repr or "serialized",
                     payload_hash=payload_hash or f"hash({serialized_repr or 'serialized'})")
        if created_at is not None:
            ref.created_at = created_at
        if upsert_rows:
            DagRef.upsert_rows(rows=ref)
        return ref
    return _f

@pytest.fixture
def taskref_factory(gen_id_f):
    def _f(dag_id=None, task_id=None, serialized_repr=None, payload_hash=None, created_at=None, upsert_rows: bool = False):
        ref = TaskRef(dag_id=dag_id or gen_id_f(),
                      task_id=task_id or f"task_{gen_id_f()}",
                      serialized_repr=serialized_repr or "serialized",
                      payload_hash=payload_hash or f"hash({serialized_repr or 'serialized'})")
        if created_at is not None:
            ref.created_at = created_at
        if upsert_rows:
            TaskRef.upsert_rows(rows=ref)
        return ref
    return _f

class SchedulerStub(Scheduler):
    def __init__(self, client_state: ClientContext, pool=None):
        if pool is None:
            pool = Pool(max_workers=1)
            pool.run()
        super().__init__(context=Mock(), pool=pool)

        job_runner = Mock(name="FakeSchedulerJobRunner")
        job_runner.__dict__.update(client_state.__dict__)
        self._init_state(job_runner)

@pytest.fixture
def scheduler_stub(env_client_context):
    return SchedulerStub(env_client_context)

@pytest.fixture
def scheduler_stub_factory(env_client_context):
    def _f(pool=None):
        return SchedulerStub(env_client_context, pool=pool)
    return _f

class ExecutorStub(Executor):
    def __init__(self, client_state: ClientContext, pool=None, task_start_interval=None):
        if pool is None:
            pool = Pool(max_workers=1)

        if task_start_interval is None:
            task_start_interval = 5.0

        super().__init__(context=Mock(), pool=pool, task_start_interval=task_start_interval)

        job_runner = Mock(name="FakeExecutorJobRunner")
        job_runner.__dict__.update(client_state.__dict__)
        self._init_state(job_runner)

@pytest.fixture
def executor_stub(env_client_context):
    return ExecutorStub(env_client_context)

@pytest.fixture
def executor_stub_factory(env_client_context):
    def _f(pool=None, task_start_interval=None):
        return ExecutorStub(env_client_context, pool=pool, task_start_interval=task_start_interval)
    return _f

class ImmediatePromise:
    def __init__(self, result):
        self.result = result
        self._completed = True
        self._callback = None

    def on_complete(self, func, owner_enqueue):
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

    def on_complete(self, func, owner_enqueue):
        self._callback = func
        return func

    def complete(self):
        self.result = self._fn()
        self._completed = True
        if self._callback is not None:
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
            try:
                return ImmediatePromise(result=context(fn))
            except Exception as e:
                return ImmediatePromise(result=e)

    def submit_or_execute(self, fn, *args, context, _execution_options: ExecutionOptions = None, **kwargs):
        return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)

    def complete_next(self):
        if not self.delayed_promises:
            raise RuntimeError("No delayed promises to complete")

        promise = self.delayed_promises.pop(0)
        promise.complete()
        return promise

class FakeDagInitializationError(DagInitializationError):
    pass

# ---------------------- UTILS ----------------------

def get_from_mock_call(mock, key, default=None):
    assert mock.called
    args, kwargs = mock.call_args
    if args:
        return args
    elif kwargs:
        return kwargs.get(key, default)
    else:
        assert False

def throw(exception: Exception):
    raise exception

def is_valid_yt_uid(uid: str) -> bool:
    parts = uid.split("-")
    return len(parts) == 4 and all(p.isalnum() for p in parts)