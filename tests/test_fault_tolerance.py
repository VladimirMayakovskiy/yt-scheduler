from __future__ import annotations
import pytest

import threading
from src.commands import add_dag
from src.dagref import DagRef
from src.config import Config
from src.job import RunnerEnv, ClientContext
from src.dag import DAG
from src.yt_wrapper import context_wrapper, ContextWrapper
from src.scheduler import Scheduler
from src.dagrun import DagRun
from src.pool_executor import PoolExecutor
from unittest.mock import Mock

import yt.wrapper as yt

# -------------- Session Fixtures --------------

@pytest.fixture(scope="session")
def prepared_config() -> Config:
    config = Config()
    config.set_proxy(value="localhost:8000")
    return config

@pytest.fixture(scope="session")
def env(prepared_config) -> RunnerEnv :
    env = RunnerEnv(job_id="test", context=ClientContext(config=prepared_config))
    return env

@pytest.fixture(scope="session")
def yt_client(env) -> yt.YtClient:
    return env.dag_context.create_client()

@pytest.fixture(scope="session")
def make_context_wrapper(env) -> ContextWrapper:
    return context_wrapper(env.dag_context, env=env)

@pytest.fixture(scope="session", autouse=True)
def init(yt_client):
    if not yt_client.exists(work_dir):
        yt_client.create("map_node", work_dir, force=True)


@pytest.fixture(scope="session")
def add_dag_fixture(prepared_config):
    def _f():
        return add_dag(config=prepared_config, spec=simple_spec, work_dir=work_dir)
    return _f

# -------------- Clear Fixtures --------------

@pytest.fixture
def clear_dag_ref_by_spec(make_context_wrapper, yt_client, clear_dag_meta):
    def _f(spec: dict) -> str | None:
        row = DagRef.get_serialized_dag(DAG.from_spec_conf(spec=spec, work_dir=work_dir, context_wrapper=make_context_wrapper))
        dag_id = DagRef._check_dag_exists_by_spec(row.payload_hash, yt_client)
        if dag_id:
            yt_client.delete_rows(DagRef.table_path, [{"dag_id": dag_id}])
            clear_dag_meta(dag_id)
        return dag_id
    return _f

@pytest.fixture
def clear_dag_meta(yt_client):
    def _f(dag_id: str, exclude: set[str] = set()):
        metas = DagRef.get_meta(dag_id, yt_client=yt_client)
        yt_client.delete_rows(DagRef.meta_row_type.table_path, [{"id": meta.id} for meta in metas if meta.id not in exclude])
    return _f

@pytest.fixture
def clear_dag_runs(make_context_wrapper, yt_client):
    def _f(dag_id: str):
        runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
        yt_client.delete_rows(DagRun.table_path, [{"run_id": r.run_id} for r in runs])
    return _f

@pytest.fixture
def clear_dag_meta_and_runs(clear_dag_meta, clear_dag_runs):
    def _f(dag_id: str, exclude_meta: set[str] = set()):
        clear_dag_meta(dag_id, exclude=exclude_meta)
        clear_dag_runs(dag_id)
    return _f

# -------------- Tests --------------

work_dir = "//tmp/test_fault_tolerance"

simple_spec = {
    "steps": {
        "just_cat": {
            "operation_type": "map",
            "pool": "my_cool_pool",
            "job_count": 10,
            "input_table_paths": [ "input_table1", "input_table2" ],
            "output_table_paths": [ "output_table" ],
            "mapper": {
                "command": "cat"
            }
        }
    }
}

class SchedulerStub(Scheduler):
    def __init__(self, env: RunnerEnv, executor=None, pool_executor=None):
        if executor is None:
            executor = Mock(name="FakeExecutor")

        if pool_executor is None:
            pool_executor = PoolExecutor(max_workers=1)

        context = Mock(pool_executor=pool_executor, find=Mock(return_value=executor))
        super().__init__(context=context)

        runner = Mock(name="FakeJobRunner")
        runner.__dict__.update(env.__dict__)
        self.set_runner(runner)


def test_dagref_try_add_dag_idempotent(add_dag_fixture, make_context_wrapper, clear_dag_ref_by_spec):
    """
    DagRef.try_add_dag should not create duplicate entries for same dag:
    i.e. should return (False, dag_id) if dag with this hash already exists in the database.
    """

    clear_dag_ref_by_spec(simple_spec)

    ret, dag_id, meta_id = add_dag_fixture()

    assert ret == True
    assert make_context_wrapper(DagRef.get, dag_id=dag_id) is not None
    assert len(make_context_wrapper(DagRef.get_meta, dag_id=dag_id)) == 1

    second_ret, second_dag_id, second_meta_id = add_dag_fixture()

    assert second_ret == False
    assert second_dag_id == dag_id
    assert second_meta_id != meta_id

    assert len(make_context_wrapper(DagRef.get_meta, dag_id=dag_id)) == 2


def test_try_claim_atomicity_on_meta_update_failure(add_dag_fixture, make_context_wrapper, env, yt_client, clear_dag_meta):
    """If update run_id in DagMeta fails, no DagRun or run_id in DagMeta should persist."""

    _, dag_id, meta_id = add_dag_fixture()
    clear_dag_meta(dag_id, exclude={meta_id})
    assert make_context_wrapper(DagRef.get, dag_id) is not None

    scheduler = SchedulerStub(env)

    insert_rows_orig = yt_client.insert_rows
    def insert_rows_inject(path, rows, *args, **kwargs):
        if path == DagRef.meta_row_type.table_path:
            raise RuntimeError("Injected failure for DagMeta update")
        return insert_rows_orig(path, rows, *args, **kwargs)

    try:
        yt_client.insert_rows = insert_rows_inject
        with pytest.raises(Exception):
            make_context_wrapper(scheduler._try_claim_dagruns)
    finally:
        yt_client.insert_rows = insert_rows_orig

    runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
    assert len(runs) == 0

    metas = make_context_wrapper(DagRef.get_meta, dag_id)
    assert all(m.run_id is None for m in metas)


def test_try_claim_atomicity_on_dagrun_insert_failure(add_dag_fixture, make_context_wrapper, env, yt_client, clear_dag_meta):
    """If dagrun creation fails, no DagRun should persist and run_id in DagMeta should remain NULL."""

    _, dag_id, meta_id = add_dag_fixture()
    clear_dag_meta(dag_id, exclude={meta_id})
    assert make_context_wrapper(DagRef.get, dag_id=dag_id) is not None

    scheduler = SchedulerStub(env)

    insert_rows_orig = yt_client.insert_rows
    def insert_rows_inject(path, rows, *args, **kwargs):
        if path == DagRun.table_path:
            raise RuntimeError("Injected failure for DagRun insert")
        return insert_rows_orig(path, rows, *args, **kwargs)

    try:
        yt_client.insert_rows = insert_rows_inject
        with pytest.raises(Exception):
            make_context_wrapper(scheduler._try_claim_dagruns)
    finally:
        yt_client.insert_rows = insert_rows_orig

    runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
    assert len(runs) == 0

    metas = make_context_wrapper(DagRef.get_meta, dag_id=dag_id)
    assert all(m.run_id is None for m in metas)


def test_concurrent_schedulers_try_claim_one_wins_no_artifacts(add_dag_fixture, make_context_wrapper, env, clear_dag_meta_and_runs):
    _, dag_id, meta_id = add_dag_fixture()
    assert make_context_wrapper(DagRef.get, dag_id=dag_id) is not None
    clear_dag_meta_and_runs(dag_id=dag_id, exclude_meta={meta_id})

    scheduler1 = SchedulerStub(env)
    scheduler2 = SchedulerStub(env)

    results = []
    exceptions = []

    def run_try_claim(sched):
        try:
            r = make_context_wrapper(sched._try_claim_dagruns)
            results.append(r)
        except Exception as e:
            exceptions.append(e)

    thread1 = threading.Thread(target=run_try_claim, args=(scheduler1,))
    thread2 = threading.Thread(target=run_try_claim, args=(scheduler2,))
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
    assert len(runs) == 1
    assert runs[0].state == DagRun.state_type.SCHEDULED

    metas = make_context_wrapper(DagRef.get_meta, dag_id=dag_id)
    assert len(metas) == 1
    assert metas[0].run_id == runs[0].run_id


def test_try_claim_retry_on_transaction_conflict(monkeypatch, add_dag_fixture, make_context_wrapper, env, yt_client, clear_dag_meta_and_runs):
    from utils import retry_on_transaction_conflict

    _, dag_id, meta_id = add_dag_fixture()
    clear_dag_meta_and_runs(dag_id=dag_id, exclude_meta={meta_id})
    assert make_context_wrapper(DagRef.get, dag_id=dag_id) is not None

    scheduler = SchedulerStub(env)

    commit_calls = 0
    transaction_orig = yt_client.Transaction

    class FakeTransaction:
        def __init__(self, *args, **kwargs):
            self._real_tx = transaction_orig(*args, **kwargs)

        def __enter__(self):
            return self._real_tx.__enter__()

        def __exit__(self, exc_type, exc_val, tb):
            try:
                return self._real_tx.__exit__(exc_type, exc_val, tb)
            finally:
                nonlocal commit_calls
                commit_calls += 1
                if commit_calls == 1:
                    raise RuntimeError("Transaction lock conflict injected")
                return None

        def __getattr__(self, item):
            return getattr(self._real_tx, item)

    monkeypatch.setattr(yt_client, "Transaction", FakeTransaction)

    wrapped = retry_on_transaction_conflict(max_retries=1)(scheduler._try_claim_dagruns)

    num_runs = make_context_wrapper(wrapped)
    assert num_runs == 0

    runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
    assert len(runs) == 1

    assert commit_calls == 2

    monkeypatch.setattr(yt_client, "Transaction", transaction_orig)

def test_try_claim_idempotency(add_dag_fixture, make_context_wrapper, env, clear_dag_meta_and_runs):
    _, dag_id, meta_id = add_dag_fixture()
    clear_dag_meta_and_runs(dag_id=dag_id, exclude_meta={meta_id})
    assert make_context_wrapper(DagRef.get, dag_id=dag_id) is not None

    scheduler = SchedulerStub(env)

    make_context_wrapper(scheduler._try_claim_dagruns)
    make_context_wrapper(scheduler._try_claim_dagruns)

    runs = make_context_wrapper(DagRun.fetch_dag_runs, dag_id=dag_id)
    assert len(runs) == 1
    assert runs[0].state == DagRun.state_type.SCHEDULED