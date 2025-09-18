import pytest

import logging
import threading
from datetime import datetime, timezone as tz
from unittest.mock import Mock

from job import JobContext, ClientState
from dag import DAG
from scheduler import Scheduler
from pool import Pool
from dagref import DagMeta, DagRef
from dagrun import DagRun
from taskrun import TaskRun

from conftest import simple_spec
from test_helpers import gen_id, scheduler_stub

logging.getLogger().setLevel(logging.CRITICAL)

def test_dagref_try_add_dag_idempotency(test_env_with_context, add_dag, clear_dag_ref):
    clear_dag_ref()

    success_first, did_first, mid_first = add_dag()
    assert success_first == True
    assert len(DagMeta.fetch_rows(dag_id=did_first)) == 1

    success_second, did_second, mid_second = add_dag()
    assert success_second == False
    assert did_second == did_first
    assert mid_second != mid_first

    assert len(DagMeta.fetch_rows(dag_id=did_second)) == 2

def test_scheduler_try_claim_atomicity_on_meta_update_failure(monkeypatch, test_env_with_context, add_dag_clean, scheduler_stub, env_client):
    dag_id, meta_id = add_dag_clean()

    insert_rows_orig = env_client.insert_rows
    def insert_rows_inject(path, rows, *args, **kwargs):
        if path == DagRef.meta_row_type.table_path:
            raise RuntimeError("Injected failure for DagMeta update")
        return insert_rows_orig(path, rows, *args, **kwargs)

    monkeypatch.setattr(env_client, "insert_rows", insert_rows_inject)

    with pytest.raises(Exception):
        scheduler_stub._try_claim_dagruns()

    runs = DagRun.fetch_rows(dag_id=dag_id)
    assert len(runs) == 0

    metas = DagMeta.fetch_rows(dag_id=dag_id)
    assert all(m.run_id is None for m in metas)

def test_concurrent_schedulers_try_claim_one_wins_no_artifacts(test_env_with_context, add_dag_clean, scheduler_stub, env_context):
    dag_id, meta_id = add_dag_clean()

    sched1 = scheduler_stub
    sched2 = scheduler_stub

    results = []
    exceptions = []

    def run_try_claim(sched):
        try:
            r = env_context(sched._try_claim_dagruns)
            results.append(r)
        except Exception as e:
            exceptions.append(e)

    thread1 = threading.Thread(target=run_try_claim, args=(sched1,))
    thread2 = threading.Thread(target=run_try_claim, args=(sched2,))
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    runs = DagRun.fetch_rows(dag_id=dag_id)
    assert len(runs) == 1
    assert runs[0].state == DagRun.state_type.SCHEDULED

    metas = DagMeta.fetch_rows(dag_id=dag_id)
    assert len(metas) == 1
    assert metas[0].run_id == runs[0].run_id

def test_try_claim_retry_on_transaction_conflict(monkeypatch, test_env_with_context, add_dag_clean, scheduler_stub, env_client):
    from utils import retry_on_transaction_conflict

    dag_id, meta_id = add_dag_clean()

    commit_calls = 0
    transaction_orig = env_client.Transaction

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

    monkeypatch.setattr(env_client, "Transaction", FakeTransaction)

    wrapped = retry_on_transaction_conflict(max_retries=1)(scheduler_stub._try_claim_dagruns)

    num_runs = wrapped()
    assert num_runs == 0

    runs = DagRun.fetch_rows(dag_id=dag_id)
    assert len(runs) == 1

    assert commit_calls == 2

def test_try_claim_idempotency(monkeypatch, test_env_with_context, add_dag_clean, scheduler_stub):
    dag_id, meta_id = add_dag_clean(clear_drs=True)

    scheduler_stub._try_claim_dagruns()
    scheduler_stub._try_claim_dagruns()

    runs = DagRun.fetch_rows(dag_id=dag_id)
    assert len(runs) == 1
    assert runs[0].state == DagRun.state_type.SCHEDULED

def test_queue_dag_run_atomicity(monkeypatch, test_env_with_context, env_client, gen_id):
    dag = DAG.from_spec_conf(spec=simple_spec, work_dir=None)

    dr = DagRun(run_id=gen_id, dag_id=gen_id, state=DagRun.state_type.SCHEDULED)

    insert_rows_orig = env_client.insert_rows
    def insert_rows_inject(path, rows, *args, **kwargs):
        if path == DagRun.table_path:
            raise RuntimeError("Injected failure for DagRun set_state")
        return insert_rows_orig(path, rows, *args, **kwargs)

    monkeypatch.setattr(env_client, "insert_rows", insert_rows_inject)

    dr.queue_dag_run(dag=dag)

    trs = TaskRun.fetch_rows(dag_run_id=dr.run_id, dag_id=dr.dag_id)
    assert len(trs) == 0

    assert dr.state == DagRun.state_type.SCHEDULED
    runs = DagRun.fetch_rows(run_id=dr.run_id, dag_id=dr.dag_id)
    assert len(runs) == 0

def test_update_state_handles_fetch_task_runs_exception(monkeypatch, gen_id):
    dr = DagRun(run_id=gen_id, dag_id=gen_id)
    monkeypatch.setattr(DagRun, "trs_scheduling_decisions",
                        lambda self, dag: (_ for _ in ()).throw(RuntimeError("Injected failure for trs_scheduling_decisions")))
    schedulable_trs = dr.update_state(Mock())
    assert schedulable_trs == []

def test_update_state_handles_set_state_failure(monkeypatch, test_env_with_context, gen_id):
    dr = DagRun(run_id=gen_id, dag_id=gen_id, state=DagRun.state_type.RUNNING)
    DagRun.create_rows(rows=dr)
    def fake_trs_scheduling_decisions(self, dag):
        return [ [], [], [], [] ]
    monkeypatch.setattr(DagRun, "trs_scheduling_decisions", fake_trs_scheduling_decisions)
    monkeypatch.setattr(DagRun, "set_state",
                        lambda self, state: (_ for _ in ()).throw(RuntimeError("Injected failure for set_state")))
    schedulable_trs = dr.update_state(Mock())
    assert schedulable_trs == []

    runs = DagRun.fetch_rows(run_id=dr.run_id, dag_id=dr.dag_id)
    assert runs[0].state == DagRun.state_type.RUNNING

def test_set_state_no_change_state_with_internal_error(monkeypatch, test_env_with_context, gen_id):
    state, start_date = DagRun.state_type.SCHEDULED, datetime.now(tz.utc).isoformat()
    dr = DagRun(run_id=gen_id, dag_id=gen_id, state=state, start_date=start_date)
    monkeypatch.setattr(DagRun, "set_state",
                        lambda self, **kwargs: (_ for _ in ()).throw(RuntimeError("Injected failure for set_state")))

    with pytest.raises(RuntimeError):
        dr.set_state(state=DagRun.state_type.RUNNING)

    assert dr.state == state and dr.start_date == start_date

def test_schedule_trs_handles_update_rows_exception(monkeypatch):
    def raise_update_rows(schedulable_trs):
        raise RuntimeError("Injected failure for update_rows")
    monkeypatch.setattr(TaskRun, "update_row", staticmethod(raise_update_rows))
    scheduled_trs = DagRun.schedule_trs([])
    assert scheduled_trs == 0

# todo разделить на шедулер/таскран/дагран