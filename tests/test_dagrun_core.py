import logging
import threading

from unittest.mock import Mock
from datetime import datetime, timezone as tz
import pytest

from dagrun import DagRun
from taskrun import TaskRun, TaskRunRow, TaskRunState
from rows_clients import DagRunClient

from conftest_helpers import (
    scheduler_stub_factory, tr_factory, dr_factory, ControlledPool, FakeDagInitializationError,
    throw, get_from_mock_call
)

logging.getLogger().setLevel(logging.CRITICAL)


def test_update_state_sets_dagrun_failed_and_skips_schedulable_tasks(test_env_with_context, tr_factory, dr_factory, mocker, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.RUNNING)

    trs = []
    for state in [TaskRunState.SCHEDULED, TaskRunState.QUEUED, TaskRunState.RUNNING,
                  TaskRunState.SUCCESS, TaskRunState.FAILED]:
        trs.append(tr_factory(task_id=str(state), dagrun_id=dr.run_id, dag_id=dr.dag_id, state=state))


    dr_set_state_calls = None
    def fake_dr_set_state(self, state, yt_client=None):
        nonlocal dr_set_state_calls
        dr_set_state_calls = state
        return self
    monkeypatch.setattr(DagRun, "set_state", fake_dr_set_state)
    monkeypatch.setattr(DagRun, "_get_ready_trs", lambda dag, sched_trs, finished_trs: sched_trs)
    monkeypatch.setattr(TaskRun, "fetch_rows", staticmethod(lambda **kwargs: trs))
    tr_set_state_mock = mocker.patch("taskrun.TaskRun.set_state", wraps=TaskRun.set_state)


    schedulable_trs = dr.update_state(Mock())
    assert isinstance(schedulable_trs, list) and len(schedulable_trs) == 0
    assert dr_set_state_calls == DagRun.state_type.FAILED

    assert tr_set_state_mock.called == 1
    rows_called = get_from_mock_call(tr_set_state_mock, "rows")
    assert len(rows_called) == 2
    assert all(isinstance(row, TaskRunRow) for row, _, _ in rows_called)
    assert all(params.get("state") == TaskRunState.SKIPPED for _, params, _ in rows_called)
    assert any(row.task_id == str(TaskRunState.SCHEDULED) for row, _, _ in rows_called)
    assert any(row.task_id == str(TaskRunState.QUEUED) for row, _, _ in rows_called)

def test_update_state_atomicity_on_tr_set_state_failure(test_env_with_context, tr_factory, dr_factory, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.RUNNING, upsert_rows=True)

    tr_scheduled = tr_factory(task_id="a", dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.SCHEDULED)
    tr_failed = tr_factory(task_id="b", dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.FAILED)
    TaskRun.upsert_rows(rows=[tr_scheduled, tr_failed])

    dr_set_state_calls = None
    def fake_dr_set_state(self, state, yt_client=None):
        nonlocal dr_set_state_calls
        dr_set_state_calls = state
        return self
    monkeypatch.setattr(DagRun, "set_state", fake_dr_set_state)
    monkeypatch.setattr(DagRun, "_get_ready_trs", lambda dag, sched_trs, finished_trs: sched_trs)
    monkeypatch.setattr(TaskRun, "set_state",
                        staticmethod(lambda rows=None: throw(RuntimeError("Injected"))))

    with pytest.raises(RuntimeError):
        _ = dr.update_state(Mock(task_ids=[tr_scheduled.task_id, tr_failed.task_id]))

    assert dr_set_state_calls is None

    drs = DagRun.fetch_rows(run_id=dr.run_id)
    assert len(drs) == 1 and drs[0].state == DagRun.state_type.RUNNING

    trs = TaskRun.fetch_rows(run_id=[tr_scheduled.run_id, tr_failed.run_id])
    assert (len(trs) == 2 and any(tr.state == TaskRunState.SCHEDULED for tr in trs)
                          and any(tr.state == TaskRunState.FAILED for tr in trs))

def test_update_state_atomicity_on_dr_set_state_failure(test_env_with_context, tr_factory, dr_factory, mocker, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.RUNNING, upsert_rows=True)

    tr_scheduled = tr_factory(task_id="a", dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.SCHEDULED)
    tr_failed = tr_factory(task_id="b", dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.FAILED)
    TaskRun.upsert_rows(rows=[tr_scheduled, tr_failed])

    tr_set_state_mock = mocker.patch("taskrun.TaskRun.set_state", wraps=TaskRun.set_state)
    monkeypatch.setattr(DagRun, "set_state",
                        lambda self, state, yt_client=None: throw(RuntimeError("Injected")))
    monkeypatch.setattr(DagRun, "_get_ready_trs", lambda dag, sched_trs, finished_trs: sched_trs)

    with pytest.raises(RuntimeError):
        _ = dr.update_state(Mock(task_ids=[tr_scheduled.task_id, tr_failed.task_id]))

    assert tr_set_state_mock.called == 1
    rows_called = get_from_mock_call(tr_set_state_mock, "rows")

    assert len(rows_called) == 1
    row, params, _ = rows_called[0]
    assert isinstance(row, TaskRunRow)
    assert params.get("state") == TaskRunState.SKIPPED and row.task_id == "a"

    drs = DagRun.fetch_rows(run_id=dr.run_id)
    assert len(drs) == 1 and drs[0].state == DagRun.state_type.RUNNING

    trs = TaskRun.fetch_rows(run_id=[tr_scheduled.run_id, tr_failed.run_id])
    assert (len(trs) == 2 and any(tr.state == TaskRunState.SCHEDULED for tr in trs)
                          and any(tr.state == TaskRunState.FAILED for tr in trs))

def test_set_state_no_change_state_on_internal_error(test_env_with_context, env_client, dr_factory, monkeypatch):
    state, start_date = DagRun.state_type.SCHEDULED, datetime.now(tz.utc).isoformat()
    dr = dr_factory(state=state, start_date=start_date)
    monkeypatch.setattr(env_client, "insert_rows",
                        lambda *args, **kwargs: throw(RuntimeError("Injected")))

    with pytest.raises(RuntimeError):
        dr.set_state(state=DagRun.state_type.RUNNING)

    assert dr.state == state and dr.start_date == start_date

def test_queue_run_atomic_skips_if_already_queued(test_env_with_context, dr_factory, scheduler_stub_factory, mocker):
    pool = ControlledPool()
    pool._mark_as_deferred_fn(DagRunClient.queue_run_atomic)
    sched = scheduler_stub_factory(pool=pool)
    sched.determine_shard_options = Mock()
    sched._get_dag = Mock()

    dr = dr_factory(state=DagRun.state_type.SCHEDULED, upsert_rows=True)

    mocker.patch.object(DagRunClient, "get", return_value=Mock())
    init_trs_for_run_mock = mocker.patch.object(DagRun, "_init_task_runs_for_run", return_value=[])
    mocker.patch.object(DagRunClient, "get_scheduled_dag_runs_to_queue", return_value=[dr])

    sched._queue_scheduled_dagruns()

    assert len(pool.delayed_promises) == 1

    dr.set_state(state=DagRun.state_type.QUEUED)
    pool.complete_next()

    init_trs_for_run_mock.assert_not_called()

def test_queue_run_atomic_atomicity_on_insert_failure(test_env_with_context, tr_factory, dr_factory, mocker, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.SCHEDULED, upsert_rows=True)
    tr = tr_factory(task_id="a", dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.SCHEDULED)

    monkeypatch.setattr(DagRun, "_init_task_runs_for_run", lambda self, dag: [tr])
    tr_upsert_rows_mock = mocker.patch("taskrun.TaskRun.upsert_rows", wraps=TaskRun.upsert_rows)
    monkeypatch.setattr(DagRun, "set_state", lambda self, state: throw(RuntimeError("Injected")))

    with pytest.raises(RuntimeError):
        dr.queue_run_atomic(Mock())

    assert tr_upsert_rows_mock.called
    called_rows = get_from_mock_call(tr_upsert_rows_mock, "rows")
    assert called_rows == [tr]

    trs = TaskRun.fetch_rows(run_id=tr.run_id)
    assert len(trs) == 0

def test_queue_run_atomic_idempotent_on_retries(test_env_with_context, dr_factory):
    dr = dr_factory(state=DagRun.state_type.SCHEDULED, upsert_rows=True)

    task_a = Mock(task_id="a", dag_id=dr.dag_id)
    task_b = Mock(task_id="b", dag_id=dr.dag_id)
    dag = Mock(dag_id=dr.dag_id, task_ids=["a", "b"], tasks=[task_a, task_b], roots=[task_a])

    dr.queue_run_atomic(dag)

    trs = TaskRun.fetch_rows(dagrun_id=dr.run_id, dag_id=dr.dag_id)
    assert len(trs) == 2
    assert any(tr.task_id == "a" and tr.state == TaskRunState.QUEUED for tr in trs)
    assert any(tr.task_id == "b" and tr.state == TaskRunState.SCHEDULED for tr in trs)

    dr.queue_run_atomic(dag)

    trs_after_retry = TaskRun.fetch_rows(dagrun_id=dr.run_id, dag_id=dr.dag_id)
    assert trs == trs_after_retry

def test_queue_run_atomic_concurrent_one_succeeds_other_conflict(test_env_with_context, env_context, dr_factory):
    from yt.wrapper.errors import YtTabletTransactionLockConflict

    dr = dr_factory(state=DagRun.state_type.SCHEDULED, upsert_rows=True)

    task_a = Mock(task_id="a", dag_id=dr.dag_id)
    dag = Mock(dag_id=dr.dag_id, task_ids=["a"], tasks=[task_a], roots=[task_a])

    results = []
    def _thread_target():
        try:
            env_context(lambda: dr.queue_run_atomic(dag))
            results.append("ok")
        except YtTabletTransactionLockConflict:
            results.append("conflict")

    t1 = threading.Thread(target=_thread_target)
    t2 = threading.Thread(target=_thread_target)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert "ok" in results
    assert ("conflict" in results) or (results.count("ok") == 2)