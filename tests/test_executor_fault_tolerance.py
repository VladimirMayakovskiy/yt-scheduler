import pytest

import logging
from unittest.mock import Mock

from taskrun import TaskRun, TaskRunState
from dagrun import DagRun
from task import Task
from errors import DagInitializationError
from rows_clients import TaskRunClient

from conftest_helpers import (
    gen_id_f, executor_stub, executor_stub_factory, dr_factory, tr_factory, taskref_factory,
    ControlledPool, FakeDagInitializationError, get_from_mock_call, throw
)

logging.getLogger().setLevel(logging.CRITICAL)

def test_get_taskrun_manifest_handles_serialization_failure_and_client_exceptions(test_env_with_context, tr_factory, dr_factory,
                                                                                  taskref_factory, monkeypatch):
    from executor import _get_task_run_manifest
    from rows_clients import DagRunClient

    dr = dr_factory(state=DagRun.state_type.QUEUED, upsert_rows=True)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.QUEUED)
    _ = taskref_factory(dag_id=tr.dag_id, task_id=tr.task_id, serialized_repr="bad", upsert_rows=True)

    monkeypatch.setattr(Task, "from_serialized_repr",
                staticmethod(lambda *args, **kwargs: throw(FakeDagInitializationError("Injected"))))

    task, dagrun = _get_task_run_manifest(tr)

    assert task is None
    assert dagrun.state == DagRun.state_type.FAILED

    fetched_dr = DagRun.fetch_rows(run_id=dr.run_id)
    assert len(fetched_dr) == 1
    assert fetched_dr[0].state == DagRun.state_type.FAILED


    DagRun.upsert_rows(rows=dr)

    monkeypatch.setattr(DagRunClient, "set_state",
                staticmethod(lambda *args, **kwargs: throw(RuntimeError("Injected"))))

    task, dagrun = _get_task_run_manifest(tr)
    assert dagrun.state == DagRun.state_type.QUEUED
    fetched_dr = DagRun.get(run_id=dr.run_id)
    assert fetched_dr.state == DagRun.state_type.QUEUED

def test_get_taskrun_manifest_returns_task_when_taskref_parses(tr_factory, taskref_factory, monkeypatch):
    from executor import _get_task_run_manifest

    tr = tr_factory(state=TaskRunState.QUEUED)
    _ = taskref_factory(dag_id=tr.dag_id, task_id=tr.task_id, serialized_repr="bad", upsert_rows=True)

    monkeypatch.setattr(Task, "from_serialized_repr",
                        staticmethod(lambda *args, **kwargs: Mock()))

    task, dagrun = _get_task_run_manifest(tr)

    assert task is not None and dagrun is None

def test_start_taskrun_marks_skipped_on_dagrun_failure(test_env_with_context, executor_stub, tr_factory, dr_factory,
                                                       monkeypatch):
    dr = dr_factory(state=DagRun.state_type.FAILED)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.QUEUED, upsert_rows=True)

    monkeypatch.setattr("executor._get_task_run_manifest",
                        staticmethod(lambda *args, **kwargs: (None, dr)))

    executor_stub._start_taskrun(tr)

    assert tr.state == TaskRunState.SKIPPED and tr.operation_id is None
    fetched_tr = TaskRun.get(run_id=tr.run_id)

    assert fetched_tr.state == TaskRunState.SKIPPED and fetched_tr.operation_id is None

def test_start_taskrun_preserves_failed_state_if_dagrun_and_taskrun_failed(test_env_with_context, executor_stub,
                                                                           tr_factory, dr_factory, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.FAILED)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.FAILED, upsert_rows=True)

    monkeypatch.setattr("executor._get_task_run_manifest",
                        staticmethod(lambda *args, **kwargs: (None, dr)))

    executor_stub._start_taskrun(tr)

    assert tr.state == TaskRunState.FAILED
    fetched_tr = TaskRun.get(run_id=tr.run_id)

    assert fetched_tr.state == TaskRunState.FAILED and fetched_tr.operation_id is None

def test_start_taskrun_marks_failed_on_start_operation_exception(test_env_with_context, executor_stub,
                                                                 tr_factory, dr_factory, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.RUNNING)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.QUEUED, upsert_rows=True)

    task = Mock(run_operation=lambda *args, **kwargs: throw(RuntimeError("Injected")))
    monkeypatch.setattr("executor._get_task_run_manifest",
                        staticmethod(lambda *args, **kwargs: (task, dr)))

    executor_stub._start_taskrun(tr)

    assert tr.state == TaskRunState.FAILED and tr.operation_id is None
    fetched_tr = TaskRun.get(run_id=tr.run_id)
    assert fetched_tr.state == TaskRunState.FAILED and fetched_tr.operation_id is None
    assert tr.run_id not in executor_stub._pending_operations
    assert tr.run_id not in executor_stub._pending_task_runs

def test_start_taskrun_records_operation_id_and_pending_state(test_env_with_context, gen_id_f, executor_stub, dr_factory, tr_factory, monkeypatch):
    dr = dr_factory(state=DagRun.state_type.RUNNING)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.QUEUED, upsert_rows=True)

    operation_id = gen_id_f()
    task = Mock(run_operation=lambda *args, **kwargs: Mock(id=operation_id))
    monkeypatch.setattr("executor._get_task_run_manifest",
                        staticmethod(lambda *args, **kwargs: (task, dr)))

    executor_stub._start_taskrun(tr)

    assert tr.state == TaskRunState.RUNNING and tr.operation_id == operation_id
    fetched_tr = TaskRun.get(run_id=tr.run_id)
    assert fetched_tr.state == TaskRunState.RUNNING and fetched_tr.operation_id == operation_id
    assert tr.run_id in executor_stub._pending_operations
    assert tr.run_id in executor_stub._pending_task_runs

def test_start_taskrun_updates_via_client_when_local_task_state_mismatch(test_env_with_context, gen_id_f, executor_stub,
                                                                         dr_factory, tr_factory, monkeypatch, mocker):
    dr = dr_factory(state=DagRun.state_type.RUNNING)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.FAILED, upsert_rows=True)

    operation_id = gen_id_f()
    task = Mock(run_operation=lambda *args, **kwargs: Mock(id=operation_id))
    monkeypatch.setattr("executor._get_task_run_manifest",
                        staticmethod(lambda *args, **kwargs: (task, dr)))

    tr_set_state_mock = mocker.patch("rows_clients.TaskRunClient.set_state", wraps=TaskRunClient.set_state)

    executor_stub._start_taskrun(tr)

    _, updates, _ = get_from_mock_call(tr_set_state_mock, "rows")
    assert updates.get("state") == TaskRunState.RUNNING and updates.get("operation_id") == operation_id

    assert tr.state == TaskRunState.FAILED and tr.operation_id is None
    fetched_tr = TaskRun.get(run_id=tr.run_id)
    assert fetched_tr.state == TaskRunState.FAILED and fetched_tr.operation_id is None
    assert tr.run_id in executor_stub._pending_operations
    assert tr.run_id in executor_stub._pending_task_runs

def test_poll_pending_operations_retries_and_updates_states(test_env_with_context, env_client, gen_id_f, executor_stub_factory,
                                                            tr_factory, monkeypatch):
    executor = executor_stub_factory(pool=ControlledPool())

    dagrun_id, dag_id = gen_id_f(), gen_id_f()
    tr1 = tr_factory(dagrun_id=dagrun_id, dag_id=dag_id, state=TaskRunState.RUNNING)
    tr2 = tr_factory(dagrun_id=dagrun_id, dag_id=dag_id, state=TaskRunState.RUNNING)
    TaskRun.upsert_rows(rows=[tr1, tr2])

    get_operation_state_calls = 0
    def fake_get_operation_state(operation):
        nonlocal get_operation_state_calls
        get_operation_state_calls += 1
        if get_operation_state_calls == 1:
            raise RuntimeError("Injected")
        return Mock(is_finished=lambda: True, is_unsuccessfully_finished=lambda: False)
    monkeypatch.setattr(env_client, "get_operation_state", fake_get_operation_state)
    executor._pending_operations = {tr1.run_id: "tr1operation", tr2.run_id: "tr2operation"}

    executor._poll_pending_operations()

    assert get_operation_state_calls == 2

    trs = TaskRun.fetch_rows(run_id=[tr1.run_id, tr2.run_id])
    assert len(trs) == 2
    assert any(tr.state == TaskRunState.RUNNING for tr in trs)
    assert any(tr.state == TaskRunState.SUCCESS for tr in trs)

    assert tr1.run_id in executor._pending_operations
    assert tr2.run_id not in executor._pending_operations

def test_orphan_cleanup_aborts_and_marks_taskruns(executor_stub_factory, tr_factory, dr_factory, monkeypatch, mocker):
    from scheduler import ShardingOptions

    executor = executor_stub_factory(pool=ControlledPool())

    dr = dr_factory(state=DagRun.state_type.FAILED, upsert_rows=True)
    tr_running = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.RUNNING, operation_id="X")
    tr_queued = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.QUEUED)
    TaskRun.upsert_rows(rows=[tr_running, tr_queued])



    mock = mocker.patch("task.Task.abort_operation", wraps=Task.abort_operation)

    monkeypatch.setattr(executor, "determine_shard_options", lambda: ShardingOptions(shard_index=0, num_shards=1, num_virtual_shards=256))

    executor._orphan_cleanup()

    ret = get_from_mock_call(mock, "operation")
    try:
        abort_operation, reason = ret
    except Exception:
        abort_operation = ret


    assert abort_operation == tr_running.operation_id
    fetched_tr_running = TaskRun.get(run_id=tr_running.run_id)
    assert fetched_tr_running.state == TaskRunState.FAILED

    fetched_tr_queued = TaskRun.get(run_id=tr_queued.run_id)
    assert fetched_tr_queued.state == TaskRunState.SKIPPED

def test_orphan_cleanup_skips_client_calls_when_deferred(tr_factory, executor_stub_factory, dr_factory, monkeypatch, mocker):
    from scheduler import ShardingOptions

    pool = ControlledPool()
    pool._mark_as_deferred_fn(TaskRunClient.set_state)
    executor = executor_stub_factory(pool=pool)

    dr = dr_factory(state=DagRun.state_type.FAILED, upsert_rows=True)
    tr = tr_factory(dagrun_id=dr.run_id, dag_id=dr.dag_id, state=TaskRunState.FAILED, upsert_rows=True)

    monkeypatch.setattr("taskrun.TaskRun.get_orphaned_task_runs", lambda *args, **kwargs: [tr])

    executor_stub_factory = mocker.patch("rows_clients.TaskRunClient.set_state", wraps=TaskRunClient.set_state)

    executor._orphan_cleanup()

    assert executor_stub_factory.call_count == 0