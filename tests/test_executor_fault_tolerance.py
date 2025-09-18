import pytest

import logging
from unittest.mock import patch, Mock

from taskrun import TaskRun
from dagrun import DagRun

from test_helpers import gen_id, ControlledPool, executor_stub_f, is_valid_yt_uid

logging.getLogger().setLevel(logging.CRITICAL)

def test_executor_start_task_runs_prevent_double_start_operation(monkeypatch, test_env_with_context, add_dag_clean, gen_id, executor_stub_f, env_client):
    dag_id, meta_id = add_dag_clean(clear_trs=True)
    row = TaskRun.row_type(task_id="just_cat", run_id=gen_id, dag_run_id=gen_id, dag_id=dag_id, state=TaskRun.state_type.QUEUED)
    tr = TaskRun(row=row)

    TaskRun.create_rows(rows=row)

    pool = ControlledPool()
    executor = executor_stub_f(pool=pool, task_start_interval=0.0)

    call_count = 0
    insert_rows_orig = env_client.insert_rows
    def insert_rows_inject(path, rows, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1 and path == TaskRun.table_path:
            raise RuntimeError("Injected failure for TaskRun row update")
        return insert_rows_orig(path, rows, *args, **kwargs)

    monkeypatch.setattr(env_client, "insert_rows", insert_rows_inject)

    executor._start_task_runs()

    runs = TaskRun.fetch_rows(run_id=tr.run_id, dag_id=tr.dag_id)
    assert len(runs) == 1
    assert len(executor._pending_task_runs) == 1
    assert len(executor._pending_operations) == 1
    assert runs[0].state == TaskRun.state_type.FAILED

    assert tr.operation_id is None

    operation = executor._pending_operations[tr.run_id]
    assert is_valid_yt_uid(operation)

    assert env_client.get_operation_state(operation) is not None

    TaskRun.update_row(row=row, state=TaskRun.state_type.QUEUED)
    executor._start_task_runs()

    runs = TaskRun.fetch_rows(run_id=tr.run_id, dag_id=tr.dag_id)
    assert len(runs) == 1
    assert len(executor._pending_task_runs) == 1
    assert len(executor._pending_operations) == 1
    assert runs[0].state == TaskRun.state_type.RUNNING
    assert runs[0].operation_id == operation

def test_executor_start_task_runs_non_runnable_tasks_skipped(monkeypatch, add_dag_clean, gen_id, executor_stub_f):
    dag_id, meta_id = add_dag_clean(clear_trs=True)
    row = TaskRun.row_type(task_id="just_cat", run_id=gen_id, dag_run_id=gen_id, dag_id=dag_id, state=TaskRun.state_type.QUEUED)
    tr = TaskRun(row=row)
    TaskRun.create_rows(rows=row)

    pool = ControlledPool()
    executor = executor_stub_f(pool=pool)

    monkeypatch.setattr("src.executor.TaskRun.get_executable_task_runs_to_queue", staticmethod(lambda: [tr]))

    with patch.object(TaskRun, "as_operator", return_value=None) as mock_as_operator:
        executor._start_task_runs()

    assert mock_as_operator.call_count == 1

    runs = TaskRun.fetch_rows(run_id=tr.run_id, dag_id=tr.dag_id)
    assert len(runs) == 1
    assert runs[0].state == TaskRun.state_type.FAILED

def test_executor_recovers_running_tasks_after_pending_reset(test_env_with_context, add_dag_clean, gen_id, executor_stub_f):
    dag_id, meta_id = add_dag_clean(clear_trs=True)
    row = TaskRun.row_type(task_id="just_cat", run_id=gen_id, dag_run_id=gen_id, dag_id=dag_id, state=TaskRun.state_type.QUEUED)
    tr = TaskRun(row=row)
    TaskRun.create_rows(rows=row)

    pool = ControlledPool()
    executor = executor_stub_f(pool=pool)

    executor._start_task_runs()
    executor._pending_task_runs.clear()
    executor._pending_operations.clear()

    original = executor._update_pending_tasks
    mock_once = Mock(side_effect=lambda *a, **kw: (
        setattr(executor, "_update_pending_tasks", original),
        None
    )[1])

    executor._update_pending_tasks = mock_once

    executor._update_pending_tasks()
    executor._loop_iter()

    runs = TaskRun.fetch_rows(run_id=tr.run_id, dag_id=tr.dag_id)
    assert len(runs) == 1
    assert runs[0].state == TaskRun.state_type.RUNNING

    assert mock_once.call_count == 1

    executor._update_pending_tasks()
    executor._loop_iter()
    runs = TaskRun.fetch_rows(run_id=tr.run_id, dag_id=tr.dag_id)
    assert len(runs) == 1
    assert runs[0].state in TaskRun.state_type.finished_states