import pytest
from unittest.mock import Mock

import logging
import threading
from types import SimpleNamespace

from dag import DAG
from scheduler import Scheduler
from dagref import DagMeta, DagRef
from dagrun import DagRun

from conftest_helpers import (
    gen_id, dagref_factory, scheduler_stub, scheduler_stub_factory, FakeDagInitializationError, ControlledPool,
    get_from_mock_call, throw
)
from conftest_env import patch_insert_rows_threadsafe

import yt.wrapper as yt

logging.getLogger().setLevel(logging.CRITICAL)

def test_get_dag_returns_none_on_parse_error_and_caches_success(scheduler_stub, dagref_factory, monkeypatch):
    dagref = dagref_factory(serialized_repr="bad")
    monkeypatch.setattr(DAG, "from_serialized_repr", staticmethod(lambda *args: throw(FakeDagInitializationError("Injected"))))

    dag = scheduler_stub._get_dag(ref=dagref)
    assert dag is None
    assert dagref.dag_id not in scheduler_stub._dag_cache

    fake_dag = SimpleNamespace(task_ids=[], tasks=[])
    monkeypatch.setattr(DAG, "from_serialized_repr", staticmethod(lambda *args: fake_dag))
    dag = scheduler_stub._get_dag(ref=dagref)

    assert dag is fake_dag
    assert scheduler_stub._dag_cache.get(dagref.dag_id) is fake_dag

def test_create_dagruns_increments_created_counter_on_success(scheduler_stub_factory, gen_id, monkeypatch):
    sched = scheduler_stub_factory(pool=ControlledPool())
    monkeypatch.setattr(sched, "_try_claim_dagruns", lambda *args, **kwargs: 2)

    sched._create_dagruns_for_dags()
    with sched._created_dagruns_lock:
        assert sched._created_dagruns_since_last_tick == 2

    monkeypatch.setattr(sched, "_try_claim_dagruns", lambda *args, **kwargs: throw(RuntimeError("Injected")))
    sched._create_dagruns_for_dags()
    with sched._created_dagruns_lock:
        assert sched._created_dagruns_since_last_tick == 2

def test_try_claim_dagruns_marks_invalid_metas_and_triggers_create(test_env_with_context, scheduler_stub_factory,
                                                                   dagref_factory, monkeypatch):
    from rows_clients import DagRefClient

    sched = scheduler_stub_factory(pool=ControlledPool())
    target_dagref = dagref_factory(serialized_repr="target")
    target_meta = DagMeta(dag_id=target_dagref.dag_id)
    invalid_dagref = dagref_factory(serialized_repr="invalid")
    invalid_meta = DagMeta(dag_id=invalid_dagref.dag_id)

    def fake_get_dag(ref):
        if ref.dag_id == invalid_dagref.dag_id:
            return None
        return SimpleNamespace(task_ids=[], tasks=[])
    monkeypatch.setattr(DagRefClient, "dags_needing_dagruns", lambda *args, **kwargs: [(target_dagref, target_meta),
                                                                                       (invalid_dagref, invalid_meta)])
    monkeypatch.setattr(DAG, "from_serialized_repr", lambda *args: throw(FakeDagInitializationError("Injected")))
    monkeypatch.setattr(sched, "_get_dag", fake_get_dag)
    monkeypatch.setattr(sched, "_create_dag_runs_atomic", lambda *args, **kwargs: 1)
    monkeypatch.setattr(DagRef, "dags_needing_dagruns_of_metas", lambda *args, **kwargs: [(invalid_dagref, invalid_meta)])

    assert sched._try_claim_dagruns() == 1
    assert DagMeta.get(id=invalid_meta.id).run_id.startswith("ERR:PARSING_FAILED")

def test_try_claim_dagruns_skips_meta_if_dag_not_found_then_skips_create(test_env_with_context, scheduler_stub_factory,
                                                                         dagref_factory, monkeypatch):
    sched = scheduler_stub_factory(pool=ControlledPool())
    dagref = dagref_factory(upsert_rows=True)
    meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=meta)

    _get_dag_calls = 0
    def fake_get_dag(ref):
        nonlocal _get_dag_calls
        _get_dag_calls += 1
        if _get_dag_calls == 1:
            return None
        return Mock()
    monkeypatch.setattr(sched, "_get_dag", lambda *args, **kwargs: fake_get_dag)
    monkeypatch.setattr(sched, "_create_dag_runs_atomic", lambda *args, **kwargs: 0)

    assert sched._try_claim_dagruns() == 0
    assert DagMeta.get(id=meta.id).run_id is None

def test_create_dag_runs_atomic_atomicity_on_meta_upsert_fails(test_env_with_context, dagref_factory, gen_id, monkeypatch, mocker):
    from scheduler import _create_dag_runs_atomic

    dagref = dagref_factory(upsert_rows=True)
    meta_no_need_run = DagMeta(dag_id=dagref.dag_id, run_id=gen_id)
    target_meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=[meta_no_need_run, target_meta])

    meta_upsert_rows_calls = []
    def fake_meta_upsert_rows(rows):
        nonlocal meta_upsert_rows_calls
        if not isinstance(rows, list):
            rows = [rows]
        meta_upsert_rows_calls.extend(rows)
        raise RuntimeError("Injected")
    monkeypatch.setattr(DagMeta, "upsert_rows", fake_meta_upsert_rows)
    mock = mocker.patch("scheduler.DagRun.upsert_rows", wraps=DagRun.upsert_rows)

    with pytest.raises(RuntimeError):
        _create_dag_runs_atomic(metas=[meta_no_need_run.id, target_meta.id])

    assert len(meta_upsert_rows_calls) == 1 and meta_upsert_rows_calls[0].id == target_meta.id

    assert mock.call_count == 1
    runs = DagRun.fetch_rows(dag_id=dagref.dag_id)
    assert len(runs) == 0

def test_concurrent_try_claim_one_wins_no_artifacts(test_env_with_context, env_client, env_context, scheduler_stub,
                                                    dagref_factory, monkeypatch):
    dagref = dagref_factory(upsert_rows=True)
    meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=meta)

    sched1 = scheduler_stub
    sched2 = scheduler_stub

    monkeypatch.setattr(sched1, "_get_dag", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(sched2, "_get_dag", lambda *args, **kwargs: Mock())

    with patch_insert_rows_threadsafe(env_client) as fake_insert_rows:
        def run_try_claim(sched):
            yt.YtClient.insert_rows = fake_insert_rows
            try:
                env_context(sched._try_claim_dagruns)
            except Exception:
                pass

        thread1 = threading.Thread(target=run_try_claim, args=(sched1,))
        thread2 = threading.Thread(target=run_try_claim, args=(sched2,))
        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        runs = DagRun.fetch_rows(dag_id=dagref.dag_id)
        assert len(runs) == 1
        assert runs[0].state == DagRun.state_type.SCHEDULED

        metas = DagMeta.fetch_rows(dag_id=dagref.dag_id)
        assert len(metas) == 1
        assert metas[0].run_id == runs[0].run_id

def test__set_invalid_meta_idempotency(test_env_with_context, env_client, dagref_factory, mocker):
    from scheduler import _set_invalid_meta

    dagref = dagref_factory(upsert_rows=True)
    meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=meta)

    mock = mocker.patch("dagref.DagMeta.upsert_rows", wraps=DagMeta.upsert_rows)

    _set_invalid_meta(invalid_metas=[meta.id], yt_client=env_client)

    assert mock.call_count == 1
    rows = get_from_mock_call(mock, "rows", default=[])[0]
    assert any(meta.id == m.id for m in rows)

    upd_meta = DagMeta.get(id=meta.id)
    assert upd_meta is not None and upd_meta.run_id.startswith("ERR:PARSING_FAILED")

    _set_invalid_meta(invalid_metas=[meta.id], yt_client=env_client)

    assert mock.call_count == 1
    upd_meta2 = DagMeta.get(id=meta.id)
    assert upd_meta2 == upd_meta

def test_create_dag_runs_atomic_idempotency(test_env_with_context, env_client, dagref_factory, mocker):
    from scheduler import _create_dag_runs_atomic

    dagref = dagref_factory(upsert_rows=True)
    meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=meta)

    mock_meta_upsert_rows = mocker.patch("dagref.DagMeta.upsert_rows", wraps=DagMeta.upsert_rows)
    mock_dr_upsert_rows = mocker.patch("dagrun.DagRun.upsert_rows", wraps=DagRun.upsert_rows)

    ret = _create_dag_runs_atomic(metas=[meta.id], yt_client=env_client)
    assert ret == 1
    assert mock_meta_upsert_rows.call_count == 1
    assert mock_dr_upsert_rows.call_count == 1

    meta_upserted_rows = get_from_mock_call(mock_meta_upsert_rows, "rows", default=[])
    assert any(meta.id == m.id for m in meta_upserted_rows)

    upd_meta = DagMeta.get(id=meta.id)
    assert upd_meta is not None and upd_meta.run_id is not None
    assert len(DagRun.fetch_rows(dag_id=dagref.dag_id)) == 1

    ret = _create_dag_runs_atomic(metas=[meta.id], yt_client=env_client)
    assert ret == 0
    assert mock_meta_upsert_rows.call_count == 1
    assert mock_dr_upsert_rows.call_count == 1

    upd_meta2 = DagMeta.get(id=meta.id)
    assert upd_meta2 == upd_meta
    assert len(DagRun.fetch_rows(dag_id=dagref.dag_id)) == 1

def test_try_claim_idempotency(test_env_with_context, scheduler_stub, dagref_factory, monkeypatch):
    dagref = dagref_factory(upsert_rows=True)
    meta = DagMeta(dag_id=dagref.dag_id)
    DagMeta.upsert_rows(rows=meta)

    monkeypatch.setattr(scheduler_stub, "_get_dag", lambda *args, **kwargs: Mock())

    scheduler_stub._try_claim_dagruns()
    scheduler_stub._try_claim_dagruns()

    runs = DagRun.fetch_rows(dag_id=dagref.dag_id)
    assert len(runs) == 1
    assert runs[0].state == DagRun.state_type.SCHEDULED