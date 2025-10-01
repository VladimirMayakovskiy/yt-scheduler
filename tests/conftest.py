from __future__ import annotations

import pytest

from cli_commands import _add_dag_impl
from job import ClientContext
from dag import DAG
from dagref import DagRef, DagMeta
from dagrun import DagRun
from taskrun import TaskRun

from conftest_env import init_test_environment
from conftest_helpers import gen_id_f

@pytest.fixture(scope="session")
def test_env(request):
    environment = init_test_environment()
    return environment

@pytest.fixture(scope="session")
def env_client(test_env):
    return test_env._yt_client

@pytest.fixture(scope="session")
def env_context(test_env):
    return test_env._context_wrapper

@pytest.fixture(scope="session")
def env_client_context(test_env):
    return test_env._client_context

@pytest.fixture(scope="session")
def test_env_with_context(test_env):
    with test_env._client_agent.client_context(test_env._client_context):
        yield

# ------------------------ DAG setup & cleanup fixtures ------------------------

from conftest_data import simple_spec

@pytest.fixture
def add_dag(test_env):
    def _f(spec=None, work_dir=None):
        spec = spec or simple_spec
        work_dir = work_dir or test_env.test_dir
        return _add_dag_impl(spec=spec, work_dir=work_dir)
    return _f

@pytest.fixture
def clear_dag_meta(env_client):
    def _f(exclude: set[str] = None, **kwargs):
        exclude = exclude or set()
        metas = DagMeta.fetch_rows(**kwargs)
        env_client.delete_rows(DagRef.meta_row_type.table_path, [{"id": m.id} for m in metas if m.id not in exclude])
    return _f

@pytest.fixture
def clear_dag_ref(test_env, clear_dag_meta):
    def _f(spec=None, work_dir=None) -> str | None:
        spec = spec or simple_spec
        work_dir = work_dir or test_env.test_dir

        _ , payload_hash = DAG.from_spec_conf(spec=spec, work_dir=work_dir).to_serialized_repr()

        found = DagRef.get(payload_hash=payload_hash)
        if found:
            test_env._yt_client.delete_rows(DagRef.table_path, [{"dag_id": found.dag_id}])
            clear_dag_meta(found.dag_id)
            return found.dag_id
        return None
    return _f

@pytest.fixture
def clear_dag_runs(env_client):
    def _f(**kwargs):
        runs = DagRun.fetch_rows(**kwargs)
        env_client.delete_rows(DagRun.table_path, [{"run_id": r.run_id} for r in runs])
    return _f

@pytest.fixture
def clear_task_runs(env_client):
    def _f(**kwargs):
        runs = TaskRun.fetch_rows(**kwargs)
        env_client.delete_rows(DagRun.table_path, [{"run_id": r.run_id} for r in runs])
    return _f

@pytest.fixture
def add_dag_clean(add_dag, clear_dag_meta, clear_dag_runs, clear_task_runs):
    def _f(spec=None, work_dir=None, clear_drs: bool = False, clear_trs: bool = False):
        s, dag_id, meta_id = add_dag(spec=spec, work_dir=work_dir)
        if not s:
            clear_dag_meta(dag_id=dag_id, exclude={meta_id})

        if clear_drs:
            clear_dag_runs(dag_id=dag_id)
        if clear_trs:
            clear_task_runs(dag_id=dag_id)

        assert DagRef.get(dag_id=dag_id) is not None
        return dag_id, meta_id
    return _f