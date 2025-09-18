from __future__ import annotations

import pytest

import os
import traceback
from typing import Iterable, Any

from rows_helpers import init_all_from_config
from base_row import YtRow
from commands import import_all_dataclasses, _ensure_table, _add_dag_impl
from job import ClientState
from dag import DAG
from dagref import DagRef, DagMeta
from dagrun import DagRun
from taskrun import TaskRun
from yt_wrapper import ClientAgent, context_wrapper
from config import update, get_default_config, Config

import yt.wrapper as yt

class TestConfig(Config):
    def __init__(self, config: dict):
        self.config = config

class TestEnvironment:
    def __init__(self, test_name, config=None, test_dir=None):
        self.test_name = test_name

        if config is None:
            config = {}
        config = update(get_default_config(), config)

        if test_dir is None:
            test_dir = [os.path.dirname(os.path.abspath(__file__))]

        self.test_dir = test_dir
        config["default_work_dir"] = self.test_dir.rstrip("/") + "/"

        self._config = TestConfig(config)

        init_all_from_config(config=self._config)

        self._client_agent = ClientAgent(config=self._config)
        self._yt_client = self._client_agent.create_client()

        self._client_state = ClientState(agent=self._client_agent)
        self._context = context_wrapper(client_state=self._client_state)

        if not self._yt_client.exists(self.test_dir):
            self._yt_client.create("map_node", self.test_dir, force=True)

        for row_type in import_all_dataclasses():
            _ensure_table(self._yt_client, row_type)

        self._per_test_contexts: dict[str, Any] = {}
        self._last_active_nodeid = None

    def get_config(self):
        return self._config


@pytest.fixture(scope="function", autouse=True)
def test_function_teardown(request, test_env):
    nodeid = request.node.nodeid

    test_env._per_test_contexts.setdefault(nodeid, {})

    test_env._last_active_nodeid = nodeid

    def _cleanup_targets_finalizer():
        targets = test_env._per_test_contexts.pop(nodeid, {})
        for table, keys in targets.items():
            if not keys:
                continue
            try:
                test_env._yt_client.delete_rows(table, keys)
            except Exception:
                traceback.print_exc()

        if test_env._last_active_nodeid == nodeid:
            test_env._last_active_nodeid = None

    request.addfinalizer(_cleanup_targets_finalizer)
    return test_env

def patch_insert_rows(environment: TestEnvironment, yt_client: yt.YtClient):
    row_key_columns: dict[type[YtRow], list[str]] = {
        row_type.table_path: row_type.key_columns for row_type in import_all_dataclasses()
    }
    original_method = yt_client.insert_rows
    def _insert_rows(table: str, rows: Iterable[dict], *args, **kwargs):
        try:
            rows = list(rows)
            targets_to_register: list[dict[str, list[str]]] = []
            kcols = row_key_columns.get(table)
            if kcols:
                for r in rows:
                    if r is None:
                        continue
                    t = {kcol: r.get(kcol) for kcol in kcols if r.get(kcol, None) is not None}
                    if t:
                        targets_to_register.append(t)
                if targets_to_register:
                    nodeid = environment._last_active_nodeid
                    if nodeid:
                        context = environment._per_test_contexts.setdefault(nodeid, {})
                        context.setdefault(table, []).extend(targets_to_register)
        except Exception:
            traceback.print_exc()
        if original_method is None:
            raise RuntimeError("Original yt_client.insert_rows not found")
        return original_method(table, rows, *args, **kwargs)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(yt_client, "insert_rows", _insert_rows)
    return environment

def init_test_environment():
    environment = TestEnvironment("TestYtWrapper", test_dir="//tests")
    environment = patch_insert_rows(environment, environment._yt_client)
    return environment

@pytest.fixture(scope="session")
def test_env(request):
    environment = init_test_environment()
    return environment

@pytest.fixture(scope="session")
def env_client(test_env):
    return test_env._yt_client

@pytest.fixture(scope="session")
def env_context(test_env):
    return test_env._context

@pytest.fixture(scope="session")
def env_client_state(test_env):
    return test_env._client_state

@pytest.fixture(scope="session")
def test_env_with_context(test_env):
    with test_env._client_agent.client_context(test_env._client_state):
        yield

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