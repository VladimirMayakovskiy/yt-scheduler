import pytest

import os
import contextlib
import traceback
import threading
from typing import Iterable, Any

from config import update, get_default_config, Config
from base_row import YtRow
from rows_helpers import init_all_from_config
from job import ClientContext
from yt_wrapper import ClientAgent, context_wrapper
from cli_commands import import_all_dataclasses, _ensure_table

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

        self._client_context = ClientContext(agent=self._client_agent)
        self._context_wrapper = context_wrapper(context=self._client_context)

        if not self._yt_client.exists(self.test_dir):
            self._yt_client.create("map_node", self.test_dir, force=True)

        for row_type in import_all_dataclasses():
            _ensure_table(self._yt_client, row_type)

        self._per_test_contexts: dict[str, Any] = {}
        self._last_active_nodeid = None
        self._per_test_contexts_lock = threading.Lock()

    def get_config(self):
        return self._config

def init_test_environment():
    environment = TestEnvironment("TestYtWrapper", test_dir="//tests")
    environment = patch_insert_rows(environment, environment._yt_client)
    return environment

def patch_insert_rows(environment: TestEnvironment, yt_client: yt.YtClient):
    row_key_columns: dict[type[YtRow], list[str]] = {
        row_type.table_path: row_type.key_columns for row_type in import_all_dataclasses() # todo
    }
    original_method = yt_client.insert_rows
    def _insert_rows(table: str, input_stream: Iterable[dict], *args, **kwargs):
        try:
            rows = list(input_stream)
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
                    with environment._per_test_contexts_lock:
                        if nodeid and nodeid in environment._per_test_contexts:
                            context = environment._per_test_contexts.setdefault(nodeid, {})
                            context.setdefault(table, []).extend(targets_to_register)
                        else:
                            for context in environment._per_test_contexts.values():
                                context.setdefault(table, []).extend(targets_to_register)

                            if not environment._per_test_contexts:
                                environment._per_test_contexts.setdefault("global", {}).setdefault(table, []).extend(targets_to_register)
        except Exception:
            traceback.print_exc()
        if original_method is None:
            raise RuntimeError("Original yt_client.insert_rows not found")
        return original_method(table, rows, *args, **kwargs)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(yt_client, "insert_rows", _insert_rows)
    return environment

@contextlib.contextmanager
def patch_insert_rows_threadsafe(yt_client: yt.YtClient):
    from rows_helpers import get_row_key_columns_map
    row_key_columns = get_row_key_columns_map()

    original_method = yt.YtClient.insert_rows
    cleanup_targets: dict[str, list[str]] = {}
    def _insert_rows(self, table, input_stream, **kwargs):
        try:
            rows = list(input_stream)
            kcols = row_key_columns.get(table)
            cleanup_targets.setdefault(table, [])
            if kcols:
                for r in rows:
                    if r is None:
                        continue
                    t = {kcol: r.get(kcol) for kcol in kcols if r.get(kcol, None) is not None}
                    if t:
                        cleanup_targets[table].append(t)
            return original_method(self, table=table, input_stream=input_stream, **kwargs)
        except Exception:
            traceback.print_exc()

    try:
        yield _insert_rows
    finally:

        for table, keys in cleanup_targets.items():
            if not keys:
                continue
            try:
                yt_client.delete_rows(table, keys)
            except Exception:
                traceback.print_exc()


@pytest.fixture(scope="function", autouse=True)
def test_function_teardown(request, test_env):
    nodeid = request.node.nodeid

    test_env._per_test_contexts.setdefault(nodeid, {})

    test_env._last_active_nodeid = nodeid

    def _cleanup_targets_finalizer():
        def _cleanup_targets(t):
            for table, keys in t.items():
                if not keys:
                    continue
                try:
                    test_env._yt_client.delete_rows(table, keys)
                except Exception:
                    traceback.print_exc()
        targets = test_env._per_test_contexts.pop(nodeid, {})
        _cleanup_targets(targets)
        global_targets = test_env._per_test_contexts.pop("global", {})
        _cleanup_targets(global_targets)

        if test_env._last_active_nodeid == nodeid:
            test_env._last_active_nodeid = None

    request.addfinalizer(_cleanup_targets_finalizer)
    return test_env