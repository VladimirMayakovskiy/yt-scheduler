import pytest

import yt.wrapper as yt

from src.cli.commands import _ensure_table
from src.dag_entity import DagEntityRow
from src.dag_run import DagRunRow
from src.task_run import TaskRunRow


@pytest.fixture(autouse=True, scope="session")
def init_flow_runner_dirs(yt_client: yt.YtClient):
    for row_type in [DagEntityRow, DagRunRow, TaskRunRow]:
        _ensure_table(yt_client, row_type)


def pytest_addoption(parser):
    parser.addoption("--proxy", action="store", default="localhost:8000")

@pytest.fixture(scope="session")
def yt_proxy(pytestconfig):
    return pytestconfig.getoption("proxy")


@pytest.fixture(scope="session")
def yt_client(yt_proxy) -> yt.YtClient:
    return yt.YtClient(proxy=yt_proxy)
