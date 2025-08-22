import pytest

import yt.wrapper as yt

def pytest_addoption(parser):
    parser.addoption("--proxy", action="store", default="localhost:8000")

@pytest.fixture(scope="session")
def yt_proxy(pytestconfig):
    return pytestconfig.getoption("proxy")


@pytest.fixture(scope="session")
def yt_client(yt_proxy) -> yt.YtClient:
    return yt.YtClient(proxy=yt_proxy)
