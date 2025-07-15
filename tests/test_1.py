import subprocess
import sys
import threading
import time
from asyncio import to_thread
from time import sleep

import pytest
import yaml
import yt.wrapper as yt
import os


@pytest.fixture(scope="session")
def yt_client() -> yt.YtClient:
    return yt.YtClient(proxy='localhost:8000')

@pytest.fixture
def spec_file():
    def _spec_file(spec_path: str):
        with open(spec_path, 'r') as file:
            spec = yaml.safe_load(file)
        return spec
    return _spec_file

def start_scheduler(estop: threading.Event):
    p = subprocess.Popen(
        [sys.executable, "src/main.py", "scheduler", "run", "--yt-proxy", 'localhost:8000'],
    )
    estop.wait()
    p.kill()
    p.wait(timeout=5)


def run_cli(cmd: list[str]):
    return subprocess.run([sys.executable, "src/main.py", *cmd], check=True, capture_output=True, text=True)

def run_add_dag(spec_path: str, workdir: str):
    completed = run_cli(["dag", "run", "--spec", spec_path, "--yt-proxy", 'localhost:8000', '--work-dir', workdir])
    assert completed.returncode == 0
    _, _, dag_id_part = completed.stdout.partition("dag_id=")
    dag_id = dag_id_part.split(",")[0]
    assert dag_id
    return dag_id


def wait_for_all_tasks(yt_client: yt.YtClient, dag_id: str, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if yt_client.exists("//tmp/dag_run") and yt_client.exists("//tmp/task_run"):
            rows = list(yt_client.select_rows(
                f"state from [{'//tmp/dag_run'}] WHERE dag_id = '{dag_id}'"
            ))
            print(rows)
            if rows and all(row["state"] in ("success", "failed") for row in rows):
                rows2 = list(yt_client.select_rows(
                    f"state from [{'//tmp/task_run'}] WHERE dag_id = '{dag_id}'"
                ))
                print(rows2)
                if rows2 and all(row["state"] in ("success", "failed") for row in rows2):
                    return True, rows, rows2
        time.sleep(1)
    return False, [], []


def test_add_and_schedule_simple(yt_client, spec_file):
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../spec_map.yaml'))
    spec = spec_file(spec_path)
    print(spec_path, spec)

    dag_id = run_add_dag(spec_path,  '//tmp/example_spec_map')
    # dag_id = run_add_dag(["dag", "run", "--spec", spec_path, "--yt-proxy", 'localhost:8000', '--work-dir', '//tmp/example_spec_map'])
    print(dag_id)
    # completed = run_cli(["dag", "run", "--spec", spec_path, "--yt-proxy", 'localhost:8000', '--work-dir', '//tmp/example_spec_map'])

    estop = threading.Event()
    thread = threading.Thread(target=start_scheduler, args=(estop,), daemon=True)
    thread.start()

    ok, dr, tr = (wait_for_all_tasks(yt_client, dag_id))
    assert ok, "не завершились"

    assert all(dagrun["state"] == "success" for dagrun in dr)
    assert all(taskrun["state"] == "success" for taskrun in dr)

    estop.set()
    thread.join(timeout=5)


def test_add_and_test_sort_simple(yt_client, spec_file):
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec2.yaml'))
    spec = spec_file(spec_path)
    print(spec)

    dag_id = run_add_dag(spec_path, '//tmp/example_sort_simple')

    yt_client.write_table("//tmp/example_sort_simple/input_table1", [{"x": 2}, {"x": 1}])

    estop = threading.Event()
    thread = threading.Thread(target=start_scheduler, args=(estop,), daemon=True)
    thread.start()

    ok, dr, tr = (wait_for_all_tasks(yt_client, dag_id))
    assert ok, "не завершились"

    assert all(dagrun["state"] == "success" for dagrun in dr)
    assert all(taskrun["state"] == "success" for taskrun in dr)

    out = list(yt_client.read_table("//tmp/example_sort_simple/output_table"))
    print(out)

    estop.set()
    thread.join(timeout=5)
