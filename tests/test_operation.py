import subprocess
import sys
import threading
import time
from time import sleep

import pytest
import yaml
import yt.wrapper as yt
import os
from src.cli.commands import add_dag
from argparse import Namespace

@pytest.fixture(scope="session")
def yt_client() -> yt.YtClient:
    return yt.YtClient(proxy='localhost:8000')

@pytest.fixture
def prepare_workdir(request, yt_client):
    # test_name = request.node.name
    # workdir = f"//tmp/{test_name}"
    workdir = f"//tmp/test_operation"
    yt_client.create("map_node", workdir, force=True)
    yt_client.write_table(f"{workdir}/input_table", [{"x": 1}, {"x": 2}], format=yt.YsonFormat())
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec_map.yaml'))

    return workdir, spec_path


def check_dag_run_completed(yt_client: yt.YtClient, dag_id: str):
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
                return True #, rows, rows2
    return False #, [], []


def run_cli(cmd: list[str]):
    return subprocess.run([sys.executable, "src/main.py", *cmd], check=True, capture_output=True, text=True)


def run_add_dag(spec_path: str, workdir: str):
    completed = run_cli(["dags", "add", "--spec", spec_path, "--yt-proxy", 'localhost:8000', '--work-dir', workdir])
    assert completed.returncode == 0
    _, _, dag_id_part = completed.stdout.partition("dag_id=")
    dag_id = dag_id_part.split(",")[0]
    assert dag_id
    return dag_id

def test_operation_success(yt_client, prepare_workdir):
    workdir, spec_path = prepare_workdir
    dag_id = add_dag(Namespace(**{"spec": spec_path, "work_dir": workdir, "yt_proxy": 'localhost:8000'}))

    while not check_dag_run_completed(yt_client, dag_id):
        time.sleep(5)

    out = list(yt_client.read_table(f"{workdir}/output_table"))

    expected = [{"x": 1}, {"x": 2}]
    assert out == expected

def test_operation_aborted(yt_client, prepare_workdir):
    workdir, spec_path = prepare_workdir
    dag_id = add_dag(Namespace(**{"spec": spec_path, "work_dir": workdir, "yt_proxy": 'localhost:8000'}))

    from src.dag_run import DagRun
    op = DagRun.fetch_task_runs(yt_client, dag_id=dag_id)
    while len(op) == 0 or op[0].operation_id is None:
        time.sleep(2)
        op = DagRun.fetch_task_runs(yt_client, dag_id=dag_id)

    print(op[0])
    yt_client.abort_operation(op[0].operation_id)

    while not check_dag_run_completed(yt_client, dag_id):
        time.sleep(5)

    dagrun = DagRun.fetch_dagruns(yt_client=yt_client, dag_id=dag_id)
    op = DagRun.fetch_task_runs(yt_client, run_id=dagrun[0].run_id)

    assert dagrun[0].state == "failed"
    assert op[0].state == "failed"


# def test_operation_suspend(yt_client, prepare_workdir):
#     workdir, spec_path = prepare_workdir
#     dag_id = add_dag(Namespace(**{"spec": spec_path, "work_dir": workdir, "yt_proxy": 'localhost:8000'}))
#
#     from src.dag_run import DagRun
#     op = DagRun.fetch_task_runs(yt_client, dag_id=dag_id)
#     while len(op) == 0 or op[0].operation_id is None:
#         time.sleep(2)
#         op = DagRun.fetch_task_runs(yt_client, dag_id=dag_id)
#
#     print(op[0])
#     yt_client.suspend_operation(op[0].operation_id, abort_running_jobs=True)
#
#     while not check_dag_run_completed(yt_client, dag_id):
#         time.sleep(5)
#
#     assert True


