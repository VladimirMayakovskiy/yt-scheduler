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


@pytest.fixture
def spec_file():
    def _spec_file(spec_path: str):
        with open(spec_path, 'r') as file:
            spec = yaml.safe_load(file)
        return spec
    return _spec_file

def start_scheduler(estop: threading.Event, yt_proxy: str):
    p = subprocess.Popen(
        [sys.executable, "src/main.py", "scheduler", "run", "--yt-proxy", yt_proxy],
    )
    estop.wait()
    p.kill()
    p.wait(timeout=5)


def run_cli(cmd: list[str]):
    return subprocess.run([sys.executable, "src/main.py", *cmd], check=True, capture_output=True, text=True)

def run_add_dag(spec_path: str, workdir: str, yt_proxy: str):
    completed = run_cli(["dags", "add", "--spec", spec_path, "--yt-proxy", yt_proxy, '--work-dir', workdir])
    assert completed.returncode == 0
    _, _, dag_id_part = completed.stdout.partition("dag_id=")
    dag_id = dag_id_part.split(",")[0]
    assert dag_id
    return dag_id


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
                return True, rows, rows2
    return False, [], []


def test_add_and_test_sort_simple(yt_client, yt_proxy, spec_file):
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec2.yaml'))
    spec = spec_file(spec_path)
    print(spec)

    dag_id = run_add_dag(spec_path, '//tmp/example_sort_simple', yt_proxy=yt_proxy)

    yt_client.write_table("//tmp/example_sort_simple/input_table1", [{"x": 2}, {"x": 1}])

    estop = threading.Event()
    thread = threading.Thread(target=start_scheduler, args=(estop, yt_proxy), daemon=True)
    thread.start()

    time.sleep(30)

    estop.set()
    thread.join(timeout=5)

    ok, dr, tr = check_dag_run_completed(yt_client, dag_id)
    assert ok, "dag run не завершился"

    out = list(yt_client.read_table("//tmp/example_sort_simple/output_table"))

    expected = [{"x": 1}, {"x": 2}]
    assert out == expected

def test_add_and_test_sort_2steps(yt_client, yt_proxy, spec_file):
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec3.yaml'))
    spec = spec_file(spec_path)
    print(spec)

    dag_id = run_add_dag(spec_path, '//tmp/example_sort_2steps', yt_proxy)

    yt_client.write_table("//tmp/example_sort_2steps/input_table1", [{"x": 5}, {"x": 4}])
    yt_client.write_table("//tmp/example_sort_2steps/input_table2", [{"x": 2}, {"x": 1}])
    yt_client.create("table", "//tmp/example_sort_2steps/output_table", force=True)
    yt_client.create("table", "//tmp/example_sort_2steps/output_table2", force=True)

    estop = threading.Event()
    thread = threading.Thread(target=start_scheduler, args=(estop, yt_proxy), daemon=True)
    thread.start()

    time.sleep(60)

    estop.set()
    thread.join(timeout=5)

    ok, dr, tr = check_dag_run_completed(yt_client, dag_id)
    assert ok, "dag run не завершился"

    out1 = list(yt_client.read_table("//tmp/example_sort_2steps/output_table"))
    out2 = list(yt_client.read_table("//tmp/example_sort_2steps/output_table2"))

    expected1 = [{"x": 4}, {"x": 5}]
    expected2 = [{'x': 1}, {'x': 2}, {'x': 4}, {'x': 5}]
    assert out1 == expected1
    assert out2 == expected2

def test_add_and_test_sort(yt_client, yt_proxy, spec_file):
    spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec_sort.yaml'))
    spec = spec_file(spec_path)
    print(spec)

    dag_id = run_add_dag(spec_path, '//tmp/example_sort', yt_proxy=yt_proxy)

    yt_client.write_table("//tmp/example_sort/input_table1", [{"x": 100}, {"x": 10}])
    yt_client.write_table("//tmp/example_sort/input_table2", [{"x": 1}, {"x": 20}])
    yt_client.write_table("//tmp/example_sort/input_table3", [{"x": 130}, {"x": 30}])
    yt_client.create("table", "//tmp/example_sort/output_table1", force=True)
    yt_client.create("table", "//tmp/example_sort/output_table2", force=True)
    yt_client.create("table", "//tmp/example_sort/output_table3", force=True)
    yt_client.create("table", "//tmp/example_sort/output_table4", force=True)

    estop = threading.Event()
    thread = threading.Thread(target=start_scheduler, args=(estop, yt_proxy), daemon=True)
    thread.start()

    time.sleep(120)

    estop.set()
    thread.join(timeout=5)

    ok, dr, tr = check_dag_run_completed(yt_client, dag_id)
    assert ok, "dag run не завершился"

    out1 = list(yt_client.read_table("//tmp/example_sort/output_table1"))
    out2 = list(yt_client.read_table("//tmp/example_sort/output_table2"))
    out3 = list(yt_client.read_table("//tmp/example_sort/output_table3"))
    out4 = list(yt_client.read_table("//tmp/example_sort/output_table4"))

    expected1 = [{"x": 10}, {"x": 100}]
    expected2 = [{'x': 1}, {'x': 10}, {'x': 20}, {'x': 100}]
    expected3 = [{'x': 10}, {'x': 30}, {'x': 100}, {'x': 130}]
    expected4 = [{'x': 1}, {'x': 10}, {'x': 10}, {'x': 20}, {'x': 30}, {'x': 100}, {'x': 100}, {'x': 130}]
    assert out1 == expected1
    assert out2 == expected2
    assert out3 == expected3
    assert out4 == expected4

# def test_map(yt_client, spec_file):
#     # spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'spec2.yaml'))
#     # spec = spec_file(spec_path)
#
#     yt_client.create("map_node", "//tmp/example_sort2", force=True)
#     yt_client.write_table("//tmp/example_sort2/input_table1", [{"x": 1}, {"x": 2}])
#     yt_client.write_table("//tmp/example_sort2/input_table2", [{"x": 4}, {"x": 5}])
#     yt_client.create("table", "//tmp/example_sort2/output_table", force=True)
#
#     spec_builder = yt.spec_builders.SortSpecBuilder() \
#         .input_table_paths(["//tmp/example_sort2/input_table1", "//tmp/example_sort2/input_table2"]) \
#         .output_table_path("//tmp/example_sort2/output_table") \
#         .sort_by("x")
#         # .begin_mapper() \
#         #     .command('cat') \
#         #     .format(yt.YsonFormat()) \
#         # .end_mapper()
#
#
#     operation_id = yt_client.run_operation(spec_builder, sync=True)
#     print(operation_id.id)
#
#     print(yt_client.get_operation_state(operation_id.id))
#     # print(yt_client.get_operation(operation_id.id))
#     sleep(5)
#     print(yt_client.get_operation_state(operation_id.id))
#     # print(yt_client.get_operation(operation_id.id))
#     # print(yt_client.complete_operation('8c90649a-6ad2cb38-103e8-4cde427c'))
#     # print(yt_client.complete_operation('ec24ce6-19003e56-103e8-f2ba79eb'))