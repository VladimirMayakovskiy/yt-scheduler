import os
from dataclasses import asdict

from scheduler import Job, Scheduler, run_job
from dag_entity import DagEntityRow
import yt.wrapper as yt
from yt.wrapper.schema import TableSchema, SortColumn


def _ensure_table(yt_client: yt.YtClient, row_type: type):
    if not yt_client.exists(row_type.table_path):
        schema = TableSchema.from_row_type(row_type, unique_keys=row_type.unique_keys)
        for key in row_type.key_columns:
            schema = schema.build_schema_sorted_by(SortColumn(key, SortColumn.ASCENDING))
        yt_client.create("table", row_type.table_path, attributes={
            "schema": schema,
            "dynamic": True
        })
        yt_client.mount_table(row_type.table_path, sync=True)

def init(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)
    from dag_entity import DagEntityRow
    from dag_run import DagRunRow
    from task_run import TaskRunRow
    for row_type in [DagEntityRow, DagRunRow, TaskRunRow]:
        _ensure_table(yt_client, row_type)


def run_scheduler(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    scheduler = Scheduler(job = Job(), yt_client=yt_client)
    run_job(job=scheduler.job, execute_callable=scheduler._execute, yt_client=yt_client)


def add_dag(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    spec_path = args.spec
    work_dir = args.work_dir

    if not yt_client.exists(work_dir):
        yt_client.create("map_node", work_dir, force=True)

    cypress_spec_path = f"{work_dir}/spec.yaml"

    yt_client.smart_upload_file(filename=spec_path, destination=cypress_spec_path, placement_strategy="replace")

    row = DagEntityRow(spec_path=cypress_spec_path, work_dir=work_dir)
    yt_client.insert_rows(DagEntityRow.table_path, [asdict(row)])
    print(f"Dag registered: dag_id={row.dag_id}, work_dir={work_dir}")
    return row.dag_id

def dag_run_state(args):
    from dag_run import DagRun
    yt_client = yt.YtClient(proxy=args.yt_proxy)
    dag_run = DagRun.fetch_dagruns(yt_client=yt_client, run_id=args.run_id)
    if len(dag_run) == 0:
        print("No dag_run found with run_id=", args.run_id)
    print(dag_run[0].state)
    return dag_run[0].state


def taskrun_list(args):
    from dag_run import DagRun
    yt_client = yt.YtClient(proxy=args.yt_proxy)
    task_runs = DagRun.fetch_task_runs(yt_client, run_id=args.run_id)
    print(task_runs)
    print([
        {
            "task_id": tr.task_id,
            "state": tr.state,
            "operation_id": tr.operation_id,
        } for tr in task_runs
    ])
    return task_runs