import os
from dataclasses import asdict

from scheduler import Job, Scheduler, run_job
from dag_entity import DagEntityRow
import yt.wrapper as yt
from yt_table_client import YtDynTableClient
from yt.wrapper.schema import TableSchema, SortColumn


def _ensure_table(yt_client: yt.YtClient, row_type: type):
    if not yt_client.exists(row_type.table_path):
        try:
            schema = TableSchema.from_row_type(row_type, unique_keys=row_type.unique_keys)
            for key in row_type.key_columns:
                schema = schema.build_schema_sorted_by(SortColumn(key, SortColumn.ASCENDING))
            yt_client.create("table", row_type.table_path, attributes={
                "schema": schema ,
                "dynamic": True
            })
            yt_client.mount_table(row_type.table_path, sync=True)
        except Exception as e:
            print(e) # TODO
            raise

def scheduler(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    _ensure_table(yt_client, DagEntityRow)
    from dag_run import DagRunRow
    _ensure_table(yt_client, DagRunRow)
    from task_run import TaskRunRow
    _ensure_table(yt_client, TaskRunRow)

    scheduler = Scheduler(job = Job(), yt_client=yt_client)
    run_job(job=scheduler.job, execute_callable=scheduler._execute, yt_client=yt_client)

def add_dag(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    spec_path = args.spec
    work_dir = args.work_dir

    if not yt_client.exists(work_dir):
        yt_client.create("map_node", work_dir, force=True)

    with open(spec_path, "rb") as f:
        spec = f.read()

    cypress_spec_path = f"{work_dir}/spec.yaml"
    yt_client.write_file(cypress_spec_path, spec)

    row = DagEntityRow(spec_path=cypress_spec_path, work_dir=work_dir)
    _ensure_table(yt_client, DagEntityRow)
    yt_client.insert_rows(DagEntityRow.table_path, [asdict(row)])

    print(f"Dag registered: dag_id={row.dag_id}, work_dir={work_dir}")
