import os
from dataclasses import asdict
from uuid import uuid4

from scheduler import Job, Scheduler, run_job
from dag_entity import DagEntityRow
import yt.wrapper as yt
from yt.wrapper.schema import TableSchema

def scheduler(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    job_runner = Scheduler(job = Job(), yt_client=yt_client)
    run_job(job=job_runner.job, execute_callable=job_runner._execute, yt_client=yt_client)

def add_dag(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)
    # загружаем spec в ytsaurus
    spec_path = args.spec
    work_dir = args.work_dir

    if not yt_client.exists(work_dir):
        yt_client.create("map_node", work_dir, force=True)

    with open(spec_path, "rb") as f:
        spec = f.read()

    cypress_spec_path = f"{work_dir}/spec.yaml"
    yt_client.write_file(cypress_spec_path, spec)

    dag_id = f"dag_{uuid4().hex[:8]}" # args.dag_id or 
    row = DagEntityRow(dag_id=dag_id, is_paused=False, spec_path=cypress_spec_path, work_dir=work_dir)

    if not yt_client.exists("//home/dag_state"):
        yt_client.create("table", "//home/dag_state", attributes={"schema": TableSchema.from_row_type(DagEntityRow) , "dynamic": True})
        yt_client.mount_table("//home/dag_state", sync=True)

    yt_client.insert_rows("//home/dag_state", [asdict(row)])
    # yt_client.write_table_structured(
    #     "//home/dag_state",
    #     DagEntityRow,
    #     [
    #         row,
    #     ],
    # )

    print(f"Dag registered: dag_id={dag_id}, work_dir={work_dir}")
