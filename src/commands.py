from __future__ import annotations

from dag import DAG
from dagref import DagRef
from job import RunnerEnv

from yt.wrapper.schema import TableSchema, SortColumn
import yt.wrapper as yt

from runtime import build_runtime
from yt_wrapper import ClientContext, context_wrapper

from base import BaseRow

def import_all_dataclasses():
    from dagrun import DagRunRow
    from taskrun import TaskRunRow
    from dagref import DagMetaRow, DagRefRow

    return locals().values()

def _ensure_table(yt_client: yt.YtClient, row_type: type[BaseRow]):
    if not yt_client.exists(row_type.table_path):
        schema = TableSchema.from_row_type(row_type, unique_keys=row_type.unique_keys)
        for key in row_type.key_columns:
            schema = schema.build_schema_sorted_by(SortColumn(key, SortColumn.ASCENDING))
        yt_client.create("table", row_type.table_path, attributes={
            "schema": schema,
            "dynamic": True
        })
        yt_client.mount_table(row_type.table_path, sync=True)

def prepare_tables(config):
    client = ClientContext(config=config).create_client()
    for row_type in import_all_dataclasses():
        _ensure_table(client, row_type)

def run_scheduler(config):
    runtime = build_runtime(config=config)

    try:
        runtime.run()
    except:
        raise
    finally:
        runtime.shutdown()

def add_dag(*, config, spec: str = None, work_dir: str = None):
    env = RunnerEnv(job_id=add_dag.__name__, context=ClientContext(config=config))
    yt_client = env.dag_context.create_client()

    if not yt_client.exists(work_dir):
        yt_client.create("map_node", work_dir, force=True)

    cypress_spec_path = f"{work_dir}/spec.yaml"
    yt_client.smart_upload_file(filename=spec, destination=cypress_spec_path, placement_strategy="replace")

    dag = DAG.from_spec_conf(spec_path=cypress_spec_path, work_dir=work_dir, context_wrapper=context_wrapper(env.dag_context, env=env), yt_client=yt_client)

    return DagRef.try_add_dag(dag=dag, yt_client=yt_client)
