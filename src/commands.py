from __future__ import annotations
import yaml

from dag import DAG
from dagref import DagRef
from job import RunnerEnv
from base import BaseRow
from runtime import build_runtime
from yt_wrapper import ClientContext, context_wrapper

from yt.wrapper.schema import TableSchema, SortColumn
import yt.wrapper as yt

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

def add_dag(*, config, spec: str | dict, work_dir: str = None):
    env = RunnerEnv(job_id=add_dag.__name__, context=ClientContext(config=config))
    yt_client = env.dag_context.create_client()

    try:
        if isinstance(spec, str):
            spec_path = spec
            with open(spec_path, "rb") as f:
                spec_conf = yaml.safe_load(f)
        elif isinstance(spec, dict):
            spec_conf = spec
        else:
            raise ValueError(f"Invalid spec type: {type(spec)}")

        if not yt_client.exists(work_dir):
            yt_client.create("map_node", work_dir, force=True)

        cypress_spec_path = f"{work_dir}/spec.yaml"
        yt_client.write_file(cypress_spec_path, yaml.safe_dump(spec_conf).encode("utf-8"))
    except Exception as e:
        print(f"Failed to load spec: {e}")
        raise

    dag = DAG.from_spec_conf(spec=spec_conf, work_dir=work_dir, context_wrapper=context_wrapper(env.dag_context, env=env))

    return DagRef.try_add_dag(dag=dag, yt_client=yt_client)
