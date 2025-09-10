from __future__ import annotations

import yaml
from typing import Optional, Union, get_type_hints

from dag import DAG
from job import JobList, LightweightJob, JobRunner, JobContext
from base_row import YtRow, Config
from rows_helpers import import_all_dataclasses, init_all_from_config
from executor import Executor
from scheduler import Scheduler
from pool import Pool
from logging_mixin import logger
from yt_wrapper import ClientAgent, with_yt_client

from yt.wrapper.schema import TableSchema, SortColumn
import yt.wrapper as yt

def run_command(config: Config, jobs: JobList, timeout: Optional[float] = 1.0):
    init_all_from_config(config=config)

    context = JobContext(config=config).build(registry=jobs)
    client_agent = ClientAgent(config=config)

    pool = None
    if any(not issubclass(type(job), (LightweightJob,)) for job in context):
        pool = Pool() # todo build from config

    for job in context:
        hints = get_type_hints(job.__class__.__init__)
        pool_required_param = hints.get("pool")
        if pool_required_param:
            origin = getattr(pool_required_param, "__origin__", None)
            args = getattr(pool_required_param, "__args__", ())
            if pool_required_param == Pool or (origin is Union and Pool in args):
                job.pool = job.pool or pool

    runners = [JobRunner(job=job, agent=client_agent) for job in context]

    try:
        if pool:
            pool.run()

        for runner in runners:
            runner.run_job()

        for runner in runners:
            runner._join()
            if runner.exception:
                logger.exception(runner.exception)
    finally:
        if pool:
            try:
                pool.shutdown(wait=True, timeout=timeout)
            except Exception as e:
                logger.exception(f"Exception occurred with pool shutdown %s", e)

        for runner in runners:
            runner.stop(timeout=timeout)
            if runner.exception:
                logger.exception(f"Job ended with an exception %s", runner.exception)

def _ensure_table(yt_client: yt.YtClient, row_type: type[YtRow]):
    if not yt_client.exists(row_type.table_path):
        schema = TableSchema.from_row_type(row_type)
        sort_columns = [SortColumn(name, SortColumn.ASCENDING) for name in row_type.key_columns]
        schema = schema.build_schema_sorted_by(sort_columns)
        schema.unique_key = row_type.unique_keys
        yt_client.create(
            "table",
            row_type.table_path,
            attributes={
                "schema": schema,
                "dynamic": True
            }
        )
        yt_client.mount_table(row_type.table_path, sync=True)

def prepare_tables(config):
    @with_yt_client
    def _prepare_tables_impl(yt_client: yt.YtClient):
        for row_type in import_all_dataclasses():
            _ensure_table(yt_client, row_type)

    job_list = JobList().append(
        LightweightJob,
        lambda ctx: LightweightJob(ctx, func=_prepare_tables_impl)
    )
    run_command(config=config, jobs=job_list)

def run_scheduler(config):
    job_list = JobList().append(Executor, lambda ctx: Executor(ctx)) \
                        .append(Scheduler, lambda ctx: Scheduler(ctx))

    run_command(config=config, jobs=job_list)

@with_yt_client
def _add_dag_impl(yt_client: yt.YtClient, *, spec: str | dict, work_dir: str = None):
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
        logger.error(f"Failed to load spec %s: %s", spec, e)
        raise

    return DAG.try_add_dag(spec=spec_conf, work_dir=work_dir)

def add_dag(*, config, spec: str | dict, work_dir: str = None):

    job_list = JobList().append(
        LightweightJob,
        lambda ctx: LightweightJob(ctx, func=lambda: _add_dag_impl(spec=spec, work_dir=work_dir))
    )
    run_command(config=config, jobs=job_list)
