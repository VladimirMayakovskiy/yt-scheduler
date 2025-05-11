from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import yaml

from dag_entity import DagEntity
from dag_run import DagRun
from operator import BaseOperator, operators
from state import DagRunState

import yt.wrapper as yt

class DAG:
    dag_id: str

    default_args: dict[str, Any]

    spec: dict
    work_dir: str

    task_dict: dict[str, BaseOperator]

    @staticmethod
    def from_dag_entity(de: DagEntity):
        dag = DAG()

        spec_path = de.spec_path
        with open(spec_path, 'r') as file:
            dag.spec = yaml.safe_load(file)

        dag.work_dir = de.work_dir
        for step_name, step_spec in dag.spec.get("steps", {}):
            params = dict(step_spec)
            inlets = params.pop('input_table_paths', [])
            outlets = params.pop('output_table_paths', [])

            if dag.work_dir is not None:
                inlets = [os.path.join(dag.work_dir, path) for path in inlets]
                outlets = [os.path.join(dag.work_dir, path) for path in outlets]

            operation = params.get("operation", "")
            operator_cls = operators.get(operation)
            if not operator_cls:
                raise ValueError(f"Unknown operator type: {operation}")

            dag.task_dict[step_name] = operator_cls(task_id=step_name, dag=dag, inlets=inlets, outlets=outlets)
        return dag

    def create_dagrun(
            self,
            *,
            dag: DAG,
            state: DagRunState,
            yt_client: yt.YtClient,
            start_date: datetime | None = None,
            creating_job_id: str | None = None,
    ) -> DagRun:
        run_id = f"{self.dag_id}__scheduled__{start_date.isoformat()}"
        run = DagRun(
            dag=dag,
            dag_id=self.dag_id,
            run_id=run_id,
            start_date=start_date,
            state=state,
            creating_job_id=creating_job_id,
        )
        run.verify_integrity(yt_client=yt_client)
        return run
