from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import yaml

from dag_entity import DagEntity
from dag_run import DagRun
from dag_node import DAGNode
from ytoperator import BaseOperator, operators
from state import DagRunState

import yt.wrapper as yt

class DAG:
    dag_id: str

    default_args: dict[str, Any]

    # spec: dict
    work_dir: str

    task_dict: dict[str, DAGNode]

    @staticmethod
    def from_dag_entity(de: DagEntity, yt_client: yt.YtClient):
        print("from_dag_entity")
        dag = DAG()

        spec_path = de.spec_path
        spec = yaml.safe_load(yt_client.read_file(spec_path))

        print("OK", spec)

        dag.work_dir = de.work_dir
        dag.task_dict = {}
        dag.dag_id = de.dag_id
        steps = spec.get("steps", {})
        print(steps)
        for step_name, step_spec in steps.items():
            print(step_name, step_spec)
            params = step_spec
            print("OK1.5")
            inlets = params.pop('input_table_paths', [])
            outlets = params.pop('output_table_paths', [])

            print("OK2")

            if dag.work_dir is not None:
                inlets = [os.path.join(dag.work_dir, path) for path in inlets]
                outlets = [os.path.join(dag.work_dir, path) for path in outlets]
                params['input_table_paths'] = inlets
                params['output_table_paths'] = outlets

            print("OK3")

            operation = params.get("operation", "")
            operator_cls = operators.get(operation)
            print(operator_cls)
            if not operator_cls:
                continue
                # raise ValueError(f"Unknown operator type: {operation}")

            print("OK4")
            dag.task_dict[step_name] = operator_cls(task_id=step_name, dag_id=dag.dag_id, spec=spec)
            print("OK5")
        return dag

    def create_dagrun(
            self,
            *,
            # dag: DAG,
            state: DagRunState,
            yt_client: yt.YtClient,
            start_date: datetime | None = None,
            creating_job_id: str | None = None,
    ) -> DagRun:
        print("create_dagrun")
        run_id = f"{self.dag_id}__scheduled__{start_date.isoformat()}"
        run = DagRun(
            dag=self,
            dag_id=self.dag_id,
            run_id=run_id,
            start_date=start_date,
            state=state,
            creating_job_id=creating_job_id,
        )
        run.dag_run_prepare_for_execution(yt_client)
        # run.verify_integrity(yt_client=yt_client)
        return run

    @property
    def tasks(self) -> list[DAGNode]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict.keys())