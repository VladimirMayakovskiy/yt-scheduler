from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import yaml

from dag_entity import DagEntity
from dag_run import DagRun, DagRunRow
from dag_node import DAGNode
from ytoperator import BaseOperator, operators
from state import DagRunState
import uuid

import yt.wrapper as yt

class DAG:
    dag_id: str

    default_args: dict[str, Any]

    work_dir: str

    task_dict: dict[str, DAGNode] # task_dict: dict[str, Operator]
    upstream: dict[str, list[str]]
    downstream: dict[str, list[str]]

    @classmethod
    def from_dag_entity(cls, de: DagEntity, yt_client: yt.YtClient):
        print("DAG.from_dag_entity")
        dag = cls.__new__(cls)

        dag.dag_id = de.dag_id
        dag.work_dir = de.work_dir
        dag.task_dict: dict[str, DAGNode] = {}

        spec = yaml.safe_load(yt_client.read_file(de.spec_path))

        outlets_producers: dict[str, list[BaseOperator]] = {} # TODO
        for task_id, params in spec.get("steps", {}).items():
            print(task_id, params)
            cfg = dict(params)
            inlets = cfg.pop('input_table_paths', [])
            outlets = cfg.pop('output_table_paths', [])

            abs_inlets = [os.path.join(dag.work_dir, p) if dag.work_dir is not None else p for p in inlets]
            abs_outlets = [os.path.join(dag.work_dir, p) if dag.work_dir is not None else p for p in outlets]

            cfg['input_table_paths'] = abs_inlets
            cfg['output_table_paths'] = abs_outlets

            operation = cfg.get("operation", "")
            operator_cls = operators.get(operation)
            print("CREATE OPERATOR: ", operator_cls)
            if not operator_cls:
                continue
                # raise ValueError(f"Unknown operator type: {operation}")

            operator = operator_cls(task_id=task_id, dag_id=dag.dag_id, spec=cfg) # TODO
            dag.task_dict[task_id] = operator
            print("OK from_dag_entity")

            for outlet in abs_outlets:
                outlets_producers.setdefault(outlet, []).append(operator)

        #TODO upstreams, downstreams
        for tid, op in dag.task_dict.items():
            for inlet in op.inlets:
                if producers := outlets_producers.get(inlet):
                    op.set_upstream(producers)

        dag.upstream = {tid: list(op.upstream_task_ids) for tid, op in dag.task_dict.items()}
        dag.downstream = {tid: list(op.downstream_task_ids) for tid, op in dag.task_dict.items()}
        print(dag.upstream, dag.downstream)
        return dag

    @staticmethod
    def create_dagrun(
            *,
            yt_client: yt.YtClient,
            dag: DAG,
            state: DagRunState,
            start_date: datetime | None = None,
            creating_job_id: str | None = None,
    ) -> DagRun:
        print("DAG.create_dagrun")
        # row = DagRunRow(dag_id=dag.dag_id, start_date=start_date, state=state) # TODO
        run = DagRun(dag_id=dag.dag_id, state=state, dag=dag, start_date=start_date, creating_job_id=creating_job_id)
        # run_id = uuid.uuid4().hex # TODO
        # run = DagRun(
        #     dag=dag,
        #     dag_id=dag.dag_id,
        #     run_id=run_id,
        #     start_date=start_date,
        #     state=state,
        #     creating_job_id=creating_job_id,
        # )
        run.dag_run_prepare_for_execution(yt_client)
        return run

    @property
    def tasks(self) -> list[DAGNode]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict.keys())

    @property
    def roots(self) -> list[DAGNode]: #TODO
        return [self.task_dict.get(tid, None) for tid, ups in self.upstream.items() if not ups]

    @property
    def leaves(self) -> list[DAGNode]:
        return [self.task_dict.get(tid, None) for tid, downs in self.downstream.items() if not downs]