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
from logging_mixin import LoggingMixin

import yt.wrapper as yt

class DAG(LoggingMixin):
    dag_id: str

    default_args: dict[str, Any]

    work_dir: str

    task_dict: dict[str, DAGNode] # task_dict: dict[str, Operator]
    upstream: dict[str, list[str]]
    downstream: dict[str, list[str]]

    def __repr__(self):
        return f"DAG(dag_id={self.dag_id}, work_dir={self.work_dir}, task_dict={self.task_dict}, upstream={self.upstream})"

    @classmethod
    def from_dag_entity(cls, de: DagEntity, yt_client: yt.YtClient):
        cls.logger.info(f"Creating DAG for dag_entity: {de}")
        dag = cls.__new__(cls)

        dag.dag_id = de.dag_id
        dag.work_dir = de.work_dir
        dag.task_dict: dict[str, DAGNode] = {}

        spec = yaml.safe_load(yt_client.read_file(de.spec_path))

        outlets_producers: dict[str, list[BaseOperator]] = {} # TODO
        for task_id, params in spec.get("steps", {}).items():
            cls.logger.info(f"STEP: {task_id}\nspec: {params}")
            cfg = dict(params)
            inlets = cfg.pop('input_table_paths', [])
            outlets = cfg.pop('output_table_paths', [])

            abs_inlets = [os.path.join(dag.work_dir, p) if dag.work_dir is not None else p for p in inlets]
            abs_outlets = [os.path.join(dag.work_dir, p) if dag.work_dir is not None else p for p in outlets]

            cfg['input_table_paths'] = abs_inlets
            cfg['output_table_paths'] = abs_outlets

            operation = cfg.get("operation", "")
            operator_cls = operators.get(operation)
            # cls.logger.info("CREATE OPERATOR: ", operator_cls) todo in operator
            if not operator_cls:
                # todo make failed
                cls.logger.error(f"Unknown operator type: {operation}")
                continue

            operator = operator_cls(task_id=task_id, dag_id=dag.dag_id, spec=cfg) # TODO
            dag.task_dict[task_id] = operator

            for outlet in abs_outlets:
                outlets_producers.setdefault(outlet, []).append(operator)

        #TODO upstreams, downstreams
        for tid, op in dag.task_dict.items():
            for inlet in op.inlets:
                if producers := outlets_producers.get(inlet):
                    op.set_upstream(producers)

        dag.upstream = {tid: list(op.upstream_task_ids) for tid, op in dag.task_dict.items()}
        dag.downstream = {tid: list(op.downstream_task_ids) for tid, op in dag.task_dict.items()}
        cls.logger.info(f"CREATED DAG: {dag}")
        return dag

    @staticmethod
    def create_dagrun(
            *,
            yt_client: yt.YtClient,
            dag: DAG,
            state: DagRunState,
            creating_job_id: str | None = None,
    ) -> DagRun:
        run = DagRun(dag_id=dag.dag_id, state=state, dag=dag, creating_job_id=creating_job_id)
        DAG.logger.info(f"CREATED DAGRUN for dag={dag.dag_id}: {run}")
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