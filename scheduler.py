from dag import DAG, Task, _build_dag_from_spec, TaskState
from dag_run import DagRunState, DagRun


class Scheduler:
    pipelines: dict[str, DAG]

    def __init__(self):
        self.pipelines = {}

    def add_pipeline(self, spec: dict, work_dir: str) -> str:
        dag = _build_dag_from_spec(spec, work_dir)
        # validate
        self.pipelines[dag.dag_id] = dag
        return dag.dag_id

    def _prepare_pipeline(self, dag_id: str):
        dag = self.pipelines.get(dag_id)
        dag_run = DagRun.load_previous(dag_id, self.client)

        if dag_run and dag:
            task_records = dag_run.get_task_states()

            for row in task_records:
                if row['status'] in {TaskState.FAILED, TaskState.COMPLETED}:
                    dag.complete_task(row['task_id'])

    def run_pipeline(self, pipeline_id: str):
        dag = self.pipelines.get(pipeline_id)
        if not dag:
            raise RuntimeError()

        dag.build()

        self._prepare_pipeline(pipeline_id)

