from __future__ import annotations
from datetime import datetime
from enum import Enum

import yt.wrapper as yt

from dag import DAG, TaskState


class DagRunState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class DagRun:
    dag_id: str
    run_id: str
    start_date: datetime | None
    end_date: datetime | None
    queued_at: datetime | None

    spec_path: str
    work_dir: str

    _state: DagRunState | None

    yt_client: yt.YtClient

    def __init__(
            self,
            *,
            dag_id: str,
            run_id: str = None,
            spec_path: str | None = None,
            work_dir: str | None = None,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
            queued_at: datetime | None,
            yt_client: yt.YtClient,
            state: DagRunState | None = None
    ):
        self.dag_id = dag_id
        self.run_id = run_id or f"{dag_id}_{datetime.now().utcnow().isoformat()}"
        self.spec_path = spec_path
        self.work_dir = work_dir
        self.start_date = start_date
        self.end_date = end_date
        self.queued_at = queued_at
        self.yt_client = yt_client
        if state is not None:
            self._state = state

    def get_task_states(self):
        query = f"""
        SELECT task_id, state FROM [TASK_TABLE]
        WHERE dag_id = '{self.dag_id}' AND run_id = '{self.run_id}'
        """
        results = self.yt_client.select_rows(query)

        return [
            {
                "task_id": row["task_id"],
                "status": TaskState(row["status"])
            }
            for row in results
        ]

    @staticmethod
    def load_previous(dag_id: str, yt_client: yt.YtClient):
        query = f"""
            SELECT run_id, status, queued_at, start_date, end_date FROM GRAPH_TABLE
            WHERE dag_id = '{dag_id}'
            ORDER BY start_date DESC LIMIT 1
            """
        result = yt_client.select_rows(query)

        if result:
            row = result[0]
            return DagRun(
                dag_id=dag_id,
                run_id=row['run_id'],
                state=DagRunState(row['status']),
                queued_at=row['queued_at'],
                start_date=row['start_date'],
                end_date=row['end_date'],
                yt_client=yt_client
            )
        return None

    def _save_state(self):
        row = {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "status": self._state,
            "start_date": self.start_date.isoformat(),
            "end_date": self.start_date.isoformat(),
            "work_dir": self.work_dir
        }
        self.yt_client.insert_rows(GRAPH_TABLE, [row])

    def get_state(self) -> DagRunState:
        return self._state

    def set_state(self, state: DagRunState) -> None:
        if self._state != state:
            if state == DagRunState.PENDING:
                self.start_date = None
                self.end_date = None
            if state == DagRunState.RUNNING:
                if self._state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.start_date = datetime.utcnow()
                else:
                    self.start_date = self.start_date or datetime.utcnow()
                self.end_date = None
            if self._state in [DagRunState.PENDING, DagRunState.RUNNING] or self._state is None:
                if state in [DagRunState.FAILED, DagRunState.SUCCESS]:
                    self.end_date = datetime.utcnow()
            self._state = state

    def refresh_state(self) -> None:
        query = f"""
        SELECT status, queued_at, start_date, end_date
        FROM {self.__table_name__}
        WHERE dag_id = '{self.dag_id}' AND run_id = '{self.run_id}'
        LIMIT 1
        """
        result = self.yt_client.select_rows(query)
        if result:
            row = result[0]
            self._state = DagRunState(row['status'])
            self.queued_at = row.get("queued_at")
            self.start_date = row.get("start_date")
            self.end_date = row.get("end_date")

    def update_state(self):
        query = f"""
        SELECT step_id, state FROM [self.__table__name__]
        WHERE dag_id = '{self.dag_id}' AND run_id = '{self.run_id}'
        """
        tasks = self.yt_client.select_rows(query)

        # finished_tasks =
        # failed_tasks =
        # running_tasks =
        # queued_tasks =

