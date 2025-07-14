from __future__ import annotations

from importlib import import_module

from typing import ClassVar
from dataclasses import field
import uuid

import yt.wrapper as yt
from yt.wrapper.schema import TableSchema

from dag_run import DagRun


@yt.yt_dataclass
class DagEntityRow:
    table_path:  ClassVar[str] = "//tmp/dag_state"
    key_columns: ClassVar[list[str]] = ["dag_id"]
    unique_keys: ClassVar[bool] = True

    spec_path: str
    work_dir: str

    dag_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    is_paused: bool = False


class DagEntity(DagEntityRow):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def get(cls, dag_id: str, yt_client: yt.YtClient) -> "DagEntity" | None:
        print("DagEntity.get")
        if yt_client.exists(DagEntityRow.table_path):
            rows = list(yt_client.select_rows(
                f"""
                ds.dag_id as dag_id,
                ds.is_paused as is_paused,
                ds.spec_path as spec_path,
                ds.work_dir as work_dir
                from [{DagEntityRow.table_path}] as ds
                where ds.dag_id = "{dag_id}"
                limit 1
                """
            ))
        else:
            rows = []
        if not rows:
            return None
        print(rows)
        return cls(**rows[0])

    @classmethod
    def dags_needing_dagruns(cls, yt_client: yt.YtClient) -> list["DagEntity"]:
        print("DagEntity.dags_needing_dagruns")

        if not yt_client.exists(DagEntityRow.table_path):
            return []

        def _query(cond):
            return f"""
                ds.dag_id as dag_id,
                ds.is_paused as is_paused,
                ds.spec_path as spec_path,
                ds.work_dir as work_dir
                FROM [{DagEntityRow.table_path}] AS ds
                {cond} 
                limit 20
                """

        try:
            rows = list(yt_client.select_rows(
                _query(f"LEFT JOIN [{DagRun.table_path}] AS dr ON ds.dag_id = dr.dag_id "
                       f"WHERE dr.dag_id IS NULL AND ds.is_paused = false ")
                if yt_client.exists(DagRun.table_path) else
                _query(f"WHERE ds.is_paused = false "),
                allow_join_without_index=True
            ))
        except Exception as e:
            print(e)
            raise
        print("rows: ", rows)
        dags_to_run = [cls(**row) for row in rows]
        return dags_to_run