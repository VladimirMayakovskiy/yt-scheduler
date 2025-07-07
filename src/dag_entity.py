from __future__ import annotations

from importlib import import_module

import yt.wrapper as yt
from yt.wrapper.schema import TableSchema

@yt.yt_dataclass
class DagEntityRow:
    dag_id: str
    is_paused: bool

    spec_path: str
    work_dir: str

class DagEntity(DagEntityRow):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def get(cls, dag_id: str, yt_client: yt.YtClient) -> "DagEntity" | None:
        print("get")
        if yt_client.exists("//home/dag_state"):
            rows = list(yt_client.select_rows(
                f"""
                ds.dag_id as dag_id,
                ds.is_paused as is_paused,
                ds.spec_path as spec_path,
                ds.work_dir as work_dir
                from [{"//home/dag_state"}] as ds
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
        if not yt_client.exists("//home/dag_state"):
            return []

        if not yt_client.exists("//home/dag_run"):
            from dag_run import DagRun
            yt_client.create("table", "//home/dag_run",
                             attributes={"schema": TableSchema.from_row_type(DagRun), "dynamic": True})
            yt_client.mount_table("//home/dag_run", sync=True)

        try:
            rows = list(yt_client.select_rows(
                f"""
                ds.dag_id as dag_id,
                ds.is_paused as is_paused,
                ds.spec_path as spec_path,
                ds.work_dir as work_dir
                from [{"//home/dag_state"}] as ds
                left join [{"//home/dag_run"}] as dr
                    on ds.dag_id = dr.dag_id
                where dr.dag_id is null 
                    and ds.is_paused = false
                limit 20
                """
            ))
        except Exception as e:
            print(e)
            raise
        print("rows: ", rows)
        dags_to_run = [cls(**row) for row in rows]
        print(len(dags_to_run))
        return dags_to_run