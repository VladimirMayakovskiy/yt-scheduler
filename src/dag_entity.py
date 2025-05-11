from __future__ import annotations
import yt.wrapper as yt

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
        with yt_client.Transaction(type="tablet"):
            rows = list(yt_client.select_rows(
                f"""
                dag_state.dag_id as dag_id,
                dag_state.is_paused as is_paused,
                dag_state.spec_path as spec_path,
                dag_state.workdir as workdir
                from [{"//home/dag_state"}] as ds
                where ds.dag_id = "{dag_id}"
                limit 1
                """
            ))
        if not rows:
            return None
        return cls(**rows[0])

    @classmethod
    def dags_needing_dagruns(cls, yt_client: yt.YtClient) -> list["DagEntity"]:
        with yt_client.Transaction(type="tablet"):
            rows = list(yt_client.select_rows(
                f"""
                dag_state.dag_id as dag_id,
                dag_state.is_paused as is_paused,
                dag_state.spec_path as spec_path,
                dag_state.workdir as workdir
                from [{"//home/dag_state"}] as ds
                left join [{"//home/dag_run"}] as dr
                    on ds.dag_id = dr.dag_id
                where ds.is_paused = false
                  and dr.dag_id is null
                limit 20
                """
            ))
        dags_to_run = [cls(**row) for row in rows]
        return dags_to_run