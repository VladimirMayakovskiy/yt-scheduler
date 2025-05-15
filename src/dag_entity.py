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
        if yt_client.exists("//home/dag_state"):
            print("here")
            try:
                rows = list(yt_client.select_rows(
                    f"""
                    ds.dag_id as dag_id,
                    ds.is_paused as is_paused,
                    ds.spec_path as spec_path,
                    ds.work_dir as work_dir
                    from [{"//home/dag_state"}] as ds
                    where ds.is_paused = false
                    limit 20
                    """
                ))
            except Exception as e:
                print(e)
                raise
            # left join [{"//home/dag_run"}] as dr
            #                     on ds.dag_id = dr.dag_id
            #                 where ds.is_paused = false
            #                   and dr.dag_id is null
            #                 limit 20
        else:
            rows = []
        print("rows: ", rows)
        dags_to_run = [cls(**row) for row in rows]
        print(len(dags_to_run))
        return dags_to_run