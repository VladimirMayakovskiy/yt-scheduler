from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Optional
from dataclasses import field, asdict
from serialized import SerializedDag
from dag import DAG
import uuid
import yt.wrapper as yt
import hashlib
from yt_wrapper import with_yt_client
from base import get_all_row_fields, BaseRow

@yt.yt_dataclass
class DagMetaRow(BaseRow):
    table_path:  ClassVar[str] = "//tmp/dag_meta"
    key_columns: ClassVar[list[str]] = ["id"]

    dag_id: str
    created_at: Optional[str] = None
    run_id: Optional[str] = None

    id: str = field(default_factory=lambda: uuid.uuid4().hex)

@yt.yt_dataclass
class DagRefRow(BaseRow):
    table_path:  ClassVar[str] = "//tmp/dags"
    key_columns: ClassVar[list[str]] = ["dag_id"]

    dag_id: str

    serialized_dag: str
    payload_hash: str
    load_at: str

class DagRef(DagRefRow):
    row_type: ClassVar[type[DagRefRow]] = DagRefRow
    meta_row_type: ClassVar[type[DagMetaRow]] = DagMetaRow


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    @with_yt_client
    def dags_needing_dagruns(cls, yt_client: yt.YtClient) -> list:
        try:
            rows = list(yt_client.select_rows(
                f"""
                    d.dag_id AS dag_id,
                    d.serialized_dag AS serialized_dag,
                    d.payload_hash AS payload_hash,
                    d.load_at AS load_at,
                    meta.id AS id,
                    meta.created_at AS created_at
                    FROM [{cls.table_path}] AS d
                    LEFT JOIN [{cls.meta_row_type.table_path}] AS meta ON d.dag_id = meta.dag_id
                    WHERE meta.run_id IS NULL
                    LIMIT 2
                """,
                allow_join_without_index=True
            ))
        except Exception as e:
            print("Failed to select rows:")
            return []
        return rows

    @staticmethod
    def get_serialized_dag(dag: DAG):
        serialized_dag = SerializedDag.to_json(SerializedDag.serialize_dag(dag))
        payload_hash = hashlib.sha256(serialized_dag.encode("utf-8")).hexdigest()
        return DagRef(serialized_dag=serialized_dag, payload_hash=payload_hash, load_at=datetime.utcnow().isoformat(),
                           dag_id=dag.dag_id)

    @staticmethod
    def try_add_dag(dag: DAG, yt_client: yt.YtClient) -> (bool, str):
        row = DagRef.get_serialized_dag(dag)
        try:
            found = list(yt_client.select_rows(
                f"""
                d.payload_hash AS payload_hash,
                d.dag_id AS dag_id
                FROM [{DagRef.table_path}] AS d
                WHERE d.payload_hash = "{row.payload_hash}"
                LIMIT 1
                """
            ))
        except Exception:
            raise

        if not found:
            yt_client.insert_rows(DagRef.table_path, [asdict(row)])
            dag_id, ret = row.dag_id, True
        else:
            dag_id, ret = found[0].get("dag_id"), False

        meta_row = DagRef.meta_row_type(dag_id=dag_id, created_at=datetime.utcnow().isoformat())
        yt_client.insert_rows(DagRef.meta_row_type.table_path, [asdict(meta_row)])
        return ret, dag_id

    @classmethod
    @with_yt_client
    def get(cls, dag_id: str, yt_client: yt.YtClient) -> "DagRef" | None:
        try:
            rows = list(yt_client.select_rows(
                f"""
                {get_all_row_fields(DagRef, alias="ds")}
                from [{DagRef.table_path}] as ds
                where ds.dag_id = "{dag_id}"
                limit 1
                """
            ))
        except Exception as e:
            # cls.logger.exception("Failed to select rows:")
            raise
        if not rows:
            print(f"Can not find DagEntity with dag_id={dag_id}")
            return None
        print(f"Get rows: {rows}")
        return cls(**rows[0])