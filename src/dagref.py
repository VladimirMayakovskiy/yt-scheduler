from __future__ import annotations

from datetime import datetime, timezone as tz
from typing import ClassVar, Optional, TYPE_CHECKING
from dataclasses import field, asdict, KW_ONLY

if TYPE_CHECKING:
    from dag import DAG

from base_row import YtRow, TablePath
from rows_helpers import make_formatted_select, format_select_columns_multi
from logging_mixin import LoggingMixin
from yt_wrapper import with_yt_client

import yt.wrapper as yt


@yt.yt_dataclass
class DagMetaRow(YtRow):
    table_path:  ClassVar[str] = TablePath("dag_meta")
    key_columns: ClassVar[list[str]] = ["id"]
    alias: ClassVar[str] = "meta"

    id: str = field(default_factory=lambda: yt.common.generate_uuid())

    _: KW_ONLY

    dag_id: str
    created_at: str = field(default_factory=lambda: datetime.now(tz.utc).isoformat())
    run_id: Optional[str] = None

class DagMeta(DagMetaRow, LoggingMixin):
    row_type: ClassVar[type[DagMetaRow]] = DagMetaRow

    @classmethod
    def fetch_rows(
        cls,
        *,
        id: str | list[str] | tuple[str] = None,
        dag_id: str | list[str] | tuple[str] = None,
        run_id: str | list[str] | tuple[str] = None,
        limit: int = None,
    ) -> list[DagMetaRow]:
        rows = make_formatted_select(
            cls=cls,
            id=id,
            dag_id=dag_id,
            run_id=run_id,
            limit=limit,
        )
        return [cls.row_type(**row) for row in rows]

    @classmethod
    def get(cls, dag_id: Optional[str] = None, run_id: Optional[str] = None, id: Optional[str] = None) -> "DagRef" | None:
        return super(DagMetaRow, cls).get(dag_id=dag_id, run_id=run_id, id=id)

@yt.yt_dataclass
class TaskRefRow(YtRow):
    table_path:  ClassVar[str] = TablePath("tasks")
    key_columns: ClassVar[list[str]] = ["dag_id", "task_id"]
    unique_keys: ClassVar[bool] = False
    alias: ClassVar[str] = "taskref"

    _: KW_ONLY

    dag_id: str
    task_id: str

    serialized_repr: str
    payload_hash: str
    created_at: str = field(default_factory=lambda: datetime.now(tz.utc).isoformat())

class TaskRef(TaskRefRow, LoggingMixin):
    row_type: ClassVar[type[TaskRefRow]] = TaskRefRow

    @classmethod
    def fetch_rows(
        cls,
        dag_id: str | list[str] | tuple[str] = None,
        task_id: str | list[str] | tuple[str] = None,
        payload_hash: str | list[str] | tuple[str] = None,
        limit: int = None,
    ) -> list[TaskRefRow]:
        rows = make_formatted_select(
            cls=cls,
            task_id=task_id,
            dag_id=dag_id,
            payload_hash=payload_hash,
            limit=limit,
        )
        return [cls.row_type(**row) for row in rows]

    @classmethod
    def get(cls, dag_id: str, task_id: str) -> "TaskRef" | None:
        return super(TaskRefRow, cls).get(dag_id=dag_id, task_id=task_id)

@yt.yt_dataclass
class DagRefRow(YtRow):
    table_path:  ClassVar[str] = TablePath("dags")
    key_columns: ClassVar[list[str]] = ["dag_id"]
    alias: ClassVar[str] = "dagref"

    _: KW_ONLY

    dag_id: str

    serialized_repr: str
    payload_hash: str
    created_at: str = field(default_factory=lambda: datetime.now(tz.utc).isoformat())

class DagRef(DagRefRow, LoggingMixin):
    row_type: ClassVar[type[DagRefRow]] = DagRefRow
    meta_row_type: ClassVar[type[DagMetaRow]] = DagMetaRow

    @classmethod
    def fetch_rows(
        cls,
        dag_id: str | list[str] | tuple[str] = None,
        payload_hash: str | list[str] | tuple[str] = None,
        limit: int = None,
    ) -> list[DagRefRow]:
        rows = make_formatted_select(
            cls=cls,
            dag_id=dag_id,
            payload_hash=payload_hash,
            limit=limit,
        )
        return [cls.row_type(**row) for row in rows]

    @classmethod
    def get(cls, dag_id: Optional[str] = None, payload_hash: Optional[str] = None) -> "DagRef" | None:
        return super(DagRefRow, cls).get(dag_id=dag_id, payload_hash=payload_hash)

    @classmethod
    @with_yt_client
    def dags_needing_dagruns(cls, yt_client: yt.YtClient) -> list:
        try:
            rows = list(yt_client.select_rows(
                f"""
                {format_select_columns_multi(cls, cls.meta_row_type, exclude_duplicates=True)}
                from [{cls.table_path}] as {cls.alias}
                left join [{cls.meta_row_type.table_path}] as {cls.meta_row_type.alias} 
                on {cls.alias}.dag_id = {cls.meta_row_type.alias}.dag_id
                where {cls.meta_row_type.alias}.run_id is NULL
                """,
                allow_join_without_index=True
            ))
        except Exception as e:
            cls.logger.exception("Failed to select rows for %s: %s", cls.__name__, e)
            return []
        return rows

    @staticmethod
    @with_yt_client
    def try_add_dag(dag: "DAG", yt_client: yt.YtClient) -> (bool, str, str):
        serialized_repr, payload_hash = dag.to_serialized_repr()

        found = DagRef.get(payload_hash=payload_hash)
        dag_id = found.dag_id if found else yt.common.generate_uuid()

        meta = DagMeta(dag_id=dag_id)
        DagMeta.create_rows(rows=meta)

        if not found:
            dag_ref = DagRef(
                dag_id=dag_id,
                serialized_repr=serialized_repr,
                payload_hash=payload_hash,
            )

            task_refs = [
                TaskRef(
                    task_id=task.task_id,
                    dag_id=dag_id,
                    serialized_repr=ser,
                    payload_hash=phash,
                )
                for task in dag.tasks
                for ser, phash in [task.to_serialized_repr()]
            ]
            with yt_client.Transaction(type="tablet"):
                yt_client.insert_rows(DagRef.table_path, [asdict(dag_ref)])
                yt_client.insert_rows(TaskRef.table_path, [asdict(task_ref) for task_ref in task_refs])
                DagMeta.create_rows(rows=meta)

            return True, dag_id, meta.id
        else:
            DagMeta.create_rows(rows=meta)
            return False, dag_id, meta.id