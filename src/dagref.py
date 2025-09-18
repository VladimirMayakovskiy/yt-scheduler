from __future__ import annotations

from datetime import datetime, timezone as tz
from typing import ClassVar, Optional, TYPE_CHECKING
from dataclasses import field, KW_ONLY

from logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from scheduler import ShardingOptions
from base_row import YtRow, TablePath
from rows_helpers import make_formatted_select, format_select_columns_multi, from_dict, process_filter_value
from yt_wrapper import with_yt_client
import yt.wrapper as yt


@yt.yt_dataclass
class DagMetaRow(YtRow):
    table_path:  ClassVar[str] = TablePath("dag_meta")
    key_columns: ClassVar[str] = ["id"]
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
    key_columns: ClassVar[str] = ["dag_id"]
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
    def dags_needing_dagruns(
            cls, shard: "ShardingOptions", yt_client: yt.YtClient
    ) -> list[tuple[DagRef.row_type, DagRef.meta_row_type]]:
        num_virtual_shards, num_shards, shard_index = shard["num_virtual_shards"], shard["num_shards"], shard["shard_index"]
        try:
            rows = list(yt_client.select_rows(
                f"""
                {format_select_columns_multi(cls, cls.meta_row_type, exclude_duplicates=True)}
                from [{cls.table_path}] as {cls.alias}
                left join [{cls.meta_row_type.table_path}] as {cls.meta_row_type.alias}
                on {cls.alias}.dag_id = {cls.meta_row_type.alias}.dag_id
                where run_id is NULL and (farm_hash(dag_id) % {num_virtual_shards}) % {num_shards} = {shard_index}
                """,
                allow_join_without_index=True
            ))
            return [(from_dict(DagRef.row_type, row), from_dict(DagRef.meta_row_type, row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select rows for %s: %s", cls.__name__, e)
            raise

    @classmethod
    @with_yt_client
    def dags_needing_dagruns_of_metas(
            cls, metas: list[str], yt_client: yt.YtClient
    ) -> list[tuple[DagRef.row_type, DagRef.meta_row_type]]:
        if not metas:
            return []
        try:
            _, metas = process_filter_value(metas)
            rows = list(yt_client.select_rows(
                f"""
                {format_select_columns_multi(cls, cls.meta_row_type, exclude_duplicates=True)}
                from [{cls.table_path}] as {cls.alias}
                left join [{cls.meta_row_type.table_path}] as {cls.meta_row_type.alias} 
                on {cls.alias}.dag_id = {cls.meta_row_type.alias}.dag_id
                where {cls.meta_row_type.alias}.id in {metas} and {cls.meta_row_type.alias}.run_id is NULL 
                """,
                allow_join_without_index=True
            ))
            return [(from_dict(DagRef.row_type, row), from_dict(DagRef.meta_row_type, row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select rows for %s: %s", cls.__name__, e)
            raise