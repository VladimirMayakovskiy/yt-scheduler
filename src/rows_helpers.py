from __future__ import annotations

import dataclasses
import typing
from datetime import datetime

from config import Config
from base_row import T
if typing.TYPE_CHECKING:
    from scheduler import ShardingOptions
    from base_row import YtRow
from yt_wrapper import with_yt_client
import yt.wrapper as yt

def copy_fields(to_obj, from_obj, cls: type[T] = None):
    if not cls:
        cls = type(to_obj)
    for field in get_all_row_fields(cls):
        try:
            setattr(to_obj, field, getattr(from_obj, field))
        except Exception as e:
            cls.logger.debug("Failed to copy field %s from %s to %s: %s",
                             field, type(from_obj).__name__, type(to_obj).__name__, e)

def from_dict(cls: type[T], data: dict) -> T:
    row_type = cls.row_type if hasattr(cls, "row_type") else cls
    row_type_fields = {f.name for f in dataclasses.fields(row_type)}
    filtered_data = {
        k: v for k, v in data.items() if k in row_type_fields
    }
    return row_type(**filtered_data)

def get_all_row_fields(cls: type[T]) -> list[str]:
    row_type = cls.row_type if hasattr(cls, "row_type") else cls
    return [field.name for field in dataclasses.fields(row_type) if field.init]

def set_param(params, name, value, transform=None):
    if value is not None:
        if transform is not None:
            params[name] = transform(value)
        else:
            params[name] = value
    return params

def process_filter_value(value):
    if isinstance(value, (list, tuple)):
        flatten = ", ".join(f"'{v}'" for v in value)
        return type(value), f"({flatten})"
    elif isinstance(value, (str, datetime)):
        return str, f"'{value}'"
    else:
        return type(value), value

def format_select_columns_multi(*classes: type, exclude_duplicates: bool = True) -> str:
    seen_aliases = set()
    columns = []
    for cls in classes:
        row_type = getattr(cls, "row_type", cls)

        for col in dataclasses.fields(row_type):
            if not col.init:
                continue

            alias = col.name
            if alias in seen_aliases:
                if exclude_duplicates:
                    continue
                else:
                    alias = f"{row_type.alias}_{col.name}"
            seen_aliases.add(alias)
            columns.append(f"{row_type.alias}.{col.name} AS {alias}")
    return ",\n".join(columns)


def format_select_columns(cls: type[T], ):
    row_type = getattr(cls, "row_type", cls)
    cols = ",\n".join(f"{row_type.alias}.{col.name} as {col.name}"
                      for col in dataclasses.fields(row_type) if col.init)
    return cols

@with_yt_client
def make_formatted_select(
    cls: type[T],
    yt_client: yt.YtClient,
    *,
    limit: int,
    shard_key: str = None,
    shard: "ShardingOptions" | None = None,
    **kwargs: str | None
) -> list[dict]:
    row_type = getattr(cls, "row_type", cls)
    cols = format_select_columns(cls)

    def _is_list_like(value_type):
        return issubclass(value_type, (list, tuple))

    filters = {}
    for col, val in kwargs.items():
        set_param(filters, col, val, transform=process_filter_value)

    if shard:
        shard_key = shard_key or (row_type.key_columns if not isinstance(row_type.key_columns, (list, tuple)) else row_type.key_columns[0])
        num_virtual_shards = shard.get("num_virtual_shards", shard["num_shards"])
        set_param(
            filters,
            f"(farm_hash({shard_key}) % {num_virtual_shards}) % {shard['num_shards']}",
            shard["shard_index"],
            transform=process_filter_value
        )
    try:
        return list(yt_client.select_rows(
            f"""
            {cols}
            from [{row_type.table_path}] as {row_type.alias}
            {"where " + " and ".join(
                [f"{name} {'in' if _is_list_like(val_type) else '='} {val}"
                 for name, (val_type, val) in filters.items()]
            ) if filters else ""}
            {f"limit {limit}" if limit else ""}
            """
        ))
    except Exception as e:
        cls.logger.exception("Failed to select rows for %s with filters %s: %s", cls.__name__, cols, e)
        raise

def import_all_dataclasses():
    from dagrun import DagRunRow
    from taskrun import TaskRunRow
    from dagref import DagMetaRow, DagRefRow, TaskRefRow
    from job import _JobRun

    return locals().values()

def init_all_from_config(config: Config):
    for cls in import_all_dataclasses():
        cls.init_from_config(config)

def get_row_key_columns_map() -> dict[type["YtRow"], list[str]]:
    return {
        row_type.table_path: row_type.key_columns for row_type in import_all_dataclasses()
    }