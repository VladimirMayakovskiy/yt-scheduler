from __future__ import annotations

import dataclasses

from config import Config
from base_row import T
from yt_wrapper import with_yt_client
import yt.wrapper as yt

def get_all_row_fields(cls: type[T], alias: str) -> str:
    row_type = cls.row_type if hasattr(cls, "row_type") else cls
    return ",\n".join(
        f"{alias}.{f.name} AS {f.name}"
        for f in dataclasses.fields(row_type) if f.init
    )

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
    else:
        return str, f"'{value}'"

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
def make_formatted_select(cls: type[T], yt_client: yt.YtClient, limit: int, **kwargs: str | None) -> list[dict]:
    row_type = getattr(cls, "row_type", cls)
    cols = format_select_columns(cls)

    def _is_list_like(value_type):
        return issubclass(value_type, (list, tuple))

    filters = {}
    for col, val in kwargs.items():
        set_param(filters, col, val, transform=process_filter_value)
    try:
        return list(yt_client.select_rows(
            f"""
            {cols}
            from [{row_type.table_path}] as {row_type.alias}
            {"where " + " and ".join(
                [f"{row_type.alias}.{name} {'in' if _is_list_like(val_type) else '='} {val}"
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

    return locals().values()

def init_all_from_config(config: Config):
    for cls in import_all_dataclasses():
        cls.init_from_config(config)