from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import ClassVar

from yt_wrapper import with_yt_client
import yt.wrapper as yt

class BaseRow:
    table_path:  ClassVar[str]
    key_columns: ClassVar[list[str]]
    unique_keys: ClassVar[bool] = True

def get_all_row_fields(cls: type[dataclass], alias: str) -> str:
    return ",\n".join(
        f"{alias}.{f.name} AS {f.name}"
        for f in dataclasses.fields(cls.row_type) if f.init
    )

@with_yt_client
def _fetch_rows(cls, rows: str, yt_client: yt.YtClient) -> list:
    try:
        return list(yt_client.select_rows(rows))
    except Exception as e:
        cls.logger.exception("Failed to select rows: %s", e)
        raise