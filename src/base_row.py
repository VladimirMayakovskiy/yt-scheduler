from __future__ import annotations

import dataclasses
from typing import ClassVar, TypeVar

from config import Config
from yt_wrapper import with_yt_client
import yt.wrapper as yt


T = TypeVar("T", bound="YtRow")

class TablePath:
    def __init__(self, table_path: str):
        self.table_path = table_path
    def __get__(self, obj, cls):
        base = cls._config.default_work_dir.rstrip("/")
        return f"{base}/{self.table_path.lstrip('/')}"

class YtRow:
    _config: ClassVar[Config]
    table_path: ClassVar[str]
    key_columns: ClassVar[str | list[str]]
    unique_keys: ClassVar[bool] = True
    alias: ClassVar[str]
    row_type: ClassVar[type[T]]

    @classmethod
    def init_from_config(cls, config: Config):
        cls._config = config

    @classmethod
    def fetch_rows(cls: type[T], **kwargs) -> list[T]:
        raise NotImplementedError

    @classmethod
    def get(cls: type[T], **kwargs) -> T | None:
        rows = cls.fetch_rows(**kwargs, limit=1)
        if not rows:
            cls.logger.warning("Cannot find %s with %s", cls.__name__, kwargs)
            return None
        if len(rows) != 1:
            cls.logger.warning("Cannot find unambiguously %s with %s: found %d", cls.__name__, kwargs, len(rows))
            return None
        return rows[0]

    @classmethod
    @with_yt_client
    def upsert_rows(cls: type[T], rows: T.row_type | list[T.row_type] | tuple[T.row_type], yt_client: yt.YtClient):
        if not isinstance(rows, (list, tuple)):
            rows = [rows]
        try:
            row_type = cls.row_type if hasattr(cls, "row_type") else cls
            yt_client.insert_rows(row_type.table_path, [dataclasses.asdict(row) for row in rows], update=True)
        except Exception as e:
            cls.logger.exception("Failed to insert rows %r: %s", rows, e)
            raise