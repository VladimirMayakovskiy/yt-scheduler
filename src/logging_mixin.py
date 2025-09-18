from __future__ import annotations

import logging
from typing import Any, TypeVar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s module=%(module)-15s %(message)s (%(pathname)s:%(lineno)d)"
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
class LoggingMixin:
    _log: logging.Logger | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.logger = cls._get_log(cls, cls)

    @classmethod
    def _get_log(cls, obj: Any, logged_class: type[_T]) -> logging.Logger:
        if obj._log is None:
            logger_name = f"{logged_class.__module__}.{logged_class.__name__}"
            obj._log = logging.getLogger(logger_name)
        return obj._log

    @property
    def log(self) -> logging.Logger:
        return LoggingMixin._get_log(self, self.__class__)