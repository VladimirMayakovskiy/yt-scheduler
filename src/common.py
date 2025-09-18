from __future__ import annotations

import functools
import itertools
import logging
import socket
import time
from copy import deepcopy
from datetime import datetime, timedelta
from distutils.debug import DEBUG
from typing import Any, Optional, Callable, TypeVar

import yt.wrapper.errors
import yt.wrapper as yt
import yt.common


class classproperty(property):
    def __get__(self, obj: Any, objtype: Optional[type] = None) -> Any:
        return self.fget(objtype)

def _chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk