from __future__ import annotations

import queue
import weakref
from typing import Callable, Optional

TCallback = Callable[[], None]

class WeakCallbackRef:
    __slots__ = ("_is_strong", "_ref")

    def __init__(self, cb: Callable[[TCallback], None] | None = None):
        self._is_strong = False
        self._ref: weakref.ref |  weakref.WeakMethod | Callable[[TCallback], None] | None = None
        if cb is None:
            return

        if hasattr(cb, "__self__") and hasattr(cb, "__func__"):
            try:
                self._ref = weakref.WeakMethod(cb)
                return
            except Exception:
                pass
        try:
            self._ref = weakref.ref(cb)
            return
        except TypeError:
            self._is_strong = True
            self._ref = cb

    def get(self) -> Callable[[TCallback], None] | None:
        if self._ref is None:
            return None

        try:
            if self._is_strong:
                return self._ref
            else:
                return self._ref()
        except Exception:
            return None

class CallbackMixin:
    def __init__(self, owner_job_id: str):
        self._owner_job_id = owner_job_id
        self._cb_q: queue.Queue[TCallback] = queue.Queue()

    def add_callback(self, _cb: TCallback):
        self._cb_q.put(_cb)

    def qsize(self) -> int:
        return self._cb_q.qsize()

    def _drain_callbacks(self, max_count: int = 50) -> int:
        processed = 0
        while processed < max_count:
            try:
                cb = self._cb_q.get_nowait()
            except queue.Empty:
                break
            try:
                cb()
            except Exception as e:
                if self.log:
                    self.log.exception("Callback execution failed for owner %s: %s", self._owner_job_id, e)
                else:
                    from logging_mixin import logger
                    logger.exception("Callback execution failed for owner %s: %s", self._owner_job_id, e)
            finally:
                self._cb_q.task_done()
            processed += 1
        return processed

    def has_callbacks(self) -> bool:
        return not self._cb_q.empty()