from __future__ import annotations

from functools import partial
from typing import Callable, Optional, TYPE_CHECKING, TypedDict, Any, NamedTuple
import concurrent.futures
from concurrent.futures import Future
import queue
import threading

from logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from yt_wrapper import ContextWrapper

from callback import WeakCallbackRef, TCallback

class ExecutionOptions(TypedDict, total=False):
    force_sync: bool
    block: bool
    timeout: Optional[float]

_default_execution_options: ExecutionOptions = {
    "force_sync": False,
    "block": False,
    "timeout": None,
}

class Promise:
    def __init__(self, queueable: "_PoolQueueable"):
        self._future = Future()
        self._q = queueable

    def on_complete(self, _cb: Callable[[], None], owner_enqueue: Optional[Callable[[TCallback], None]]):
        self._q.set_callback(_callable=_cb, owner_enqueue=owner_enqueue)
        return _cb

    def __getattr__(self, item):
        return getattr(self._future, item)

    def wait(self, timeout: Optional[float] = None) -> bool:
        try:
            self._future.result(timeout=timeout)
            return True
        except concurrent.futures.TimeoutError:
            return False
        except Exception:
            return True

    @property
    def result(self, timeout: Optional[float] = None):
        return self._future.result(timeout=timeout)

class _PoolQueueable(LoggingMixin):
    __slots__ = ("_fn", "promise", "callback", "_owner_enqueue_ref", "_sync")

    def __init__(
        self,
        fn: Callable,
        context: "ContextWrapper",
        args: tuple = (),
        kwargs: Optional[dict] = None,
        sync: bool = False
    ):
        _fn = partial(fn, *args, **(kwargs or {}))
        self._fn = context.bind(func=_fn)

        self.promise = Promise(self)
        self.callback: Optional[TCallback] = None
        self._owner_enqueue_ref = None

        self._sync = sync

    def execute(self):
        try:
            result = self._fn()
            self.promise.set_result(result)
        except Exception as e:
            self.log.exception("Pool job %s failed with exception: %s", self, e)
            self.promise.set_exception(e)

        if self.callback is not None:
            self._forward_callback(self.callback)

    def set_callback(self, _callable: TCallback, owner_enqueue: Optional[Callable[[TCallback], None]] = None):
        self.callback = _callable
        self._owner_enqueue_ref = WeakCallbackRef(owner_enqueue)

        if self.promise.done():
            self._forward_callback(_callable)

    def _forward_callback(self, cb: TCallback) -> None:
        if self._owner_enqueue_ref:
            forward_callback = self._owner_enqueue_ref.get()
            if forward_callback is not None:
                try:
                    forward_callback(cb)
                except Exception as e:
                    self.log.exception("Failed to forward callback %s to owner %s: %s", cb, self._owner_enqueue_ref, e)
                    pass

    def __repr__(self):
        work_context_type = "_SyncUnqueueable" if self._sync else "_PoolQueueable"
        return f"<{work_context_type} fn={getattr(self._fn, '__name__', str(self._fn))}>"

class Pool(LoggingMixin):
    def __init__(self, max_workers: int = 4, max_queue_size: int = 1000):
        self._q: queue.Queue[_PoolQueueable] = queue.Queue(maxsize=max_queue_size)

        self.max_workers = max_workers
        self._stop_event = threading.Event()
        self._workers: list[threading.Thread] = []

        for i in range(max_workers):
            t = threading.Thread(target=self._worker_loop, name=f"pool_worker-{i}", daemon=True)
            self._workers.append(t)

    def run(self):
        self.log.info("Starting pool with %d workers", self.max_workers)
        self._start_workers()

    def _start_workers(self):
        for t in self._workers:
            t.start()

    def submit(self, fn: Callable, *args, context,
               _execution_options: ExecutionOptions = None, **kwargs) -> Promise:
        _execution_options = {**_default_execution_options, **(_execution_options or {})}
        q = _PoolQueueable(fn=fn, args=args, kwargs=kwargs, context=context)
        try:
            self._q.put(q, block=_execution_options.get("block", False), timeout=_execution_options["timeout"])
        except queue.Full as e:
            self.log.exception("Failed to submit job %s to pool: %s", q, e)
            raise
        return q.promise

    def submit_or_execute(self, fn: Callable, *args, context: "ContextWrapper",
                          _execution_options: ExecutionOptions = None, **kwargs) -> Promise:
        _execution_options = {**_default_execution_options, **(_execution_options or {})}
        def _sync_fallback():
            sync_q = _PoolQueueable(fn=fn, args=args, kwargs=kwargs, context=context, sync=True)
            sync_q.execute()
            return sync_q.promise

        if _execution_options["force_sync"]:
            return _sync_fallback()
        try:
            return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)
        except queue.Full as e:
            self.log.error("Pool is full, cannot submit job %s: %s", getattr(fn, '__name__', str(fn)), e)
            try:
                _execution_options = {**_execution_options, "block": True}
                return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)
            except queue.Full as e:
                self.log.warning("Pool is full, cannot submit job %s after %.2f seconds: %s, executing synchronously",
                                 fn, _execution_options["timeout"], e)
                return _sync_fallback()

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                q: _PoolQueueable = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                q.execute()
            finally:
                self._q.task_done()

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None):
        self._stop_event.set()

        while True:
            try:
                q: _PoolQueueable = self._q.get_nowait()
            except queue.Empty:
                break
            try:
                q.promise.set_exception(RuntimeError("Pool shutdown before execution"))
            finally:
                self._q.task_done()

        if wait:
            for w in self._workers:
                w.join(timeout=timeout)

    def qsize(self) -> int:
        return self._q.qsize()