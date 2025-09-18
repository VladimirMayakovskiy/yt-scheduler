from functools import partial
from typing import Callable, Optional, TYPE_CHECKING, TypedDict
import concurrent.futures
from concurrent.futures import Future
import queue
import threading
import time

from logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from yt_wrapper import ContextWrapper

class Promise:
    def __init__(self, queueable: "_PoolQueueable"):
        self._future = Future()
        self._q = queueable

    def on_complete(self, _cb: Callable[[], None]):
        self._q.set_callback(_callable=_cb)
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
    __slots__ = ("fn", "args", "kwargs", "promise", "callback", "retries", "backoff", "retry_on", "queued_at",
                 "_context", "_sync")

    def __init__(
        self,
        pool: "Pool",
        fn: Callable,
        context: "ContextWrapper",
        args: tuple = (),
        kwargs: Optional[dict] = None,
        retries: int = 0,
        backoff: float = 0.5,
        retry_on: Optional[Callable[[Exception], bool]] = None,
        sync: bool = False
    ):
        self.pool = pool

        self._fn = fn
        self._args = args or ()
        self._kwargs = kwargs or {}
        self.fn = partial(self._fn, *self._args, **self._kwargs)


        self.promise = Promise(self)
        self.callback: Optional[Callable[[], None]] = None

        self.retries = retries
        self.backoff = backoff
        self.retry_on = retry_on

        self._context = context
        self._sync = sync

    def execute(self):
        attempt = 0
        while True:
            try:
                result = self._context(self.fn)
                self.promise.set_result(result)
                break
            except Exception as e:
                attempt += 1
                if self.retry_on is not None and not self.retry_on(e):
                    self.promise.set_exception(e)
                    self.log.warning("Job %s failed with non-retriable error: %s", self, e)
                    break

                if attempt > self.retries:
                    self.promise.set_exception(e)
                    break

                sleep_for = self.backoff * attempt
                self.log.warning(
                    "Job %s failed on attempt %d: %s. Retrying in %.2f seconds...",
                    self, attempt + 1, e, sleep_for
                )
                time.sleep(sleep_for)

    def set_callback(self, _callable: Callable[[], None]):
        self.callback = _callable

        if self.promise._future.done():
            self.pool._cb_q.put(_callable)

    def __repr__(self):
        work_context_type = "_SyncUnqueueable" if self._sync else "_PoolQueueable"
        return f"<{work_context_type} fn={getattr(self.fn, '__name__', str(self.fn))}>"

class ExecutionOptions(TypedDict, total=False):
    retries: int
    backoff: float
    retry_on: Optional[Callable[[Exception], bool]]
    force_sync: bool
    block: bool
    timeout: Optional[float]

_default_execution_options: ExecutionOptions = {
    "retries": 0,
    "backoff": 0.5,
    "retry_on": None,
    "force_sync": False,
    "block": False,
    "timeout": None,
}

class Pool(LoggingMixin):
    def __init__(
        self,
        max_workers: int = 4,
        max_queue_size: int = 1000
    ):
        self._q: queue.Queue[_PoolQueueable] = queue.Queue(maxsize=max_queue_size)
        self._cb_q: queue.Queue[Callable[[], None]] = queue.Queue()
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

    def submit(
        self,
        fn: Callable,
        *args,
        context: "ContextWrapper",
        _execution_options: ExecutionOptions = None,
        **kwargs
    ) -> Promise:
        _execution_options = {**_default_execution_options, **(_execution_options or {})}
        q = _PoolQueueable(
            pool=self,
            fn=fn,
            context=context,
            args=args,
            kwargs=kwargs,
            retries=_execution_options["retries"],
            backoff=_execution_options["backoff"],
            retry_on=_execution_options["retry_on"],
        )
        try:
            self._q.put(q, block=_execution_options.get("block", False), timeout=_execution_options["timeout"])
        except queue.Full as e:
            self.log.exception("Failed to submit job %s to pool: %s", q, e)
            raise
        return q.promise

    def submit_or_execute(
        self,
        fn: Callable,
        *args,
        context: "ContextWrapper",
        _execution_options: ExecutionOptions = None,
        **kwargs
    ):
        _execution_options = {**_default_execution_options, **(_execution_options or {})}
        retries = _execution_options["retries"]
        backoff = _execution_options["backoff"]
        retry_on = _execution_options["retry_on"]
        force_sync = _execution_options["force_sync"]
        timeout = _execution_options["timeout"]

        def _sync_fallback():
            sync_q = _PoolQueueable(
                pool=self,
                fn=fn,
                args=args,
                kwargs=kwargs,
                context=context,
                retries=retries,
                backoff=backoff,
                retry_on=retry_on,
                sync=True
            )
            sync_q.execute()
            return sync_q.promise

        if force_sync:
            return _sync_fallback()
        try:
            return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)
        except queue.Full as e:
            self.log.error("Pool is full, cannot submit job %s: %s", getattr(fn, '__name__', str(fn)), e)
            try:
                _execution_options = {**_execution_options, "block": True}
                return self.submit(fn, *args, context=context, _execution_options=_execution_options, **kwargs)
            except queue.Full as e:
                self.log.warning("Pool is full, cannot submit job %s after %.2f seconds: %s, executing synchronously", fn, timeout, e)
                return _sync_fallback()

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                runnable: _PoolQueueable = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                runnable.execute()
                if runnable.callback is not None:
                    self._cb_q.put(runnable.callback)
            finally:
                self._q.task_done()

    def process_callbacks(self, max_count: int = 1000):
        processed = 0
        while processed < max_count:
            try:
                callback = self._cb_q.get_nowait()
            except queue.Empty:
                break
            try:
                callback()
            except Exception as e:
                self.log.exception("Failed to process callback: %s", e)
            finally:
                self._cb_q.task_done()
            processed += 1

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None):
        self._stop_event.set()

        while True:
            try:
                queueable: _PoolQueueable = self._q.get_nowait()
            except queue.Empty:
                break
            try:
                queueable.promise.set_exception(RuntimeError("Pool shutdown before execution"))
            finally:
                self._q.task_done()

        if wait:
            start = time.time()
            for w in self._workers:
                if timeout is not None:
                    timeout = max(0., timeout - (time.time() - start))
                w.join(timeout=timeout)

    def qsize(self) -> int:
        return self._q.qsize()

    def callbacks_qsize(self) -> int:
        return self._cb_q.qsize()