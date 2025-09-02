from typing import Callable, Optional
import concurrent.futures
from concurrent.futures import Future
import queue
import threading
import time
import uuid

from yt_wrapper import ContextWrapper

class Promise:
    def __init__(self, queueable: "_PoolQueueable"):
        self._future = Future()
        self._q = queueable
        self.result = None

    def on_complete(self, func: Callable[[], None]):
        def _cb(future: Future):
            try:
                self.result = future.result()
                func()
            except Exception as e:
                print(f"Promise error: {e}")
                raise
        self._q.set_callback(_callable=_cb, future=self._future)
        return func

    def __getattr__(self, item):
        return getattr(self._future, item)

class _PoolQueueable:
    __slots__ = ("id", "fn", "args", "kwargs", "promise", "callback", "retries", "backoff", "created_at", "_contextualize")

    def __init__(
        self,
        fn: Callable,
        context: ContextWrapper,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        retries: int = 0,
        backoff: float = 0.5,
    ):
        self.id = uuid.uuid4().hex
        self.fn = fn
        self.args = args or ()
        self.kwargs = kwargs or {}

        self.promise = Promise(self)
        self.callback: Optional[tuple[Callable[[Future], None], Future]] = None

        self.retries = retries
        self.backoff = backoff
        self.created_at = time.time()

        self._contextualize = context

    def run(self):
        return self._contextualize(self.fn,*self.args, **self.kwargs)

    def set_callback(self, _callable: Callable[[Future], None], future: Future):
        self.callback = (_callable, future)

    def __repr__(self):
        return f"<_ThreadJob id={self.id} fn={getattr(self.fn, '__name__', str(self.fn))} retries={self.retries}>"

class PoolExecutor:
    def __init__(
        self,
        max_workers: int = 4,
        max_queue_size: int = 1000
    ):
        self._q: queue.Queue[_PoolQueueable] = queue.Queue(maxsize=max_queue_size)
        self._cb_q: queue.Queue[tuple[Callable[[Future], None], Future]] = queue.Queue()
        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()

        for i in range(max_workers):
            t = threading.Thread(target=self._worker_loop, name=f"pool_worker-{i}", daemon=True)
            self._workers.append(t)

    def _start_workers(self):
        for t in self._workers:
            t.start()

    def submit(
        self,
        fn: Callable,
        *args,
        context: ContextWrapper,
        retries: int = 0,
        backoff: float = 0.5,
        block: bool = False,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Promise:
        queueable = _PoolQueueable(fn=fn, context=context, args=args, kwargs=kwargs, retries=retries, backoff=backoff)
        try:
            self._q.put(queueable, block=block, timeout=timeout)
        except queue.Full as e:
            print("ERROR IN SUBMIT", e)
            raise
        return queueable.promise

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                runnable: _PoolQueueable = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                attempt = 0
                while True:
                    try:
                        result = runnable.run()
                        runnable.promise.set_result(result)
                        break
                    except Exception as e:
                        attempt += 1
                        if attempt > runnable.retries:
                            runnable.promise.set_exception(e)
                            break
                        sleep_for = runnable.backoff * attempt # todo
                        time.sleep(sleep_for)
                if runnable.callback is not None:
                    self._cb_q.put(runnable.callback)
            finally:
                try:
                    self._q.task_done()
                except Exception:
                    pass # todo

    def process_callbacks(self, max_count: int = 1000):
        processed = 0
        while processed < max_count:
            try:
                callback, future = self._cb_q.get_nowait()
            except queue.Empty:
                break
            try:
                callback(future)
            except Exception as e:
                # todo logger.exception("Exception in callback for job %s", job)
                pass
            finally:
                try:
                    self._cb_q.task_done()
                except Exception:
                    pass
            processed += 1

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None):
        self._stop_event.set()
        if wait:
            start = time.time()
            for w in self._workers:
                remaining = None
                if timeout is not None:
                    elapsed = time.time() - start
                    remaining = max(0., timeout - elapsed)
                w.join(timeout=remaining)

    def qsize(self) -> int:
        return self._q.qsize()

    def callbacks_qsize(self) -> int:
        return self._cb_q.qsize()