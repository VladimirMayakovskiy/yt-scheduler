import contextvars
import functools
import threading
import typing
from collections.abc import Callable
from functools import lru_cache
from typing import Optional, Any, Protocol

import yt.wrapper as yt

from config import Config

if typing.TYPE_CHECKING:
    from job import ClientContext

_current_context: contextvars.ContextVar["ClientContext"] = contextvars.ContextVar("_current_context")

def get_current_context() -> Optional["ClientContext"]:
    try:
        return _current_context.get()
    except LookupError:
        return None

@lru_cache(maxsize=128)
def _create_client_cached(proxy: Optional[str], prefix: Optional[str], thread_id: int) -> yt.YtClient:
    kwargs = {}
    if proxy:
        kwargs["proxy"] = proxy
    client = yt.YtClient(**kwargs)
    if prefix:
        prefix = yt.ypath.YPath(prefix.rstrip("/") + "/")
        yt.ypath.get_config(client)["prefix"] = prefix
    return client

class _ClientContextManager:
    def __init__(self, ctx: "ClientContext", client: yt.YtClient):
        self.ctx = ctx
        self.client = client
        self._ctx_var_token = None
        self._client_var_token = None
        self._client_set = False

    def _enter(self):
        self._ctx_var_token = _current_context.set(self.ctx)
        if self.ctx.client_var.get(None) != self.client:
            self._client_var_token = self.ctx.client_var.set(self.client)
            self._client_set = True
        return self.client

    def _exit(self):
        if self._client_set:
            self.ctx.client_var.reset(self._client_var_token)
        _current_context.reset(self._ctx_var_token)

    def __enter__(self):
        return self._enter()
    def __exit__(self, exc_type, exc_val, tb):
        self._exit()

    def __aenter__(self):
        return self._enter()
    def __aexit__(self, exc_type, exc_val, tb):
        self._exit()

class ClientAgent:
    def __init__(self, config: Config):
        self.config = config

    @staticmethod
    def client_var_template(job_id: str) -> contextvars.ContextVar:
        return contextvars.ContextVar(f"client_var_{job_id}_ContextVar")

    def create_client(self, *, proxy: Optional[str] = None, prefix: Optional[str] = None) -> yt.YtClient: # todo check when proxy set
        proxy = proxy or self.config.get_proxy()
        prefix = prefix or self.config.default_work_dir

        return _create_client_cached(proxy=proxy, prefix=prefix, thread_id=threading.get_ident())

    def client_context(self, context, *, proxy=None, prefix=None) -> _ClientContextManager:
        client = self.create_client(proxy=proxy, prefix=prefix)
        return _ClientContextManager(context, client)

    @staticmethod
    def run_with_client(context: "ClientContext", *, func: Callable[[], Any], proxy=None, prefix=None):
        with context._agent.client_context(context, proxy=proxy, prefix=prefix):
            return func()

class ContextWrapper(Protocol):
    def __call__(self, func: Callable, *f_args, **f_kwargs) -> Any: ...
    def bind(self, **kwargs) -> "ContextWrapper": ...
    def get_bound(self) -> dict[str, Any]: ...
    _target: Callable

def context_wrapper(**bound):
    target_func = ClientAgent.run_with_client
    def get_bound() -> dict[str, Any]:
        return dict(bound)
    def bind(**kwargs):
        kwargs.update(get_bound())
        return context_wrapper(**kwargs)

    def _callable(func: Optional[Callable[[], Any]] = None):
        context_args = get_bound()
        context = context_args.pop("context", None)
        if context is None:
            context = get_current_context()
        if context is None:
            raise RuntimeError("No current job set; context_wrapper requires active RunnerEnv")

        if func is None:
            func = context_args.pop("func", None)
        if func is None:
            raise TypeError("No function provided to wrapper (neither at create-time nor call-time).")
        if not callable(func):
            raise TypeError(f"Expected callable, got {type(func)}")

        return target_func(context, func=func, **context_args)

    wrapper = _callable
    wrapper.bind = bind
    wrapper.get_bound = get_bound
    wrapper._target = target_func

    try:
        functools.update_wrapper(wrapper, target_func)
    except Exception:
        pass

    return wrapper

def with_yt_client(func_opt=None, *, client_param: str = "yt_client"):
    def _d(func):
        import inspect
        signature = inspect.signature(func)
        accepts_client_param = client_param in signature.parameters

        @functools.wraps(func)
        def _sync_wrapper(*args, **kwargs):
            job = get_current_context()
            client = None

            if job is not None:
                try:
                    client = job.client_var.get()
                except LookupError:
                    client = None

            if client is None:
                if job is not None:
                    client = job._agent.create_client()
                else:
                    client = _create_client_cached(None, None, threading.get_ident())

            if accepts_client_param:
                if client_param not in kwargs or kwargs.get(client_param) is None:
                    kwargs[client_param] = client
                return func(*args, **kwargs)
            else:
                if job is None:
                    return func(*args, **kwargs)
                with _ClientContextManager(job, client):
                    return func(*args, **kwargs)

        return _sync_wrapper

    if callable(func_opt):
        return _d(func_opt)
    return _d

def with_context(func_opt, *, context_attr="context", client_param: str = "yt_client"):
    def _d(func):
        @functools.wraps(func)
        def _sync_wrapper(self, *args, **kwargs):
            context = next(
                (getattr(self, attr) for attr in (context_attr, f"_{context_attr}", f"__{context_attr}") if hasattr(self, attr)),
                None,
            )
            wrapped_with_client = with_yt_client(func, client_param=client_param)

            if context is None:
                self.log.info(f"Missing context attribute {context_attr} on {self!r}")
                return wrapped_with_client(self, *args, **kwargs)

            return context(lambda: wrapped_with_client(self, *args, **kwargs))
        return _sync_wrapper
    if callable(func_opt):
        return _d(func_opt)
    return _d