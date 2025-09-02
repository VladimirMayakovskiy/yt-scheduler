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
    from job import RunnerEnv

_current_job: contextvars.ContextVar["RunnerEnv"] = contextvars.ContextVar("_current_job")

def get_current_job() -> Optional["RunnerEnv"]: # todo check type
    try:
        return _current_job.get()
    except LookupError:
        return None

@lru_cache(maxsize=128)
def _create_client_cached(proxy: Optional[str], work_dir: Optional[str], thread_id: int) -> yt.YtClient:
    kwargs = {}
    if proxy:
        kwargs["proxy"] = proxy
    client = yt.YtClient(**kwargs)
    if work_dir:
        prefix = yt.ypath.YPath(work_dir.rstrip("/") + "/") # todo create prefix from work_dir func
        yt.ypath.get_config(client)["prefix"] = prefix
    return client

class _ClientContextManager:
    def __init__(self, env: "RunnerEnv", client):
        self.env = env
        self.client = client
        self._job_var_token = None
        self._client_var_token = None
        self._client_set = False

    def _enter(self):
        self._job_var_token = _current_job.set(self.env)
        if self.env.client_var.get(None) != self.client:
            self._client_var_token = self.env.client_var.set(self.client)
            self._client_set = True
        return self.client

    def _exit(self):
        if self._client_set:
            self.env.client_var.reset(self._client_var_token)
        _current_job.reset(self._job_var_token)

    def __enter__(self):
        return self._enter()
    def __exit__(self, exc_type, exc_val, tb):
        self._exit()

    def __aenter__(self):
        return self._enter()
    def __aexit__(self, exc_type, exc_val, tb):
        self._exit()

class ClientContext:
    def __init__(self, config: Config):
        self.config = config

    def create_client(self, *, proxy: Optional[str] = None, work_dir: Optional[str] = None) -> yt.YtClient: # todo check when proxy set
        proxy = proxy or self.config.get_proxy()
        work_dir = work_dir or self.config.default_work_dir
        return _create_client_cached(proxy=proxy, work_dir=work_dir, thread_id=threading.get_ident())

    # todo rename with context
    def dag_context(self, env: "RunnerEnv", *, proxy: Optional[str] = None, work_dir: Optional[str] = None) -> _ClientContextManager:
        client = self.create_client(proxy=proxy, work_dir=work_dir)
        return _ClientContextManager(env, client)

    @staticmethod
    def client_var_template(job_id: str) -> contextvars.ContextVar:
        return contextvars.ContextVar(f"client_job_{job_id}_ContextVar")

    def run_in_context(self, env: "RunnerEnv", func: Callable, work_dir: Optional[str] = None, *args, **kwargs):
        with self.dag_context(env=env, work_dir=work_dir):
            return func(*args, **kwargs)

class ContextWrapper(Protocol):
    def __call__(self, func: Callable, *f_args, **f_kwargs) -> Any: ...
    def bind(self, **kwargs) -> "ContextWrapper": ...
    def get_bound(self) -> dict[str, Any]: ...
    _target: Callable

def context_wrapper(context: ClientContext, /, **bound):
    target_func = ClientContext.run_in_context

    def get_bound() -> dict[str, Any]:
        return dict(bound)

    def bind(**kwargs) -> Callable[..., Any]:
        kwargs.update(get_bound())
        return context_wrapper(context, **kwargs)

    def _callable(func: Callable, *f_args, **f_kwargs):
        if not callable(func):
            raise TypeError(f"Expected callable, got {type(func)}")

        context_args = get_bound()
        wrapped_func = functools.partial(func, *f_args, **f_kwargs)

        return target_func(self=context, func=wrapped_func, **context_args)

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
    def _d(f):
        import inspect
        signature = inspect.signature(f)
        accepts_client_param = client_param in signature.parameters

        @functools.wraps(f)
        def _sync_wrapper(*args, **kwargs):
            job = get_current_job()
            client = None
            if job is not None:
                try:
                    client = job.client_var.get()
                except LookupError:
                    client = None

            if client is None:
                client = _create_client_cached(None, None, threading.get_ident())

            if accepts_client_param:
                if client_param not in kwargs or kwargs.get(client_param) is None:
                    kwargs[client_param] = client
                return f(*args, **kwargs)
            else:
                if job is None:
                    return f(*args, **kwargs)
                with _ClientContextManager(job, client):
                    return f(*args, **kwargs)

        return _sync_wrapper

    if callable(func_opt):
        return _d(func_opt)
    return _d