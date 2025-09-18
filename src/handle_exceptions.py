from __future__ import annotations

import functools
import logging
from enum import Enum
from typing import Callable, Any, Protocol

from retrier import run_with_retries

class _ExceptionAction(str, Enum):
    RETRY = "retry"
    RETHROW = "rethrow"
    IGNORE = "ignore"

    def __str__(self) -> str:
        return self.value

def _action_retry(obj, fn_callable: Callable[[], Any], params: dict):
    logger = getattr(obj, "log") or getattr(obj, "logger")
    retry_options = params.get("retry_options") or {}
    return run_with_retries(fn=fn_callable, retry_options=retry_options, logger=logger)

def _action_rethrow(obj, fn_callable: Callable[[], Any], params: dict):
    exception = params.get("exception")
    if exception is not None:
        raise exception
    raise RuntimeError("Re-throw requested, but no exception provided")

def _action_ignore(obj, fn_callable: Callable[[], Any], params: dict):
    logger = getattr(obj, "log", None) or getattr(obj, "logger")
    if logger:
        logger.info("Ignored exception in wrapped %s call: %s",
                    getattr(fn_callable, "__name__", str(fn_callable)), params.get("exception"))
    return None


class HandleExceptions(Protocol):
    def __call__(self, fn: Callable) -> Callable: ...
    def set_handler(
        self,
        *,
        exception_types: tuple[type[BaseException], ...],
        retry_options: dict[str, Any] | None = None,
        exception_action: _ExceptionAction | str | None = None,
    ) -> HandleExceptions: ...

def handle_exceptions(fn_opt=None, *, default_retry_options: dict | None = None) -> HandleExceptions:
    custom_handlers: list[tuple[tuple[type[BaseException], ...], dict, _ExceptionAction | str | None]] = []

    def _d(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            obj = args[0] if len(args) else None
            logger = getattr(obj, "log", None) or getattr(obj, "logger", logging.Logger("handle_exceptions"))
            try:
                return fn(*args, **kwargs)
            except Exception as exception:
                logger.debug("Exception occurred in %s: %s", getattr(fn, __name__, str(fn)), exception, exc_info=True)

                action, params = None, {"exception": exception, "retry_options": default_retry_options or {}}

                for exception_types, retry_options, exception_action in custom_handlers:
                    if isinstance(exception, exception_types):
                        if exception_action is None:
                            pass
                        elif exception_action == _ExceptionAction.IGNORE:
                            action = _action_ignore
                        elif exception_action == _ExceptionAction.RETHROW:
                            action = _action_rethrow
                        elif exception_action == _ExceptionAction.RETRY:
                            action = _action_retry
                        else:
                            assert False, f"Unknown exception action: {exception_action}"

                        if retry_options:
                            params["retry_options"].update(retry_options)
                        # params["retry_options"] = {**(params.get("retry_options", {}) or {}), **retry_options}
                if not action:
                    if params.get("retry_options"):
                        exceptions = params["retry_options"].get("exceptions")
                        if exceptions is None or isinstance(exception, exceptions):
                            action = _action_retry
                        else:
                            action = _action_rethrow
                    else:
                        action = _action_rethrow

                # todo logger?

                return action(obj, functools.partial(fn, *args, **kwargs), params)
        return _wrapper

    def set_handler(*, exception_types, retry_options=None, exception_action: _ExceptionAction | str | None = None):
        nonlocal custom_handlers
        custom_handlers.append((exception_types, retry_options or {}, exception_action))
        return _d

    _d.set_handler = set_handler

    if callable(fn_opt):
        return _d(fn_opt)
    return _d

handle_exceptions: HandleExceptions = handle_exceptions