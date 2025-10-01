from __future__ import annotations

import logging
import time
from copy import deepcopy
from datetime import timedelta, datetime
from typing import TypedDict, Callable, Any

class RetryOptions(TypedDict, total=False):
    retries: int
    attempts: int
    initial_backoff: float
    max_backoff: float | None
    total_timeout: float | timedelta | None
    exceptions: tuple[type[BaseException], ...]
    ignore_exceptions: tuple[type[BaseException], ...]
    retry_on: Callable[[Exception], bool] | None
    raise_on_exhaust: bool
    on_exhaust: Callable[[Exception], Any] | None

class Retrier:
    def __init__(self, retry_options: RetryOptions, logger: logging.Logger = None):
        self.config = deepcopy(retry_options or {})
        self.exceptions = self.config.get("exceptions", (Exception,))
        self.ignore_exceptions = self.config.get("ignore_exceptions", ())
        self.attempts = self.config.get("attempts", self.config.get("retries", 1) + 1)
        if retries := self.config.get("retries"):
            assert self.attempts == retries + 1
        self.initial_backoff = float(self.config.get("initial_backoff", 0.5))
        self.max_backoff = float(self.config["max_backoff"]) if self.config.get("max_backoff") else None
        self.retry_on = self.config.get("retry_on")
        self.total_timeout = self.config.get("total_timeout")
        if self.total_timeout is not None and not isinstance(self.total_timeout, timedelta):
            self.total_timeout = timedelta(seconds=float(self.total_timeout))
        self.raise_on_exhaust = self.config.get("raise_on_exhaust", True)
        self.on_exhaust = self.config.get("on_exhaust")

        self.logger = logger

    def run(self, fn: Callable[[], Any]) -> Any:
        attempt = 1
        backoff = self.initial_backoff
        start_date = datetime.now()
        while True:
            try:
                return fn()
            except self.exceptions as e:
                last_exception = e
                if isinstance(e, self.ignore_exceptions):
                    raise

                if self.retry_on and not self.retry_on(e):
                    raise

                if attempt == self.attempts:
                    break

                if self.total_timeout is not None and datetime.now() - start_date > self.total_timeout:
                    break

                if self.logger:
                    self.logger.warning(
                        f"Retry %d/%d failed for: %s. Retrying in %.2fs...", attempt, e, backoff, exc_info=False
                    )
                time.sleep(backoff)
                backoff = backoff * 2
                if self.max_backoff:
                    backoff = min(backoff, self.max_backoff)
                attempt += 1

        if self.on_exhaust:
            return self.on_exhaust(last_exception)
        elif not self.raise_on_exhaust:
            return None
        else:
            raise last_exception

def run_with_retries(fn: Callable[[], Any], retry_options: RetryOptions, logger: logging.Logger) -> Any:
    return Retrier(retry_options=retry_options, logger=logger).run(fn=fn)