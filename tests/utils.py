from functools import wraps
from typing import Callable, Any


def retry_on_transaction_conflict(max_retries: int = 5):
    def _decorator(fn: Callable[..., Any]):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    is_conflict = (str(e) == "Transaction lock conflict injected")
                    attempt += 1
                    if not is_conflict or attempt > max_retries:
                        print("Non-retryable error or max attempts reached")
                        raise
                    print(f"Transaction conflict detected, retrying attempt {attempt}/{max_retries}")
        return wrapper
    return _decorator