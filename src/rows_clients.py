from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, ClassVar

from handle_exceptions import handle_exceptions

from errors import TRANSIENT_LOCK_ERRORS

def make_transient_lock_errors_retry_options(
    *,
    attempts: int = None,
    retries: int = None,
    initial_backoff: float = None,
    max_backoff: float | None = None,
    total_timeout: float | timedelta | None = None,
    exception_types: tuple[type[BaseException], ...] = None,
    ignore_exceptions: tuple[type[BaseException], ...] = None,
    retry_on: Callable[[Exception], bool] | None = None,
    raise_on_exhaust: bool = False,
    on_exhaust: Callable[[Exception], Any] | None = None,
) -> dict[str, Any]:
    exceptions = TRANSIENT_LOCK_ERRORS
    if exception_types:
        exceptions += exception_types

    retry_options = {
        "exceptions": exceptions,
        "max_backoff": max_backoff,
        "total_timeout": total_timeout,
        "retry_on": retry_on,
        "raise_on_exhaust": raise_on_exhaust,
        "on_exhaust": on_exhaust,
    }
    if retries is not None:
        retry_options["retries"] = retries
    if attempts is not None:
        retry_options["attempts"] = attempts
    if initial_backoff is not None:
        retry_options["initial_backoff"] = initial_backoff
    if ignore_exceptions is not None:
        retry_options["ignore_exceptions"] = ignore_exceptions

    return retry_options

_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS = make_transient_lock_errors_retry_options(
    retries=2,
    raise_on_exhaust=False,
    on_exhaust=lambda e: [],
)

_READ_OPTIONAL_TRANSIENT_RETRY_OPTIONS = make_transient_lock_errors_retry_options(
    retries=2,
    raise_on_exhaust=False,
    on_exhaust=lambda e: None,
)

_READ_COUNTER_TRANSIENT_RETRY_OPTIONS = make_transient_lock_errors_retry_options(
    retries=2,
    raise_on_exhaust=False,
    on_exhaust=lambda e: 0,
)

_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS = make_transient_lock_errors_retry_options(
    retries=2,
    raise_on_exhaust=True,
)

class DagMetaClient:
    from dagref import DagMeta
    _impl: ClassVar[type[DagMeta]] = DagMeta

    upsert_rows = handle_exceptions(_impl.upsert_rows,
                                    default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS)

class TaskRefClient:
    from dagref import TaskRef
    _impl: ClassVar[type[TaskRef]] = TaskRef

    get = handle_exceptions(_impl.get,
                            default_retry_options=_READ_OPTIONAL_TRANSIENT_RETRY_OPTIONS)

class DagRefClient:
    from dagref import DagRef
    _impl: ClassVar[type[DagRef]] = DagRef

    get = handle_exceptions(_impl.get,
                            default_retry_options=_READ_OPTIONAL_TRANSIENT_RETRY_OPTIONS)

    dags_needing_dagruns = handle_exceptions(
        _impl.dags_needing_dagruns, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )

class DagRunClient:
    from dagrun import DagRun
    _impl: ClassVar[type[DagRun]] = DagRun

    get = handle_exceptions(_impl.get,
                            default_retry_options=_READ_OPTIONAL_TRANSIENT_RETRY_OPTIONS)

    upsert_rows = handle_exceptions(_impl.upsert_rows,
                                    default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS)

    set_state = handle_exceptions(_impl.set_state,
                                  default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS)

    get_scheduled_dag_runs_to_queue = handle_exceptions(
        _impl.get_scheduled_dag_runs_to_queue, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )

    get_queued_dag_runs_to_set_running = handle_exceptions(
        _impl.get_queued_dag_runs_to_set_running, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )

    get_running_dag_runs_to_examine = handle_exceptions(
        _impl.get_running_dag_runs_to_examine, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS)

    queue_run_atomic = handle_exceptions(
        _impl.queue_run_atomic, default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS
    )

    update_state = handle_exceptions(_impl.update_state,
                                     default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS)

    schedule_trs = handle_exceptions(_impl.schedule_trs,
                                     default_retry_options=_READ_COUNTER_TRANSIENT_RETRY_OPTIONS)

class TaskRunClient:
    from taskrun import TaskRun
    _impl: ClassVar[type[TaskRun]] = TaskRun

    set_state = handle_exceptions(_impl.set_state,
                                  default_retry_options=_WRITE_ATOMIC_TRANSIENT_RETRY_OPTIONS)

    get_executable_task_runs_to_queue = handle_exceptions(
        _impl.get_executable_task_runs_to_queue, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )

    get_running_task_runs_to_poll = handle_exceptions(
        _impl.get_running_task_runs_to_poll, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )

    get_orphaned_task_runs = handle_exceptions(
        _impl.get_orphaned_task_runs, default_retry_options=_READ_COLLECTION_TRANSIENT_RETRY_OPTIONS
    )