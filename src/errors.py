from yt.wrapper.errors import (
    YtCypressTransactionLockConflict, YtTabletTransactionLockConflict,
    YtConcurrentTransactionLockConflict, YtRowIsBlocked, YtBlockedRowWaitTimeout
)

TRANSIENT_LOCK_ERRORS = (YtCypressTransactionLockConflict, YtTabletTransactionLockConflict,
                         YtConcurrentTransactionLockConflict, YtRowIsBlocked, YtBlockedRowWaitTimeout,)


class DagInitializationError(Exception):
    pass