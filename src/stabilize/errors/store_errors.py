"""Is the store, not the data, the problem? Then the message is redelivered and the result held."""

from __future__ import annotations

from stabilize.errors.chain import walk_causes
from stabilize.errors.sqlstates import is_store_unavailable_sqlstate, sqlstate

UNAVAILABLE_TYPE_NAMES = frozenset(
    {"OperationalError", "InterfaceError", "PoolTimeout", "PoolClosed", "ConcurrencyError", "TooManyConnections"}
)


def is_store_unavailable(error: BaseException) -> bool:
    """True when a failed store call may succeed on redelivery.

    Connection loss, pool exhaustion, server shutdown and recovery, a cached plan
    invalidated by a migration (0A000) and optimistic-lock conflicts count; a
    constraint violation, a missing table or a bad statement do not.
    """
    from stabilize.errors.utils import is_transient

    for exc in walk_causes(error):
        if isinstance(exc, (ConnectionError, TimeoutError)):
            return True
        if is_store_unavailable_sqlstate(sqlstate(exc)):
            return True
        if any(cls.__name__ in UNAVAILABLE_TYPE_NAMES for cls in type(exc).__mro__):
            return True
    return isinstance(error, Exception) and is_transient(error)
