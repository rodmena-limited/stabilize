"""A psycopg_pool ConnectionPool that runs a caller hook on every borrow."""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any


class BorrowHookError(RuntimeError):
    """PoolOptions.on_borrow left the borrowed connection inside a transaction."""


@functools.cache
def borrow_hook_pool_class() -> type[Any]:
    """The ConnectionPool subclass used when PoolOptions.on_borrow is set."""
    from psycopg.pq import TransactionStatus
    from psycopg_pool import ConnectionPool

    class BorrowHookPool(ConnectionPool[Any]):
        def __init__(self, *args: Any, on_borrow: Callable[[Any], None], **kwargs: Any) -> None:
            self._stabilize_on_borrow = on_borrow
            super().__init__(*args, **kwargs)

        @contextmanager
        def connection(self, timeout: float | None = None) -> Iterator[Any]:
            with super().connection(timeout) as conn:
                self._stabilize_on_borrow(conn)
                if conn.info.transaction_status != TransactionStatus.IDLE:
                    raise BorrowHookError(
                        "PoolOptions.on_borrow left the connection inside a transaction; "
                        "set session-level state and commit before returning"
                    )
                yield conn

    return BorrowHookPool
