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


def engine_connection_class() -> type[Any]:
    """psycopg's Connection with one addition: a pending tenant arm runs before the next statement.

    ``armed()`` marks the connection pending; the cursor's ``execute`` arms it before the first
    statement, and ``commit``/``rollback`` mark it pending again so the next statement re-arms.
    A commit that ends a borrow therefore costs nothing.
    """
    import psycopg

    class EngineCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            self.connection._stabilize_arm_if_pending()  # type: ignore[attr-defined]
            return super().execute(query, params, **kwargs)

        def executemany(self, query: Any, params_seq: Any, **kwargs: Any) -> Any:
            self.connection._stabilize_arm_if_pending()  # type: ignore[attr-defined]
            return super().executemany(query, params_seq, **kwargs)

    class EngineConnection(psycopg.Connection):
        _stabilize_arm: Callable[[], Any] | None = None
        _stabilize_pending: bool = False

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.cursor_factory = EngineCursor

        def commit(self) -> None:
            super().commit()
            if self._stabilize_arm is not None:
                self._stabilize_pending = True

        def rollback(self) -> None:
            super().rollback()
            if self._stabilize_arm is not None:
                self._stabilize_pending = True

        def _stabilize_arm_if_pending(self) -> None:
            arm = self._stabilize_arm
            if arm is None or not self._stabilize_pending:
                return
            self._stabilize_pending = False
            arm()

    return EngineConnection
