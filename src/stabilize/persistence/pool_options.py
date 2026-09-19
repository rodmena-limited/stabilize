"""Caller-supplied options for PostgreSQL connection pools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_MIN_SIZE = 5
DEFAULT_MAX_SIZE = 15

DEFAULT_HEALTH_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class PoolOptions:
    """Options applied when a PostgreSQL pool is created.

    stabilize supplies no ``statement_timeout`` and no ``lock_timeout`` of its
    own: without ``connect_kwargs`` or a DSN carrying ``options=-c ...``, a
    connection inherits the server defaults, and a blocked query holds a
    backend slot for as long as the server allows.

    ``acquire_timeout`` bounds how long a borrow waits for a free connection.
    psycopg_pool's own default is 30s, which is the latency an unreachable
    database imposes on every borrow, including a health check.

    Pools are keyed by connection string AND by these options, so two callers
    asking for different options get different pools rather than silently
    sharing whichever was created first.

    ``configure`` runs once per new connection and must leave that connection
    outside a transaction: psycopg_pool discards any connection a configure
    callback leaves INTRANS, so a callback issuing ``SET`` without a commit
    destroys every connection and borrows then fail with PoolTimeout. Call
    ``conn.commit()``, or set ``conn.autocommit = True`` first.
    """

    min_size: int = DEFAULT_MIN_SIZE
    max_size: int = DEFAULT_MAX_SIZE
    acquire_timeout: float | None = None
    connect_kwargs: dict[str, Any] = field(default_factory=dict)
    configure: Callable[[Any], None] | None = None

    def key(self) -> tuple[Any, ...]:
        """A hashable identity for pool sharing."""
        return (
            self.min_size,
            self.max_size,
            self.acquire_timeout,
            tuple(sorted(self.connect_kwargs.items())),
            id(self.configure) if self.configure is not None else None,
        )


DEFAULT_POOL_OPTIONS = PoolOptions()
