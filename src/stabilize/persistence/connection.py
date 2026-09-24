"""
Singleton connection manager for database connections.

Provides centralized connection pooling for PostgreSQL and thread-local
connections for SQLite, ensuring efficient resource usage across all
repository and queue instances.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import DictRow
    from psycopg_pool import ConnectionPool


from stabilize.persistence.pool_options import DEFAULT_POOL_OPTIONS, PoolOptions


class SingletonMeta(type):
    """
    Thread-safe metaclass for singleton pattern.

    Ensures only one instance of a class exists, even when accessed
    from multiple threads simultaneously.
    """

    _instances: dict[type, Any] = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            with cls._lock:
                # Double-check locking pattern
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]

    @classmethod
    def reset(mcs, cls: type) -> None:
        """Reset singleton instance (for testing)."""
        with mcs._lock:
            if cls in mcs._instances:
                instance = mcs._instances.pop(cls)
                if hasattr(instance, "close_all"):
                    instance.close_all()


def require_parseable_conninfo(connection_string: str) -> None:
    """Raise a redacted ValueError when libpq cannot parse *connection_string*.

    psycopg_pool retries a failed connection in its worker threads and logs
    each failure, and a libpq parse error quotes the whole input back,
    password included.
    """
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    from stabilize.redaction import redact_text

    try:
        conninfo_to_dict(connection_string)
    except psycopg.ProgrammingError as exc:
        raise ValueError(
            f"PostgreSQL connection string could not be parsed: {redact_text(str(exc))}"
        ) from None


class ConnectionManager(metaclass=SingletonMeta):
    """
    Singleton connection manager for all database connections.

    Manages:
    - PostgreSQL connection pools (one pool per connection string)
    - SQLite thread-local connections (one connection per thread per db path)

    Usage:
        manager = ConnectionManager()
        pool = manager.get_postgres_pool("postgresql://...")
        conn = manager.get_sqlite_connection("sqlite:///./db.sqlite")
    """

    def __init__(self) -> None:
        self._postgres_pools: dict[Any, ConnectionPool[Connection[DictRow]]] = {}
        self._postgres_holders: dict[Any, int] = {}
        self._postgres_keys_by_dsn: dict[str, list[Any]] = {}
        self._postgres_lock = threading.Lock()

        self._sqlite_local = threading.local()
        self._sqlite_lock = threading.Lock()
        self._sqlite_paths: set[str] = set()

    def get_postgres_pool(
        self,
        connection_string: str,
        min_size: int | None = None,
        max_size: int | None = None,
        options: PoolOptions | None = None,
    ) -> ConnectionPool[Connection[DictRow]]:
        """
        Get or create a PostgreSQL connection pool.

        Pools are shared between callers asking for the same connection string
        AND the same options. Each call registers a holder; the pool is closed
        only when every holder has released it, so a store and a queue built on
        one DSN no longer close each other's pool.

        With no options, connections inherit the server's defaults and carry no
        ``statement_timeout`` and no ``lock_timeout``. Supply them through
        ``PoolOptions.connect_kwargs``, a ``configure`` callback, or an
        ``options=-c statement_timeout=...`` parameter on the DSN.

        Args:
            connection_string: PostgreSQL connection string
            min_size: Minimum pool size (overrides options.min_size)
            max_size: Maximum pool size (overrides options.max_size)
            options: Connection and pool options

        Returns:
            Shared ConnectionPool instance for this connection string+options
        """
        resolved = options or DEFAULT_POOL_OPTIONS
        if min_size is not None or max_size is not None:
            resolved = replace(
                resolved,
                min_size=resolved.min_size if min_size is None else min_size,
                max_size=resolved.max_size if max_size is None else max_size,
            )
        key = (connection_string, resolved.key())

        with self._postgres_lock:
            pool = self._postgres_pools.get(key)
            if pool is None:
                from psycopg.rows import dict_row
                from psycopg_pool import ConnectionPool

                require_parseable_conninfo(connection_string)

                kwargs: dict[str, Any] = {"row_factory": dict_row}
                kwargs.update(resolved.connect_kwargs)
                pool_kwargs: dict[str, Any] = {
                    "min_size": resolved.min_size,
                    "max_size": resolved.max_size,
                    "open": True,
                    "kwargs": kwargs,
                }
                if resolved.acquire_timeout is not None:
                    pool_kwargs["timeout"] = resolved.acquire_timeout
                if resolved.configure is not None:
                    pool_kwargs["configure"] = resolved.configure

                pool = cast(
                    "ConnectionPool[Connection[DictRow]]",
                    ConnectionPool(connection_string, **pool_kwargs),
                )
                self._postgres_pools[key] = pool
                self._postgres_keys_by_dsn.setdefault(connection_string, []).append(key)

            self._postgres_holders[key] = self._postgres_holders.get(key, 0) + 1
            return pool

    def get_sqlite_connection(self, connection_string: str) -> sqlite3.Connection:
        """
        Get or create a thread-local SQLite connection.

        Args:
            connection_string: SQLite connection string

        Returns:
            Thread-local Connection instance for this database
        """
        db_path = self._parse_sqlite_path(connection_string)

        # Track paths for cleanup
        with self._sqlite_lock:
            self._sqlite_paths.add(db_path)

        # Get thread-local storage
        if not hasattr(self._sqlite_local, "connections"):
            self._sqlite_local.connections = {}

        connections: dict[str, sqlite3.Connection] = self._sqlite_local.connections

        if db_path not in connections or connections[db_path] is None:
            conn = sqlite3.connect(
                db_path,
                timeout=30,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row

            if db_path == ":memory:":
                # In-memory databases: only foreign keys needed
                conn.execute("PRAGMA foreign_keys = ON")
            else:
                # File-based databases: apply full optimization config
                from stabilize.persistence.sqlite_config import get_sqlite_config

                config = get_sqlite_config()
                for statement in config.get_pragma_statements():
                    conn.execute(statement)

            connections[db_path] = conn

        return connections[db_path]

    def _parse_sqlite_path(self, connection_string: str) -> str:
        """Parse SQLite connection string to extract database path."""
        if connection_string.startswith("sqlite:///"):
            return connection_string[10:]
        elif connection_string.startswith("sqlite://"):
            return connection_string[9:]
        return connection_string

    def close_postgres_pool(self, connection_string: str) -> None:
        """Release this caller's hold on the pools for *connection_string*.

        A pool is closed only once every holder has released it. Closing a
        store no longer closes the pool a queue on the same DSN is still using.
        """
        with self._postgres_lock:
            for key in list(self._postgres_keys_by_dsn.get(connection_string, [])):
                holders = self._postgres_holders.get(key, 0)
                if holders > 1:
                    self._postgres_holders[key] = holders - 1
                    continue
                self._postgres_holders.pop(key, None)
                pool = self._postgres_pools.pop(key, None)
                self._postgres_keys_by_dsn[connection_string].remove(key)
                if pool is not None:
                    pool.close()
            if not self._postgres_keys_by_dsn.get(connection_string):
                self._postgres_keys_by_dsn.pop(connection_string, None)

    def release_postgres_pool(self, pool: Any) -> None:
        """Release one hold on *pool*, closing it when no hold remains."""
        with self._postgres_lock:
            key = next((k for k, v in self._postgres_pools.items() if v is pool), None)
            if key is None:
                return
            holders = self._postgres_holders.get(key, 0)
            if holders > 1:
                self._postgres_holders[key] = holders - 1
                return
            self._postgres_holders.pop(key, None)
            self._postgres_pools.pop(key, None)
            keys = self._postgres_keys_by_dsn.get(key[0], [])
            if key in keys:
                keys.remove(key)
            if not keys:
                self._postgres_keys_by_dsn.pop(key[0], None)
            pool.close()

    def close_sqlite_connection(self, connection_string: str) -> None:
        """Close SQLite connection for current thread."""
        db_path = self._parse_sqlite_path(connection_string)
        if hasattr(self._sqlite_local, "connections"):
            connections: dict[str, sqlite3.Connection | None] = self._sqlite_local.connections
            conn = connections.get(db_path)
            if conn is not None:
                conn.close()
                connections[db_path] = None

    def close_all(self) -> None:
        """Close all connections (for shutdown/testing)."""
        # Close all PostgreSQL pools
        with self._postgres_lock:
            for pool in self._postgres_pools.values():
                pool.close()
            self._postgres_pools.clear()
            self._postgres_holders.clear()
            self._postgres_keys_by_dsn.clear()

        # Close SQLite connections for current thread
        if hasattr(self._sqlite_local, "connections"):
            connections: dict[str, sqlite3.Connection] = self._sqlite_local.connections
            for conn in connections.values():
                if conn is not None:
                    conn.close()
            connections.clear()


def get_connection_manager() -> ConnectionManager:
    """Get the singleton ConnectionManager instance."""
    return ConnectionManager()


def release_pool_once(owner: Any, manager: ConnectionManager, pool: Any) -> None:
    """Release *owner*'s hold on *pool*; later calls by the same owner do nothing."""
    if getattr(owner, "_stabilize_pool_released", False):
        return
    owner._stabilize_pool_released = True
    manager.release_postgres_pool(pool)
