"""
Singleton connection manager for database connections.

Provides centralized connection pooling for PostgreSQL and thread-local
connections for SQLite, ensuring efficient resource usage across all
repository and queue instances.
"""

from __future__ import annotations

import sqlite3
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool


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
        self._postgres_pools: dict[str, ConnectionPool] = {}
        self._postgres_lock = threading.Lock()

        self._sqlite_local = threading.local()
        self._sqlite_lock = threading.Lock()
        self._sqlite_paths: set[str] = set()

    def get_postgres_pool(
        self,
        connection_string: str,
        min_size: int = 5,
        max_size: int = 15,
    ) -> ConnectionPool:
        """
        Get or create a PostgreSQL connection pool.

        Args:
            connection_string: PostgreSQL connection string
            min_size: Minimum pool size
            max_size: Maximum pool size

        Returns:
            Shared ConnectionPool instance for this connection string
        """
        if connection_string not in self._postgres_pools:
            with self._postgres_lock:
                if connection_string not in self._postgres_pools:
                    from psycopg.rows import dict_row
                    from psycopg_pool import ConnectionPool

                    pool = ConnectionPool(
                        connection_string,
                        min_size=min_size,
                        max_size=max_size,
                        open=True,
                        kwargs={"row_factory": dict_row},
                    )
                    self._postgres_pools[connection_string] = pool
        return self._postgres_pools[connection_string]

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
            conn.execute("PRAGMA foreign_keys = ON")
            if db_path != ":memory:":
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA busy_timeout = 30000")
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
        """Close a specific PostgreSQL pool."""
        with self._postgres_lock:
            if connection_string in self._postgres_pools:
                pool = self._postgres_pools.pop(connection_string)
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
