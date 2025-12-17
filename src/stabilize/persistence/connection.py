from __future__ import annotations
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

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
