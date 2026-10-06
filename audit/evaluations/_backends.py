"""Backends for the 0.32.0 probes.

SQLite always runs. PostgreSQL runs when STABILIZE_PROBE_DSN names a database
(migrated here with mg_up), or else in a throwaway testcontainers instance;
with neither available the PostgreSQL leg reports SKIP, never PASS.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def sqlite_backend() -> Iterator[tuple[Any, Any]]:
    from stabilize import SqliteQueue, SqliteWorkflowStore

    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/probe.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        try:
            yield store, queue
        finally:
            queue.close()
            store.close()


def _container_dsn() -> tuple[str | None, Any]:
    try:
        from testcontainers.postgres import PostgresContainer
    except Exception:
        return None, None
    try:
        c = PostgresContainer("postgres:16")
        c.start()
    except Exception:
        return None, None
    url = c.get_connection_url().replace("+psycopg2", "")
    return url, c


@contextmanager
def postgres_dsn() -> Iterator[str | None]:
    dsn = os.environ.get("STABILIZE_PROBE_DSN")
    container = None
    if not dsn:
        dsn, container = _container_dsn()
    if dsn is None:
        yield None
        return
    from stabilize.cli.commands import mg_up

    mg_up(dsn)
    try:
        yield dsn
    finally:
        if container is not None:
            container.stop()


@contextmanager
def postgres_backend(dsn: str) -> Iterator[tuple[Any, Any]]:
    from stabilize import PostgresQueue, PostgresWorkflowStore

    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn, table_name="queue_messages")
    queue.clear()
    try:
        yield store, queue
    finally:
        queue.clear()
        queue.close()
        store.close()


def backends() -> Iterator[tuple[str, Any]]:
    """Yield (name, context manager factory) for every available backend."""
    yield "sqlite", sqlite_backend
    with postgres_dsn() as dsn:
        if dsn is None:
            yield "postgres", None
        else:
            yield "postgres", lambda: postgres_backend(dsn)
