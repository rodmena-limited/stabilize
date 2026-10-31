"""Backends for the 0.32.0 probes.

SQLite always runs, set up as QueueProcessor's docstring shows; set
STABILIZE_PROBE_SQLITE_QUEUE_TABLES=1 to also call SqliteQueue._create_table(),
which releases before 0.32.0 needed for a dead-letter table. PostgreSQL runs when STABILIZE_PROBE_DSN names a database
(migrated here with mg_up), or else in a throwaway testcontainers instance;
with neither available the PostgreSQL leg reports SKIP, never PASS.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SKIP = 77


def skip(reason: str) -> None:
    """Exit with code 77: this probe could not run here. run_all.sh reports it as SKIP, never PASS."""
    print(f"SKIP: {reason}", flush=True)
    raise SystemExit(SKIP)


@contextmanager
def sqlite_backend() -> Iterator[tuple[Any, Any]]:
    from stabilize import SqliteQueue, SqliteWorkflowStore

    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/probe.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        if os.environ.get("STABILIZE_PROBE_SQLITE_QUEUE_TABLES") == "1":
            queue._create_table()
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


def dedicated_dsn(base: str, suffix: str = "probes") -> str:
    """A database of its own next to base's, so these probes never touch base's schemas."""
    import sys
    from urllib.parse import urlsplit, urlunsplit

    import psycopg

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from tests.mg_support import require_gate_cluster

    parts = urlsplit(base)
    name = f"{parts.path.lstrip('/') or 'postgres'}_{suffix}"
    with psycopg.connect(base, autocommit=True) as conn:
        require_gate_cluster(conn)
        if conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is None:
            conn.execute(f'CREATE DATABASE "{name}"')
    return urlunsplit((parts.scheme, parts.netloc, f"/{name}", parts.query, parts.fragment))


def probe_dsn(suffix: str) -> str:
    """A database of this probe's own next to STABILIZE_PROBE_DSN's; without a DSN the probe exits SKIP."""
    base = os.environ.get("STABILIZE_PROBE_DSN")
    if not base:
        skip("no PostgreSQL available (set STABILIZE_PROBE_DSN)")
    return dedicated_dsn(base, suffix)


def unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:6]}"


@contextmanager
def fresh_database(suffix: str, roles: list[str] | None = None) -> Iterator[str]:
    """An empty database for this run, dropped afterwards with the roles the probe lists; SKIP without a DSN."""
    from urllib.parse import urlsplit

    import psycopg

    base = os.environ.get("STABILIZE_PROBE_DSN")
    if not base:
        skip("no PostgreSQL available (set STABILIZE_PROBE_DSN)")
    dsn = dedicated_dsn(base, unique_name(suffix))
    try:
        yield dsn
    finally:
        name = urlsplit(dsn).path.lstrip("/")
        for role in roles or []:
            try:
                with psycopg.connect(dsn, autocommit=True) as conn:
                    conn.execute(f'DROP OWNED BY "{role}"')
            except Exception:
                pass
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            for role in roles or []:
                conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def dsn_as(admin: str, user: str, password: str, query: str = "") -> str:
    """The admin DSN's server and database, as another role."""
    from urllib.parse import quote, urlsplit

    parts = urlsplit(admin)
    host = parts.hostname or "127.0.0.1"
    port = f":{parts.port}" if parts.port else ""
    return f"postgresql://{quote(user)}:{quote(password)}@{host}{port}{parts.path}{query}"


@contextmanager
def postgres_dsn() -> Iterator[str]:
    dsn = os.environ.get("STABILIZE_PROBE_DSN")
    container = None
    if dsn:
        dsn = dedicated_dsn(dsn)
    else:
        dsn, container = _container_dsn()
    if dsn is None:
        skip("no PostgreSQL available (set STABILIZE_PROBE_DSN or enable docker)")
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
