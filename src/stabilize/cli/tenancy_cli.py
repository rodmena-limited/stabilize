"""stabilize mg-tenancy: switch a schema between single-tenant and tenancy mode while it holds no rows."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from stabilize.cli import migretti_runner as runner
from stabilize.cli.adoption import AdoptionRefusedError
from stabilize.cli.driver import announce_driver, import_psycopg
from stabilize.cli.grants import engine_tables
from stabilize.cli.legacy_history import TakeoverRefusedError
from stabilize.cli.migrate import DEFAULT_LOCK_TIMEOUT_SECONDS, _acquire_lock, _describe, _prepare, _target
from stabilize.cli.migrations import get_migrations
from stabilize.persistence.tenancy import MODE_SINGLE, MODE_TABLE, MODE_TENANCY
from stabilize.redaction import redact_text

STOP_RULE = "Every engine process must be stopped before this command runs and started again only after it reports OK."
QUEUE_SHAPE = ("message_id", "message_type", "payload", "deliver_at", "attempts")
ACTIONS = ("enable", "disable", "status")


def _read_record(conn: Any) -> tuple[str, Any, Any] | None:
    row = conn.execute(f"SELECT mode, enabled_at, enabled_by FROM {MODE_TABLE} WHERE id = 1").fetchone()
    if row is None:
        return None
    return str(row[0]), row[1], row[2]


def _custom_queue_tables(conn: Any, schema: str) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.columns WHERE table_schema = %s "
        "GROUP BY table_name HAVING array_agg(column_name::text) @> %s::text[]",
        (schema, list(QUEUE_SHAPE)),
    ).fetchall()
    names = {str(r[0]) for r in rows}
    return sorted(n for n in names if n != "queue_messages" and f"{n}_dlq" in names)


def _tables_to_check(conn: Any, schema: str, queue_tables: list[str]) -> list[str]:
    tables = list(engine_tables(get_migrations()))
    for name in _custom_queue_tables(conn, schema) + list(queue_tables):
        for candidate in (name, f"{name}_dlq"):
            if candidate not in tables:
                tables.append(candidate)
    return tables


def _row_counts(conn: Any, schema: str, tables: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        present = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f'"{schema}"."{table}"',)).fetchone()
        if not (present and present[0]):
            print(f"Error: table {schema}.{table} does not exist; run stabilize mg-up or check --queue-table")
            sys.exit(1)
        row = conn.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()
        counts[table] = int(row[0]) if row else 0
    return counts


def _status(conn: Any, schema: str) -> None:
    record = _read_record(conn)
    if record is None:
        print(f"Error: {MODE_TABLE} has no row in schema {schema}; run stabilize mg-up")
        sys.exit(1)
    mode, enabled_at, enabled_by = record
    print(f"Schema: {schema}")
    print(f"mode: {mode}")
    print(f"enabled_at: {enabled_at.isoformat() if enabled_at is not None else '-'}")
    print(f"enabled_by: {enabled_by or '-'}")


def _flip(conn: Any, wanted: str) -> None:
    with conn.transaction():
        if wanted == MODE_TENANCY:
            conn.execute(
                f"UPDATE {MODE_TABLE} SET mode = %s, enabled_at = now(), enabled_by = current_user WHERE id = 1",
                (wanted,),
            )
        else:
            conn.execute(
                f"UPDATE {MODE_TABLE} SET mode = %s, enabled_at = NULL, enabled_by = NULL WHERE id = 1", (wanted,)
            )


def _sqlite_path(db_url: str | None) -> str | None:
    if not db_url or not db_url.startswith("sqlite:"):
        return None
    path = db_url.split("sqlite:///", 1)[1] if "sqlite:///" in db_url else db_url.split("sqlite:", 1)[1]
    if not path or path == ":memory:":
        print("Error: a tenancy mode needs a file-backed SQLite database, not :memory:")
        sys.exit(1)
    return path


def _mg_tenancy_sqlite(action: str, path: str, wanted: str) -> None:
    import getpass
    import sqlite3
    from datetime import UTC, datetime

    from stabilize.persistence.sqlite.tenant_schema import engine_shaped_tables, ensure_tenant_schema

    conn = sqlite3.connect(path)
    try:
        ensure_tenant_schema(conn)
        row = conn.execute(f"SELECT mode, enabled_at, enabled_by FROM {MODE_TABLE} WHERE id = 1").fetchone()
        if row is None:
            print(f"Error: {MODE_TABLE} has no row in {path}")
            sys.exit(1)
        mode, enabled_at, enabled_by = str(row[0]), row[1], row[2]
        if action == "status":
            print(f"Schema: {path}")
            print(f"mode: {mode}")
            print(f"enabled_at: {enabled_at or '-'}")
            print(f"enabled_by: {enabled_by or '-'}")
            return
        if mode == wanted:
            print(f"OK: {path} is already in {wanted} mode; nothing changed")
            return
        busy = {}
        for table in engine_shaped_tables(conn):
            count = conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            if count:
                busy[table] = int(count)
        if busy:
            detail = ", ".join(f"{table} ({n} row{'s' if n != 1 else ''})" for table, n in busy.items())
            print(
                f"Error: {path} is not empty, so its tenancy mode cannot change: {detail}. "
                "Tenancy mode is enabled on a fresh file; export, recreate and import to convert data."
            )
            sys.exit(1)
        conn.execute("BEGIN IMMEDIATE")
        if wanted == MODE_TENANCY:
            conn.execute(
                f"UPDATE {MODE_TABLE} SET mode = ?, enabled_at = ?, enabled_by = ? WHERE id = 1",
                (wanted, datetime.now(UTC).isoformat(), getpass.getuser()),
            )
        else:
            conn.execute(
                f"UPDATE {MODE_TABLE} SET mode = ?, enabled_at = NULL, enabled_by = NULL WHERE id = 1", (wanted,)
            )
        conn.commit()
        print(f"OK: {path} is now in {wanted} mode. {STOP_RULE}")
    finally:
        conn.close()


def mg_tenancy(
    action: str,
    db_url: str | None = None,
    queue_tables: list[str] | None = None,
    config_path: Path | None = None,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> None:
    """Report or change the tenancy mode of the target schema; a change needs an empty, fully migrated schema."""
    if action not in ACTIONS:
        print(f"Error: action must be one of {', '.join(ACTIONS)}")
        sys.exit(2)
    wanted = MODE_TENANCY if action == "enable" else MODE_SINGLE
    sqlite_path = _sqlite_path(db_url)
    if sqlite_path is not None:
        _mg_tenancy_sqlite(action, sqlite_path, wanted)
        return
    psycopg = import_psycopg()
    params, schema, history = _target(db_url, None, config_path)
    announce_driver(psycopg)
    print(f"Database: {_describe(params)}")
    try:
        with psycopg.connect(**params, autocommit=True) as conn:
            if action == "status":
                if schema:
                    conn.execute(f'SET search_path TO "{schema}"')
                _status(conn, schema or str(conn.execute("SELECT current_schema()").fetchone()[0]))
                return
            _acquire_lock(psycopg, conn, lock_timeout)
            run = _prepare(conn, params, schema, history)
            with runner.staged([]) as empty:
                state = runner.snapshot(empty, run.conninfo)
            pending = sorted(run.shipped_ids - set(state.applied))
            target = run.layout.target
            if pending:
                print(
                    f"Error: {len(pending)} shipped migration(s) are not applied in schema {target} "
                    f"({', '.join(pending[:3])}{', ...' if len(pending) > 3 else ''}); run stabilize mg-up first"
                )
                sys.exit(1)
            record = _read_record(conn)
            if record is None:
                print(f"Error: {MODE_TABLE} has no row in schema {target}; run stabilize mg-up")
                sys.exit(1)
            if record[0] == wanted:
                print(f"OK: schema {target} is already in {wanted} mode; nothing changed")
                return
            counts = _row_counts(conn, target, _tables_to_check(conn, target, queue_tables or []))
            busy = {table: n for table, n in counts.items() if n}
            if busy:
                detail = ", ".join(f"{table} ({n} row{'s' if n != 1 else ''})" for table, n in busy.items())
                print(
                    f"Error: schema {target} is not empty, so its tenancy mode cannot change: {detail}. "
                    "Tenancy mode is enabled on a fresh schema; export, recreate and import to convert data."
                )
                sys.exit(1)
            _flip(conn, wanted)
            print(f"OK: schema {target} is now in {wanted} mode. {STOP_RULE}")
    except (TakeoverRefusedError, AdoptionRefusedError, runner.MigrettiFailedError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)
