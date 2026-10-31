"""stabilize mg-queue-table NAME: a custom queue table pair in the shape of queue_messages, fenced in tenancy mode."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

from stabilize.cli.driver import announce_driver, import_psycopg
from stabilize.cli.migrate import _describe, _target
from stabilize.cli.tenancy_cli import _read_record
from stabilize.persistence.tenancy import MODE_TENANCY
from stabilize.redaction import redact_text

NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _fence(conn: Any, schema: str, table: str) -> None:
    exists = conn.execute(
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND t.tgname = 'stabilize_tenant_fence'",
        (schema, table),
    ).fetchone()
    if exists is None:
        conn.execute(
            f'CREATE TRIGGER stabilize_tenant_fence BEFORE INSERT OR UPDATE OR DELETE ON "{schema}"."{table}" '
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".stabilize_tenant_fence()'
        )
    has_check = conn.execute(
        "SELECT 1 FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND k.conname = 'stabilize_tenant_nonempty'",
        (schema, table),
    ).fetchone()
    if has_check is None:
        conn.execute(
            f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT stabilize_tenant_nonempty '
            "CHECK (stabilize_tenant <> '')"
        )
    conn.execute(f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN stabilize_tenant DROP DEFAULT')


def mg_queue_table(name: str, db_url: str | None = None, config_path: Path | None = None) -> None:
    """Create NAME and NAME_dlq in the shape of the built-in queue tables, fenced in tenancy mode."""
    if not NAME_RE.match(name) or name.endswith("_dlq"):
        print(f"Error: {name!r} is not a usable queue table name (lower-case identifier, not ending in _dlq)")
        sys.exit(2)
    psycopg = import_psycopg()
    params, schema, _history = _target(db_url, None, config_path)
    announce_driver(psycopg)
    print(f"Database: {_describe(params)}")
    try:
        with psycopg.connect(**params, autocommit=True) as conn:
            if schema:
                conn.execute(f'SET search_path TO "{schema}"')
            target = schema or str(conn.execute("SELECT current_schema()").fetchone()[0])
            record = _read_record(conn)
            if record is None:
                print("Error: stabilize_tenancy has no row; run stabilize mg-up first")
                sys.exit(1)
            with conn.transaction():
                conn.execute(
                    f'CREATE TABLE IF NOT EXISTS "{target}"."{name}" (LIKE "{target}".queue_messages INCLUDING ALL)'
                )
                conn.execute(
                    f'CREATE TABLE IF NOT EXISTS "{target}"."{name}_dlq" '
                    f'(LIKE "{target}".queue_messages_dlq INCLUDING ALL)'
                )
                if record[0] == MODE_TENANCY:
                    for table in (name, f"{name}_dlq"):
                        _fence(conn, target, table)
            print(f"OK: {target}.{name} and {target}.{name}_dlq are ready ({record[0]} mode)")
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)
