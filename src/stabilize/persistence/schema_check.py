"""The first-use check that an engine table is in the shape this stabilize needs, with the DDL that fixes it."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from typing import Any

from stabilize.persistence.columns import ENGINE_COLUMNS, columns
from stabilize.persistence.tenancy import MODE_TENANCY, TENANT_CHECK, TENANT_COLUMN, EngineSchemaError

logger = logging.getLogger(__name__)

FENCE_TRIGGER = "stabilize_tenant_fence"


def _shape_for(table: str) -> str:
    if table in ENGINE_COLUMNS:
        return table
    return "queue_messages_dlq" if table.endswith("_dlq") else "queue_messages"


def _problems_for_columns(table: str, present: dict[str, dict[str, Any]], backend: str, mode: str) -> list[str]:
    problems: list[str] = []
    missing = [c for c in columns(_shape_for(table), backend) if c not in present]
    if TENANT_COLUMN in missing:
        problems.append(f"ALTER TABLE {table} ADD COLUMN {TENANT_COLUMN} TEXT NOT NULL DEFAULT ''")
        missing.remove(TENANT_COLUMN)
    if missing:
        problems.append(f"{table} lacks column(s) {', '.join(missing)}; run stabilize mg-up")
    tenant = present.get(TENANT_COLUMN)
    if tenant is not None:
        if tenant["type"] not in ("text", "TEXT"):
            problems.append(f"ALTER TABLE {table} ALTER COLUMN {TENANT_COLUMN} TYPE text")
        if tenant["nullable"]:
            problems.append(f"ALTER TABLE {table} ALTER COLUMN {TENANT_COLUMN} SET NOT NULL")
        default = tenant["default"]
        if backend == "postgres" and mode == MODE_TENANCY and default is not None:
            problems.append(f"ALTER TABLE {table} ALTER COLUMN {TENANT_COLUMN} DROP DEFAULT")
        if (backend == "sqlite" or mode != MODE_TENANCY) and default not in ("''::text", "''"):
            problems.append(f"ALTER TABLE {table} ALTER COLUMN {TENANT_COLUMN} SET DEFAULT ''")
    return problems


def check_postgres_tables(conn: Any, schema: str | None, tables: list[str], mode: Callable[[], str]) -> None:
    """Raise EngineSchemaError unless every table has the listed columns, the tenant shape and the fence.

    *mode* is read after the catalog queries, because the first statement on an armed connection is what
    reads the mode record.
    """
    from psycopg.rows import tuple_row

    with conn.cursor(row_factory=tuple_row) as cur:
        if schema is None:
            cur.execute("SELECT current_schema()")
            row = cur.fetchone()
            schema = str(row[0]) if row else "public"
        cur.execute(
            "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull, "
            "pg_get_expr(d.adbin, d.adrelid) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
            "LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum "
            "WHERE n.nspname = %s AND c.relname = ANY(%s) AND c.relkind IN ('r', 'p')",
            (schema, tables),
        )
        present: dict[str, dict[str, dict[str, Any]]] = {}
        for table, column, type_name, nullable, default in cur.fetchall():
            present.setdefault(str(table), {})[str(column)] = {
                "type": str(type_name),
                "nullable": bool(nullable),
                "default": default,
            }
        cur.execute(
            "SELECT c.relname, t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = ANY(%s) AND NOT t.tgisinternal",
            (schema, tables),
        )
        triggers: dict[str, set[str]] = {}
        for table, name in cur.fetchall():
            triggers.setdefault(str(table), set()).add(str(name))
        cur.execute(
            "SELECT c.relname, k.conname FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = ANY(%s)",
            (schema, tables),
        )
        constraints: dict[str, set[str]] = {}
        for table, name in cur.fetchall():
            constraints.setdefault(str(table), set()).add(str(name))
        cur.execute(
            "SELECT ic.relname FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid "
            "JOIN pg_class tc ON tc.oid = i.indrelid JOIN pg_namespace n ON n.oid = tc.relnamespace "
            "WHERE n.nspname = %s AND tc.relname = ANY(%s) AND NOT i.indisvalid",
            (schema, tables),
        )
        invalid = [str(r[0]) for r in cur.fetchall()]
    for name in invalid:
        logger.warning("index %s is INVALID; the planner cannot use it. Run REINDEX INDEX %s.%s", name, schema, name)
    problems: list[str] = []
    current = mode()
    for table in tables:
        if table not in present:
            problems.append(f"table {table} does not exist; run stabilize mg-up")
            continue
        problems.extend(_problems_for_columns(table, present[table], "postgres", current))
        if current == MODE_TENANCY:
            if FENCE_TRIGGER not in triggers.get(table, set()):
                problems.append(
                    f"trigger {FENCE_TRIGGER} is missing on {table}; run stabilize mg-tenancy disable then enable "
                    "on an empty schema, or recreate it from the fence DDL"
                )
            if TENANT_CHECK not in constraints.get(table, set()):
                problems.append(f"ALTER TABLE {table} ADD CONSTRAINT {TENANT_CHECK} CHECK ({TENANT_COLUMN} <> '')")
    if problems:
        raise EngineSchemaError(
            f"the schema is not in the shape stabilize 0.34 needs ({current} mode); run, as the owner: "
            + "; ".join(problems)
        )


def check_sqlite_tables(conn: sqlite3.Connection, tables: list[str], mode: str) -> None:
    problems: list[str] = []
    existing = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    triggers = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()}
    for table in tables:
        if table not in existing:
            continue
        present: dict[str, dict[str, Any]] = {}
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall():
            present[str(row[1])] = {"type": str(row[2]).upper(), "nullable": not row[3], "default": row[4]}
        for problem in _problems_for_columns(table, present, "sqlite", mode):
            problems.append(
                problem.replace("ALTER COLUMN stabilize_tenant TYPE text", "recreate stabilize_tenant as TEXT")
            )
        if mode == MODE_TENANCY and f"stabilize_fence_del_{table}" not in triggers:
            problems.append(f"trigger stabilize_fence_del_{table} is missing; run stabilize mg-tenancy enable")
    if problems:
        raise EngineSchemaError(
            f"the file is not in the shape stabilize 0.34 needs ({mode} mode): " + "; ".join(problems)
        )
