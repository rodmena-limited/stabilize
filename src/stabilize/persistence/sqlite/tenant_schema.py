"""The tenant column and tenancy-mode record on SQLite, added at first use to every engine table in a file."""

from __future__ import annotations

import sqlite3

TENANT_COLUMN = "stabilize_tenant"
MODE_TABLE = "stabilize_tenancy"
MODE_SINGLE = "single"
MODE_TENANCY = "tenancy"

ENGINE_TABLES = (
    "pipeline_executions",
    "stage_executions",
    "task_executions",
    "queue_messages",
    "queue_messages_dlq",
    "processed_messages",
    "workflow_signals",
    "stage_claims",
    "events",
    "snapshots",
    "event_subscriptions",
    "task_leases",
)

_QUEUE_COLUMNS = {"message_id", "message_type", "payload", "deliver_at", "attempts"}

_MODE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {MODE_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    mode TEXT NOT NULL,
    enabled_at TEXT,
    enabled_by TEXT
)
"""


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def engine_shaped_tables(conn: sqlite3.Connection) -> list[str]:
    """Every engine table in the file: the 12 known names plus custom queue tables and their DLQs."""
    present = _tables(conn)
    found = [name for name in ENGINE_TABLES if name in present]
    for name in sorted(present):
        if name in found or name.startswith("sqlite_"):
            continue
        if _QUEUE_COLUMNS <= _columns(conn, name) and f"{name}_dlq" in present:
            found.extend([name, f"{name}_dlq"])
    return found


def _fence_missing(conn: sqlite3.Connection) -> bool:
    existing = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()}
    return any(f"stabilize_fence_del_{t}" not in existing for t in engine_shaped_tables(conn))


def tables_missing_tenant(conn: sqlite3.Connection) -> list[str]:
    return [name for name in engine_shaped_tables(conn) if TENANT_COLUMN not in _columns(conn, name)]


def ensure_tenant_schema(conn: sqlite3.Connection) -> list[str]:
    """Add the tenant column where missing and seed the mode record. Returns the tables changed."""
    if not tables_missing_tenant(conn) and MODE_TABLE in _tables(conn):
        if read_mode(conn) == MODE_TENANCY and _fence_missing(conn):
            conn.execute("BEGIN IMMEDIATE") if not conn.in_transaction else None
            ensure_fence(conn)
            conn.commit()
        return []
    if conn.in_transaction:
        conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    try:
        changed = tables_missing_tenant(conn)
        for name in changed:
            conn.execute(f"ALTER TABLE \"{name}\" ADD COLUMN {TENANT_COLUMN} TEXT NOT NULL DEFAULT ''")
        conn.execute(_MODE_TABLE_DDL)
        conn.execute(f"INSERT OR IGNORE INTO {MODE_TABLE} (id, mode) VALUES (1, ?)", (MODE_SINGLE,))
        if read_mode(conn) == MODE_TENANCY:
            ensure_fence(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return changed


def read_mode(conn: sqlite3.Connection) -> str:
    from stabilize.persistence.tenancy import mode_from_row

    return mode_from_row(conn.execute(f"SELECT mode FROM {MODE_TABLE} WHERE id = 1").fetchone())


FENCE_PREFIXES = ("stabilize_fence_ins_", "stabilize_fence_insm_", "stabilize_fence_upd_", "stabilize_fence_del_")


def _fence_ddl(table: str) -> list[str]:
    q = f'"{table}"'
    return [
        f"CREATE TRIGGER IF NOT EXISTS stabilize_fence_ins_{table} BEFORE INSERT ON {q} FOR EACH ROW "
        f"WHEN NEW.{TENANT_COLUMN} IS NULL OR NEW.{TENANT_COLUMN} = '' "
        "BEGIN SELECT RAISE(ABORT, 'ST001: stabilize_tenant is empty; stabilize 0.34 sets it on every row'); END",
        f"CREATE TRIGGER IF NOT EXISTS stabilize_fence_insm_{table} BEFORE INSERT ON {q} FOR EACH ROW "
        f"WHEN NEW.{TENANT_COLUMN} IS NOT NULL AND NEW.{TENANT_COLUMN} <> '' "
        f"AND stabilize_tenant() <> '*' AND NEW.{TENANT_COLUMN} <> stabilize_tenant() "
        "BEGIN SELECT RAISE(ABORT, 'ST002: row tenant does not match the connection tenant'); END",
        f"CREATE TRIGGER IF NOT EXISTS stabilize_fence_upd_{table} BEFORE UPDATE ON {q} FOR EACH ROW "
        f"WHEN stabilize_tenant() <> '*' AND (OLD.{TENANT_COLUMN} <> stabilize_tenant() "
        f"OR NEW.{TENANT_COLUMN} IS NOT OLD.{TENANT_COLUMN}) "
        "BEGIN SELECT RAISE(ABORT, 'ST002: row tenant does not match the connection tenant'); END",
        f"CREATE TRIGGER IF NOT EXISTS stabilize_fence_del_{table} BEFORE DELETE ON {q} FOR EACH ROW "
        f"WHEN stabilize_tenant() <> '*' AND OLD.{TENANT_COLUMN} <> stabilize_tenant() "
        "BEGIN SELECT RAISE(ABORT, 'ST002: row tenant does not match the connection tenant'); END",
    ]


def ensure_fence(conn: sqlite3.Connection) -> list[str]:
    """Put the fence triggers on every engine-shaped table that lacks them; returns the tables touched."""
    touched: list[str] = []
    existing = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()}
    for table in engine_shaped_tables(conn):
        if f"stabilize_fence_del_{table}" in existing:
            continue
        for statement in _fence_ddl(table):
            conn.execute(statement)
        touched.append(table)
    return touched


def drop_fence(conn: sqlite3.Connection) -> int:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
    names = [str(r[0]) for r in rows if str(r[0]).startswith(FENCE_PREFIXES)]
    for name in names:
        conn.execute(f'DROP TRIGGER IF EXISTS "{name}"')
    return len(names)
