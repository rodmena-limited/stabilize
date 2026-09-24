"""Report which engine tables a database role cannot use (#44)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from stabilize.cli.migrations import extract_up_migration

TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE")

_CREATE_RE = re.compile(r"(?i)\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?([A-Za-z_][A-Za-z0-9_]*)\"?")
_DROP_RE = re.compile(r"(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?\"?([A-Za-z_][A-Za-z0-9_]*)\"?")


def engine_tables(migrations: list[tuple[str, str]]) -> list[str]:
    """Return the tables the migrations leave in place, in creation order."""
    tables: list[str] = []
    for _name, content in migrations:
        up = extract_up_migration(content)
        for statement in up.split(";"):
            created = _CREATE_RE.search(statement)
            if created and created.group(1) not in tables:
                tables.append(created.group(1))
            dropped = _DROP_RE.search(statement)
            if dropped and dropped.group(1) in tables:
                tables.remove(dropped.group(1))
    return tables


@dataclass
class GrantReport:
    role: str
    schema: str
    tables: list[str]
    missing_tables: list[str] = field(default_factory=list)
    missing_privileges: dict[str, list[str]] = field(default_factory=dict)
    missing_sequences: dict[str, str] = field(default_factory=dict)
    schema_usage: bool = True
    control_failures: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not (self.missing_tables or self.missing_privileges or self.missing_sequences or not self.schema_usage)


def check_role_grants(cur: Any, role: str, schema: str, tables: list[str]) -> GrantReport | None:
    """Evaluate *role*'s access to every engine table, or return None when the role does not exist."""
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    if cur.fetchone() is None:
        return None

    report = GrantReport(role=role, schema=schema, tables=tables)
    cur.execute("SELECT has_schema_privilege(%s, %s, 'USAGE')", (role, schema))
    row = cur.fetchone()
    report.schema_usage = bool(row and row[0])

    for table in tables:
        qualified = f'"{schema}"."{table}"'
        cur.execute(
            "SELECT c.oid, pg_get_userbyid(c.relowner) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p')",
            (schema, table),
        )
        found = cur.fetchone()
        if found is None:
            report.missing_tables.append(table)
            continue
        oid, owner = found

        cur.execute("SELECT has_table_privilege(%s, %s::oid, 'SELECT')", (owner, oid))
        control = cur.fetchone()
        if not (control and control[0]):
            report.control_failures.append(f"{table}: owner {owner} reported without SELECT")
            continue

        missing = []
        for privilege in TABLE_PRIVILEGES:
            cur.execute("SELECT has_table_privilege(%s, %s::oid, %s)", (role, oid, privilege))
            granted = cur.fetchone()
            if not (granted and granted[0]):
                missing.append(privilege)
        if missing:
            report.missing_privileges[table] = missing

        cur.execute(
            "SELECT pg_get_serial_sequence(%s, a.attname) FROM pg_attribute a "
            "WHERE a.attrelid = %s::oid AND a.attnum > 0 AND NOT a.attisdropped "
            "AND pg_get_serial_sequence(%s, a.attname) IS NOT NULL",
            (qualified, oid, qualified),
        )
        for (sequence,) in cur.fetchall():
            cur.execute("SELECT has_sequence_privilege(%s, %s, 'USAGE')", (role, sequence))
            usage = cur.fetchone()
            if not (usage and usage[0]):
                report.missing_sequences[table] = sequence

    return report


def format_report(report: GrantReport) -> list[str]:
    """Render *report* as the lines the CLI prints."""
    header = f"Role {report.role!r}, schema {report.schema!r}"
    lines = [f"{header}, {len(report.tables)} engine table(s) from the shipped migrations"]
    if not report.schema_usage:
        lines.append(f"  MISSING  USAGE on schema {report.schema}")
    for table in report.missing_tables:
        lines.append(
            f"  MISSING  table {report.schema}.{table} does not exist (migrations not applied, or wrong schema)"
        )
    for table, privileges in report.missing_privileges.items():
        lines.append(f"  MISSING  {', '.join(privileges)} on {report.schema}.{table}")
    for table, sequence in report.missing_sequences.items():
        lines.append(f"  MISSING  USAGE on sequence {sequence} (INSERT into {table} needs it)")
    if report.complete:
        lines.append(f"OK: {report.role} can SELECT, INSERT, UPDATE and DELETE every engine table")
    return lines
