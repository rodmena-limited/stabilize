"""Rule 1: one column list per engine table, equal to the catalog on both backends and to what the converter reads."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.persistence.columns import ENGINE_COLUMNS, columns, select_list
from tests.mg_support import output, scratch_database, stabilize

SRC = Path(__file__).resolve().parents[1] / "src" / "stabilize"
STAR = re.compile(r"\bSELECT\s+\*|\bRETURNING\s+\*|\b[A-Za-z_][A-Za-z0-9_]*\.\*(?=[\s,])")
ENGINE_TABLE_RE = re.compile(
    r"\b(pipeline_executions|stage_executions|task_executions|queue_messages|queue_messages_dlq|"
    r"processed_messages|workflow_signals|stage_claims|events|snapshots|event_subscriptions|task_leases)\b"
)


def _sqlite_catalog(path: Path) -> dict[str, set[str]]:
    conn = sqlite3.connect(path)
    try:
        tables = [str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        return {t: {str(r[1]) for r in conn.execute(f'PRAGMA table_info("{t}")')} for t in tables}
    finally:
        conn.close()


def test_twelve_engine_tables_are_listed() -> None:
    assert len(ENGINE_COLUMNS) == 12
    for table, listed in ENGINE_COLUMNS.items():
        assert len(set(listed)) == len(listed), f"{table} lists a column twice"
        assert listed[-1] == "stabilize_tenant", f"{table} does not end with the tenant column"
    assert select_list("task_leases", "postgres", alias="l") == (
        "l.task_id, l.owner, l.acquired_at, l.expires_at, l.stabilize_tenant"
    )


def test_sqlite_catalog_equals_the_column_lists(tmp_path: Path) -> None:
    from stabilize.events.store.sqlite.store import SqliteEventStore
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.persistence.sqlite.signals import create_signals_table
    from stabilize.persistence.task_lease import TaskLeaseManager
    from stabilize.queue.sqlite import SqliteQueue

    path = tmp_path / "catalog.db"
    url = f"sqlite:///{path}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    SqliteEventStore(url)
    try:
        TaskLeaseManager(store)._ensure_table()
        create_signals_table(store._get_connection())
    finally:
        queue.close()
        store.close()
    catalog = _sqlite_catalog(path)
    mismatches = {
        table: (
            sorted(set(columns(table, "sqlite")) - catalog.get(table, set())),
            sorted(catalog.get(table, set()) - set(columns(table, "sqlite"))),
        )
        for table in ENGINE_COLUMNS
        if set(columns(table, "sqlite")) != catalog.get(table, set())
    }
    assert not mismatches, f"(listed but absent, present but unlisted): {mismatches}"


psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgcol") as url:
        yield url


def test_postgres_catalog_equals_the_column_lists(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0, output(stabilize("mg-status", "--db-url", db))
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT table_name, array_agg(column_name::text ORDER BY ordinal_position) "
            "FROM information_schema.columns WHERE table_schema = 'public' GROUP BY table_name"
        ).fetchall()
    catalog = {str(table): set(cols) for table, cols in rows}
    mismatches = {
        table: (
            sorted(set(columns(table, "postgres")) - catalog.get(table, set())),
            sorted(catalog.get(table, set()) - set(columns(table, "postgres"))),
        )
        for table in ENGINE_COLUMNS
        if set(columns(table, "postgres")) != catalog.get(table, set())
    }
    assert not mismatches, f"(listed but absent, present but unlisted): {mismatches}"


def test_no_engine_statement_selects_star() -> None:
    offenders: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text()
        for match in STAR.finditer(text):
            before = text[max(0, match.start() - 300) : match.start()]
            if match.group(0).endswith(".*") and not re.search(r"\bSELECT\b", before):
                continue
            window = text[max(0, match.start() - 400) : match.end() + 400]
            if ENGINE_TABLE_RE.search(window) or "{self.table_name}" in window or "{table}" in window:
                line = text.count("\n", 0, match.start()) + 1
                offenders.append(f"{path.relative_to(SRC.parents[1])}:{line}: {match.group(0)}")
    assert not offenders, "star selects on engine tables:\n" + "\n".join(offenders)


SAMPLE_ROWS: dict[str, dict[str, Any]] = {
    "pipeline_executions": {
        "id": "01JLIVEPROBE0000000000RUN0",
        "type": "PIPELINE",
        "application": "app",
        "name": "n",
        "status": "RUNNING",
        "start_time": 1,
        "end_time": None,
        "start_time_expiry": None,
        "trigger": {},
        "context": {},
        "is_canceled": False,
        "canceled_by": None,
        "cancellation_reason": None,
        "paused": None,
        "pipeline_config_id": None,
        "is_limit_concurrent": False,
        "max_concurrent_executions": 0,
        "keep_waiting_pipelines": False,
        "origin": "api",
        "created_at": None,
        "queue_table": None,
        "config_version": None,
        "stabilize_tenant": "",
    },
    "stage_executions": {
        "id": "01JLIVEPROBE0000000000STG0",
        "execution_id": "01JLIVEPROBE0000000000RUN0",
        "ref_id": "a",
        "type": "t",
        "name": "n",
        "status": "NOT_STARTED",
        "context": {},
        "outputs": {},
        "requisite_stage_ref_ids": [],
        "parent_stage_id": None,
        "synthetic_stage_owner": None,
        "start_time": None,
        "end_time": None,
        "start_time_expiry": None,
        "scheduled_time": None,
        "version": 0,
        "join_type": "AND",
        "join_threshold": 0,
        "split_type": "AND",
        "split_conditions": {},
        "mi_config": None,
        "deferred_choice_group": None,
        "milestone_ref_id": None,
        "milestone_status": None,
        "mutex_key": None,
        "cancel_region": None,
        "stabilize_tenant": "",
    },
    "task_executions": {
        "id": "01JLIVEPROBE0000000000TSK0",
        "stage_id": "01JLIVEPROBE0000000000STG0",
        "name": "n",
        "implementing_class": "stabilize.tasks.ShellTask",
        "status": "NOT_STARTED",
        "start_time": None,
        "end_time": None,
        "stage_start": False,
        "stage_end": False,
        "loop_start": False,
        "loop_end": False,
        "task_exception_details": None,
        "version": 0,
        "stabilize_tenant": "",
    },
}

NOT_READ_BY_CONVERTER: dict[str, set[str]] = {
    "pipeline_executions": {"created_at", "stabilize_tenant"},
    "stage_executions": {"execution_id", "stabilize_tenant"},
    "task_executions": {"stage_id", "stabilize_tenant"},
}


@pytest.mark.parametrize("table", sorted(SAMPLE_ROWS))
def test_postgres_converters_read_every_listed_column_strictly(table: str) -> None:
    from stabilize.persistence.postgres import converters

    convert = {
        "pipeline_executions": converters.row_to_execution,
        "stage_executions": converters.row_to_stage,
        "task_executions": converters.row_to_task,
    }[table]
    full = SAMPLE_ROWS[table]
    assert set(full) == set(columns(table, "postgres")), "the sample row must carry exactly the listed columns"
    assert convert(dict(full)).id == full["id"]
    for column in columns(table, "postgres"):
        partial = dict(full)
        del partial[column]
        if column in NOT_READ_BY_CONVERTER[table]:
            convert(partial)
            continue
        with pytest.raises(KeyError, match=column):
            convert(partial)
