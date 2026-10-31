"""WI-9 slice 9b (A15, B40): processed_at becomes TIMESTAMPTZ, and mg-up names the zone it converts under."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.mg_support import SHIPPED, mg, output, scratch_database, stabilize, stage

psycopg = pytest.importorskip("psycopg")

PROCESSED_AT_FILE = "01M68QC4M0YQASJV3NP98564RB_processed_at_timestamptz.sql"


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgzone") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _column_type(url: str) -> str:
    with psycopg.connect(url) as conn:
        row = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'processed_messages' AND column_name = 'processed_at'"
        ).fetchone()
    assert row is not None
    return str(row[0])


def test_processed_at_is_timestamptz_and_mg_up_prints_its_zone(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    assert _column_type(db) == "timestamp with time zone"
    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0, output(again)
    assert "TimeZone:" in again.stdout, again.stdout
    store = PostgresWorkflowStore(db)
    try:
        store.mark_message_processed("zone-keep", "StartWorkflow", "01M68Q00000000000000000000")
        assert store.is_message_processed("zone-keep")
        assert store.cleanup_old_processed_messages(max_age_hours=1) == 0
        assert store.cleanup_old_processed_messages(max_age_hours=0) >= 1
        assert not store.is_message_processed("zone-keep")
    finally:
        store.close()


def test_mg_up_refuses_a_foreign_writer_zone_unless_told_which_to_assume(
    postgres_container: Any, tmp_path: Path
) -> None:
    role = f"writer_tokyo_{uuid.uuid4().hex[:6]}"
    with scratch_database(postgres_container, "mgzone") as url:
        before = [(path.name, path.read_text()) for path in SHIPPED if path.name < PROCESSED_AT_FILE]
        project = stage(tmp_path, before)
        applied = mg(project, url, "apply", "--yes")
        assert applied.returncode == 0, output(applied)
        with psycopg.connect(url, autocommit=True) as conn:
            assert _column_type(url) == "timestamp without time zone"
            conn.execute(f'CREATE ROLE "{role}"')
            database = conn.info.dbname
            conn.execute(f'ALTER ROLE "{role}" IN DATABASE "{database}" SET TimeZone = \'Asia/Tokyo\'')
            conn.execute(
                "INSERT INTO processed_messages "
                "(message_id, processed_at, handler_type, execution_id, stabilize_tenant) "
                "VALUES ('zone-1', '2026-01-01 12:00:00', 'StartWorkflow', NULL, '')"
            )
        try:
            refused = stabilize("mg-up", "--db-url", url)
            assert refused.returncode == 1, "mg-up converted processed_at without asking whose zone the values are in"
            text = output(refused)
            assert "Asia/Tokyo" in text and role in text and "--assume-writer-timezone" in text, text
            assert _column_type(url) == "timestamp without time zone"

            accepted = stabilize("mg-up", "--db-url", url, "--assume-writer-timezone", "Asia/Tokyo")
            assert accepted.returncode == 0, output(accepted)
            assert _column_type(url) == "timestamp with time zone"
            with psycopg.connect(url) as conn:
                row = conn.execute(
                    "SELECT processed_at AT TIME ZONE 'UTC' FROM processed_messages WHERE message_id = 'zone-1'"
                ).fetchone()
            assert row is not None
            assert row[0] == datetime(2026, 1, 1, 3, 0), f"the naive value was not read as Tokyo time: {row[0]}"

            once_more = stabilize("mg-up", "--db-url", url)
            assert once_more.returncode == 0, output(once_more)
            assert PROCESSED_AT_FILE in {path.name for path in SHIPPED}, (
                "the conversion ran from a file that is not shipped"
            )
        finally:
            with psycopg.connect(url, autocommit=True) as conn:
                conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def test_an_unknown_assumed_zone_is_refused_before_anything_runs(db: str) -> None:
    result = stabilize("mg-up", "--db-url", db, "--assume-writer-timezone", "Mars/Olympus_Mons")
    assert result.returncode == 1, output(result)
    assert "Mars/Olympus_Mons" in output(result)
    assert datetime.now(UTC).year >= 2026
