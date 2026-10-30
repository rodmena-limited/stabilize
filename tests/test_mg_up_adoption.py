"""mg-up adopts a schema that has stabilize's tables but no migration history (#98)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.mg_support import (
    SHIPPED,
    SHIPPED_COUNT,
    mg,
    migretti_counts,
    output,
    relation_exists,
    scratch_database,
    stabilize,
    stage,
)

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgadopt") as url:
        yield url


def _untracked(db: str, tmp_path: Path, level: int) -> None:
    built = mg(stage(tmp_path, [(p.name, p.read_text()) for p in SHIPPED[:level]]), db, "apply", "--yes")
    assert built.returncode == 0, output(built)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("DROP TABLE _migrations, _migrations_log")


@pytest.mark.parametrize("level", range(1, SHIPPED_COUNT + 1))
def test_a_schema_without_history_is_adopted_and_completed(db: str, tmp_path: Path, level: int) -> None:
    _untracked(db, tmp_path, level)
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, f"mg-up could not adopt a schema built to level {level}: {output(result)}"
    project = stage(tmp_path)
    applied, pending = migretti_counts(project, db)
    assert (applied, pending) == (SHIPPED_COUNT, 0), f"migretti records {applied} of {SHIPPED_COUNT} after adoption"
    verify = mg(project, db, "verify")
    assert verify.returncode == 0, output(verify)


def test_a_column_of_the_wrong_type_stops_adoption_before_any_change(db: str, tmp_path: Path) -> None:
    _untracked(db, tmp_path, 8)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("ALTER TABLE stage_executions ALTER COLUMN join_threshold TYPE text")
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "stage_executions.join_threshold" in output(result), (
        f"the refusal does not name the mismatch: {output(result)}"
    )
    assert not relation_exists(db, "_migrations"), "mg-up changed the database before refusing"


def test_a_schema_matching_no_level_stops_adoption_before_any_change(db: str, tmp_path: Path) -> None:
    _untracked(db, tmp_path, 6)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("ALTER TABLE processed_messages ALTER COLUMN message_id TYPE text")
        conn.execute("DROP INDEX idx_execution_application_status")
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "no level of the shipped migrations matches" in output(result), (
        f"the refusal does not say that no level matches: {output(result)}"
    )
    assert not relation_exists(db, "_migrations")


def test_mg_status_names_the_level_an_untracked_schema_would_be_adopted_at(db: str, tmp_path: Path) -> None:
    _untracked(db, tmp_path, 8)
    status = stabilize("mg-status", "--db-url", db)
    assert status.returncode == 0, output(status)
    assert output(status).count("untracked ") == 8, (
        f"mg-status did not mark the 8 adoptable migrations: {output(status)}"
    )
    assert not relation_exists(db, "_migrations"), "mg-status created migretti's tracking table"
