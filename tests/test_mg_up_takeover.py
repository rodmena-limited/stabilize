"""Databases set up by stabilize's own runner (<= 0.33.1) are handed to migretti in place (#95).

The old databases are built by the published releases named in STABILIZE_OLD_RUNNERS
("0.33.1=/path/to/bin/stabilize;0.32.1=/path/to/bin/stabilize"). With
STABILIZE_REQUIRE_OLD_RUNNERS=1 a missing release is a failure, not a skip.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from tests.mg_support import (
    SHIPPED,
    SHIPPED_COUNT,
    legacy_rows,
    mg,
    migretti_counts,
    old_runner,
    old_stabilize,
    output,
    relation_exists,
    scratch_database,
    search_path_url,
    stabilize,
    stage,
    with_schema,
)

psycopg = pytest.importorskip("psycopg")

PLACEHOLDER = "-- migrate: up\nSELECT 1;\n-- migrate: down\nSELECT 1;\n"
RAG_EMBEDDINGS = ("01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql", "0c05b1306b39c39841bf4a82d229d7f5")
INDEXES_BEFORE_8F12B3F = ("01KE39HEAN221S7AWNS4A0H750_add_performance_indexes.sql", "4f5c0456e333bbece1f355960e4e7755")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgold") as url:
        yield url


def _old_mg_up(version: str, url: str, tmp_path: Path) -> None:
    result = old_stabilize(old_runner(version), "mg-up", "--db-url", url, cwd=tmp_path)
    assert result.returncode == 0, f"published {version} mg-up failed: {output(result)}"


@pytest.mark.parametrize("version", ["0.33.1", "0.32.1"])
def test_a_database_built_by_an_old_release_is_taken_over_in_place(db: str, tmp_path: Path, version: str) -> None:
    _old_mg_up(version, db, tmp_path)
    before = legacy_rows(db)

    started = time.monotonic()
    result = stabilize("mg-up", "--db-url", db)
    elapsed = time.monotonic() - started
    assert result.returncode == 0, output(result)

    project = stage(tmp_path)
    applied, pending = migretti_counts(project, db)
    assert (applied, pending) == (SHIPPED_COUNT, 0), (
        f"migretti records {applied} of {SHIPPED_COUNT} applied after taking over a {version} database"
    )
    verify = mg(project, db, "verify")
    assert verify.returncode == 0, output(verify)
    assert legacy_rows(db) == before, "mg-up changed stabilize_migrations"
    assert elapsed < 60, f"takeover of a {version} database took {elapsed:.1f} s"

    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0 and "No pending migrations" in output(again), output(again)

    downgraded = old_stabilize(old_runner(version), "mg-up", "--db-url", db, cwd=tmp_path)
    assert downgraded.returncode == 0, f"published {version} mg-up fails after the takeover: {output(downgraded)}"
    assert "All migrations already applied" in output(downgraded), output(downgraded)


def test_a_takeover_stays_inside_the_target_schema(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", with_schema(db, "orch"), tmp_path)
    before = legacy_rows(db, "orch")

    result = stabilize("mg-up", "--db-url", with_schema(db, "orch"))
    assert result.returncode == 0, output(result)

    project = stage(tmp_path)
    applied, _pending = migretti_counts(project, search_path_url(db, "orch"))
    assert applied == SHIPPED_COUNT, f"migretti records {applied} of {SHIPPED_COUNT} applied in schema orch"
    assert migretti_counts(project, db) == (0, SHIPPED_COUNT), "migretti history landed outside schema orch"
    assert legacy_rows(db, "orch") == before


def test_a_history_row_with_an_unknown_checksum_stops_the_takeover_before_any_change(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", db, tmp_path)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("UPDATE stabilize_migrations SET checksum = %s WHERE name = %s", ("0" * 32, SHIPPED[3].name))

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, f"mg-up accepted a stabilize_migrations row it cannot vouch for: {output(result)}"
    assert SHIPPED[3].name in output(result), output(result)
    assert not relation_exists(db, "_migrations"), "mg-up changed the database before refusing"


def test_a_history_row_for_a_file_stabilize_never_shipped_stops_the_takeover(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", db, tmp_path)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO stabilize_migrations (name, checksum) VALUES (%s, %s)",
            ("01M60ZZZZZZZZZZZZZZZZZZZZZ_hand_written.sql", "f" * 32),
        )

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, f"mg-up accepted a stabilize_migrations row it cannot vouch for: {output(result)}"
    assert "never shipped" in output(result), output(result)
    assert not relation_exists(db, "_migrations"), "mg-up changed the database before refusing"


def test_retired_and_superseded_history_from_early_releases_is_accepted(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", db, tmp_path)
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("INSERT INTO stabilize_migrations (name, checksum) VALUES (%s, %s)", RAG_EMBEDDINGS)
        conn.execute(
            "UPDATE stabilize_migrations SET checksum = %s WHERE name = %s",
            (INDEXES_BEFORE_8F12B3F[1], INDEXES_BEFORE_8F12B3F[0]),
        )

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, output(result)
    assert RAG_EMBEDDINGS[0] in output(result) and "no longer shipped" in output(result), (
        f"mg-up did not report the retired migration: {output(result)}"
    )

    project = stage(tmp_path)
    applied, pending = migretti_counts(project, db)
    assert (applied, pending) == (SHIPPED_COUNT, 0), (
        f"migretti records {applied} of {SHIPPED_COUNT} applied after taking over early history"
    )
    assert mg(project, db, "verify").returncode == 0


def test_an_interrupted_takeover_is_completed_by_the_next_run(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", db, tmp_path)
    started = stage(tmp_path, [(p.name, PLACEHOLDER) for p in SHIPPED[:7]])
    assert mg(started, db, "apply", "--yes").returncode == 0
    project = stage(tmp_path)
    for path in SHIPPED[:3]:
        assert mg(project, db, "fix", path.name.split("_", 1)[0], "--applied", "--yes").returncode == 0

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, output(result)
    applied, pending = migretti_counts(project, db)
    assert (applied, pending) == (SHIPPED_COUNT, 0), (
        f"migretti records {applied} of {SHIPPED_COUNT} applied after resuming a takeover"
    )
    verify = mg(project, db, "verify")
    assert verify.returncode == 0, f"an interrupted takeover left placeholders behind: {output(verify)}"


def test_four_concurrent_runs_take_over_once_and_all_succeed(db: str, tmp_path: Path) -> None:
    _old_mg_up("0.33.1", db, tmp_path)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: stabilize("mg-up", "--db-url", db), range(4)))

    codes = [r.returncode for r in results]
    assert codes == [0, 0, 0, 0], f"concurrent mg-up exit codes {codes}: {[output(r) for r in results if r.returncode]}"
    recorders = [r for r in results if "placeholder entries" in output(r)]
    assert len(recorders) == 1, f"{len(recorders)} of 4 concurrent runs recorded the takeover"

    project = stage(tmp_path)
    applied, _pending = migretti_counts(project, db)
    assert applied == SHIPPED_COUNT, f"migretti records {applied} of {SHIPPED_COUNT} applied after concurrent runs"
    assert mg(project, db, "verify").returncode == 0
