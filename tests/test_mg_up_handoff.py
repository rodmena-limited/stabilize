"""stabilize mg-up and mg-status hand off to migretti (#95), checked with migretti's own CLI."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from tests.mg_support import (
    SHIPPED,
    SHIPPED_COUNT,
    clean_env,
    mg,
    migretti_counts,
    output,
    relation_exists,
    scratch_database,
    search_path_url,
    stabilize,
    stage,
    with_schema,
)

psycopg = pytest.importorskip("psycopg")

APP_MIGRATION = (
    "01M64AAAAAAAAAAAAAAAAAAAAA_app_orders.sql",
    "-- migrate: up\nCREATE TABLE app_orders (id int);\n-- migrate: down\nDROP TABLE app_orders;\n",
)
MG_UP_LOCK = (0x53544D47, "stabilize mg-up")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container) as url:
        yield url


def test_mg_up_applies_every_migration_through_migretti(db: str, tmp_path: Path) -> None:
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, output(result)

    project = stage(tmp_path)
    applied, pending = migretti_counts(project, db)
    assert (applied, pending) == (SHIPPED_COUNT, 0), (
        f"migretti records {applied} of {SHIPPED_COUNT} applied after stabilize mg-up"
    )
    verify = mg(project, db, "verify")
    assert verify.returncode == 0, output(verify)
    assert not relation_exists(db, "stabilize_migrations"), "mg-up created the old runner's stabilize_migrations table"

    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0, output(again)
    assert "No pending migrations" in output(again), output(again)


def test_mg_up_keeps_migretti_history_in_the_target_schema(db: str, tmp_path: Path) -> None:
    result = stabilize("mg-up", "--db-url", with_schema(db, "orch"))
    assert result.returncode == 0, output(result)

    project = stage(tmp_path)
    applied, pending = migretti_counts(project, search_path_url(db, "orch"))
    assert (applied, pending) == (SHIPPED_COUNT, 0), (
        f"migretti records {applied} of {SHIPPED_COUNT} applied in schema orch"
    )
    assert migretti_counts(project, db) == (0, SHIPPED_COUNT), "migretti history landed outside schema orch"
    assert not relation_exists(db, "public.pipeline_executions"), "stabilize tables landed in public"


def test_mg_variables_meant_for_another_project_do_not_redirect_mg_up(
    db: str, tmp_path: Path, postgres_container: Any
) -> None:
    with scratch_database(postgres_container, "mgdecoy") as decoy:
        env = clean_env(
            {
                "MG_ENV": "prod",
                "MG_DATABASE_URL": decoy,
                "MG_DB_HOST": "nowhere.invalid",
                "MG_DB_NAME": "wrong",
                "MG_LOCK_ID": "1",
            }
        )
        result = stabilize("mg-up", "--db-url", db, env=env)
        assert result.returncode == 0, output(result)

        project = stage(tmp_path)
        applied, _pending = migretti_counts(project, db)
        assert applied == SHIPPED_COUNT, f"migretti records {applied} of {SHIPPED_COUNT} applied in the --db-url target"
        decoy_counts = migretti_counts(project, decoy)
        assert decoy_counts == (0, SHIPPED_COUNT), "MG_DATABASE_URL in the environment redirected mg-up"


def test_another_projects_migretti_history_in_the_same_schema_is_refused(db: str, tmp_path: Path) -> None:
    app = stage(tmp_path, [APP_MIGRATION])
    assert mg(app, db, "apply", "--yes").returncode == 0

    shared = stabilize("mg-up", "--db-url", db)
    assert shared.returncode == 1, f"mg-up exit {shared.returncode} with another project's history in the schema"
    assert "does not ship" in output(shared) and "?schema=" in output(shared), output(shared)
    assert not relation_exists(db, "pipeline_executions"), "mg-up created tables beside another project's history"

    own = stabilize("mg-up", "--db-url", with_schema(db, "orch"))
    assert own.returncode == 0, output(own)
    other = mg(app, db, "verify")
    assert other.returncode == 0, f"the other project's mg verify fails beside stabilize: {output(other)}"


def test_an_applied_migration_that_no_longer_matches_its_file_stops_mg_up(db: str, tmp_path: Path) -> None:
    first = stage(tmp_path, [(p.name, p.read_text()) for p in SHIPPED[:-1]])
    assert mg(first, db, "apply", "--yes").returncode == 0
    target = SHIPPED[1]
    edited = stage(tmp_path, [(target.name, target.read_text() + "\n-- edited after it was applied\n")])
    fixed = mg(edited, db, "fix", target.name.split("_", 1)[0], "--applied", "--yes")
    assert fixed.returncode == 0, output(fixed)

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, f"mg-up exit {result.returncode} although an applied migration changed"
    assert "Checksum mismatch" in output(result), f"mg-up did not name the changed migration: {output(result)}"
    after = migretti_counts(stage(tmp_path), db)
    assert after == (SHIPPED_COUNT - 1, 1), "mg-up applied a migration after verify failed"


def test_a_failed_migration_in_migrettis_history_stops_mg_up(db: str, tmp_path: Path) -> None:
    broken = stage(
        tmp_path,
        [(SHIPPED[0].name, "-- migrate: up\n-- migrate: no-transaction\nSELECT 1/0;\n-- migrate: down\nSELECT 1;\n")],
    )
    assert mg(broken, db, "apply", "--yes").returncode == 1

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, f"mg-up exit {result.returncode} with a failed migration in migretti's history"
    assert "mg fix" in output(result), output(result)
    assert not relation_exists(db, "pipeline_executions"), "mg-up applied migrations over a dirty history"


def test_a_held_migration_lock_stops_mg_up_and_releasing_it_lets_mg_up_run(db: str, tmp_path: Path) -> None:
    project = stage(tmp_path)
    holder = psycopg.connect(db, autocommit=True)
    try:
        holder.execute("SELECT pg_advisory_lock(%s, hashtext(%s))", MG_UP_LOCK)
        started = time.monotonic()
        blocked = stabilize("mg-up", "--db-url", db, "--lock-timeout", "2")
        elapsed = time.monotonic() - started
        assert blocked.returncode == 1, (
            f"mg-up did not stop at the held lock: exit {blocked.returncode} {output(blocked)}"
        )
        assert "held the migration lock" in output(blocked), output(blocked)
        assert elapsed < 30, f"mg-up waited {elapsed:.1f} s on a 2 s lock timeout"
        assert migretti_counts(project, db) == (0, SHIPPED_COUNT), "mg-up changed the database while the lock was held"
    finally:
        holder.close()

    released = stabilize("mg-up", "--db-url", db, "--lock-timeout", "2")
    assert released.returncode == 0, output(released)
    assert migretti_counts(project, db) == (SHIPPED_COUNT, 0)


def test_mg_status_reports_migrettis_state_and_creates_nothing(db: str) -> None:
    before = stabilize("mg-status", "--db-url", db)
    assert before.returncode == 0, output(before)
    assert f"Applied 0 of {SHIPPED_COUNT}" in output(before), (
        f"mg-status did not report migretti's count: {output(before)}"
    )
    assert not relation_exists(db, "_migrations"), "mg-status created migretti's tracking table"
    assert not relation_exists(db, "stabilize_migrations"), "mg-status created the old runner's table"

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    after = stabilize("mg-status", "--db-url", db)
    assert f"Applied {SHIPPED_COUNT} of {SHIPPED_COUNT}" in output(after), output(after)
    assert "pending" not in output(after), output(after)


def test_a_target_schema_named_after_the_connecting_role_is_refused(db: str, tmp_path: Path) -> None:
    role = urlsplit(db).username
    assert role, db
    with psycopg.connect(db) as conn:
        assert conn.execute("SHOW search_path").fetchone()[0].startswith('"$user"'), "the role's search_path"
    result = stabilize("mg-up", "--db-url", with_schema(db, role))
    assert result.returncode == 1, f"mg-up accepted a schema that shadows public for role {role}: {output(result)}"
    assert "$user" in output(result) and role in output(result), output(result)
    assert not relation_exists(db, f"{role}.pipeline_executions") and not relation_exists(db, f"{role}._migrations")
