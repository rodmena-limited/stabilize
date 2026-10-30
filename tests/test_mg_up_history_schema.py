"""stabilize's tables can share a host's schema while its migretti history lives apart (#99)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.mg_support import (
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

HISTORY = "stabilize_history"
HOST_TENANT = (
    "01M2D8DA000000000000000001_tenant.sql",
    "-- migrate: up\nCREATE TABLE tenant (id text PRIMARY KEY);\n-- migrate: down\nDROP TABLE tenant;\n",
)
HOST_ON_STABILIZE = (
    "01M2D8DB0GZZNW04HGFN6GVBQ3_tenant_on_stabilize.sql",
    "-- migrate: up\nALTER TABLE public.pipeline_executions ADD COLUMN tenant_id text;\n-- migrate: down\nSELECT 1;\n",
)


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mghist") as url:
        yield url


def _stabilize_history(tmp_path: Path, db: str, schema: str = HISTORY) -> int:
    applied, _pending = migretti_counts(stage(tmp_path), search_path_url(db, schema))
    return applied


def test_an_existing_install_beside_a_host_history_is_taken_over_in_place(db: str, tmp_path: Path) -> None:
    built = old_stabilize(old_runner("0.33.1"), "mg-up", "--db-url", db, cwd=tmp_path)
    assert built.returncode == 0, output(built)
    host = stage(tmp_path, [HOST_TENANT, HOST_ON_STABILIZE])
    assert mg(host, db, "apply", "--yes").returncode == 0
    before = legacy_rows(db)

    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, f"mg-up exit {result.returncode} beside a host's migretti history: {output(result)}"
    assert f"kept in schema {HISTORY}" in output(result), output(result)
    assert _stabilize_history(tmp_path, db) == SHIPPED_COUNT, "stabilize's history is not complete in its own schema"
    assert migretti_counts(host, db) == (2, 0), "the host's own migretti history changed"
    host_verify = mg(host, db, "verify")
    assert host_verify.returncode == 0, f"the host's mg verify fails after stabilize's mg-up: {output(host_verify)}"
    assert relation_exists(db, "public.pipeline_executions") and relation_exists(db, "public.events")
    assert not relation_exists(db, f"{HISTORY}.pipeline_executions"), "stabilize's tables moved out of the host schema"
    assert legacy_rows(db) == before, "mg-up changed stabilize_migrations"

    status = stabilize("mg-status", "--db-url", db)
    assert f"Applied {SHIPPED_COUNT} of {SHIPPED_COUNT}" in output(status), output(status)
    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0 and "No pending migrations" in output(again), output(again)


def test_a_fresh_install_with_a_history_schema_lets_the_host_migrate_after_it(db: str, tmp_path: Path) -> None:
    result = stabilize("mg-up", "--db-url", db, "--history-schema", HISTORY)
    assert result.returncode == 0, f"mg-up --history-schema failed on an empty database: {output(result)}"
    host = stage(tmp_path, [HOST_TENANT, HOST_ON_STABILIZE])
    applied = mg(host, db, "apply", "--yes")
    assert applied.returncode == 0, f"the host's migrations fail after stabilize: {output(applied)}"
    assert mg(host, db, "verify").returncode == 0
    assert migretti_counts(host, db) == (2, 0)
    assert _stabilize_history(tmp_path, db) == SHIPPED_COUNT
    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0 and f"kept in schema {HISTORY}" in output(again), output(again)


def test_the_host_first_then_stabilize_with_a_history_schema(db: str, tmp_path: Path) -> None:
    host = stage(tmp_path, [HOST_TENANT])
    assert mg(host, db, "apply", "--yes").returncode == 0
    result = stabilize("mg-up", "--db-url", db, "--history-schema", "stab_meta")
    assert result.returncode == 0, f"mg-up --history-schema failed after the host: {output(result)}"
    assert _stabilize_history(tmp_path, db, "stab_meta") == SHIPPED_COUNT
    assert mg(host, db, "verify").returncode == 0 and migretti_counts(host, db) == (1, 0)


def test_a_host_history_without_a_way_to_keep_them_apart_is_refused_naming_the_remedy(db: str, tmp_path: Path) -> None:
    host = stage(tmp_path, [HOST_TENANT])
    assert mg(host, db, "apply", "--yes").returncode == 0
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "--history-schema" in output(result), f"the refusal does not name --history-schema: {output(result)}"
    assert not relation_exists(db, "pipeline_executions"), "mg-up changed the database before refusing"


def test_a_history_schema_equal_to_the_target_is_refused(db: str) -> None:
    result = stabilize("mg-up", "--db-url", db, "--history-schema", "public")
    assert result.returncode == 1, f"mg-up accepted the target schema as its history schema: {output(result)}"
    assert not relation_exists(db, "_migrations") and not relation_exists(db, "pipeline_executions")


def test_moving_history_that_already_lives_in_the_target_is_refused(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    result = stabilize("mg-up", "--db-url", db, "--history-schema", HISTORY)
    assert result.returncode == 1, f"mg-up split stabilize's history across two schemas: {output(result)}"
    assert not relation_exists(db, f"{HISTORY}._migrations")


def test_a_history_schema_of_any_name_is_found_on_later_runs(db: str, tmp_path: Path) -> None:
    host = stage(tmp_path, [HOST_TENANT])
    assert mg(host, db, "apply", "--yes").returncode == 0
    assert stabilize("mg-up", "--db-url", db, "--history-schema", "stab_meta").returncode == 0
    again = stabilize("mg-up", "--db-url", db)
    assert again.returncode == 0, f"a later mg-up did not find the history in stab_meta: {output(again)}"
    assert "kept in schema stab_meta" in output(again) and "No pending migrations" in output(again), output(again)
    status = stabilize("mg-status", "--db-url", db)
    assert f"Applied {SHIPPED_COUNT} of {SHIPPED_COUNT}" in output(status), output(status)


def test_two_stabilize_schemas_in_one_database_keep_their_own_histories(db: str, tmp_path: Path) -> None:
    first = stabilize("mg-up", "--db-url", with_schema(db, "orch_a"), "--history-schema", HISTORY)
    assert first.returncode == 0, output(first)
    second = stabilize("mg-up", "--db-url", with_schema(db, "orch_b"))
    assert second.returncode == 0, f"a second stabilize schema took the first one's history: {output(second)}"
    assert relation_exists(db, "orch_b.pipeline_executions") and relation_exists(db, "orch_b._migrations")
    assert migretti_counts(stage(tmp_path), search_path_url(db, "orch_b")) == (SHIPPED_COUNT, 0)
    assert _stabilize_history(tmp_path, db) == SHIPPED_COUNT
    again = stabilize("mg-up", "--db-url", with_schema(db, "orch_a"))
    assert again.returncode == 0 and f"kept in schema {HISTORY}" in output(again), output(again)


def test_a_history_schema_that_belongs_elsewhere_is_refused(db: str, tmp_path: Path) -> None:
    assert stabilize("mg-up", "--db-url", with_schema(db, "orch_a"), "--history-schema", HISTORY).returncode == 0
    other = stabilize("mg-up", "--db-url", with_schema(db, "orch_b"), "--history-schema", HISTORY)
    assert other.returncode == 1, f"mg-up wrote orch_b's history into orch_a's: {output(other)}"
    assert "another schema" in output(other), output(other)
    with psycopg.connect(db) as conn:
        made = conn.execute("SELECT count(*) FROM pg_namespace WHERE nspname = 'orch_b'").fetchone()[0]
    assert made == 0, "mg-up created the target schema before refusing"
    host = stage(tmp_path, [HOST_TENANT])
    assert mg(host, db, "apply", "--yes").returncode == 0
    into_host = stabilize("mg-up", "--db-url", with_schema(db, "orch_c"), "--history-schema", "public")
    assert into_host.returncode == 1 and "another project's migretti history" in output(into_host), output(into_host)
    assert migretti_counts(host, db) == (1, 0), "the host's history changed"
