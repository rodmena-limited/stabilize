"""The engine checks the bodies of its PostgreSQL functions, not just that they exist (#105, Casebound caveat)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from stabilize.persistence.function_sources import OWNER_BODY, fence_body
from stabilize.persistence.tenancy import EngineSchemaError
from tests.mg_support import output, scratch_database, stabilize

REPO = Path(__file__).resolve().parents[1]
TENANT_KEY_MIGRATION = REPO / "migrations" / "01M67VCHWECA748FVXG88MF3YF_tenant_key.sql"

LENIENT_FENCE = """
CREATE OR REPLACE FUNCTION stabilize_tenant_fence() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $fence$
BEGIN
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END
$fence$
"""

LENIENT_OWNER = """
CREATE OR REPLACE FUNCTION stabilize_is_table_owner(rel regclass) RETURNS boolean
    LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp
    AS $$ SELECT true $$
"""


@pytest.fixture()
def migrated_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "fnbody") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _first_use(url: str, tenant: str = "") -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(connection_string=url, tenant=tenant)
    try:
        store.exists("01NOSUCHWORKFLOW0000000000")
    finally:
        store.close()


def _replace(url: str, ddl: str) -> None:
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(ddl)


def test_the_owner_body_constant_is_the_migration_file_body() -> None:
    text = TENANT_KEY_MIGRATION.read_text()
    start = text.index("stabilize_is_table_owner")
    body = text[start:].split("$$")[1]
    assert body.strip() == OWNER_BODY.strip()
    assert "RAISE EXCEPTION USING ERRCODE = 'ST001'" in fence_body()


def test_the_installed_functions_pass_in_both_modes(migrated_db: str) -> None:
    _first_use(migrated_db)
    enabled = stabilize("mg-tenancy", "enable", "--db-url", migrated_db)
    assert enabled.returncode == 0, output(enabled)
    _first_use(migrated_db, tenant="acme")


def test_a_rewritten_fence_function_is_refused_at_first_use(migrated_db: str) -> None:
    enabled = stabilize("mg-tenancy", "enable", "--db-url", migrated_db)
    assert enabled.returncode == 0, output(enabled)
    _replace(migrated_db, LENIENT_FENCE)
    with pytest.raises(EngineSchemaError, match="stabilize_tenant_fence"):
        _first_use(migrated_db, tenant="acme")


def test_a_rewritten_owner_function_is_refused_at_first_use(migrated_db: str) -> None:
    _replace(migrated_db, LENIENT_OWNER)
    with pytest.raises(EngineSchemaError, match="stabilize_is_table_owner"):
        _first_use(migrated_db)


def test_a_security_definer_fence_is_refused(migrated_db: str) -> None:
    enabled = stabilize("mg-tenancy", "enable", "--db-url", migrated_db)
    assert enabled.returncode == 0, output(enabled)
    with psycopg.connect(migrated_db, autocommit=True) as conn:
        conn.execute("ALTER FUNCTION stabilize_tenant_fence() SECURITY DEFINER")
    with pytest.raises(EngineSchemaError, match="SECURITY DEFINER"):
        _first_use(migrated_db, tenant="acme")
