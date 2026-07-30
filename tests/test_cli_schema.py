"""Tests for CLI target-schema support (mg-up landing tables in a configured schema).

Regression tests for the SponsorSignal report: `stabilize mg-up` wrote its
tables into the connection's default schema (public) with no way to target a
dedicated schema.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from stabilize.cli.config import load_config, parse_db_url, validate_schema_name

try:
    import psycopg

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# =============================================================================
# URL / config parsing (no database required)
# =============================================================================


class TestSchemaConfig:
    def test_parse_db_url_extracts_schema(self) -> None:
        config = parse_db_url("postgres://user:pass@host:5433/mydb?schema=stabilize")
        assert config["dbname"] == "mydb"
        assert config["port"] == 5433
        assert config["schema"] == "stabilize"

    def test_parse_db_url_without_query_has_no_schema(self) -> None:
        config = parse_db_url("postgres://user:pass@host:5432/mydb")
        assert "schema" not in config

    def test_parse_db_url_ignores_unrelated_params(self) -> None:
        config = parse_db_url("postgres://user:pass@host/mydb?sslmode=require")
        assert config["dbname"] == "mydb"
        assert "schema" not in config

    def test_mg_schema_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MG_DATABASE_URL", "postgres://u:p@h:5432/d")
        monkeypatch.setenv("MG_SCHEMA", "stabilize")
        config = load_config()
        assert config["schema"] == "stabilize"

    def test_validate_schema_name_accepts_identifier(self) -> None:
        assert validate_schema_name("stabilize") == "stabilize"
        assert validate_schema_name("_app_2") == "_app_2"

    @pytest.mark.parametrize(
        "bad",
        ['stabilize"; DROP SCHEMA public; --', "a.b", "1abc", "", "a-b", "a" * 64],
    )
    def test_validate_schema_name_rejects_non_identifiers(self, bad: str) -> None:
        with pytest.raises(SystemExit):
            validate_schema_name(bad)

    def test_mg_up_refuses_invalid_schema_before_connecting(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        if not HAS_POSTGRES:
            pytest.skip("psycopg not installed")
        from stabilize.cli.commands import mg_up

        # Host is unresolvable: if validation didn't run first, we'd see a
        # database error instead of the schema error.
        with pytest.raises(SystemExit):
            mg_up("postgres://u:p@host-that-does-not-exist/db?schema=bad-name")
        out = capsys.readouterr().out
        assert "Invalid schema name" in out


# =============================================================================
# Live mg-up against PostgreSQL (Docker)
# =============================================================================


def _repo_migrations() -> list[tuple[str, str]]:
    """Load migrations from the repo checkout (editable installs package none)."""
    migrations = sorted((PROJECT_ROOT / "migrations").glob("*.sql"))
    assert migrations, "expected repo-root migrations/*.sql"
    return [(p.name, p.read_text()) for p in migrations]


def _tables_in_schema(url: str, schema: str) -> set[str]:
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (schema,),
        )
        return {r[0] for r in cur.fetchall()}


@pytest.fixture()
def fresh_database(postgres_container: Any) -> Any:
    """Create a dedicated database in the shared container, yield its URL."""
    if not HAS_POSTGRES:
        pytest.skip("psycopg not installed")
    base_url = postgres_container.get_connection_url().replace("+psycopg2", "")
    dbname = "mg_schema_test"
    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
    yield base_url.rsplit("/", 1)[0] + f"/{dbname}"
    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)")


class TestMgUpSchemaPostgres:
    def test_mg_up_lands_tables_in_target_schema_postgres(
        self, fresh_database: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from stabilize.cli import commands

        monkeypatch.setattr(commands, "get_migrations", _repo_migrations)

        commands.mg_up(fresh_database + "?schema=stabilize")

        in_schema = _tables_in_schema(fresh_database, "stabilize")
        for expected in ("stabilize_migrations", "queue_messages", "stage_executions", "task_executions"):
            assert expected in in_schema, f"{expected} not created in stabilize schema"

        in_public = _tables_in_schema(fresh_database, "public")
        assert not (in_public & in_schema), f"stabilize tables leaked into public: {in_public & in_schema}"

        # Idempotence: a second run applies nothing and does not error.
        commands.mg_up(fresh_database + "?schema=stabilize")
        assert "All migrations already applied" in capsys.readouterr().out

        # The documented runtime path resolves tables in the target schema:
        # a DSN with options=-csearch_path connects and queries cleanly.
        from stabilize.persistence.postgres import PostgresWorkflowStore
        from stabilize.persistence.store import WorkflowNotFoundError

        runtime_url = fresh_database + "?options=-csearch_path%3Dstabilize"
        store = PostgresWorkflowStore(connection_string=runtime_url)
        try:
            with pytest.raises(WorkflowNotFoundError):
                store.retrieve("01JLIVEPROBE0000000000RUN0")
        finally:
            store.close()

    def test_mg_up_without_schema_still_lands_in_public_postgres(
        self, fresh_database: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from stabilize.cli import commands

        monkeypatch.setattr(commands, "get_migrations", _repo_migrations)

        commands.mg_up(fresh_database)

        in_public = _tables_in_schema(fresh_database, "public")
        for expected in ("stabilize_migrations", "queue_messages", "stage_executions"):
            assert expected in in_public, f"{expected} not created in public schema"

    def test_mg_status_reads_target_schema_postgres(
        self, fresh_database: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from stabilize.cli import commands

        monkeypatch.setattr(commands, "get_migrations", _repo_migrations)

        commands.mg_up(fresh_database + "?schema=stabilize")
        capsys.readouterr()

        commands.mg_status(fresh_database + "?schema=stabilize")
        out = capsys.readouterr().out
        assert "pending" not in out
        assert "applied" in out
