"""stabilize mg-check-grants against a real PostgreSQL (#44)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from stabilize.cli import commands
from stabilize.cli.grants import engine_tables
from stabilize.cli.migrations import get_migrations

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def test_engine_tables_come_from_the_shipped_migrations() -> None:
    tables = engine_tables(get_migrations())
    assert len(tables) == 8
    assert {"pipeline_executions", "queue_messages", "workflow_signals", "stage_claims"} <= set(tables)
    assert "stabilize_migrations" not in tables


def test_a_table_dropped_by_a_later_migration_is_not_required() -> None:
    migrations = [
        (
            "01_a.sql",
            "-- migrate: up\nCREATE TABLE keep_me (id int);\n"
            "CREATE TABLE IF NOT EXISTS gone (id int);\n-- migrate: down\nDROP TABLE keep_me;",
        ),
        ("02_b.sql", "-- migrate: up\nDROP TABLE IF EXISTS gone;\n-- migrate: down\n"),
    ]
    assert engine_tables(migrations) == ["keep_me"]


def test_down_sections_do_not_remove_tables() -> None:
    migrations = [("01_a.sql", "-- migrate: up\nCREATE TABLE t1 (id int);\n-- migrate: down\nDROP TABLE t1;")]
    assert engine_tables(migrations) == ["t1"]


@pytest.fixture
def pg(postgres_url: str) -> Any:
    import psycopg

    conn = psycopg.connect(postgres_url, autocommit=True)
    yield conn
    conn.close()


def _make_role(pg: Any, grant_all: bool) -> str:
    role = f"grants_probe_{uuid.uuid4().hex[:10]}"
    pg.execute(f'CREATE ROLE "{role}" LOGIN')
    pg.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
    if grant_all:
        for table in engine_tables(get_migrations()):
            pg.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON public."{table}" TO "{role}"')
        pg.execute(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO "{role}"')
    return role


def _drop_role(pg: Any, role: str) -> None:
    pg.execute(f'DROP OWNED BY "{role}"')
    pg.execute(f'DROP ROLE "{role}"')


def _run(role: str, url: str, capsys: Any) -> tuple[int, str]:
    with pytest.raises(SystemExit) as raised:
        commands.mg_check_grants(role, url)
    return int(raised.value.code or 0), capsys.readouterr().out


def test_fully_granted_role_passes(postgres_url: str, pg: Any, capsys: Any) -> None:
    role = _make_role(pg, grant_all=True)
    try:
        code, out = _run(role, postgres_url, capsys)
    finally:
        _drop_role(pg, role)
    assert code == 0, out
    assert "OK:" in out
    assert "MISSING" not in out


def test_role_with_no_table_grants_fails_on_every_table(postgres_url: str, pg: Any, capsys: Any) -> None:
    role = _make_role(pg, grant_all=False)
    try:
        code, out = _run(role, postgres_url, capsys)
    finally:
        _drop_role(pg, role)
    assert code == 1
    assert out.count("MISSING  SELECT, INSERT, UPDATE, DELETE on public.") == 8


def test_one_missing_privilege_and_one_missing_sequence_are_named(postgres_url: str, pg: Any, capsys: Any) -> None:
    role = _make_role(pg, grant_all=True)
    pg.execute(f'REVOKE DELETE ON public.stage_claims FROM "{role}"')
    pg.execute(f'REVOKE USAGE ON SEQUENCE public.queue_messages_dlq_id_seq FROM "{role}"')
    try:
        code, out = _run(role, postgres_url, capsys)
    finally:
        _drop_role(pg, role)
    assert code == 1
    assert "MISSING  DELETE on public.stage_claims" in out
    assert "queue_messages_dlq_id_seq" in out
    assert "INSERT into queue_messages_dlq needs it" in out
    assert "OK:" not in out
    assert out.count("MISSING") == 2


def test_missing_schema_usage_is_reported(postgres_url: str, pg: Any, capsys: Any) -> None:
    role = _make_role(pg, grant_all=True)
    pg.execute(f'REVOKE USAGE ON SCHEMA public FROM "{role}"')
    pg.execute("REVOKE USAGE ON SCHEMA public FROM PUBLIC")
    try:
        code, out = _run(role, postgres_url, capsys)
    finally:
        pg.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
        _drop_role(pg, role)
    assert code == 1
    assert "MISSING  USAGE on schema public" in out


def test_unknown_role_is_refused_not_reported_clean(postgres_url: str, capsys: Any) -> None:
    code, out = _run(f"no_such_role_{uuid.uuid4().hex[:8]}", postgres_url, capsys)
    assert code == 2
    assert "does not exist" in out
    assert "OK:" not in out


def test_wrong_schema_reports_missing_tables(postgres_url: str, pg: Any, capsys: Any) -> None:
    schema = f"empty_{uuid.uuid4().hex[:8]}"
    pg.execute(f'CREATE SCHEMA "{schema}"')
    role = _make_role(pg, grant_all=True)
    try:
        code, out = _run(role, f"{postgres_url}?schema={schema}", capsys)
    finally:
        _drop_role(pg, role)
        pg.execute(f'DROP SCHEMA "{schema}"')
    assert code == 1
    assert out.count("does not exist") == 8
    assert "OK:" not in out
