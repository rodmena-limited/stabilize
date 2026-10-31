"""stabilize mg-tenancy enable | disable | status against a real PostgreSQL (WI-2 slice 3)."""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import pytest

from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgten") as url:
        yield url


def _mode(db: str) -> tuple[str, str, str]:
    status = stabilize("mg-tenancy", "status", "--db-url", db)
    assert status.returncode == 0, output(status)
    fields = dict(re.findall(r"^(mode|enabled_at|enabled_by): (.*)$", output(status), re.MULTILINE))
    return fields["mode"], fields["enabled_at"], fields["enabled_by"]


def test_status_reports_single_after_mg_up(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    status = stabilize("mg-tenancy", "status", "--db-url", db)
    assert status.returncode == 0, output(status)
    assert "mode: single" in output(status), output(status)


def test_enable_flips_the_record_on_an_empty_schema_and_names_the_stop_rule(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    result = stabilize("mg-tenancy", "enable", "--db-url", db)
    assert result.returncode == 0, output(result)
    assert "every engine process" in output(result).lower(), output(result)
    mode, enabled_at, enabled_by = _mode(db)
    assert mode == "tenancy"
    assert enabled_at != "-"
    assert enabled_by == "stabilize"

    again = stabilize("mg-tenancy", "enable", "--db-url", db)
    assert again.returncode == 0, output(again)
    assert "already" in output(again).lower(), output(again)

    disabled = stabilize("mg-tenancy", "disable", "--db-url", db)
    assert disabled.returncode == 0, output(disabled)
    assert _mode(db)[0] == "single"


def test_enable_refuses_a_schema_with_rows_and_changes_nothing(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO pipeline_executions (id, type, application, status) "
            "VALUES ('01JLIVEPROBE0000000000RUN0', 'PIPELINE', 'app', 'RUNNING')"
        )
    result = stabilize("mg-tenancy", "enable", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "pipeline_executions" in output(result), output(result)
    assert _mode(db)[0] == "single"


def test_enable_refuses_rows_in_a_custom_queue_table(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("CREATE TABLE busy_queue (LIKE queue_messages INCLUDING ALL)")
        conn.execute("CREATE TABLE busy_queue_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
        conn.execute(
            "INSERT INTO pipeline_executions (id, type, application, status, queue_table) "
            "VALUES ('01JLIVEPROBE0000000000RUN1', 'PIPELINE', 'app', 'SUCCEEDED', 'busy_queue')"
        )
        conn.execute("DELETE FROM pipeline_executions")
        conn.execute(
            "INSERT INTO busy_queue (message_id, message_type, payload, deliver_at) "
            "VALUES ('m1', 'StartWorkflow', '{}', now())"
        )
    result = stabilize("mg-tenancy", "enable", "--db-url", db, "--queue-table", "busy_queue")
    assert result.returncode == 1, output(result)
    assert "busy_queue" in output(result), output(result)
    assert _mode(db)[0] == "single"


def test_enable_refuses_when_migrations_are_pending(db: str) -> None:
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE stabilize_tenancy (id SMALLINT PRIMARY KEY CHECK (id = 1), mode TEXT NOT NULL, "
            "enabled_at TIMESTAMPTZ, enabled_by TEXT)"
        )
        conn.execute("INSERT INTO stabilize_tenancy (id, mode) VALUES (1, 'single')")
    result = stabilize("mg-tenancy", "enable", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "mg-up" in output(result), output(result)
    assert _mode(db)[0] == "single"


def test_disable_refuses_a_schema_with_rows(db: str) -> None:
    assert stabilize("mg-up", "--db-url", db).returncode == 0
    assert stabilize("mg-tenancy", "enable", "--db-url", db).returncode == 0
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO pipeline_executions (id, type, application, status, stabilize_tenant) "
            "VALUES ('01JLIVEPROBE0000000000RUN2', 'PIPELINE', 'app', 'RUNNING', 'acme')"
        )
    result = stabilize("mg-tenancy", "disable", "--db-url", db)
    assert result.returncode == 1, output(result)
    assert "pipeline_executions" in output(result), output(result)
    assert _mode(db)[0] == "tenancy"


def test_the_engine_follows_the_flipped_mode(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.persistence.tenancy import TenancyModeError

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    assert stabilize("mg-tenancy", "enable", "--db-url", db).returncode == 0
    plain = PostgresWorkflowStore(db)
    acme = PostgresWorkflowStore(db, tenant="acme")
    try:
        with pytest.raises(TenancyModeError, match="tenancy mode"):
            plain.count_by_application("nobody")
        assert acme.count_by_application("nobody") == 0
    finally:
        acme.close()
        plain.close()


def test_a_sqlite_file_can_be_switched_and_the_engine_follows(tmp_path: Any) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.persistence.tenancy import TenancyModeError

    url = f"sqlite:///{tmp_path / 'tenancy.db'}"
    assert _mode(url)[0] == "single"
    enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
    assert enabled.returncode == 0, output(enabled)
    assert "every engine process" in output(enabled).lower()
    assert _mode(url)[0] == "tenancy"

    with pytest.raises(TenancyModeError, match="tenancy mode"):
        SqliteWorkflowStore(url, create_tables=True)
    acme = SqliteWorkflowStore(url, create_tables=True, tenant="acme")
    try:
        acme.store(_workflow())
        assert acme.count_by_application("mg-tenancy") == 1
    finally:
        acme.close()

    refused = stabilize("mg-tenancy", "disable", "--db-url", url)
    assert refused.returncode == 1, output(refused)
    assert "pipeline_executions" in output(refused)
    assert _mode(url)[0] == "tenancy"


def _workflow() -> Any:
    from stabilize.models.workflow import Workflow

    return Workflow.create(application="mg-tenancy", name="w", stages=[])
