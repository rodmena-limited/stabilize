"""CLI command implementations for Stabilize."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from stabilize.cli.config import (
    apply_schema_override,
    build_db_url,
    connection_params,
    load_config,
    parse_db_url,
    validate_schema_name,
)
from stabilize.cli.driver import (
    PSYCOPG_DISTRIBUTIONS,
    announce_driver,
    describe_driver,
    import_psycopg,
    print_install_hint,
)
from stabilize.cli.migrate import mg_status, mg_up
from stabilize.cli.migrations import get_migrations
from stabilize.cli.prompt_text import PROMPT_TEXT
from stabilize.persistence.tenancy import MODE_TENANCY, validate_tenant
from stabilize.redaction import redact_db_url, redact_text

__all__ = [
    "announce_driver",
    "describe_driver",
    "import_psycopg",
    "mg_check_grants",
    "mg_status",
    "mg_up",
    "monitor",
    "prompt",
    "prune_signals",
]

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


def prompt() -> None:
    """Output comprehensive documentation for AI coding agents."""
    print(PROMPT_TEXT)


def monitor_target(config: dict[str, Any]) -> tuple[str, str | None]:
    """The URL and schema the monitor reads, resolved as mg-up and the engine resolve them."""
    schema = config.get("schema")
    return build_db_url(config), validate_schema_name(schema) if schema else None


def resolve_monitor_tenant(mode: str, tenant: str | None, all_tenants: bool) -> str | None:
    """The tenant the monitor filters on: None means every tenant; '' is the single-tenant install."""
    if tenant is not None and all_tenants:
        print("Error: --tenant and --all-tenants exclude each other")
        sys.exit(1)
    if all_tenants:
        return None
    if tenant is not None:
        return validate_tenant(tenant)
    if mode == MODE_TENANCY:
        print(
            "Error: this schema is in tenancy mode; pass --tenant NAME to watch one tenant "
            "or --all-tenants for every one"
        )
        sys.exit(1)
    return ""


def _schema_mode(store: Any) -> str:
    from stabilize.persistence.sqlite.tenant_schema import read_mode
    from stabilize.persistence.tenancy import postgres_mode

    if hasattr(store, "_pool"):
        with store._pool.connection() as conn:
            return str(postgres_mode(conn))
    return read_mode(store._get_connection())


def monitor(
    db_url: str | None,
    app_filter: str | None,
    refresh_interval: int,
    status_filter: str,
    config_path: Path | None = None,
    tenant: str | None = None,
    all_tenants: bool = False,
) -> None:
    """Launch the real-time monitoring dashboard."""
    from stabilize.monitor import run_monitor

    schema: str | None = None
    if db_url is None:
        config = load_config(config_path)
        db_url, schema = monitor_target(config)
    elif not db_url.startswith("sqlite"):
        db_url, schema = monitor_target(apply_schema_override(parse_db_url(db_url)))

    # Determine store type from URL
    store: WorkflowStore
    queue: Queue | None = None
    if db_url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        store = SqliteWorkflowStore(db_url, create_tables=False)
        # Try to create queue for stats
        try:
            queue = SqliteQueue(db_url, table_name="queue_messages")
        except Exception:
            queue = None
    elif db_url.startswith("postgres"):
        try:
            from stabilize.persistence.postgres import PostgresWorkflowStore
            from stabilize.queue import PostgresQueue

            store = PostgresWorkflowStore(db_url, schema=schema)
            try:
                queue = PostgresQueue(db_url, schema=schema)
            except Exception:
                queue = None
        except ModuleNotFoundError as exc:
            if exc.name not in PSYCOPG_DISTRIBUTIONS:
                raise
            print_install_hint()
            sys.exit(1)
    else:
        print(f"Error: Unsupported database URL: {redact_db_url(db_url)}")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)

    print(f"Connecting to {redact_db_url(db_url)}")
    watched = resolve_monitor_tenant(_schema_mode(store), tenant, all_tenants)
    run_monitor(
        store=store,
        queue=queue,
        app_filter=app_filter,
        refresh_interval=refresh_interval,
        status_filter=status_filter,
        tenant=watched,
    )


def mg_check_grants(role: str, db_url: str | None = None, config_path: Path | None = None) -> None:
    """Report which engine tables *role* cannot SELECT, INSERT, UPDATE or DELETE."""
    from stabilize.cli.grants import check_role_grants, format_report, required_tables

    psycopg = import_psycopg()
    config = apply_schema_override(parse_db_url(db_url)) if db_url else load_config(config_path)
    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)
    tables = required_tables(get_migrations())
    if not tables:
        print("Error: no engine tables found in the shipped migrations; refusing to report on an empty set")
        sys.exit(2)

    params = connection_params(config)
    announce_driver(psycopg)
    try:
        with psycopg.connect(**params) as conn:
            with conn.cursor() as cur:
                if not schema:
                    cur.execute("SELECT current_schema()")
                    row = cur.fetchone()
                    schema = row[0] if row else None
                if not schema:
                    print("Error: no current schema; pass ?schema= in the database URL")
                    sys.exit(2)
                report = check_role_grants(cur, role, schema, tables)
    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(2)

    if report is None:
        print(f"Error: role {role!r} does not exist")
        sys.exit(2)
    if report.control_failures:
        print("Error: the privilege check could not confirm a table owner's own access; the result would mean nothing")
        for failure in report.control_failures:
            print(f"  {failure}")
        sys.exit(2)
    for line in format_report(report):
        print(line)
    sys.exit(0 if report.complete else 1)


def prune_signals(
    db_url: str | None,
    include_active: bool = False,
    dry_run: bool = False,
    statuses: list[str] | None = None,
) -> None:
    """Remove unconsumable WCP-24 persistent-signal buffers from stage contexts."""
    from stabilize.persistence.factory import create_repository
    from stabilize.persistence.signal_scope import UnknownStatusError

    if db_url is None:
        print("Error: --db-url is required")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)

    if statuses:
        scope = f"stages in {', '.join(statuses)}"
    elif include_active:
        scope = "all stages"
    else:
        scope = "completed stages"

    store = create_repository(db_url, create_tables=False)
    try:
        if dry_run:
            count = store.count_buffered_signal_stages(only_complete=not include_active, statuses=statuses)
            print(f"{count} stage row(s) in {scope} carry a _buffered_signals buffer")
            print("Re-run without --dry-run to strip them")
            return

        rows = store.cleanup_buffered_signals(only_complete=not include_active, statuses=statuses)
        print(f"Stripped _buffered_signals from {rows} stage row(s) in {scope}")
    except UnknownStatusError as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()
