"""CLI command implementations for Stabilize."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any

from stabilize.cli.config import (
    MIGRATION_TABLE,
    apply_schema_override,
    build_db_url,
    connection_params,
    load_config,
    parse_db_url,
    validate_schema_name,
)
from stabilize.cli.migrations import (
    compute_checksum,
    extract_up_migration,
    get_migrations,
)
from stabilize.cli.prompt_text import PROMPT_TEXT
from stabilize.redaction import redact_db_url, redact_text

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


_PSYCOPG_DISTRIBUTIONS = frozenset({"psycopg", "psycopg_pool"})


def _print_install_hint() -> None:
    print("Error: psycopg not installed")
    print("Install with: pip install stabilize[postgres]")


def import_psycopg() -> Any:
    """Import psycopg, printing the install hint only when psycopg itself is absent.

    Any other ImportError, including a psycopg_c that does not match the
    installed psycopg, propagates unchanged with its own message.
    """
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        if exc.name not in _PSYCOPG_DISTRIBUTIONS:
            raise
        _print_install_hint()
        sys.exit(1)
    return psycopg


def describe_driver(psycopg: Any) -> str:
    """Describe the psycopg implementation and the libpq that will verify the connection."""
    impl = psycopg.pq.__impl__
    libpq = psycopg.pq.version()
    source = "bundled inside the psycopg-binary wheel" if impl == "binary" else "system libpq"
    return f"psycopg {psycopg.__version__} impl={impl}, libpq {libpq} ({source})"


def announce_driver(psycopg: Any) -> None:
    """Print which driver and libpq will open the connection, refusing when that cannot be determined."""
    try:
        description = describe_driver(psycopg)
    except Exception as exc:
        print(f"Error: cannot determine the psycopg implementation or libpq version: {type(exc).__name__}")
        print("Refusing to connect without knowing which libpq verifies TLS")
        sys.exit(1)
    print(f"Driver: {description}")


def mg_up(db_url: str | None = None) -> None:
    """Apply pending migrations to PostgreSQL database."""
    psycopg = import_psycopg()

    # Load config
    if db_url:
        config = apply_schema_override(parse_db_url(db_url))
    else:
        config = load_config()

    # Optional target schema (db URL ?schema=, mg.yaml schema key, or MG_SCHEMA).
    # Validated before any connection is opened.
    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)

    params = connection_params(config)
    announce_driver(psycopg)

    try:
        with psycopg.connect(**params) as conn:
            with conn.cursor() as cur:
                if schema:
                    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                    cur.execute(f'SET search_path TO "{schema}"')
                    conn.commit()
                    print(f"Target schema: {schema}")

                # Ensure migration tracking table exists
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        checksum VARCHAR(32) NOT NULL,
                        applied_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()

                # Get applied migrations
                cur.execute(f"SELECT name, checksum FROM {MIGRATION_TABLE}")
                applied = {row[0]: row[1] for row in cur.fetchall()}

                # Get available migrations
                migrations = get_migrations()


                # Apply pending migrations
                pending = 0
                for name, content in migrations:
                    if name in applied:
                        # Verify checksum
                        expected = compute_checksum(content)
                        if applied[name] != expected:
                            # A mismatch means an already-applied migration's
                            # file changed under us — a tampering/drift signal.
                            # Warn by default (backward compatible); with
                            # STABILIZE_STRICT_MIGRATIONS=1 refuse to proceed.
                            msg = f"Checksum mismatch for {name}"
                            if os.getenv("STABILIZE_STRICT_MIGRATIONS", "").lower() in {"1", "true", "yes"}:
                                raise RuntimeError(
                                    f"{msg}: applied migration has changed on disk "
                                    "(STABILIZE_STRICT_MIGRATIONS is set)"
                                )
                            print(f"Warning: {msg}")
                        continue

                    pending += 1
                    print(f"Applying: {name}")

                    up_sql: Any = extract_up_migration(content)
                    cur.execute(up_sql)

                    checksum = compute_checksum(content)
                    cur.execute(
                        f"INSERT INTO {MIGRATION_TABLE} (name, checksum) VALUES (%s, %s)",
                        (name, checksum),
                    )
                    conn.commit()

                if pending == 0:
                    print("All migrations already applied")
                else:
                    print(f"Applied {pending} migration(s)")

    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)


def prompt() -> None:
    """Output comprehensive documentation for AI coding agents."""
    print(PROMPT_TEXT)


def monitor(
    db_url: str | None,
    app_filter: str | None,
    refresh_interval: int,
    status_filter: str,
) -> None:
    """Launch the real-time monitoring dashboard."""
    from stabilize.monitor import run_monitor

    # Create store based on db_url
    if db_url is None:
        # Try to load from config
        try:
            config = load_config()
            db_url = build_db_url(config)
        except SystemExit:
            print("Error: No database configuration found.")
            print("Provide --db-url or set up mg.yaml / MG_DATABASE_URL")
            sys.exit(1)

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

            store = PostgresWorkflowStore(db_url)
            try:
                queue = PostgresQueue(db_url)
            except Exception:
                queue = None
        except ModuleNotFoundError as exc:
            if exc.name not in _PSYCOPG_DISTRIBUTIONS:
                raise
            _print_install_hint()
            sys.exit(1)
    else:
        print(f"Error: Unsupported database URL: {redact_db_url(db_url)}")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)

    print(f"Connecting to {redact_db_url(db_url)}")
    run_monitor(
        store=store,
        queue=queue,
        app_filter=app_filter,
        refresh_interval=refresh_interval,
        status_filter=status_filter,
    )


def mg_status(db_url: str | None = None) -> None:
    """Show migration status."""
    psycopg = import_psycopg()

    # Load config
    if db_url:
        config = apply_schema_override(parse_db_url(db_url))
    else:
        config = load_config()

    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)

    params = connection_params(config)
    announce_driver(psycopg)

    try:
        with psycopg.connect(**params) as conn:
            with conn.cursor() as cur:
                if schema:
                    cur.execute(f'SET search_path TO "{schema}"')
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = %s AND table_schema = %s
                        )
                    """,
                        (MIGRATION_TABLE, schema),
                    )
                else:
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = %s
                        )
                    """,
                        (MIGRATION_TABLE,),
                    )
                row = cur.fetchone()
                table_exists = row[0] if row else False

                applied = {}
                if table_exists:
                    cur.execute(f"SELECT name, checksum, applied_at FROM {MIGRATION_TABLE} ORDER BY applied_at")
                    applied = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

                migrations = get_migrations()

                print(f"{'Status':<10} {'Migration':<50} {'Applied At'}")
                print("-" * 80)

                for name, content in migrations:
                    if name in applied:
                        checksum, applied_at = applied[name]
                        expected = compute_checksum(content)
                        status = "applied" if checksum == expected else "MISMATCH"
                        print(f"{status:<10} {name:<50} {applied_at}")
                    else:
                        print(f"{'pending':<10} {name:<50} -")

    except psycopg.Error as e:
        print(f"Database error: {redact_text(str(e))}")
        sys.exit(1)


def mg_check_grants(role: str, db_url: str | None = None) -> None:
    """Report which engine tables *role* cannot SELECT, INSERT, UPDATE or DELETE."""
    from stabilize.cli.grants import check_role_grants, engine_tables, format_report

    psycopg = import_psycopg()
    config = apply_schema_override(parse_db_url(db_url)) if db_url else load_config()
    schema = config.get("schema")
    if schema:
        schema = validate_schema_name(schema)
    tables = engine_tables(get_migrations())
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
            count = store.count_buffered_signal_stages(
                only_complete=not include_active, statuses=statuses
            )
            print(f"{count} stage row(s) in {scope} carry a _buffered_signals buffer")
            print("Re-run without --dry-run to strip them")
            return

        rows = store.cleanup_buffered_signals(
            only_complete=not include_active, statuses=statuses
        )
        print(f"Stripped _buffered_signals from {rows} stage row(s) in {scope}")
    except UnknownStatusError as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()
