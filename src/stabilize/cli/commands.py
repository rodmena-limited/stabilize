"""CLI command implementations for Stabilize."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from stabilize.cli.config import MIGRATION_TABLE, load_config, parse_db_url
from stabilize.cli.migrations import (
    compute_checksum,
    extract_up_migration,
    get_migrations,
)
from stabilize.cli.prompt_text import PROMPT_TEXT

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


def mg_up(db_url: str | None = None) -> None:
    """Apply pending migrations to PostgreSQL database."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    # Connect to database
    conninfo = (
        f"host={config['host']} port={config.get('port', 5432)} "
        f"user={config.get('user', 'postgres')} password={config.get('password', '')} "
        f"dbname={config['dbname']}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
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

                if not migrations:
                    print("No migrations found in package")
                    return

                # Apply pending migrations
                pending = 0
                for name, content in migrations:
                    if name in applied:
                        # Verify checksum
                        expected = compute_checksum(content)
                        if applied[name] != expected:
                            print(f"Warning: Checksum mismatch for {name}")
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
        print(f"Database error: {e}")
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
            db_url = (
                f"postgres://{config.get('user', 'postgres')}:"
                f"{config.get('password', '')}@"
                f"{config.get('host', 'localhost')}:"
                f"{config.get('port', 5432)}/"
                f"{config.get('dbname', 'stabilize')}"
            )
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
        except ImportError:
            print("Error: psycopg not installed")
            print("Install with: pip install stabilize[postgres]")
            sys.exit(1)
    else:
        print(f"Error: Unsupported database URL: {db_url}")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)

    print(f"Connecting to {db_url[:50]}...")
    run_monitor(
        store=store,
        queue=queue,
        app_filter=app_filter,
        refresh_interval=refresh_interval,
        status_filter=status_filter,
    )


def mg_status(db_url: str | None = None) -> None:
    """Show migration status."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    conninfo = (
        f"host={config['host']} port={config.get('port', 5432)} "
        f"user={config.get('user', 'postgres')} password={config.get('password', '')} "
        f"dbname={config['dbname']}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Check if tracking table exists
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
        print(f"Database error: {e}")
        sys.exit(1)
