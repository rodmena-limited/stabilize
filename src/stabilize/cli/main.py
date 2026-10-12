"""Main CLI entry point for Stabilize."""

from __future__ import annotations

import argparse
import sys

from stabilize.cli.commands import mg_check_grants, mg_status, mg_up, monitor, prompt, prune_signals
from stabilize.cli.migrations import MigrationsNotFoundError


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="stabilize",
        description="Stabilize - Workflow Engine CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # mg-up command
    up_parser = subparsers.add_parser("mg-up", help="Apply pending PostgreSQL migrations")
    up_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # mg-status command
    status_parser = subparsers.add_parser("mg-status", help="Show migration status")
    status_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    grants_parser = subparsers.add_parser(
        "mg-check-grants",
        help="Report which engine tables a database role cannot use (exit 1 if any)",
    )
    grants_parser.add_argument("--role", required=True, help="Runtime database role to check")
    grants_parser.add_argument(
        "--db-url",
        help="Database URL of a user that can read the catalogue (postgres://user:pass@host:port/dbname)",
    )

    # prompt command
    subparsers.add_parser(
        "prompt",
        help="Output comprehensive documentation for pipeline code generation",
    )

    # monitor command
    monitor_parser = subparsers.add_parser(
        "monitor",
        help="Real-time workflow monitoring dashboard (htop-like)",
    )
    monitor_parser.add_argument(
        "--app",
        help="Filter by application name",
    )
    monitor_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://... or sqlite:///...)",
    )
    monitor_parser.add_argument(
        "--refresh",
        type=int,
        default=2,
        help="Refresh interval in seconds (default: 2)",
    )
    monitor_parser.add_argument(
        "--status",
        choices=["all", "running", "failed", "recent"],
        default="all",
        help="Filter workflows by status (default: all)",
    )

    # prune-signals command
    prune_parser = subparsers.add_parser(
        "prune-signals",
        help="Strip unconsumable WCP-24 persistent-signal buffers from stage contexts",
    )
    prune_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://... or sqlite:///...)",
    )
    prune_parser.add_argument(
        "--include-active",
        action="store_true",
        help="Also strip buffers from stages that are not complete",
    )
    prune_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many stage rows carry a buffer without modifying them",
    )
    prune_parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        metavar="STATUS",
        help="Restrict to stages in this status (repeatable, e.g. --status SUCCEEDED)",
    )

    from stabilize.cli.maintenance_commands import PRUNE_PROCESSED_HELP, prune_processed, purge_workflows

    processed_parser = subparsers.add_parser(
        "prune-processed", help="Delete old processed-message records in batches", description=PRUNE_PROCESSED_HELP
    )
    processed_parser.add_argument("--db-url", help="Database URL (postgres://... or sqlite:///...)")
    processed_parser.add_argument("--older-than-hours", type=float, default=168.0, help="Age cutoff (default 168)")
    processed_parser.add_argument("--batch-size", type=int, default=10_000, help="Rows per transaction (default 10000)")
    processed_parser.add_argument("--max-rows", type=int, default=None, help="Stop after this many rows (no limit)")
    processed_parser.add_argument(
        "--queue-table",
        action="append",
        default=None,
        help="Queue table whose records are pruned (repeatable; default queue_messages)",
    )
    processed_parser.add_argument(
        "--queue-db-url", default=None, help="Database holding the queue tables, when it is not --db-url"
    )

    purge_parser = subparsers.add_parser(
        "purge-workflows",
        help="Delete completed workflows, with their owned rows and dead-lettered messages",
        description=(
            "Delete workflows whose status is complete and whose end_time is older than "
            "--completed-before-days, through the store's delete(), in batches. "
            "Workflows that are not complete are never selected; event-store rows are kept."
        ),
    )
    purge_parser.add_argument("--db-url", help="Database URL (postgres://... or sqlite:///...)")
    purge_parser.add_argument("--completed-before-days", type=float, required=True)
    purge_parser.add_argument("--application", default=None, help="Only this application")
    purge_parser.add_argument("--queue-table", default="queue_messages", help="Queue table whose DLQ is purged too")
    purge_parser.add_argument("--batch-size", type=int, default=500)
    purge_parser.add_argument("--dry-run", action="store_true", help="Count without deleting")

    args = parser.parse_args()

    if args.command == "mg-up":
        try:
            mg_up(args.db_url)
        except MigrationsNotFoundError as exc:
            print(exc, file=sys.stderr)
            sys.exit(1)
    elif args.command == "mg-status":
        try:
            mg_status(args.db_url)
        except MigrationsNotFoundError as exc:
            print(exc, file=sys.stderr)
            sys.exit(1)
    elif args.command == "mg-check-grants":
        try:
            mg_check_grants(args.role, args.db_url)
        except MigrationsNotFoundError as exc:
            print(exc, file=sys.stderr)
            sys.exit(2)
    elif args.command == "prompt":
        prompt()
    elif args.command == "monitor":
        monitor(args.db_url, args.app, args.refresh, args.status)
    elif args.command == "prune-signals":
        prune_signals(args.db_url, args.include_active, args.dry_run, args.statuses)
    elif args.command == "prune-processed":
        prune_processed(
            args.db_url,
            args.older_than_hours,
            args.batch_size,
            args.max_rows,
            args.queue_table or ["queue_messages"],
            args.queue_db_url,
        )
    elif args.command == "purge-workflows":
        purge_workflows(
            args.db_url,
            args.completed_before_days,
            args.application,
            args.queue_table,
            args.batch_size,
            args.dry_run,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
