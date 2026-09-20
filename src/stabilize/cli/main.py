"""Main CLI entry point for Stabilize."""

from __future__ import annotations

import argparse
import sys

from stabilize.cli.commands import mg_status, mg_up, monitor, prompt, prune_signals
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
    elif args.command == "prompt":
        prompt()
    elif args.command == "monitor":
        monitor(args.db_url, args.app, args.refresh, args.status)
    elif args.command == "prune-signals":
        prune_signals(args.db_url, args.include_active, args.dry_run, args.statuses)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
