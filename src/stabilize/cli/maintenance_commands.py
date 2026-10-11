"""CLI commands for bulk maintenance: processed-message retention and workflow purge."""

from __future__ import annotations

import sys
import time

PRUNE_PROCESSED_HELP = (
    "Delete processed_messages records older than --older-than-hours, in batches. "
    "Each batch is one transaction that deletes at most --batch-size rows by primary key, "
    "selected oldest first; on PostgreSQL the selected rows are locked FOR UPDATE SKIP LOCKED, "
    "so only those rows are locked, queue_messages is not touched, and concurrent "
    "mark-processed inserts are not blocked. Prints the number of rows removed."
)


def _require(db_url: str | None) -> str:
    if not db_url:
        print("Error: --db-url is required")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)
    return db_url


def prune_processed(db_url: str | None, older_than_hours: float, batch_size: int, max_rows: int | None) -> None:
    from stabilize.persistence.factory import create_repository

    store = create_repository(_require(db_url), create_tables=False)
    started = time.monotonic()
    try:
        removed = store.cleanup_old_processed_messages(
            max_age_hours=older_than_hours, batch_size=batch_size, max_rows=max_rows
        )
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
    print(
        f"Removed {removed} processed-message record(s) older than {older_than_hours} h "
        f"in batches of {batch_size} ({time.monotonic() - started:.1f} s)"
    )


def purge_workflows(
    db_url: str | None,
    completed_before_days: float,
    application: str | None,
    queue_table: str,
    batch_size: int,
    dry_run: bool,
) -> None:
    from stabilize.maintenance import purge_completed_workflows
    from stabilize.persistence.factory import create_queue, create_repository

    url = _require(db_url)
    cutoff_ms = int((time.time() - completed_before_days * 86400) * 1000)
    store = create_repository(url, create_tables=False)
    queue = None
    try:
        if dry_run:
            count = len(store.completed_workflow_ids(cutoff_ms, 10**9, application))
            print(f"{count} completed workflow(s) ended more than {completed_before_days} day(s) ago")
            print("Re-run without --dry-run to delete them")
            return
        queue = create_queue(url, table_name=queue_table)
        deleted = purge_completed_workflows(store, queue, cutoff_ms, batch_size=batch_size, application=application)
        print(f"Purged {deleted} completed workflow(s) ended more than {completed_before_days} day(s) ago")
    finally:
        for resource in (queue, store):
            close = getattr(resource, "close", None)
            if callable(close):
                close()
