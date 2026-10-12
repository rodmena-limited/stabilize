"""CLI commands for bulk maintenance: processed-message retention and workflow purge."""

from __future__ import annotations

import sys
import time

PRUNE_PROCESSED_HELP = (
    "Delete processed_messages records older than --older-than-hours that belong to the messages of "
    "each --queue-table (default queue_messages). Each batch selects at most --batch-size such records, "
    "oldest first, asks the queue table which of their messages are still present, and deletes the "
    "others by primary key in one transaction. A record whose message is still in the queue table is "
    "kept, and records of queue tables not named are not touched. No queue row is changed. "
    "The queue tables are read from --queue-db-url when given, otherwise from --db-url, so "
    "--queue-db-url is required when the queue is in another database: a queue table of the "
    "same name in --db-url that the processors do not use reports every message as gone. If a "
    "queue table cannot be read the command fails and deletes nothing for that table. "
    "Prints the number of rows removed."
)


def _require(db_url: str | None) -> str:
    if not db_url:
        print("Error: --db-url is required")
        print("Use sqlite:///path or postgres://...")
        sys.exit(1)
    return db_url


def prune_processed(
    db_url: str | None,
    older_than_hours: float,
    batch_size: int,
    max_rows: int | None,
    queue_tables: list[str] | None = None,
    queue_db_url: str | None = None,
) -> None:
    from stabilize.persistence.factory import create_queue, create_repository

    url = _require(db_url)
    store = create_repository(url, create_tables=False)
    started = time.monotonic()
    removed = 0
    tables = queue_tables or ["queue_messages"]
    try:
        for table in tables:
            queue = create_queue(queue_db_url or url, table_name=table)
            try:
                left = None if max_rows is None else max_rows - removed
                if left is not None and left <= 0:
                    break
                removed += store.cleanup_old_processed_messages(
                    max_age_hours=older_than_hours,
                    batch_size=batch_size,
                    max_rows=left,
                    table_name=table,
                    live=queue.live_delivery_keys,
                )
            finally:
                close_queue = getattr(queue, "close", None)
                if callable(close_queue):
                    close_queue()
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
    print(
        f"Removed {removed} processed-message record(s) older than {older_than_hours} h "
        f"for queue table(s) {', '.join(tables)}, in batches of {batch_size} "
        f"({time.monotonic() - started:.1f} s)"
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
