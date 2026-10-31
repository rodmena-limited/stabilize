"""CLI commands for bulk maintenance: processed-message retention and workflow purge."""

from __future__ import annotations

import sys
import time
from typing import Any

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
    tenant: str | None = None,
    all_tenants: bool = False,
) -> None:
    from stabilize.cli.commands import resolve_cli_tenant, schema_mode, tenants_in

    url = _require(db_url)
    watched = resolve_cli_tenant(schema_mode(url), tenant, all_tenants)
    targets = tenants_in(url) if watched is None else [watched]
    if not targets:
        print("No tenant has rows; nothing to do")
        return
    for one in targets:
        _prune_processed_for(url, one, older_than_hours, batch_size, max_rows, queue_tables, queue_db_url)


def _prune_processed_for(
    url: str,
    tenant: str,
    older_than_hours: float,
    batch_size: int,
    max_rows: int | None,
    queue_tables: list[str] | None,
    queue_db_url: str | None,
) -> None:
    from stabilize.persistence.factory import create_queue, create_repository

    store = create_repository(url, create_tables=False, tenant=tenant)
    started = time.monotonic()
    removed = 0
    tables = queue_tables or ["queue_messages"]
    try:
        for table in tables:
            queue = create_queue(queue_db_url or url, table_name=table, tenant=tenant)
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
    label = f" for tenant {tenant!r}" if tenant else ""
    print(
        f"Removed {removed} processed-message record(s) older than {older_than_hours} h "
        f"for queue table(s) {', '.join(tables)}{label}, in batches of {batch_size} "
        f"({time.monotonic() - started:.1f} s)"
    )


def purge_workflows(
    db_url: str | None,
    completed_before_days: float,
    application: str | None,
    queue_table: str,
    batch_size: int,
    dry_run: bool,
    tenant: str | None = None,
    all_tenants: bool = False,
) -> None:
    from stabilize.cli.commands import resolve_cli_tenant, schema_mode, tenants_in

    url = _require(db_url)
    watched = resolve_cli_tenant(schema_mode(url), tenant, all_tenants)
    targets = tenants_in(url) if watched is None else [watched]
    if not targets:
        print("No tenant has rows; nothing to do")
        return
    for one in targets:
        _purge_workflows_for(url, one, completed_before_days, application, queue_table, batch_size, dry_run)


def _purge_workflows_for(
    url: str,
    tenant: str,
    completed_before_days: float,
    application: str | None,
    queue_table: str,
    batch_size: int,
    dry_run: bool,
) -> None:
    from stabilize.maintenance import purge_completed_workflows
    from stabilize.persistence.factory import create_queue, create_repository

    cutoff_ms = int((time.time() - completed_before_days * 86400) * 1000)
    store = create_repository(url, create_tables=False, tenant=tenant)
    queue = None
    try:
        if dry_run:
            count = len(store.completed_workflow_ids(cutoff_ms, 10**9, application))
            print(f"{count} completed workflow(s) ended more than {completed_before_days} day(s) ago")
            print("Re-run without --dry-run to delete them")
            return
        queue = create_queue(url, table_name=queue_table, tenant=tenant)
        deleted = purge_completed_workflows(store, queue, cutoff_ms, batch_size=batch_size, application=application)
        print(f"Purged {deleted} completed workflow(s) ended more than {completed_before_days} day(s) ago")
    finally:
        for resource in (queue, store):
            close = getattr(resource, "close", None)
            if callable(close):
                close()


PURGE_EVENTS_HELP = (
    "Delete event-store rows below a sequence or before a time, in batches. Refuses while an enabled "
    "durable subscription still has undelivered events inside the bound, unless --ignore-subscriptions is given."
)


def purge_events(
    db_url: str | None,
    before_sequence: int | None,
    older_than_days: float | None,
    ignore_subscriptions: bool,
    dry_run: bool,
    batch_size: int,
    tenant: str | None = None,
    all_tenants: bool = False,
) -> None:
    from stabilize.cli.commands import resolve_cli_tenant, schema_mode, tenants_in

    if before_sequence is None and older_than_days is None:
        raise SystemExit("purge-events needs --before-sequence or --older-than-days")
    url = _require(db_url)
    watched = resolve_cli_tenant(schema_mode(url), tenant, all_tenants)
    targets = tenants_in(url) if watched is None else [watched]
    if not targets:
        print("No tenant has rows; nothing to do")
        return
    before = None
    if older_than_days is not None:
        from datetime import UTC, datetime, timedelta

        before = datetime.now(UTC) - timedelta(days=older_than_days)
    for one in targets:
        _purge_events_for(url, one, before_sequence, before, ignore_subscriptions, dry_run, batch_size)


def _event_store(url: str, tenant: str) -> Any:
    if url.startswith("sqlite"):
        from stabilize.events.store.sqlite.store import SqliteEventStore

        return SqliteEventStore(url, create_tables=False, tenant=tenant)
    from stabilize.events.store.postgres.store import PostgresEventStore

    return PostgresEventStore(url, create_tables=False, tenant=tenant)


def _purge_events_for(
    url: str,
    tenant: str,
    before_sequence: int | None,
    before: Any,
    ignore_subscriptions: bool,
    dry_run: bool,
    batch_size: int,
) -> None:
    from stabilize.events.store.retention import SubscriptionBehindError, describe_bound

    store = _event_store(url, tenant)
    label = f" for tenant {tenant}" if tenant else ""
    try:
        try:
            count = store.purge_events(
                before_sequence=before_sequence,
                before=before,
                ignore_subscriptions=ignore_subscriptions,
                batch_size=batch_size,
                dry_run=dry_run,
            )
        except SubscriptionBehindError as exc:
            raise SystemExit(f"Refused{label}: {exc}") from exc
        bound = describe_bound(before_sequence, before)
        if dry_run:
            print(f"{count} event(s) {bound}{label}")
            print("Re-run without --dry-run to delete them")
        else:
            print(f"Purged {count} event(s) {bound}{label}")
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
