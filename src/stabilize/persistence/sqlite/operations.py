"""Extended operations for SQLite persistence (pause, resume, cancel, deduplication)."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails
from stabilize.persistence.signal_scope import signal_status_filter as _signal_status_filter
from stabilize.persistence.sqlite.converters import paused_to_dict
from stabilize.queue.message_ids import owner_prefix

logger = logging.getLogger(__name__)


def pause_execution(
    conn: sqlite3.Connection,
    execution_id: str,
    paused_by: str,
) -> None:
    """Pause an execution."""
    paused = PausedDetails(
        paused_by=paused_by,
        pause_time=int(time.time() * 1000),
    )

    conn.execute(
        """
        UPDATE pipeline_executions SET
            status = :status,
            paused = :paused
        WHERE id = :id
        """,
        {
            "id": execution_id,
            "status": WorkflowStatus.PAUSED.name,
            "paused": json.dumps(paused_to_dict(paused)),
        },
    )
    conn.commit()


def resume_execution(
    conn: sqlite3.Connection,
    execution_id: str,
) -> None:
    """Resume a paused execution.

    Uses atomic UPDATE with status check to prevent race conditions.
    Calculates resume_time and paused_ms atomically in the UPDATE to avoid
    race conditions between retrieve and update.
    """
    current_time = int(time.time() * 1000)

    # Atomic update: calculate resume_time and paused_ms in the UPDATE itself
    # This prevents race conditions where paused details could change between read and write
    cursor = conn.execute(
        """
        UPDATE pipeline_executions SET
            status = :status,
            paused = CASE
                WHEN paused IS NOT NULL THEN
                    json_set(
                        json_set(paused, '$.resume_time', :resume_time),
                        '$.paused_ms',
                        :resume_time - CAST(json_extract(paused, '$.pause_time') AS INTEGER)
                    )
                ELSE NULL
            END
        WHERE id = :id AND status = 'PAUSED'
        """,
        {
            "id": execution_id,
            "status": WorkflowStatus.RUNNING.name,
            "resume_time": current_time,
        },
    )
    conn.commit()
    if cursor.rowcount == 0:
        logger.warning(
            "Resume had no effect for execution %s - not in PAUSED status",
            execution_id,
        )


def cancel_execution(
    conn: sqlite3.Connection,
    execution_id: str,
    canceled_by: str,
    reason: str,
) -> None:
    """Cancel an execution."""
    conn.execute(
        """
        UPDATE pipeline_executions SET
            is_canceled = 1,
            canceled_by = :canceled_by,
            cancellation_reason = :reason
        WHERE id = :id
        """,
        {
            "id": execution_id,
            "canceled_by": canceled_by,
            "reason": reason,
        },
    )
    conn.commit()


def is_message_processed(
    conn: sqlite3.Connection,
    message_id: str,
) -> bool:
    """Check if a message has already been processed."""
    result = conn.execute(
        "SELECT 1 FROM processed_messages WHERE message_id = :message_id",
        {"message_id": message_id},
    ).fetchone()
    return result is not None


def get_processed_message_ids(
    conn: sqlite3.Connection,
    limit: int | None = None,
) -> list[str]:
    """Return processed message IDs, for hydrating an in-memory dedup cache."""
    if limit is not None:
        rows = conn.execute(
            "SELECT message_id FROM processed_messages LIMIT :limit",
            {"limit": limit},
        ).fetchall()
    else:
        rows = conn.execute("SELECT message_id FROM processed_messages").fetchall()
    return [row[0] for row in rows]


def mark_message_processed(
    conn: sqlite3.Connection,
    message_id: str,
    handler_type: str | None = None,
    execution_id: str | None = None,
) -> None:
    """Mark a message as successfully processed."""
    conn.execute(
        """
        INSERT OR IGNORE INTO processed_messages (
            message_id, processed_at, handler_type, execution_id
        ) VALUES (
            :message_id, :processed_at, :handler_type, :execution_id
        )
        """,
        {
            "message_id": message_id,
            "processed_at": datetime.now(UTC).isoformat(),
            "handler_type": handler_type,
            "execution_id": execution_id,
        },
    )
    conn.commit()


SQLITE_RETENTION_BATCH = 2_000

_DELETE_STALE_BATCH = (
    "DELETE FROM processed_messages WHERE rowid IN ("
    "SELECT rowid FROM processed_messages "
    "WHERE processed_at < :cutoff AND julianday(processed_at) < julianday(:cutoff) "
    "ORDER BY processed_at LIMIT :n"
    ") AND processed_at < :cutoff AND julianday(processed_at) < julianday(:cutoff)"
)


def cleanup_old_processed_messages(
    conn: sqlite3.Connection,
    max_age_hours: float = 24.0,
    batch_size: int | None = None,
    max_rows: int | None = None,
) -> int:
    """Delete processed_messages rows older than max_age_hours, one transaction per batch."""
    cutoff = (datetime.now(UTC) - timedelta(hours=max_age_hours)).isoformat()
    batch = int(batch_size or SQLITE_RETENTION_BATCH)
    total = 0
    while max_rows is None or total < max_rows:
        n = batch if max_rows is None else min(batch, max_rows - total)
        cursor = conn.execute(_DELETE_STALE_BATCH, {"cutoff": cutoff, "n": n})
        conn.commit()
        deleted = int(cursor.rowcount or 0)
        total += deleted
        if deleted < n:
            break
    return total


def cleanup_processed_not_in_queue(
    conn: sqlite3.Connection,
    max_age_hours: float,
    batch_size: int | None,
    max_rows: int | None,
    table_name: str,
    live: Callable[[list[str]], set[str]],
) -> int:
    """Delete stale processed_messages rows of table_name's messages that live() reports gone from the queue."""
    cutoff = (datetime.now(UTC) - timedelta(hours=max_age_hours)).isoformat()
    prefix = owner_prefix(table_name)
    if prefix is None:
        owner = "message_id <> '' AND message_id NOT GLOB '*[^0-9]*'"
    else:
        owner = "substr(message_id, 1, :plen) = :prefix"
    stale = "processed_at < :cutoff AND julianday(processed_at) < julianday(:cutoff)"
    batch = int(batch_size or SQLITE_RETENTION_BATCH)
    total = 0
    after: tuple[str, str] | None = None
    while max_rows is None or total < max_rows:
        params: dict[str, Any] = {"cutoff": cutoff, "n": batch, "prefix": prefix, "plen": len(prefix or "")}
        page = ""
        if after is not None:
            page = " AND (processed_at, message_id) > (:after_at, :after_id)"
            params.update(after_at=after[0], after_id=after[1])
        rows = conn.execute(
            f"SELECT message_id, processed_at FROM processed_messages WHERE {stale} AND {owner}{page} "
            "ORDER BY processed_at, message_id LIMIT :n",
            params,
        ).fetchall()
        conn.commit()
        if not rows:
            break
        keys = [str(r[0]) for r in rows]
        after = (str(rows[-1][1]), keys[-1])
        still_queued = live(keys)
        gone = [k for k in keys if k not in still_queued]
        if max_rows is not None:
            gone = gone[: max_rows - total]
        for i in range(0, len(gone), 500):
            chunk = gone[i : i + 500]
            marks = ",".join(f":k{j}" for j in range(len(chunk)))
            args: dict[str, Any] = {f"k{j}": k for j, k in enumerate(chunk)}
            args["cutoff"] = cutoff
            cursor = conn.execute(f"DELETE FROM processed_messages WHERE message_id IN ({marks}) AND {stale}", args)
            total += int(cursor.rowcount or 0)
        conn.commit()
        if len(rows) < batch:
            break
    return total


def cleanup_completed_stage_claims(conn: sqlite3.Connection) -> int:
    """Delete stage claims belonging to executions in terminal states.

    Claims of live executions are never touched: removing one would
    resurrect the mutex/deferred-choice race the claim exists to prevent.
    """
    from stabilize.models.status import WorkflowStatus

    terminal = [s.name for s in WorkflowStatus if s.is_complete]
    placeholders = ", ".join("?" for _ in terminal)
    cursor = conn.execute(
        f"""
        DELETE FROM stage_claims
        WHERE execution_id IN (
            SELECT id FROM pipeline_executions
            WHERE status IN ({placeholders})
        )
        """,
        terminal,
    )
    conn.commit()
    return cursor.rowcount


def cleanup_buffered_signals(
    conn: sqlite3.Connection,
    only_complete: bool = True,
    statuses: list[str] | None = None,
) -> int:
    """Strip _buffered_signals from stage contexts that can never consume them."""
    sql = """
        UPDATE stage_executions
        SET context = json_remove(context, '$._buffered_signals')
        WHERE json_extract(context, '$._buffered_signals') IS NOT NULL
    """
    selected = _signal_status_filter(only_complete, statuses)
    params: list[str] = []
    if selected is not None:
        sql += f" AND status IN ({', '.join('?' for _ in selected)})"
        params = selected

    cursor = conn.execute(sql, params)
    conn.commit()
    return cursor.rowcount or 0


def count_buffered_signal_stages(
    conn: sqlite3.Connection,
    only_complete: bool = True,
    statuses: list[str] | None = None,
) -> int:
    """Count stage rows carrying a _buffered_signals buffer."""
    sql = """
        SELECT COUNT(*) FROM stage_executions
        WHERE json_extract(context, '$._buffered_signals') IS NOT NULL
    """
    selected = _signal_status_filter(only_complete, statuses)
    params: list[str] = []
    if selected is not None:
        sql += f" AND status IN ({', '.join('?' for _ in selected)})"
        params = selected

    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0
