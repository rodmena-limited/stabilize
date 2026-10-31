"""Extended operations for PostgreSQL persistence (pause, resume, cancel, deduplication)."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails
from stabilize.persistence.control_states import PAUSABLE
from stabilize.persistence.postgres.converters import paused_to_dict
from stabilize.persistence.signal_scope import signal_status_filter as _signal_status_filter
from stabilize.persistence.tenancy import refuse_foreign_row
from stabilize.queue.message_ids import owner_prefix

logger = logging.getLogger(__name__)


def pause_execution(engine: Any, execution_id: str, paused_by: str) -> None:
    """Pause an execution."""
    paused = PausedDetails(
        paused_by=paused_by,
        pause_time=int(time.time() * 1000),
    )

    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    paused = %(paused)s::jsonb
                WHERE id = %(id)s AND status = ANY(%(pausable)s) AND stabilize_tenant = %(tenant)s
                """,
                {
                    "id": execution_id,
                    "tenant": engine.tenant,
                    "pausable": [s.name for s in PAUSABLE],
                    "status": WorkflowStatus.PAUSED.name,
                    "paused": json.dumps(paused_to_dict(paused)),
                },
            )
        conn.commit()


def resume_execution(engine: Any, execution_id: str) -> None:
    """Resume a paused execution.

    Uses atomic UPDATE with status check to prevent race conditions.
    """
    current_time = int(time.time() * 1000)

    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    paused = CASE
                        WHEN paused IS NOT NULL THEN
                            jsonb_set(
                                jsonb_set(paused, '{resume_time}', to_jsonb(%(resume_time)s)),
                                '{paused_ms}',
                                to_jsonb(%(resume_time)s - (paused->>'pause_time')::bigint)
                            )
                        ELSE NULL
                    END
                WHERE id = %(id)s AND status = 'PAUSED' AND stabilize_tenant = %(tenant)s
                """,
                {
                    "id": execution_id,
                    "tenant": engine.tenant,
                    "status": WorkflowStatus.RUNNING.name,
                    "resume_time": current_time,
                },
            )
            if cur.rowcount == 0:
                logger.warning(
                    "Resume had no effect for execution %s - not in PAUSED status",
                    execution_id,
                )
        conn.commit()


def cancel_execution(engine: Any, execution_id: str, canceled_by: str, reason: str) -> None:
    """Cancel an execution."""
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    is_canceled = TRUE,
                    canceled_by = %(canceled_by)s,
                    cancellation_reason = %(reason)s
                WHERE id = %(id)s AND NOT (status = ANY(%(complete)s)) AND stabilize_tenant = %(tenant)s
                """,
                {
                    "id": execution_id,
                    "tenant": engine.tenant,
                    "complete": [s.name for s in WorkflowStatus if s.is_complete and s is not WorkflowStatus.CANCELED],
                    "canceled_by": canceled_by,
                    "reason": reason,
                },
            )
        conn.commit()


def is_message_processed(engine: Any, message_id: str) -> bool:
    """Check if a message has already been processed."""
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_messages WHERE message_id = %(message_id)s AND stabilize_tenant = %(tenant)s",
                {"message_id": message_id, "tenant": engine.tenant},
            )
            return cur.fetchone() is not None


def get_processed_message_ids(engine: Any, limit: int | None = None) -> list[str]:
    """Return processed message IDs, for hydrating an in-memory dedup cache."""
    with engine.connection() as conn:
        with conn.cursor() as cur:
            if limit is not None:
                cur.execute(
                    "SELECT message_id FROM processed_messages WHERE stabilize_tenant = %(tenant)s LIMIT %(limit)s",
                    {"limit": limit, "tenant": engine.tenant},
                )
            else:
                cur.execute(
                    "SELECT message_id FROM processed_messages WHERE stabilize_tenant = %(tenant)s",
                    {"tenant": engine.tenant},
                )
            # Pool uses dict_row: rows are dict-like, access by column name.
            return [row["message_id"] for row in cur.fetchall()]


def mark_message_processed(
    engine: Any,
    message_id: str,
    handler_type: str | None = None,
    execution_id: str | None = None,
) -> None:
    """Mark a message as successfully processed."""
    claim_message(engine, message_id, handler_type, execution_id)


def claim_message(
    engine: Any,
    message_id: str,
    handler_type: str | None = None,
    execution_id: str | None = None,
) -> bool:
    """Insert the processed-message row; True only when this call inserted it."""
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_messages (
                    message_id, processed_at, handler_type, execution_id, stabilize_tenant
                ) VALUES (
                    %(message_id)s, NOW(), %(handler_type)s, %(execution_id)s, %(tenant)s
                )
                ON CONFLICT (message_id) DO NOTHING
                """,
                {
                    "message_id": message_id,
                    "handler_type": handler_type,
                    "execution_id": execution_id,
                    "tenant": engine.tenant,
                },
            )
            inserted = bool(cur.rowcount == 1)
            if not inserted:
                cur.execute(
                    "SELECT stabilize_tenant AS owner FROM processed_messages WHERE message_id = %(message_id)s",
                    {"message_id": message_id},
                )
                owner = cur.fetchone()
                refuse_foreign_row("processed_messages", message_id, owner["owner"] if owner else None, engine.tenant)
        conn.commit()
    return inserted


PG_RETENTION_BATCH = 10_000

_DELETE_STALE_BATCH = """
    WITH victims AS MATERIALIZED (
        SELECT message_id FROM processed_messages
        WHERE processed_at < NOW() - %(hours)s * INTERVAL '1 hour' AND stabilize_tenant = %(tenant)s
        ORDER BY processed_at, message_id
        LIMIT %(n)s
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM processed_messages p
    USING victims v
    WHERE p.message_id = v.message_id AND p.stabilize_tenant = %(tenant)s
"""


def cleanup_old_processed_messages(
    engine: Any,
    max_age_hours: float = 24.0,
    batch_size: int | None = None,
    max_rows: int | None = None,
) -> int:
    """Delete processed_messages rows older than max_age_hours, one transaction per batch."""
    batch = int(batch_size or PG_RETENTION_BATCH)
    total = 0
    while max_rows is None or total < max_rows:
        n = batch if max_rows is None else min(batch, max_rows - total)
        with engine.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_DELETE_STALE_BATCH, {"hours": float(max_age_hours), "n": n, "tenant": engine.tenant})
                deleted = int(cur.rowcount or 0)
            conn.commit()
        total += deleted
        if deleted < n:
            break
    return total


def _field(row: Any, name: str, index: int) -> Any:
    return row[name] if isinstance(row, dict) else row[index]


KEPT_LIMIT = 5_000


def cleanup_processed_not_in_queue(
    engine: Any,
    max_age_hours: float,
    batch_size: int | None,
    max_rows: int | None,
    table_name: str,
    live: Callable[[list[str]], set[str]],
) -> int:
    """Delete stale processed_messages rows of table_name's messages that live() reports gone from the queue."""
    prefix = owner_prefix(table_name)
    owner = "message_id ~ '^[0-9]+$'" if prefix is None else "left(message_id, %(plen)s) = %(prefix)s"
    batch = int(batch_size or PG_RETENTION_BATCH)
    stale = "processed_at < NOW() - %(hours)s * INTERVAL '1 hour' AND stabilize_tenant = %(tenant)s"
    total = 0
    kept: list[str] = []
    while max_rows is None or total < max_rows:
        params: dict[str, Any] = {"hours": float(max_age_hours), "n": batch, "kept": kept, "tenant": engine.tenant}
        params.update(prefix=prefix, plen=len(prefix or ""))
        with engine.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT message_id FROM processed_messages WHERE {stale} AND {owner} "
                    "AND message_id <> ALL(%(kept)s) ORDER BY processed_at LIMIT %(n)s",
                    params,
                )
                rows = cur.fetchall()
            conn.commit()
        if not rows:
            break
        keys = [str(_field(r, "message_id", 0)) for r in rows]
        still_queued = live(keys)
        gone = [k for k in keys if k not in still_queued]
        kept.extend(k for k in keys if k in still_queued)
        if max_rows is not None:
            gone = gone[: max_rows - total]
        if gone:
            with engine.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM processed_messages WHERE message_id = ANY(%(ids)s) AND {stale}",
                        {"ids": gone, "hours": float(max_age_hours), "tenant": engine.tenant},
                    )
                    total += int(cur.rowcount or 0)
                conn.commit()
        if len(rows) < batch or len(kept) > KEPT_LIMIT:
            break
    return total


def cleanup_completed_stage_claims(engine: Any) -> int:
    """Delete stage claims belonging to executions in terminal states.

    Claims of live executions are never touched: removing one would
    resurrect the mutex/deferred-choice race the claim exists to prevent.
    """
    from stabilize.models.status import WorkflowStatus

    terminal = [s.name for s in WorkflowStatus if s.is_complete]
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM stage_claims
                WHERE stabilize_tenant = %(tenant)s AND execution_id IN (
                    SELECT id FROM pipeline_executions
                    WHERE status = ANY(%(statuses)s) AND stabilize_tenant = %(tenant)s
                )
                """,
                {"statuses": terminal, "tenant": engine.tenant},
            )
            deleted: int = cur.rowcount or 0
        conn.commit()
        return deleted


def cleanup_buffered_signals(
    engine: Any,
    only_complete: bool = True,
    statuses: list[str] | None = None,
) -> int:
    """Strip _buffered_signals from stage contexts that can never consume them."""
    sql = """
        UPDATE stage_executions
        SET context = context - '_buffered_signals'
        WHERE context ? '_buffered_signals' AND stabilize_tenant = %(tenant)s
    """
    selected = _signal_status_filter(only_complete, statuses)
    params: dict[str, Any] = {"tenant": engine.tenant}
    if selected is not None:
        sql += " AND status = ANY(%(statuses)s)"
        params["statuses"] = selected

    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount
        conn.commit()
    return int(count or 0)


def count_buffered_signal_stages(
    engine: Any,
    only_complete: bool = True,
    statuses: list[str] | None = None,
) -> int:
    """Count stage rows carrying a _buffered_signals buffer."""
    sql = (
        "SELECT COUNT(*) AS n FROM stage_executions "
        "WHERE context ? '_buffered_signals' AND stabilize_tenant = %(tenant)s"
    )
    selected = _signal_status_filter(only_complete, statuses)
    params: dict[str, Any] = {"tenant": engine.tenant}
    if selected is not None:
        sql += " AND status = ANY(%(statuses)s)"
        params["statuses"] = selected

    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    if row is None:
        return 0
    return int(row["n"] if isinstance(row, dict) else row[0])
