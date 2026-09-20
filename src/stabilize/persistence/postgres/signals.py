"""Signal storage for the PostgreSQL workflow store."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

SIGNAL_TABLE = "workflow_signals"

GRANT_HINT = (
    f"GRANT SELECT, INSERT, UPDATE, DELETE ON {SIGNAL_TABLE} TO <runtime_role>; "
    "under row-level security the table also needs the same tenant policy as "
    "the other engine tables."
)


class PostgresSignalMixin:
    """Persistent signal buffering, guarded by a one-time usability check.

    `workflow_signals` arrived in 0.28.1 and is read on every stage completion.
    A deployment whose runtime role holds explicit per-table grants has no
    privilege on it, because nothing referenced it before. Claiming support on
    the strength of the backend class turns that into InsufficientPrivilege in
    the middle of a workflow, so support is established by using the table once
    and is withdrawn -- loudly -- when it cannot be used.
    """

    _signal_storage_usable: bool | None = None

    def _signal_storage_unusable(self, exc: Exception) -> None:
        if self._signal_storage_usable is not False:
            logger.warning(
                "Signal storage disabled: %s is unreachable (%s). Signals now "
                "buffer in stage_executions.context under '_buffered_signals' "
                "instead -- the pre-0.28.1 behaviour, which remains correct but "
                "reintroduces the unbounded growth issue 15 capped. IF %s IS "
                "PROTECTED BY ROW-LEVEL SECURITY AND stage_executions IS NOT, "
                "THIS MOVES SIGNAL PAYLOADS OUTSIDE THAT BOUNDARY. To enable "
                "storage: %s",
                SIGNAL_TABLE,
                exc,
                SIGNAL_TABLE,
                GRANT_HINT,
            )
        self._signal_storage_usable = False

    def supports_signal_storage(self) -> bool:
        if self._signal_storage_usable is not None:
            return self._signal_storage_usable

        try:
            with self._pool.connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    cur.execute(f"SELECT 1 FROM {SIGNAL_TABLE} WHERE FALSE")
                    cur.fetchall()
                conn.rollback()
        except Exception as exc:
            self._signal_storage_unusable(exc)
            return False

        self._signal_storage_usable = True
        return True

    def buffer_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str,
        signal_data: dict[str, Any] | None = None,
    ) -> int:
        if not self.supports_signal_storage():
            return 0
        try:
            with self._pool.connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {SIGNAL_TABLE} "
                        "(execution_id, stage_ref_id, signal_name, signal_data) "
                        "VALUES (%s, %s, %s, %s) RETURNING id",
                        (
                            execution_id,
                            stage_ref_id,
                            signal_name,
                            json.dumps(signal_data or {}, default=str),
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
        except Exception as exc:
            self._signal_storage_unusable(exc)
            return 0
        return int(row["id"] if isinstance(row, dict) else row[0]) if row else 0

    def consume_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Claim the oldest unconsumed signal atomically.

        FOR UPDATE SKIP LOCKED so two workers racing on the same suspended stage
        cannot both claim one signal; the UPDATE and the read are one statement
        so a crash between them cannot consume a signal without delivering it.
        """
        if not self.supports_signal_storage():
            return None
        clause = "AND signal_name = %s" if signal_name else ""
        params: tuple[Any, ...] = (
            (execution_id, stage_ref_id, signal_name)
            if signal_name
            else (execution_id, stage_ref_id)
        )
        try:
            with self._pool.connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {SIGNAL_TABLE} SET consumed = TRUE, consumed_at = NOW() "
                        f"WHERE id = (SELECT id FROM {SIGNAL_TABLE} "
                        "            WHERE execution_id = %s AND stage_ref_id = %s "
                        f"          AND consumed = FALSE {clause} "
                        "            ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) "
                        "RETURNING signal_name, signal_data",
                        params,
                    )
                    row = cur.fetchone()
                conn.commit()
        except Exception as exc:
            self._signal_storage_unusable(exc)
            return None
        if not row:
            return None
        if isinstance(row, dict):
            return {"signal_name": row["signal_name"], "signal_data": row["signal_data"] or {}}
        return {"signal_name": row[0], "signal_data": row[1] or {}}

    def pending_signal_count(self, execution_id: str, stage_ref_id: str) -> int:
        if not self.supports_signal_storage():
            return 0
        try:
            with self._pool.connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT count(*) AS n FROM {SIGNAL_TABLE} "
                        "WHERE execution_id = %s AND stage_ref_id = %s AND consumed = FALSE",
                        (execution_id, stage_ref_id),
                    )
                    row = cur.fetchone()
                conn.rollback()
        except Exception as exc:
            self._signal_storage_unusable(exc)
            return 0
        if not row:
            return 0
        return int(row["n"] if isinstance(row, dict) else row[0])

    def discard_signals(self, execution_id: str, stage_ref_id: str) -> int:
        if not self.supports_signal_storage():
            return 0
        try:
            with self._pool.connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {SIGNAL_TABLE} WHERE execution_id = %s "
                        "AND stage_ref_id = %s AND consumed = FALSE",
                        (execution_id, stage_ref_id),
                    )
                    dropped = cur.rowcount or 0
                conn.commit()
        except Exception as exc:
            self._signal_storage_unusable(exc)
            return 0
        return dropped
