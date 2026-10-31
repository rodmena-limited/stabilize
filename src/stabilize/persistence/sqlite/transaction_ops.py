"""The SQLite transaction's row operations: queued messages, claims, locks, admission and signals."""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class SqliteTransactionOps:
    _conn: sqlite3.Connection
    _tenant: str
    _queue_table: str

    def purge_stage_messages(self, stage_id: str, *, except_message_id: str | None = None) -> int:
        keep = _queue_row_id(except_message_id)
        cursor = self._conn.execute(
            f"DELETE FROM {self._queue_table} WHERE json_extract(payload, '$.stage_id') = :stage_id "
            "AND stabilize_tenant = :tenant AND (:keep IS NULL OR id <> :keep)",
            {"stage_id": stage_id, "tenant": self._tenant, "keep": keep},
        )
        return int(cursor.rowcount or 0)

    def release_claims(self, execution_id: str, stage_id: str) -> int:
        cursor = self._conn.execute(
            "DELETE FROM stage_claims WHERE execution_id = :execution_id AND stage_id = :stage_id "
            "AND stabilize_tenant = :tenant",
            {"execution_id": execution_id, "stage_id": stage_id, "tenant": self._tenant},
        )
        return int(cursor.rowcount or 0)

    def count_claims(self, execution_id: str, prefix: str) -> int:
        row = self._conn.execute(
            "SELECT count(*) FROM stage_claims WHERE execution_id = :execution_id "
            "AND substr(claim_key, 1, :length) = :prefix AND stabilize_tenant = :tenant",
            {"execution_id": execution_id, "length": len(prefix), "prefix": prefix, "tenant": self._tenant},
        ).fetchone()
        return int(row[0]) if row else 0

    def claim_owner(self, execution_id: str, claim_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT stage_id FROM stage_claims WHERE execution_id = :execution_id AND claim_key = :claim_key "
            "AND stabilize_tenant = :tenant",
            {"execution_id": execution_id, "claim_key": claim_key, "tenant": self._tenant},
        ).fetchone()
        return str(row[0]) if row else None

    def remove_stage(self, stage_id: str) -> None:
        self.purge_stage_messages(stage_id)
        params = {"stage_id": stage_id, "tenant": self._tenant}
        self._conn.execute(
            "DELETE FROM task_executions WHERE stage_id = :stage_id AND stabilize_tenant = :tenant", params
        )
        self._conn.execute("DELETE FROM stage_claims WHERE stage_id = :stage_id AND stabilize_tenant = :tenant", params)
        self._conn.execute("DELETE FROM stage_executions WHERE id = :stage_id AND stabilize_tenant = :tenant", params)

    def try_lock_recovery(self, key: str) -> bool:
        if not self._conn.in_transaction:
            self._conn.execute("BEGIN IMMEDIATE")
        return True

    def lock_admission(self, key: str) -> None:
        if self._conn.in_transaction:
            raise RuntimeError("lock_admission must be the first statement of its transaction")
        self._conn.execute("BEGIN IMMEDIATE")

    def count_running(self, pipeline_config_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pipeline_executions "
            "WHERE pipeline_config_id = ? AND status = 'RUNNING' AND stabilize_tenant = ?",
            (pipeline_config_id, self._tenant),
        ).fetchone()
        return int(row[0])

    def buffered_ids(self, pipeline_config_id: str, queue_table: str | None = None) -> list[str]:
        scope = ""
        params: list[Any] = [pipeline_config_id, self._tenant]
        if queue_table is not None:
            scope = " AND (queue_table = ? OR (? = 1 AND queue_table IS NULL))"
            params.extend([queue_table, 1 if queue_table == "queue_messages" else 0])
        rows = self._conn.execute(
            "SELECT id FROM pipeline_executions "
            f"WHERE pipeline_config_id = ? AND status = 'BUFFERED' AND stabilize_tenant = ?{scope} ORDER BY id",
            params,
        ).fetchall()
        return [r[0] for r in rows]

    def workflow_status(self, workflow_id: str) -> tuple[str | None, bool]:
        row = self._conn.execute(
            "SELECT status, is_canceled FROM pipeline_executions WHERE id = ? AND stabilize_tenant = ?",
            (workflow_id, self._tenant),
        ).fetchone()
        return (None, False) if row is None else (str(row[0]), bool(row[1]))

    def paused_stage_ids(self, workflow_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT id FROM stage_executions "
            "WHERE execution_id = ? AND status = 'PAUSED' AND stabilize_tenant = ? ORDER BY id",
            (workflow_id, self._tenant),
        ).fetchall()
        return [str(r[0]) for r in rows]

    def consume_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str | None = None,
        *,
        row_id: int | None = None,
    ) -> dict[str, Any] | None:
        from stabilize.persistence.sqlite.signals import consume_signal

        signal = consume_signal(self._conn, execution_id, stage_ref_id, signal_name, tenant=self._tenant, row_id=row_id)
        if signal is None:
            return None
        return {"signal_name": signal.signal_name, "signal_data": signal.signal_data}

    def buffer_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str,
        signal_data: dict[str, Any] | None = None,
        *,
        consumed: bool = False,
        user: str = "",
        persistent: bool = True,
    ) -> int:
        from stabilize.persistence.sqlite.signals import buffer_signal

        return buffer_signal(
            self._conn,
            execution_id,
            stage_ref_id,
            signal_name,
            signal_data,
            tenant=self._tenant,
            consumed=consumed,
            user=user,
            persistent=persistent,
        )

    def discard_signals(self, execution_id: str, stage_ref_id: str) -> int:
        cursor = self._conn.execute(
            "DELETE FROM workflow_signals WHERE execution_id = :e AND stage_ref_id = :s AND consumed = 0 "
            "AND stabilize_tenant = :tenant",
            {"e": execution_id, "s": stage_ref_id, "tenant": self._tenant},
        )
        return int(cursor.rowcount or 0)

    def _discard_if_complete(self, stage: StageExecution) -> None:
        if not stage.status.is_complete:
            return
        cursor = self._conn.execute(
            "DELETE FROM workflow_signals WHERE consumed = 0 AND stabilize_tenant = :tenant AND stage_ref_id = :ref "
            "AND execution_id = (SELECT execution_id FROM stage_executions WHERE id = :stage_id "
            "AND stabilize_tenant = :tenant)",
            {"tenant": self._tenant, "ref": stage.ref_id, "stage_id": stage.id},
        )
        dropped = int(cursor.rowcount or 0)
        if dropped:
            logger.warning(
                "Discarded %d pending signal(s) on stage %s entering %s", dropped, stage.id, stage.status.name
            )


def _queue_row_id(message_id: str | None) -> int | None:
    """The queue row behind a delivered message id; None when the id names no row (a test or foreign id)."""
    if not message_id:
        return None
    from stabilize.queue.message_ids import row_id

    try:
        return int(row_id(message_id))
    except (TypeError, ValueError):
        return None
