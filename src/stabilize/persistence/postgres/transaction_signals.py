"""Signal rows and the stage row lock on the transaction's own connection (PostgreSQL)."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class PostgresSignalTransactionMixin:
    _conn: Any
    _tenant: str
    _store: Any

    def _discard_if_complete(self, stage: StageExecution) -> None:
        if not stage.status.is_complete or not self._store.supports_signal_storage():
            return
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM workflow_signals WHERE consumed = FALSE AND stabilize_tenant = %(tenant)s "
                "AND stage_ref_id = %(ref)s AND execution_id = (SELECT execution_id FROM stage_executions "
                "WHERE id = %(stage_id)s AND stabilize_tenant = %(tenant)s)",
                {"tenant": self._tenant, "ref": stage.ref_id, "stage_id": stage.id},
            )
            dropped = int(cur.rowcount or 0)
        if dropped:
            logger.warning(
                "Discarded %d pending signal(s) on stage %s entering %s", dropped, stage.id, stage.status.name
            )

    def lock_stage(self, stage_id: str) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM stage_executions WHERE id = %(id)s AND stabilize_tenant = %(tenant)s FOR UPDATE",
                {"id": stage_id, "tenant": self._tenant},
            )
            row = cur.fetchone()
        if row is None:
            return None
        return str(row["status"] if isinstance(row, dict) else row[0])

    def consume_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str | None = None,
        *,
        row_id: int | None = None,
    ) -> dict[str, Any] | None:
        clauses = ""
        params: dict[str, Any] = {"e": execution_id, "s": stage_ref_id, "tenant": self._tenant}
        if signal_name:
            clauses += " AND signal_name = %(name)s"
            params["name"] = signal_name
        if row_id is not None:
            clauses += " AND id = %(row_id)s"
            params["row_id"] = row_id
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE workflow_signals SET consumed = TRUE, consumed_at = NOW() WHERE id = ("
                "SELECT id FROM workflow_signals WHERE execution_id = %(e)s AND stage_ref_id = %(s)s "
                f"AND stabilize_tenant = %(tenant)s AND consumed = FALSE{clauses} "
                "ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING signal_name, signal_data",
                params,
            )
            row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return {"signal_name": row["signal_name"], "signal_data": row["signal_data"] or {}}
        return {"signal_name": row[0], "signal_data": row[1] or {}}

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
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO workflow_signals (execution_id, stage_ref_id, signal_name, signal_data, consumed, "
                "consumed_at, signal_user, persistent, stabilize_tenant) VALUES (%(e)s, %(s)s, %(n)s, %(d)s, %(c)s, "
                "CASE WHEN %(c)s THEN NOW() ELSE NULL END, %(u)s, %(p)s, %(tenant)s) RETURNING id",
                {
                    "e": execution_id,
                    "s": stage_ref_id,
                    "n": signal_name,
                    "d": json.dumps(signal_data or {}, default=str),
                    "c": consumed,
                    "u": user,
                    "p": persistent,
                    "tenant": self._tenant,
                },
            )
            row = cur.fetchone()
        if not row:
            return 0
        return int(row["id"] if isinstance(row, dict) else row[0])

    def discard_signals(self, execution_id: str, stage_ref_id: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM workflow_signals WHERE execution_id = %(e)s AND stage_ref_id = %(s)s "
                "AND consumed = FALSE AND stabilize_tenant = %(tenant)s",
                {"e": execution_id, "s": stage_ref_id, "tenant": self._tenant},
            )
            return int(cur.rowcount or 0)
