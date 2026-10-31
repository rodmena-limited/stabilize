"""What the SQLite queue can be asked about its rows: pending messages, dead-letter stamps, purges."""

from __future__ import annotations

import sqlite3
from typing import Any


class SqliteQueueLookupsMixin:
    table_name: str
    tenant: str
    _pending: dict[int, dict[str, Any]]

    def _get_connection(self) -> sqlite3.Connection:
        raise NotImplementedError

    def has_pending_message_for_stage(self, stage_id: str) -> bool:
        row = (
            self._get_connection()
            .execute(
                f"SELECT 1 FROM {self.table_name} WHERE CASE WHEN json_valid(payload) "
                "THEN json_extract(payload, '$.stage_id') END = ? AND stabilize_tenant = ? LIMIT 1",
                (stage_id, self.tenant),
            )
            .fetchone()
        )
        return row is not None

    def has_pending_message_for_config(self, pipeline_config_id: str) -> bool:
        conn = self._get_connection()
        row = conn.execute(
            f"SELECT 1 FROM {self.table_name} "
            "WHERE json_extract(payload, '$.pipeline_config_id') = :config AND stabilize_tenant = :tenant LIMIT 1",
            {"config": pipeline_config_id, "tenant": self.tenant},
        ).fetchone()
        return row is not None

    def dead_letter_moved_at_ms(self, message_type: str, execution_id: str, target_id: str | None) -> int | None:
        conn = self._get_connection()
        row = conn.execute(
            f"SELECT CAST((julianday(max(moved_at)) - 2440587.5) * 86400000 AS INTEGER) FROM {self.table_name}_dlq "
            "WHERE message_type = :type AND json_extract(payload, '$.execution_id') = :execution_id "
            "AND stabilize_tenant = :tenant AND (:target IS NULL "
            "OR json_extract(payload, '$.task_id') = :target OR json_extract(payload, '$.stage_id') = :target)",
            {"type": message_type, "execution_id": execution_id, "tenant": self.tenant, "target": target_id},
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def has_pending_message_for_execution(self, execution_id: str) -> bool:
        """Whether any queued message targets this workflow."""
        row = (
            self._get_connection()
            .execute(
                f"SELECT 1 FROM {self.table_name} WHERE CASE WHEN json_valid(payload) "
                "THEN json_extract(payload, '$.execution_id') END = ? AND stabilize_tenant = ? LIMIT 1",
                (execution_id, self.tenant),
            )
            .fetchone()
        )
        return row is not None

    def purge_workflow(self, execution_id: str) -> int:
        """Delete every queued and dead-lettered message for one workflow."""
        conn = self._get_connection()
        present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
        deleted = 0
        for table in (t for t in (self.table_name, f"{self.table_name}_dlq") if t in present):
            cur = conn.execute(
                f"DELETE FROM {table} WHERE CASE WHEN json_valid(payload) "
                "THEN json_extract(payload, '$.execution_id') END = ? AND stabilize_tenant = ?",
                (execution_id, self.tenant),
            )
            deleted += cur.rowcount
        conn.commit()
        return deleted

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        conn = self._get_connection()
        result = conn.execute(
            f"""
            SELECT 1 FROM {self.table_name}
            WHERE json_extract(payload, '$.task_id') = :task_id AND stabilize_tenant = :tenant
            LIMIT 1
            """,
            {"task_id": task_id, "tenant": self.tenant},
        )
        return result.fetchone() is not None
