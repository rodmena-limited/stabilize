"""What the PostgreSQL queue can be asked about its rows: pending messages, dead-letter stamps, purges."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any


class PostgresQueueLookupsMixin:
    table_name: str
    tenant: str
    _size_cache: int | None

    def _borrow(self) -> AbstractContextManager[Any]:
        raise NotImplementedError

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        with self._borrow() as conn:
            with conn.cursor() as cur:
                # Exact top-level match; served by the expression index
                # idx_<table>_task_id (see add_queue_task_id_index migration).
                cur.execute(
                    f"""
                    SELECT 1 FROM {self.table_name}
                    WHERE payload ->> 'task_id' = %s AND stabilize_tenant = %s
                    LIMIT 1
                    """,
                    (task_id, self.tenant),
                )
                return cur.fetchone() is not None

    def has_pending_message_for_stage(self, stage_id: str) -> bool:
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {self.table_name} "
                    "WHERE payload ->> 'stage_id' = %s AND stabilize_tenant = %s LIMIT 1",
                    (stage_id, self.tenant),
                )
                return cur.fetchone() is not None

    def has_pending_message_for_config(self, pipeline_config_id: str) -> bool:
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {self.table_name} "
                    "WHERE payload ->> 'pipeline_config_id' = %s AND stabilize_tenant = %s LIMIT 1",
                    (pipeline_config_id, self.tenant),
                )
                return cur.fetchone() is not None

    def dead_letter_moved_at_ms(self, message_type: str, execution_id: str, target_id: str | None) -> int | None:
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT (EXTRACT(EPOCH FROM max(moved_at)) * 1000)::bigint AS ms FROM {self.table_name}_dlq "
                    "WHERE message_type = %(type)s AND payload ->> 'execution_id' = %(execution_id)s "
                    "AND stabilize_tenant = %(tenant)s AND (%(target)s::text IS NULL "
                    "OR payload ->> 'task_id' = %(target)s OR payload ->> 'stage_id' = %(target)s)",
                    {"type": message_type, "execution_id": execution_id, "tenant": self.tenant, "target": target_id},
                )
                row = cur.fetchone()
        value = row["ms"] if row else None
        return int(value) if value is not None else None

    def has_pending_message_for_execution(self, execution_id: str) -> bool:
        """Whether any queued message targets this workflow."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {self.table_name} "
                    "WHERE payload ->> 'execution_id' = %s AND stabilize_tenant = %s LIMIT 1",
                    (execution_id, self.tenant),
                )
                return cur.fetchone() is not None

    def purge_workflow(self, execution_id: str) -> int:
        """Delete every queued and dead-lettered message for one workflow."""
        deleted = 0
        with self._borrow() as conn:
            with conn.cursor() as cur:
                for table in (self.table_name, f"{self.table_name}_dlq"):
                    cur.execute(
                        f"DELETE FROM {table} WHERE payload ->> 'execution_id' = %s AND stabilize_tenant = %s",
                        (execution_id, self.tenant),
                    )
                    deleted += cur.rowcount
            conn.commit()
        self._size_cache = None
        return deleted

    # ========== Dead Letter Queue Methods ==========
