"""Cursor positions and event purge for the SQLite event store."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.store.retention import (
    DEFAULT_PURGE_BATCH,
    SubscriptionBehindError,
    bound_clauses,
    describe_bound,
)
from stabilize.events.store.subscription_names import unqualify


class SqliteRetentionMixin:
    tenant: str

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

        def get_current_sequence(self) -> int: ...

    def current_commit_cursor(self) -> tuple[str, int]:
        """SQLite commits in sequence order, so the position is the latest sequence."""
        return str(self.get_current_sequence()), 0

    def commit_cursor_for_sequence(self, sequence: int) -> tuple[str, int]:
        return str(int(sequence)), 0

    def purge_events(
        self,
        *,
        before_sequence: int | None = None,
        before: datetime | None = None,
        ignore_subscriptions: bool = False,
        batch_size: int = DEFAULT_PURGE_BATCH,
        dry_run: bool = False,
    ) -> int:
        """Delete this tenant's events inside the bound in batches; count them instead under *dry_run*."""
        clauses, params = bound_clauses(before_sequence, before, "sqlite", alias="e.")
        params["tenant"] = self.tenant
        bound = " AND ".join(clauses)
        conn = self._get_connection()
        if not ignore_subscriptions:
            behind = self._subscriptions_behind(conn, bound, params)
            if behind:
                raise SubscriptionBehindError(behind, describe_bound(before_sequence, before))
        if dry_run:
            row = conn.execute(
                f"SELECT count(*) FROM events e WHERE e.stabilize_tenant = :tenant AND {bound}", params
            ).fetchone()
            return int(row[0]) if row else 0
        deleted = 0
        while True:
            cursor = conn.execute(
                "DELETE FROM events WHERE rowid IN ("
                f"SELECT e.rowid FROM events e WHERE e.stabilize_tenant = :tenant AND {bound} LIMIT :batch)",
                {**params, "batch": int(batch_size)},
            )
            removed = cursor.rowcount
            conn.commit()
            deleted += max(removed, 0)
            if removed < batch_size:
                return deleted

    def _subscriptions_behind(self, conn: sqlite3.Connection, bound: str, params: dict[str, Any]) -> list[str]:
        rows = conn.execute(
            "SELECT s.id FROM event_subscriptions s "
            "WHERE s.stabilize_tenant = :tenant AND s.enabled "
            "AND EXISTS (SELECT 1 FROM events e WHERE e.stabilize_tenant = s.stabilize_tenant "
            f"AND {bound} AND e.sequence > max(CAST(s.last_commit_cursor AS INTEGER), "
            "s.last_commit_sequence, s.last_sequence)) "
            "ORDER BY s.id",
            params,
        ).fetchall()
        return [unqualify(self.tenant, str(row[0])) for row in rows]
