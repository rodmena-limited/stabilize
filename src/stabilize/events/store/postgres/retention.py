"""Commit-cursor positions and event purge for the PostgreSQL event store."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.store.retention import (
    DEFAULT_PURGE_BATCH,
    SubscriptionBehindError,
    bound_clauses,
    describe_bound,
)
from stabilize.events.store.subscription_names import unqualify

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import DictRow
    from psycopg_pool import ConnectionPool

_WATERMARK = "SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS wm"


class PostgresRetentionMixin:
    tenant: str
    _borrow: Any

    if TYPE_CHECKING:
        _pool: ConnectionPool[Connection[DictRow]]

        def supports_commit_cursor(self) -> bool: ...

    def current_commit_cursor(self) -> tuple[str, int]:
        """The position after every transaction that has committed so far."""
        if not self.supports_commit_cursor():
            return "0", 0
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(_WATERMARK)
                row = cur.fetchone()
        return (str(row["wm"]) if row else "0"), 0

    def commit_cursor_for_sequence(self, sequence: int) -> tuple[str, int]:
        """A position from which every event after *sequence* is still delivered.

        The smallest commit id among the later events is the safe choice: nothing after
        *sequence* is below it, while events at or before *sequence* that committed late
        may be delivered again.
        """
        if not self.supports_commit_cursor():
            return "0", 0
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT min(coalesce(commit_xid, '0'::xid8))::text AS xid FROM events "
                    "WHERE stabilize_tenant = %(tenant)s AND sequence > %(sequence)s",
                    {"tenant": self.tenant, "sequence": int(sequence)},
                )
                row = cur.fetchone()
                xid = row["xid"] if row else None
                if xid is None:
                    cur.execute(_WATERMARK)
                    row = cur.fetchone()
                    return (str(row["wm"]) if row else "0"), 0
        return str(xid), 0

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
        clauses, params = bound_clauses(before_sequence, before, "postgres", alias="e.")
        params["tenant"] = self.tenant
        bound = " AND ".join(clauses)
        with self._borrow() as conn:
            with conn.cursor() as cur:
                if not ignore_subscriptions:
                    behind = self._subscriptions_behind(cur, bound, params)
                    if behind:
                        raise SubscriptionBehindError(behind, describe_bound(before_sequence, before))
                if dry_run:
                    cur.execute(
                        f"SELECT count(*) AS n FROM events e WHERE e.stabilize_tenant = %(tenant)s AND {bound}",
                        params,
                    )
                    row = cur.fetchone()
                    return int(row["n"]) if row else 0
            deleted = 0
            while True:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM events WHERE ctid = ANY(ARRAY("
                        f"SELECT e.ctid FROM events e WHERE e.stabilize_tenant = %(tenant)s AND {bound} "
                        "LIMIT %(batch)s))",
                        {**params, "batch": int(batch_size)},
                    )
                    removed = int(cur.rowcount)
                conn.commit()
                deleted += max(removed, 0)
                if removed < batch_size:
                    return deleted

    def _subscriptions_behind(self, cur: Any, bound: str, params: dict[str, Any]) -> list[str]:
        if self.supports_commit_cursor():
            undelivered = (
                "(coalesce(e.commit_xid, '0'::xid8), e.sequence) > (s.last_commit_cursor::xid8, s.last_commit_sequence)"
            )
        else:
            undelivered = "e.sequence > s.last_sequence"
        cur.execute(
            "SELECT s.id FROM event_subscriptions s "
            "WHERE s.stabilize_tenant = %(tenant)s AND s.enabled "
            "AND EXISTS (SELECT 1 FROM events e WHERE e.stabilize_tenant = s.stabilize_tenant "
            f"AND {bound} AND {undelivered}) "
            "ORDER BY s.id",
            params,
        )
        return [unqualify(self.tenant, str(row["id"])) for row in cur.fetchall()]
