"""Optional distributed task lease for cross-process duplicate-execution defense.

The in-process execution lock in ``RunTaskHandler`` prevents a single process
from running the same task twice concurrently. In a multi-process / multi-node
deployment that lock does not help: two workers could both pick up duplicate
RunTask messages for the same task and execute its side effect concurrently.

This module adds an opt-in, DB-backed lease so that across processes only one
worker executes a given task at a time. It is **disabled by default** — when off,
behavior is identical to before. It is a *window-narrowing* mechanism, not a
correctness guarantee: a worker can still crash after committing a side effect
but before releasing the lease (the lease then expires and recovery re-queues),
so tasks with external side effects should still be idempotent.

The lease table is created on demand (same pattern as the queue/DLQ tables), so
enabling the feature requires no migration.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from typing import TYPE_CHECKING

from stabilize.persistence.factory import detect_backend
from stabilize.persistence.tenancy import refuse_foreign_row

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore


class TaskLeaseUnavailableError(RuntimeError):
    """Raised when leasing was requested but cannot be provided.

    Setting STABILIZE_TASK_LEASE asks for single-execution across processes.
    Substituting no leasing for the leasing that was asked for would let a task
    with external side effects run twice while the operator believed otherwise,
    so the request fails rather than degrades.
    """


def _now_ms() -> int:
    return int(time.time() * 1000)


class TaskLeaseManager:
    """Acquire/release short-lived leases on task execution across processes.

    Args:
        store: The workflow store (used for its connection/pool and backend).
        owner: Stable identifier for this worker/process. Defaults to a random
            per-instance id.
        ttl_seconds: How long an acquired lease is valid before it may be stolen
            by another worker (covers the crash-without-release case).
    """

    def __init__(
        self,
        store: WorkflowStore,
        owner: str | None = None,
        ttl_seconds: float = 3600.0,
    ) -> None:
        self._store = store
        self.owner = owner or f"worker-{uuid.uuid4()}"
        self.ttl_ms = int(ttl_seconds * 1000)
        self._backend = detect_backend(getattr(store, "connection_string", "sqlite"))
        self._tenant: str = getattr(store, "tenant", "")
        self._ensure_table()

    # -- table management ---------------------------------------------------

    def _ensure_table(self) -> None:
        if self._backend == "sqlite":
            conn = self._store._get_connection()  # type: ignore[attr-defined]
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_leases (
                    task_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    acquired_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
                """
            )
            conn.commit()
            from stabilize.persistence.sqlite.tenant_schema import ensure_tenant_schema

            ensure_tenant_schema(conn)
        else:
            pool = self._store._engine  # type: ignore[attr-defined]
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('task_leases') IS NOT NULL AS present")
                    row = cur.fetchone()
                conn.commit()
            present = row["present"] if isinstance(row, dict) else (row[0] if row else False)
            if not present:
                raise TaskLeaseUnavailableError(
                    "the task_leases table does not exist on this connection's search_path; "
                    "run stabilize mg-up (0.33.2 or later) with the owner role to create it"
                )

    # -- lease operations ---------------------------------------------------

    def acquire(self, task_id: str) -> bool:
        """Atomically acquire the lease for a task.

        Returns True if this worker now holds the lease (either it was free, the
        previous lease had expired, or this worker already held it). Returns
        False if another worker holds a non-expired lease.
        """
        now = _now_ms()
        expires = now + self.ttl_ms
        if self._backend == "sqlite":
            return self._acquire_sqlite(task_id, now, expires)
        return self._acquire_postgres(task_id, now, expires)

    def _acquire_sqlite(self, task_id: str, now: int, expires: int) -> bool:
        conn = self._store._get_connection()  # type: ignore[attr-defined]
        try:
            # Single transaction: clear an expired/owned lease, then claim.
            with conn:
                conn.execute(
                    "DELETE FROM task_leases "
                    "WHERE task_id = ? AND (expires_at < ? OR owner = ?) AND stabilize_tenant = ?",
                    (task_id, now, self.owner, self._tenant),
                )
                conn.execute(
                    "INSERT INTO task_leases (task_id, owner, acquired_at, expires_at, stabilize_tenant) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (task_id, self.owner, now, expires, self._tenant),
                )
            return True
        except sqlite3.IntegrityError:
            held = conn.execute("SELECT stabilize_tenant FROM task_leases WHERE task_id = ?", (task_id,)).fetchone()
            refuse_foreign_row("task_leases", task_id, held[0] if held else None, self._tenant)
            return False

    def _acquire_postgres(self, task_id: str, now: int, expires: int) -> bool:
        pool = self._store._engine  # type: ignore[attr-defined]
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO task_leases (task_id, owner, acquired_at, expires_at, stabilize_tenant)
                    VALUES (%(task_id)s, %(owner)s, %(now)s, %(expires)s, %(tenant)s)
                    ON CONFLICT (task_id) DO UPDATE
                        SET owner = EXCLUDED.owner,
                            acquired_at = EXCLUDED.acquired_at,
                            expires_at = EXCLUDED.expires_at
                        WHERE (task_leases.expires_at < %(now)s OR task_leases.owner = %(owner)s)
                          AND task_leases.stabilize_tenant = EXCLUDED.stabilize_tenant
                    RETURNING owner
                    """,
                    {"task_id": task_id, "owner": self.owner, "now": now, "expires": expires, "tenant": pool.tenant},
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT stabilize_tenant AS owner FROM task_leases WHERE task_id = %(task_id)s",
                        {"task_id": task_id},
                    )
                    held = cur.fetchone()
                    refuse_foreign_row("task_leases", task_id, held["owner"] if held else None, pool.tenant)
            conn.commit()
        if row is None:
            return False
        # dict_row factory -> dict; be tolerant of tuple rows too.
        owner = row["owner"] if isinstance(row, dict) else row[0]
        return bool(owner == self.owner)

    def release(self, task_id: str) -> None:
        """Release the lease for a task if this worker holds it."""
        if self._backend == "sqlite":
            conn = self._store._get_connection()  # type: ignore[attr-defined]
            with conn:
                conn.execute(
                    "DELETE FROM task_leases WHERE task_id = ? AND owner = ? AND stabilize_tenant = ?",
                    (task_id, self.owner, self._tenant),
                )
        else:
            pool = self._store._engine  # type: ignore[attr-defined]
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM task_leases WHERE task_id = %(task_id)s AND owner = %(owner)s "
                        "AND stabilize_tenant = %(tenant)s",
                        {"task_id": task_id, "owner": self.owner, "tenant": pool.tenant},
                    )
                conn.commit()
