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

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore


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
        else:
            pool = self._store._pool  # type: ignore[attr-defined]
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS task_leases (
                            task_id TEXT PRIMARY KEY,
                            owner TEXT NOT NULL,
                            acquired_at BIGINT NOT NULL,
                            expires_at BIGINT NOT NULL
                        )
                        """
                    )
                conn.commit()

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
                    "DELETE FROM task_leases WHERE task_id = ? AND (expires_at < ? OR owner = ?)",
                    (task_id, now, self.owner),
                )
                conn.execute(
                    "INSERT INTO task_leases (task_id, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
                    (task_id, self.owner, now, expires),
                )
            return True
        except sqlite3.IntegrityError:
            # A non-expired lease owned by another worker exists.
            return False

    def _acquire_postgres(self, task_id: str, now: int, expires: int) -> bool:
        pool = self._store._pool  # type: ignore[attr-defined]
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO task_leases (task_id, owner, acquired_at, expires_at)
                    VALUES (%(task_id)s, %(owner)s, %(now)s, %(expires)s)
                    ON CONFLICT (task_id) DO UPDATE
                        SET owner = EXCLUDED.owner,
                            acquired_at = EXCLUDED.acquired_at,
                            expires_at = EXCLUDED.expires_at
                        WHERE task_leases.expires_at < %(now)s
                           OR task_leases.owner = %(owner)s
                    RETURNING owner
                    """,
                    {"task_id": task_id, "owner": self.owner, "now": now, "expires": expires},
                )
                row = cur.fetchone()
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
                    "DELETE FROM task_leases WHERE task_id = ? AND owner = ?",
                    (task_id, self.owner),
                )
        else:
            pool = self._store._pool  # type: ignore[attr-defined]
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM task_leases WHERE task_id = %(task_id)s AND owner = %(owner)s",
                        {"task_id": task_id, "owner": self.owner},
                    )
                conn.commit()
