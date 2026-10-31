"""
PostgreSQL-backed queue implementation.

Uses FOR UPDATE SKIP LOCKED for concurrent message processing across
multiple workers. Messages are stored in a table with delivery time.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from stabilize.persistence.borrow import EngineBorrower
from stabilize.persistence.committed_marks import record_bare_push
from stabilize.persistence.connection import release_pool_once
from stabilize.persistence.pool_options import PoolOptions, with_schema
from stabilize.persistence.schema_check import check_postgres_tables
from stabilize.persistence.tenancy import (
    EngineSchemaError,
    ModeReader,
    armed,
    postgres_mode,
    validate_tenant,
)
from stabilize.queue.decode import MessageDecodeError, decode_message
from stabilize.queue.interface import Queue
from stabilize.queue.message_ids import delivery_key, own_row_ids, row_id
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.postgres_lookups import DELIVERABLE, PostgresQueueLookupsMixin

_VALID_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

logger = logging.getLogger(__name__)

_FENCE = " AND (%(attempts)s::int IS NULL OR attempts = %(attempts)s::int)"
_TENANT = " AND stabilize_tenant = %(tenant)s"
_DELIVERABLE = DELIVERABLE


class PostgresQueue(PostgresQueueLookupsMixin, Queue):
    """
    PostgreSQL-backed queue implementation.

    Uses FOR UPDATE SKIP LOCKED for concurrent message processing across
    multiple workers. Messages are stored in a table with delivery time.

    Connection pools are managed by singleton ConnectionManager for
    efficient resource sharing across all queue instances.
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "queue_messages",
        lock_duration: timedelta = timedelta(seconds=60),
        max_attempts: int = 10,
        schema: str | None = None,
        options: PoolOptions | None = None,
        tenant: str = "",
        store_retry_budget: timedelta = timedelta(minutes=30),
    ) -> None:
        """
        Initialize the PostgreSQL queue.

        Args:
            connection_string: PostgreSQL connection string
            table_name: Name of the queue table
            schema: PostgreSQL schema holding the queue tables. Applied as a
                search_path connect option.
            lock_duration: How long to lock messages during processing.
                THE DEFAULT ASSUMES AN ACTIVE LEASE RENEWER. QueueProcessor
                renews it from start()/the poll loop, but the SYNCHRONOUS path
                (process_all/process_one) starts no heartbeat: a handler that
                outlives this duration makes its message visible to another
                consumer while it is still running, and a later poll can
                execute it again. Embedders driving the queue synchronously
                should size this above their worst-case single message, e.g.
                lock_duration=timedelta(minutes=30).
            max_attempts: Maximum retry attempts before dropping message
            options: Pool and connection options. Pass the same PoolOptions
                instance given to PostgresWorkflowStore to share one pool.
        """
        from stabilize.persistence.connection import get_connection_manager

        self.tenant = validate_tenant(tenant)
        self.connection_string = connection_string
        if not _VALID_TABLE_NAME_RE.match(table_name):
            raise ValueError(f"Invalid table name: {table_name!r}")
        self.table_name = table_name
        self.lock_duration = lock_duration
        self.max_attempts = max_attempts
        self.store_retry_budget = store_retry_budget
        self.schema = schema
        self._pool_options = with_schema(options, schema, connection_string)
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string, options=self._pool_options)
        self._tenant_setting = self._pool_options.tenant_setting if self._pool_options is not None else None
        self._mode = ModeReader(postgres_mode)
        self._schema_state: BaseException | bool = False
        self._engine = EngineBorrower(self._borrow, self.tenant)
        self._pending: dict[int, dict[str, Any]] = {}
        # Size caching
        self._size_cache: int | None = None
        self._last_size_check: float = 0
        self._size_cache_ttl: float = 5.0  # seconds

    def _get_pool(self) -> Any:
        """Return the pool acquired at construction."""
        return self._pool

    @contextmanager
    def _borrow(self) -> Iterator[Any]:
        """Every engine borrow: the pool's connection, admitted against the schema's tenancy mode."""
        with self._pool.connection() as conn, armed(conn, self.tenant, self._tenant_setting, self._mode):
            self._check_schema_once(conn)
            yield conn

    def _check_schema_once(self, conn: Any) -> None:
        if self._schema_state is True:
            return
        if self._schema_state is not False:
            raise self._schema_state
        try:
            check_postgres_tables(
                conn, self.schema, [self.table_name, f"{self.table_name}_dlq"], lambda: self._mode.last or "single"
            )
        except EngineSchemaError as exc:
            self._schema_state = exc
            raise
        self._schema_state = True

    def close(self) -> None:
        """Release this queue's hold on its pool."""
        release_pool_once(self, self._manager, self._pool)

    def _serialize_message(self, message: Message) -> str:
        """Serialize a message to JSON."""
        from enum import Enum

        data = {}
        for key, value in message.__dict__.items():
            if key.startswith("_"):
                continue
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.name  # Use name, not value (which may be a tuple)
            else:
                data[key] = value
        return json.dumps(data)

    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
        connection: Any | None = None,
    ) -> None:
        """Push a message onto the queue."""

        message_type = get_message_type_name(message)
        message_id = str(uuid.uuid4())
        payload = self._serialize_message(message)

        if delay:
            deliver_at_expr = "NOW() + %(delay)s * INTERVAL '1 second'"
            extra_params = {"delay": delay.total_seconds()}
        else:
            deliver_at_expr = "NOW()"
            extra_params = {}

        sql = f"""
            INSERT INTO {self.table_name}
            (message_id, message_type, payload, deliver_at, attempts, stabilize_tenant)
            VALUES (%(message_id)s, %(type)s, %(payload)s::jsonb, {deliver_at_expr}, 0, %(tenant)s)
        """
        params = {
            "message_id": message_id,
            "type": message_type,
            "payload": payload,
            "tenant": self.tenant,
            **extra_params,
        }

        if connection:
            with connection.cursor() as cur:
                cur.execute(sql, params)
        else:
            with self._borrow() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                conn.commit()
            record_bare_push(message_type)

        # Invalidate size cache
        self._size_cache = None

    def poll(self, callback: Callable[[Message], None]) -> None:
        """Poll for a message and process it with the callback."""
        message = self.poll_one()
        if message:
            try:
                callback(message)
                self.ack(message)
            except Exception:
                # Message will be retried after lock expires
                raise

    def poll_one(self) -> Message | None:
        """Poll for a single message without callback."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET locked_until = NOW() + %(lock_seconds)s * INTERVAL '1 second',
                        attempts = attempts + 1
                    WHERE id = (
                        SELECT id FROM {self.table_name}
                        WHERE deliver_at <= NOW()
                        AND stabilize_tenant = %(tenant)s
                        AND (locked_until IS NULL OR locked_until < NOW())
                        AND {_DELIVERABLE}
                        ORDER BY deliver_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, message_type, payload::text AS payload, attempts, store_retries
                    """,
                    {
                        "lock_seconds": self.lock_duration.total_seconds(),
                        "max_attempts": self.max_attempts,
                        "budget": self.store_retry_budget.total_seconds(),
                        "tenant": self.tenant,
                    },
                )
                row = cur.fetchone()
            conn.commit()

        if not row:
            return None

        msg_id = row["id"]
        msg_type = row["message_type"]
        try:
            message = decode_message(msg_type, row["payload"])
        except MessageDecodeError as exc:
            logger.error("Moving undecodable message %s to the DLQ: %s", msg_id, exc)
            self.move_to_dlq(msg_id, error=f"Deserialization failed: {exc}")
            return None

        message.message_id = delivery_key(self.table_name, msg_id)
        message.attempts = row["attempts"]
        message.store_retries = int(row["store_retries"] or 0)
        self._pending[msg_id] = {
            "message": message,
            "type": msg_type,
        }
        return message

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from the queue."""
        if not message.message_id:
            return

        try:
            msg_id = row_id(message.message_id)
        except ValueError:
            logger.error("Invalid message_id format in ack(): %s", message.message_id)
            return

        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.table_name} WHERE id = %(id)s{_FENCE}{_TENANT}",
                    {"id": msg_id, "attempts": getattr(message, "attempts", None), "tenant": self.tenant},
                )
                if cur.rowcount == 0:
                    logger.warning("ack(%s): the lease was taken by another worker; not deleted", message.message_id)
            conn.commit()

        self._pending.pop(msg_id, None)
        # Invalidate size cache
        self._size_cache = None

    def extend_lock(self, message: Message, duration: timedelta | None = None) -> bool:
        """Extend the visibility lock of an in-flight message (heartbeat)."""
        if not message.message_id:
            return False
        msg_id = row_id(message.message_id)

        seconds = (duration or self.lock_duration).total_seconds()
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {self.table_name} SET locked_until = NOW() + %(seconds)s * INTERVAL '1 second' "
                    f"WHERE id = %(id)s{_FENCE}{_TENANT}",
                    {
                        "seconds": seconds,
                        "id": msg_id,
                        "attempts": getattr(message, "attempts", None),
                        "tenant": self.tenant,
                    },
                )
                extended = bool(cur.rowcount == 1)
            conn.commit()
        return extended

    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Ensure a message is in the queue with the given delay."""
        # For simplicity, just push the message
        # In production, would check for duplicates
        self.push(message, delay)

    def reschedule(
        self,
        message: Message,
        delay: timedelta,
        *,
        store_unavailable: bool = False,
    ) -> None:
        """Reschedule a message; a store outage spends the store-retry budget, anything else closes it."""
        if not message.message_id:
            return

        msg_id = row_id(message.message_id)

        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET deliver_at = NOW() + %(delay)s * INTERVAL '1 second',
                        locked_until = NULL,
                        store_retries = CASE WHEN %(store)s THEN store_retries + 1 ELSE 0 END,
                        store_retry_since = CASE WHEN %(store)s THEN COALESCE(store_retry_since, NOW()) ELSE NULL END
                    WHERE id = %(id)s{_FENCE}{_TENANT}
                    """,
                    {
                        "id": msg_id,
                        "store": bool(store_unavailable),
                        "delay": delay.total_seconds(),
                        "attempts": getattr(message, "attempts", None),
                        "tenant": self.tenant,
                    },
                )
            conn.commit()

        self._pending.pop(msg_id, None)

    def live_delivery_keys(self, keys: list[str]) -> set[str]:
        owned = own_row_ids(self.table_name, keys)
        if not owned:
            return set()
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id FROM {self.table_name} WHERE id = ANY(%(ids)s){_TENANT}",
                    {"ids": list(owned), "tenant": self.tenant},
                )
                rows = cur.fetchall()
        return {owned[int(r["id"])] for r in rows}

    def size(self) -> int:
        """Get the number of messages in the queue (cached)."""
        now = time.time()
        if self._size_cache is not None and now - self._last_size_check < self._size_cache_ttl:
            return self._size_cache

        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) as cnt FROM {self.table_name} WHERE stabilize_tenant = %(tenant)s",
                    {"tenant": self.tenant},
                )
                row = cur.fetchone()
                count = row["cnt"] if row else 0

        self._size_cache = count
        self._last_size_check = now
        return count

    def next_ready_in(self) -> float | None:
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT EXTRACT(EPOCH FROM (MIN(deliver_at) - NOW())) AS wait FROM {self.table_name} "
                    "WHERE stabilize_tenant = %(tenant)s AND (locked_until IS NULL OR locked_until < NOW()) "
                    f"AND {_DELIVERABLE}",
                    {
                        "tenant": self.tenant,
                        "max_attempts": self.max_attempts,
                        "budget": self.store_retry_budget.total_seconds(),
                    },
                )
                row = cur.fetchone()
            conn.commit()
        wait = row["wait"] if row else None
        return None if wait is None else max(0.0, float(wait))

    def clear(self) -> None:
        """Clear all messages from the queue."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.table_name} WHERE stabilize_tenant = %(tenant)s", {"tenant": self.tenant}
                )
            conn.commit()

        self._pending.clear()
        # Invalidate size cache
        self._size_cache = None

    def move_to_dlq(
        self,
        message_id: int | str,
        error: str | None = None,
    ) -> None:
        """Move a message to the Dead Letter Queue."""
        from stabilize.queue.dlq import move_to_dlq as _move_to_dlq

        _move_to_dlq(self._engine, self.table_name, self._pending, message_id, error)

    def list_dlq(
        self,
        limit: int = 100,
        message_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """List messages in the Dead Letter Queue."""
        from stabilize.queue.dlq import list_dlq as _list_dlq

        return _list_dlq(self._engine, self.table_name, limit, message_type)

    def replay_dlq(self, dlq_id: int) -> bool:
        """Replay a message from the DLQ back to the main queue."""
        from stabilize.queue.dlq import replay_dlq as _replay_dlq

        return _replay_dlq(self._engine, self.table_name, dlq_id)

    def dlq_size(self) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        from stabilize.queue.dlq import dlq_size as _dlq_size

        return _dlq_size(self._engine, self.table_name)

    def clear_dlq(self) -> int:
        """Clear all messages from the Dead Letter Queue."""
        from stabilize.queue.dlq import clear_dlq as _clear_dlq

        return _clear_dlq(self._engine, self.table_name)

    def check_and_move_expired(self) -> int:
        """Check for messages exceeding max_attempts and move to DLQ."""
        from stabilize.queue.dlq import check_and_move_expired as _check_and_move_expired

        return _check_and_move_expired(
            self._engine, self.table_name, self._pending, self.max_attempts, self.store_retry_budget
        )
