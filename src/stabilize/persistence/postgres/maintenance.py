"""Message-dedup bookkeeping and reclamation for the PostgreSQL store."""

from __future__ import annotations

from typing import Any

from stabilize.persistence.postgres.operations import (
    cleanup_buffered_signals as _cleanup_buffered_signals,
)
from stabilize.persistence.postgres.operations import (
    cleanup_completed_stage_claims as _cleanup_claims,
)
from stabilize.persistence.postgres.operations import (
    cleanup_old_processed_messages as _cleanup_old_processed_messages,
)
from stabilize.persistence.postgres.operations import (
    count_buffered_signal_stages as _count_buffered_signal_stages,
)
from stabilize.persistence.postgres.operations import (
    get_processed_message_ids as _get_processed_message_ids,
)
from stabilize.persistence.postgres.operations import is_message_processed as _is_message_processed
from stabilize.persistence.postgres.operations import (
    mark_message_processed as _mark_message_processed,
)


class PostgresMaintenanceMixin:
    """Dedup bookkeeping and row reclamation, split out of the store body."""

    _pool: Any

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        return _is_message_processed(self._pool, message_id)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        _mark_message_processed(self._pool, message_id, handler_type, execution_id)

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """Clean up old processed message records."""
        return _cleanup_old_processed_messages(self._pool, max_age_hours)

    def cleanup_completed_stage_claims(self) -> int:
        """Delete stage claims of executions in terminal states."""
        return _cleanup_claims(self._pool)

    def cleanup_buffered_signals(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """Strip unconsumable WCP-24 signal buffers from stage contexts."""
        return _cleanup_buffered_signals(self._pool, only_complete, statuses)

    def get_processed_message_ids(self, limit: int | None = None) -> list[str] | None:
        """Return processed message IDs, for hydrating an in-memory dedup cache."""
        return _get_processed_message_ids(self._pool, limit)

    def count_buffered_signal_stages(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """Count stage rows carrying a WCP-24 signal buffer."""
        return _count_buffered_signal_stages(self._pool, only_complete, statuses)
