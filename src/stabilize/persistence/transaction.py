"""
Transaction helper service.

Encapsulates common transaction patterns to reduce code duplication
in handlers.

Includes deadlock retry logic using resilient-circuit for database
operations that may encounter lock contention.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING

from resilient_circuit import ExponentialDelay, RetryWithBackoffPolicy
from resilient_circuit.exceptions import RetryLimitReached

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.messages import Message
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)

# Deadlock retry configuration using resilient-circuit
DEADLOCK_BACKOFF = ExponentialDelay(
    min_delay=timedelta(milliseconds=50),
    max_delay=timedelta(seconds=5),
    factor=2,
    jitter=0.2,
)


def is_deadlock_error(error: Exception) -> bool:
    """Check if an error is a database deadlock or lock contention error.

    Detects deadlock conditions for:
    - SQLite: "database is locked"
    - PostgreSQL: SQLSTATE 40001 (serialization failure) or 40P01 (deadlock)
    - Generic: ConcurrencyError from stabilize

    Args:
        error: The exception to check

    Returns:
        True if the error indicates a deadlock/lock contention
    """
    # Check for SQLite lock errors
    if isinstance(error, sqlite3.OperationalError):
        error_msg = str(error).lower()
        if "database is locked" in error_msg or "locked" in error_msg:
            return True

    # Check for PostgreSQL deadlock/serialization errors
    # psycopg2 uses pgcode attribute
    pgcode = getattr(error, "pgcode", None)
    if pgcode in ("40001", "40P01"):  # serialization_failure, deadlock_detected
        return True

    # Check error message for common deadlock patterns
    error_msg = str(error).lower()
    deadlock_patterns = [
        "deadlock",
        "lock wait timeout",
        "database is locked",
        "serialization failure",
        "could not serialize",
        "concurrent update",
    ]
    if any(pattern in error_msg for pattern in deadlock_patterns):
        return True

    # Check for stabilize ConcurrencyError (import locally to avoid circular import)
    try:
        from stabilize.errors import ConcurrencyError

        if isinstance(error, ConcurrencyError):
            return True
    except ImportError:
        pass

    return False


# Deadlock retry policy
DEADLOCK_RETRY_POLICY = RetryWithBackoffPolicy(
    max_retries=5,
    backoff=DEADLOCK_BACKOFF,
    should_handle=is_deadlock_error,
)


class TransactionHelper:
    """Helper for executing atomic transactions with common patterns.

    Includes automatic retry on deadlock/lock contention errors using
    exponential backoff.
    """

    def __init__(self, repository: WorkflowStore, queue: Queue) -> None:
        self.repository = repository
        self.queue = queue

    def execute_atomic(
        self,
        stage: StageExecution | None = None,
        source_message: Message | None = None,
        messages_to_push: Sequence[tuple[Message, int | None]] | None = None,
        handler_name: str = "UnknownHandler",
    ) -> None:
        """
        Execute an atomic transaction to update state and queue messages.

        Automatically retries on deadlock/lock contention errors with
        exponential backoff.

        Args:
            stage: Optional stage to store/update
            source_message: Optional message to mark as processed
            messages_to_push: List of (message, delay_seconds) tuples to push
            handler_name: Name of the handler for logging/metrics

        Raises:
            Exception: If the transaction fails after all retries
        """
        messages_to_push = messages_to_push or []

        @DEADLOCK_RETRY_POLICY
        def _execute() -> None:
            with self.repository.transaction(self.queue) as txn:
                if stage:
                    txn.store_stage(stage)

                if source_message and source_message.message_id:
                    execution_id = getattr(source_message, "execution_id", None)
                    txn.mark_message_processed(
                        message_id=source_message.message_id,
                        handler_type=handler_name,
                        execution_id=execution_id,
                    )

                for msg, delay in messages_to_push:
                    txn.push_message(msg, delay or 0)

        try:
            _execute()
        except RetryLimitReached as e:
            logger.error(
                "Transaction failed after max retries due to lock contention: %s",
                handler_name,
            )
            # Re-raise the original exception
            raise e.__cause__ if e.__cause__ else e from e
