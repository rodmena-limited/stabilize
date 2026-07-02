"""Abstract transaction interface for atomic store + queue operations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue.messages import Message


class StoreTransaction(ABC):
    """Abstract interface for atomic store + queue transactions.

    Implementations must ensure that store_stage() and push_message()
    are committed atomically - either both succeed or both are rolled back.
    """

    @property
    def is_atomic(self) -> bool:
        """Whether this transaction provides true database-level atomicity.

        Returns True if all operations within this transaction are guaranteed
        to either all succeed or all fail atomically (no partial state).

        Override this in subclasses. Default is False for safety.
        """
        return False

    @abstractmethod
    def store_stage(self, stage: StageExecution, expected_phase: str | None = None) -> None:
        """Store or update a stage within the transaction.

        Args:
            stage: The stage to store
            expected_phase: If provided, adds status check to WHERE clause for
                           phase-aware optimistic locking (CAS pattern).
        """
        pass

    @abstractmethod
    def update_workflow_status(self, workflow: Workflow) -> None:
        """Update workflow status within the transaction."""
        pass

    @abstractmethod
    def push_message(self, message: Message, delay: float = 0) -> None:
        """Push a message to the queue within the transaction."""
        pass

    @abstractmethod
    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """
        Mark a message as successfully processed within the transaction.

        This ensures that message processing is only marked complete if
        the transaction commits successfully.
        """
        pass

    def acquire_claim(
        self,
        execution_id: str,
        claim_key: str,
        stage_id: str,
        steal_if_owner_terminal: bool = False,
    ) -> bool:
        """Atomically acquire a named claim for a stage within the transaction.

        Used to serialize sibling stages competing for the same resource —
        mutex keys (WCP-17/39/40) and deferred-choice groups (WCP-16) — via a
        unique (execution_id, claim_key) row. The per-stage-row CAS cannot do
        this: two DIFFERENT stages each pass their own row's CAS.

        Args:
            execution_id: The workflow execution
            claim_key: Namespaced key, e.g. "mutex:<key>" or "choice:<group>"
            stage_id: The stage attempting to acquire
            steal_if_owner_terminal: Transfer the claim when the current
                owner stage has reached a terminal status (mutex release
                semantics). Leave False for claims that are decided once
                (deferred choice).

        Returns:
            True if this stage holds the claim (newly acquired, already
            owned, or transferred); False if another live stage holds it.

        Note:
            The default implementation returns True (no enforcement) so
            custom store transactions without claim support keep their
            existing behavior.
        """
        return True
