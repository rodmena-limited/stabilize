"""Default (no signal storage) behaviour of WorkflowStore's signal methods."""

from __future__ import annotations

from typing import Any


class SignalStoreDefaults:
    """Signal-buffer methods for a store without signal storage; backends override them."""

    def buffer_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str,
        signal_data: dict[str, Any] | None = None,
    ) -> int:
        """Buffer a persistent signal for a stage that has not yet suspended.

        Stored in the workflow_signals table rather than appended to
        stage_executions.context: the context copy grows unbounded (issue 15
        capped it at 1000 entries as a stopgap) and is visible to every consumer
        reading that column.

        Returns 0 on a backend without signal storage, which tells the caller to
        fall back to the context buffer.
        """
        return 0

    def consume_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Consume the oldest unconsumed buffered signal for a stage.

        Returns a mapping carrying signal_name and signal_data, or None when no
        signal is waiting or the backend has no signal storage.
        """
        return None

    def pending_signal_count(self, execution_id: str, stage_ref_id: str) -> int:
        """How many unconsumed signals are buffered for a stage."""
        return 0

    def discard_signals(self, execution_id: str, stage_ref_id: str) -> int:
        """Drop unconsumed signals for a stage that can no longer receive them.

        A stage entering a terminal status will never suspend again, so anything
        still buffered for it is undeliverable and would otherwise accumulate.
        """
        return 0

    def supports_signal_storage(self) -> bool:
        """Whether this store persists signals outside stage context.

        The buffering caller needs to distinguish "no signal waiting" from
        "this backend cannot store signals", because those require different
        behaviour and both otherwise look like a zero.
        """
        return False

    def count_buffered_signal_stages(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """
        Count stage rows carrying a WCP-24 persistent-signal buffer.

        Args:
            only_complete: When True, count only stages whose status is complete.

        Returns:
            Number of stage rows carrying a buffer (0 for stores without contexts)
        """
        return 0

    def cleanup_buffered_signals(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """
        Remove the WCP-24 persistent-signal buffer from stage contexts.

        Args:
            only_complete: When True, restrict removal to stages whose status
                is complete, whose buffer can never be consumed. When False,
                every stage carrying a buffer is stripped.

        Returns:
            Number of stage rows modified (0 for stores without contexts)
        """
        return 0
