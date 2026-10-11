"""Default maintenance operations of a WorkflowStore: retention and purge."""

from __future__ import annotations


class MaintenanceDefaults:
    def cleanup_old_processed_messages(
        self,
        max_age_hours: float = 24.0,
        batch_size: int | None = None,
        max_rows: int | None = None,
    ) -> int:
        """
        Clean up old processed message records.

        Args:
            max_age_hours: Delete records older than this many hours
            batch_size: Rows deleted per transaction (backend default when None)
            max_rows: Stop after this many rows (no limit when None)

        Returns:
            Number of records deleted
        """
        return 0

    def completed_workflow_ids(
        self, completed_before_ms: int, limit: int, application: str | None = None
    ) -> list[str]:
        """Ids of complete workflows whose end_time is before completed_before_ms, oldest first."""
        raise NotImplementedError(f"{type(self).__name__} does not support purge")

    def purge_completed_workflows(
        self,
        completed_before_ms: int,
        batch_size: int = 500,
        max_workflows: int | None = None,
        application: str | None = None,
    ) -> int:
        """Delete complete workflows that ended before completed_before_ms, with their owned rows."""
        from stabilize.maintenance import purge_completed_workflows

        return purge_completed_workflows(
            self,
            None,
            completed_before_ms,
            batch_size=batch_size,
            max_workflows=max_workflows,
            application=application,
        )
