"""
Frozen state snapshots for read-only stage/task inspection.

Provides immutable snapshots of stage and task state for:
- Thread-safe concurrent access
- Caching without mutation risk
- Audit logging and debugging
- Pass-by-value to external systems

Usage:
    snapshot = stage.state_snapshot()
    # snapshot is immutable and safe to pass around
    log_audit_event(snapshot.id, snapshot.status, snapshot.version)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.status import WorkflowStatus


@dataclass(frozen=True)
class StageStateSnapshot:
    """Frozen, read-only view of stage state.

    Use this when you need to:
    - Pass stage state to external systems safely
    - Cache stage state without risk of mutation
    - Log or audit stage state at a point in time
    - Compare state before/after an operation

    Attributes:
        id: Unique stage identifier (ULID)
        ref_id: Reference ID for DAG relationships
        status: Current workflow status
        version: Optimistic locking version
        context: Frozen copy of stage context (inputs/runtime state)
        outputs: Frozen copy of stage outputs (values for downstream)
        start_time: Epoch milliseconds when stage started
        end_time: Epoch milliseconds when stage completed
        parent_stage_id: Parent stage ID for synthetic stages
    """

    id: str
    ref_id: str
    status: WorkflowStatus
    version: int
    context: tuple[tuple[str, Any], ...]  # Frozen dict as tuple of tuples
    outputs: tuple[tuple[str, Any], ...]  # Frozen dict as tuple of tuples
    start_time: int | None = None
    end_time: int | None = None
    parent_stage_id: str | None = None

    @property
    def phase_version(self) -> tuple[str, int]:
        """Return (status_name, version) for display/logging."""
        return (self.status.name, self.version)

    def context_dict(self) -> dict[str, Any]:
        """Convert frozen context back to a mutable dict."""
        return dict(self.context)

    def outputs_dict(self) -> dict[str, Any]:
        """Convert frozen outputs back to a mutable dict."""
        return dict(self.outputs)


@dataclass(frozen=True)
class TaskStateSnapshot:
    """Frozen, read-only view of task state.

    Use this when you need to:
    - Pass task state to external systems safely
    - Log or audit task state at a point in time
    - Compare state before/after an operation

    Attributes:
        id: Unique task identifier (ULID)
        name: Human-readable task name
        status: Current workflow status
        version: Optimistic locking version
        implementing_class: Fully qualified class name
        start_time: Epoch milliseconds when task started
        end_time: Epoch milliseconds when task completed
        exception_details: Frozen copy of exception info if task failed
    """

    id: str
    name: str
    status: WorkflowStatus
    version: int
    implementing_class: str = ""
    start_time: int | None = None
    end_time: int | None = None
    exception_details: tuple[tuple[str, Any], ...] = ()

    @property
    def phase_version(self) -> tuple[str, int]:
        """Return (status_name, version) for display/logging."""
        return (self.status.name, self.version)

    def exception_dict(self) -> dict[str, Any]:
        """Convert frozen exception details back to a mutable dict."""
        return dict(self.exception_details)


def _freeze_dict(d: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    """Convert a dict to a frozen tuple of tuples.

    This makes the dict hashable and immutable for use in frozen dataclasses.
    Note: nested mutable objects within values are not frozen.

    Args:
        d: Dictionary to freeze

    Returns:
        Tuple of (key, value) tuples, sorted by key for consistency.
    """
    return tuple(sorted(d.items()))
