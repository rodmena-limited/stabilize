"""Status scoping for WCP-24 signal-buffer reclamation."""

from __future__ import annotations

from stabilize.models.status import WorkflowStatus


class UnknownStatusError(ValueError):
    """Raised when a caller names a status the engine does not define."""


def complete_status_names() -> list[str]:
    """Names of every status from which a stage can never consume a buffer."""
    return [s.name for s in WorkflowStatus if s.is_complete]


def signal_status_filter(
    only_complete: bool,
    statuses: list[str] | None = None,
) -> list[str] | None:
    """Resolve the status filter for a reclamation call.

    Returns None when every stage carrying a buffer is in scope.

    Raises:
        UnknownStatusError: If a named status is not a WorkflowStatus, so a typo
            selects nothing rather than silently matching no rows and reporting
            a successful cleanup of zero.
    """
    if statuses:
        known = {s.name for s in WorkflowStatus}
        unknown = [s for s in statuses if s not in known]
        if unknown:
            raise UnknownStatusError(
                f"unknown status {', '.join(sorted(unknown))}; "
                f"expected one of {', '.join(sorted(known))}"
            )
        return list(statuses)
    if only_complete:
        return complete_status_names()
    return None
