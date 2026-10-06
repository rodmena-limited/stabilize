"""Construction of the opt-in distributed task lease for RunTaskHandler."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.persistence.task_lease import TaskLeaseManager

logger = logging.getLogger(__name__)


def build_task_lease(repository: WorkflowStore) -> TaskLeaseManager | None:
    """Return a TaskLeaseManager when STABILIZE_TASK_LEASE is set, else None.

    Raises:
        TaskLeaseUnavailableError: leasing was requested and cannot be provided.
    """
    if os.environ.get("STABILIZE_TASK_LEASE", "").lower() not in ("1", "true", "yes"):
        return None

    from stabilize.persistence.task_lease import TaskLeaseManager, TaskLeaseUnavailableError

    raw_ttl = os.environ.get("STABILIZE_TASK_LEASE_TTL_SECONDS", "3600")
    try:
        ttl = float(raw_ttl)
    except ValueError as e:
        raise TaskLeaseUnavailableError(
            f"STABILIZE_TASK_LEASE is set but STABILIZE_TASK_LEASE_TTL_SECONDS={raw_ttl!r} "
            "is not a number, so single-execution leasing cannot be configured"
        ) from e

    try:
        lease = TaskLeaseManager(repository, ttl_seconds=ttl)
    except Exception as e:
        raise TaskLeaseUnavailableError(
            "STABILIZE_TASK_LEASE is set but the lease manager could not be "
            f"initialised ({type(e).__name__}: {e}). Leasing is what keeps a task "
            "from executing on two workers at once; starting without it would "
            "silently allow the double execution this setting exists to prevent. "
            "Grant the runtime role rights to create the task_leases table, or "
            "unset STABILIZE_TASK_LEASE."
        ) from e

    logger.info("Distributed task lease enabled (owner=%s)", lease.owner)
    return lease
