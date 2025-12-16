from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus

def _generate_task_id() -> str:
    """Generate a unique task ID using ULID."""
    import ulid

    return str(ulid.new())
