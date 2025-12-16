from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus

def _generate_execution_id() -> str:
    """Generate a unique execution ID using ULID."""
    import ulid

    return str(ulid.new())

class WorkflowType(Enum):
    """
    Type of execution.

    PIPELINE: A full pipeline execution
    ORCHESTRATION: An ad-hoc orchestration (single stage)
    """
    PIPELINE = 'PIPELINE'
    ORCHESTRATION = 'ORCHESTRATION'

@dataclass
class Trigger:
    """
    Trigger information for a pipeline execution.

    Contains details about what triggered the pipeline (manual, webhook, cron, etc.)
    and any parameters passed to the execution.
    """
    type: str = 'manual'
    user: str = 'anonymous'
    parameters: dict[str, Any] = field(default_factory=dict)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert trigger to dictionary for storage."""
        return {
            "type": self.type,
            "user": self.user,
            "parameters": self.parameters,
            "artifacts": self.artifacts,
            "payload": self.payload,
        }

    def from_dict(cls, data: dict[str, Any]) -> Trigger:
        """Create trigger from dictionary."""
        return cls(
            type=data.get("type", "manual"),
            user=data.get("user", "anonymous"),
            parameters=data.get("parameters", {}),
            artifacts=data.get("artifacts", []),
            payload=data.get("payload", {}),
        )

@dataclass
class PausedDetails:
    """
    Details about a paused execution.
    """
    paused_by: str = ''
    pause_time: int | None = None
    resume_time: int | None = None
    paused_ms: int = 0
