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

    def is_paused(self) -> bool:
        """Check if currently paused."""
        return self.pause_time is not None and self.resume_time is None

@dataclass
class Workflow:
    """
    Represents a pipeline execution.

    This is the top-level container for all execution state. It holds all stages
    and tracks the overall execution status.

    Attributes:
        id: Unique identifier (ULID)
        type: PIPELINE or ORCHESTRATION
        application: Application name this pipeline belongs to
        name: Pipeline name
        status: Current execution status
        stages: All stages in this execution (including synthetic)
        trigger: Trigger information
        start_time: Epoch milliseconds when execution started
        end_time: Epoch milliseconds when execution completed
        start_time_expiry: If not started by this time, cancel
        is_canceled: Whether execution has been canceled
        canceled_by: User who canceled the execution
        cancellation_reason: Reason for cancellation
        paused: Pause details if execution is paused
        pipeline_config_id: ID of the pipeline configuration
        is_limit_concurrent: Whether to limit concurrent executions
        max_concurrent_executions: Max concurrent executions allowed
        keep_waiting_pipelines: Keep queued pipelines on cancel
        origin: Origin of the execution (e.g., "api", "deck")
    """
    id: str = field(default_factory=_generate_execution_id)
    type: WorkflowType = WorkflowType.PIPELINE
    application: str = ''
    name: str = ''
    status: WorkflowStatus = WorkflowStatus.NOT_STARTED
    stages: list[StageExecution] = field(default_factory=list)
    trigger: Trigger = field(default_factory=Trigger)
    start_time: int | None = None
    end_time: int | None = None
    start_time_expiry: int | None = None
    is_canceled: bool = False
    canceled_by: str | None = None
    cancellation_reason: str | None = None
    paused: PausedDetails | None = None
    pipeline_config_id: str | None = None
    is_limit_concurrent: bool = False
    max_concurrent_executions: int = 0
    keep_waiting_pipelines: bool = False
    origin: str = 'unknown'

    def __post_init__(self) -> None:
        """Set execution reference on all stages after construction."""
        for stage in self.stages:
            stage._execution = self

    def add_stage(self, stage: StageExecution) -> None:
        """Add a stage to this execution."""
        stage._execution = self
        self.stages.append(stage)

    def remove_stage(self, stage_id: str) -> None:
        """Remove a stage from this execution."""
        self.stages = [s for s in self.stages if s.id != stage_id]

    def stage_by_id(self, stage_id: str) -> StageExecution:
        """
        Get a stage by its ID.

        Raises:
            ValueError: If stage not found
        """
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        raise ValueError(f"Stage {stage_id} not found")
