from stabilize import (
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.context.stage_context import StageContext
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import setup_stabilize

class SetupTask(Task):
    """Phase 1: Setup task that outputs PHASE1_SETUP."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase1_token": "PHASE1_SETUP"})

class RetryTask(Task):
    """Branch A: Simulates retry - first call returns RUNNING, second returns SUCCESS."""
    _attempts: dict[str, int] = {}
