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

    def execute(self, stage: StageExecution) -> TaskResult:
        stage_id = stage.id
        attempts = RetryTask._attempts.get(stage_id, 0)
        RetryTask._attempts[stage_id] = attempts + 1

        if attempts < 1:
            return TaskResult.running(context={"retry_attempt": attempts + 1})

        return TaskResult.success(outputs={"phase2a_token": "PHASE2A_RETRY_SUCCESS"})

class TimeoutTask(Task):
    """Branch B: Simulates timeout with FAILED_CONTINUE."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue(
            error="Task timed out",
            outputs={"phase2b_timed_out": True},
        )

class CompensationTask(Task):
    """Branch B Compensation: Runs after TimeoutTask fails."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase2b_token": "PHASE2B_TIMEOUT_COMPENSATED"})

class EventEmitterTask(Task):
    """Branch C: Event emitter - just succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"event_emitted": True})

class EventReceiverTask(Task):
    """Branch D: Receives event from Branch C."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase3_token": "PHASE3_EVENT_RECEIVED"})

class JoinGateTask(Task):
    """Phase 4: Join gate that waits for all branches."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase4_token": "PHASE4_SYNC_GATE_PASSED"})

class LoopIterationTask(Task):
    """Phase 5: Loop iteration with conditional logic."""
