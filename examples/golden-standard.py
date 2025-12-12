from __future__ import annotations
import sys
from pathlib import Path
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.context.stage_context import StageContext

class SetupTask(Task):
    """Phase 1: Setup task that outputs PHASE1_SETUP."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase1_token": "PHASE1_SETUP"})

class RetryTask(Task):
    """
    Branch A: Simulates a task that fails once and succeeds on retry.

    Uses RUNNING status to simulate polling behavior - first call returns
    RUNNING (re-queued), second call returns SUCCESS.
    """
    _attempts: dict[str, int] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        stage_id = stage.id
        attempts = RetryTask._attempts.get(stage_id, 0)
        RetryTask._attempts[stage_id] = attempts + 1

        if attempts < 1:
            # First attempt: return RUNNING to simulate "fail and retry"
            return TaskResult.running(context={"retry_attempt": attempts + 1})

        # Second attempt: success
        return TaskResult.success(outputs={"phase2a_token": "PHASE2A_RETRY_SUCCESS"})

class TimeoutTask(Task):
    """
    Branch B: Simulates a task that times out.

    Returns FAILED_CONTINUE to allow pipeline to continue while
    indicating this task failed. The compensation stage will run.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        # Simulate timeout by returning FAILED_CONTINUE
        # This allows the compensation stage to run
        return TaskResult.failed_continue(
            error="Task timed out",
            outputs={"phase2b_timed_out": True},
        )
