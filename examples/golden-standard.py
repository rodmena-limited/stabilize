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
