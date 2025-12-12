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

class CompensationTask(Task):
    """
    Branch B Compensation: Runs after TimeoutTask fails.

    Outputs the compensation token.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase2b_token": "PHASE2B_TIMEOUT_COMPENSATED"})

class EventEmitterTask(Task):
    """
    Branch C: Simulates an event emitter.

    Just completes successfully - downstream stages depend on this.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"event_emitted": True})

class EventReceiverTask(Task):
    """
    Branch D: Receives event from Branch C.

    This stage depends on Branch C (event emitter), so it only runs
    after the event is "emitted" (i.e., Branch C completes).
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase3_token": "PHASE3_EVENT_RECEIVED"})

class JoinGateTask(Task):
    """
    Phase 4: Join gate that waits for all branches.

    This stage depends on all parallel branches completing.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase4_token": "PHASE4_SYNC_GATE_PASSED"})

class LoopIterationTask(Task):
    """
    Phase 5: Loop iteration with conditional logic.

    Reads counter from stage context and outputs based on value:
    - counter == 2: PHASE5_LOOP_IF_2
    - counter != 2: PHASE5_LOOP_ELSE_{counter}
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        counter = stage.context.get("counter", 1)

        if counter == 2:
            token = "PHASE5_LOOP_IF_2"
        else:
            token = f"PHASE5_LOOP_ELSE_{counter}"

        output_key = f"loop{counter}_token"
        return TaskResult.success(outputs={output_key: token})

class FinalizeTask(Task):
    """
    Phase 6: Finalize - collect all tokens and assemble result.

    Reads all tokens from ancestor outputs in the correct order
    and produces the final result string.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)

        # Collect tokens in expected order
        tokens = [
            context.get("phase1_token", "MISSING_PHASE1"),
            context.get("phase2a_token", "MISSING_PHASE2A"),
            context.get("phase2b_token", "MISSING_PHASE2B"),
            context.get("phase3_token", "MISSING_PHASE3"),
            context.get("phase4_token", "MISSING_PHASE4"),
            context.get("loop1_token", "MISSING_LOOP1"),
            context.get("loop2_token", "MISSING_LOOP2"),
            context.get("loop3_token", "MISSING_LOOP3"),
            "PHASE6_END",
        ]

        result = "::".join(tokens)
        return TaskResult.success(outputs={"final_result": result})
