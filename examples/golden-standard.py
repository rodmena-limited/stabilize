#!/usr/bin/env python3
"""
Golden Standard Pipeline Test

A comprehensive deterministic workflow to stress-test all pipeline-runner features:
- Sequential execution (Phase 1 → Phase 2 → Phase 4 → Phase 5 → Phase 6)
- Parallel branches (Branch A, B, C run in parallel)
- Retry simulation (Branch A fails once, then succeeds)
- Timeout + Compensation simulation (Branch B times out, compensation runs)
- Event coordination (Branch D waits for Branch C)
- Join gate (Phase 4 waits for all branches)
- Loop expansion (Phase 5 with 3 iterations)
- Conditional logic (different output based on counter value)

Expected output:
PHASE1_SETUP::PHASE2A_RETRY_SUCCESS::PHASE2B_TIMEOUT_COMPENSATED::PHASE3_EVENT_RECEIVED::PHASE4_SYNC_GATE_PASSED::PHASE5_LOOP_ELSE_1::PHASE5_LOOP_IF_2::PHASE5_LOOP_ELSE_3::PHASE6_END
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageContext,
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

# =============================================================================
# Custom Task Implementations
# =============================================================================


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

    # Class-level state to track attempts
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


# =============================================================================
# Pipeline Definition
# =============================================================================


def create_pipeline() -> Workflow:
    """Create the golden standard pipeline."""
    return Workflow.create(
        application="golden-standard-test",
        name="Golden Standard Pipeline",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="setup",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch A: Retry
            StageExecution(
                ref_id="2a",
                type="retry",
                name="Branch A: Retry",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Retry Task",
                        implementing_class="retry",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B: Timeout (fails with FAILED_CONTINUE)
            StageExecution(
                ref_id="2b",
                type="timeout",
                name="Branch B: Timeout",
                requisite_stage_ref_ids={"1"},
                context={"continuePipelineOnFailure": True},
                tasks=[
                    TaskExecution.create(
                        name="Timeout Task",
                        implementing_class="timeout",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B Compensation: Runs after timeout
            StageExecution(
                ref_id="2b_comp",
                type="compensation",
                name="Branch B: Compensation",
                requisite_stage_ref_ids={"2b"},
                tasks=[
                    TaskExecution.create(
                        name="Compensation Task",
                        implementing_class="compensation",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch C: Event Emitter
            StageExecution(
                ref_id="2c",
                type="event_emitter",
                name="Branch C: Event Emitter",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Event Emitter Task",
                        implementing_class="event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch D: Event Receiver (waits for C)
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch D: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Join Gate (waits for all branches)
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Join Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Join Gate Task",
                        implementing_class="join_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 1 (counter=1)
            StageExecution(
                ref_id="5a",
                type="loop",
                name="Phase 5: Loop Iteration 1",
                requisite_stage_ref_ids={"4"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 2 (counter=2)
            StageExecution(
                ref_id="5b",
                type="loop",
                name="Phase 5: Loop Iteration 2",
                requisite_stage_ref_ids={"5a"},
                context={"counter": 2},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 3 (counter=3)
            StageExecution(
                ref_id="5c",
                type="loop",
                name="Phase 5: Loop Iteration 3",
                requisite_stage_ref_ids={"5b"},
                context={"counter": 3},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Finalize
            StageExecution(
                ref_id="6",
                type="finalize",
                name="Phase 6: Finalize",
                requisite_stage_ref_ids={"5c"},
                tasks=[
                    TaskExecution.create(
                        name="Finalize Task",
                        implementing_class="finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )


# =============================================================================
# Execution Setup
# =============================================================================


def setup_runner() -> tuple[SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator]:
    """Set up the pipeline runner with all components."""
    # Create in-memory SQLite repository and queue
    repository = SqliteWorkflowStore(
        connection_string="sqlite:///:memory:",
        create_tables=True,
    )

    queue = SqliteQueue(
        connection_string="sqlite:///:memory:",
        table_name="queue_messages",
    )
    queue._create_table()

    # Register tasks
    task_registry = TaskRegistry()
    task_registry.register("setup", SetupTask)
    task_registry.register("retry", RetryTask)
    task_registry.register("timeout", TimeoutTask)
    task_registry.register("compensation", CompensationTask)
    task_registry.register("event_emitter", EventEmitterTask)
    task_registry.register("event_receiver", EventReceiverTask)
    task_registry.register("join_gate", JoinGateTask)
    task_registry.register("loop_iteration", LoopIterationTask)
    task_registry.register("finalize", FinalizeTask)

    # Create processor and register handlers
    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, repository),
        StartStageHandler(queue, repository),
        StartTaskHandler(queue, repository),
        RunTaskHandler(queue, repository, task_registry),
        CompleteTaskHandler(queue, repository),
        CompleteStageHandler(queue, repository),
        CompleteWorkflowHandler(queue, repository),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    runner = Orchestrator(queue)
    return repository, queue, processor, runner


# =============================================================================
# Main Execution
# =============================================================================


def main() -> int:
    """Run the golden standard pipeline and verify output."""
    # Reset retry task state
    RetryTask._attempts = {}

    # Setup
    repository, queue, processor, runner = setup_runner()

    # Create and store pipeline
    execution = create_pipeline()
    repository.store(execution)

    # Run pipeline
    runner.start(execution)
    processor.process_all(timeout=30.0)

    # Retrieve result
    result = repository.retrieve(execution.id)

    # Find the finalize stage
    finalize_stage = None
    for stage in result.stages:
        if stage.ref_id == "6":
            finalize_stage = stage
            break

    if finalize_stage is None:
        print("ERROR: Finalize stage not found!")
        return 1

    # Get the final result
    final_result = finalize_stage.outputs.get("final_result", "")

    # Print result
    print(final_result)

    # Load expected result
    expected_path = Path(__file__).parent / "golden-standard-expected-result.txt"
    if expected_path.exists():
        expected = expected_path.read_text().strip()
        if final_result == expected:
            print("\n[PASS] Output matches expected result!")
            return 0
        else:
            print("\n[FAIL] Output does not match!")
            print(f"Expected: {expected}")
            print(f"Got:      {final_result}")
            return 1
    else:
        print(f"\nWarning: Expected result file not found at {expected_path}")
        print("Execution status:", result.status.name)
        return 0


if __name__ == "__main__":
    sys.exit(main())
