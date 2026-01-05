"""V2 Golden Standard Workflow - File-Based Output.

Based on highway-workflow-engine/demo/v2.py

This workflow writes to an actual file (non-ACID medium) to test engine correctness.
Each task appends its token to the output file using shell commands.
The final file content must match the expected golden standard.

If the engine has bugs (wrong order, missing retries, broken timeout),
the file content will be wrong and the test will fail.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from datetime import timedelta
from typing import ClassVar

from stabilize import (
    StageContext,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.tasks.interface import RetryableTask

# =============================================================================
# File paths - created once per test run
# =============================================================================


def get_temp_files() -> tuple[str, str]:
    """Get temp file paths for output and retry flag."""
    temp_dir = tempfile.gettempdir()
    output_file = os.path.join(temp_dir, "golden_v2_output.txt")
    retry_flag = os.path.join(temp_dir, "golden_v2_retry.flag")
    return output_file, retry_flag


# =============================================================================
# Custom Task Implementations - Write to Files
# =============================================================================


class SetupTask(Task):
    """Phase 1: Setup - creates output file with first token."""

    def execute(self, stage: StageExecution) -> TaskResult:
        output_file, retry_flag = get_temp_files()

        # Clean up and write first token (no :: prefix for first token)
        result = subprocess.run(
            f"rm -f '{output_file}' '{retry_flag}' && printf 'PHASE1_SETUP' > '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            return TaskResult.success(outputs={"output_file": output_file, "retry_flag": retry_flag})
        else:
            return TaskResult.terminal(error=f"Setup failed: {result.stderr}")


class RetryTask(RetryableTask):
    """Branch A: Tests retry mechanism by writing to file.

    First attempt: returns RUNNING (engine must re-execute)
    Second attempt: appends token to file

    Timing: ~200ms (completes first among parallel branches)
    """

    _attempts: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset(cls) -> None:
        cls._attempts = {}

    def get_timeout(self) -> timedelta:
        return timedelta(seconds=30)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=200)  # Completes at ~200ms

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        retry_flag = context.get("retry_flag")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        stage_id = stage.id
        attempts = RetryTask._attempts.get(stage_id, 0) + 1
        RetryTask._attempts[stage_id] = attempts

        if attempts < 2:
            # First attempt: create flag, return RUNNING to trigger re-execution
            subprocess.run(f"touch '{retry_flag}'", shell=True)
            return TaskResult.running()

        # Second attempt: append token to file
        result = subprocess.run(
            f"printf '::PHASE2A_RETRY_SUCCESS' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class TimeoutTask(RetryableTask):
    """Branch B: Tests timeout mechanism.

    Returns RUNNING forever. Engine must timeout this task.
    on_timeout returns FAILED_CONTINUE so workflow continues.

    Timing: ~300ms timeout (completes second among parallel branches)
    """

    def get_timeout(self) -> timedelta:
        return timedelta(milliseconds=300)  # Times out at ~300ms

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        # Never completes - engine MUST timeout this task
        return TaskResult.running()

    def on_timeout(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue(error="Task timed out as expected", outputs={"timeout_occurred": True})


class CompensationTask(Task):
    """Branch B Compensation: Appends compensation token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        timeout_occurred = context.get("timeout_occurred", False)

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        if not timeout_occurred:
            # Timeout didn't occur - this is an engine bug
            result = subprocess.run(
                f"printf '::ERROR_NO_TIMEOUT' >> '{output_file}'", shell=True, capture_output=True, text=True
            )
        else:
            result = subprocess.run(
                f"printf '::PHASE2B_TIMEOUT_COMPENSATED' >> '{output_file}'", shell=True, capture_output=True, text=True
            )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class EventEmitterTask(RetryableTask):
    """Branch C: Produces event payload for receiver.

    Takes time before completing to ensure event chain finishes last.
    Timing: ~500ms (so D completes after A and B_comp)
    """

    _emitter_attempts: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset_emitter(cls) -> None:
        cls._emitter_attempts = {}

    def get_timeout(self) -> timedelta:
        return timedelta(seconds=30)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=500)  # Wait 500ms before completing

    def execute(self, stage: StageExecution) -> TaskResult:
        stage_id = stage.id
        attempts = EventEmitterTask._emitter_attempts.get(stage_id, 0) + 1
        EventEmitterTask._emitter_attempts[stage_id] = attempts

        if attempts < 2:
            # First attempt: return RUNNING to wait
            return TaskResult.running()

        # Second attempt: complete with event payload
        return TaskResult.success(outputs={"event_payload": "EVENT_DATA_12345"})


class EventReceiverTask(Task):
    """Branch D: Verifies event propagation and appends token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        event_payload = context.get("event_payload", "")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        if event_payload == "EVENT_DATA_12345":
            token = "::PHASE3_EVENT_RECEIVED"
        else:
            token = f"::ERROR_NO_EVENT_{event_payload}"

        result = subprocess.run(f"printf '{token}' >> '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class JoinGateTask(Task):
    """Phase 4: Verifies all branches completed, appends token."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '::PHASE4_SYNC_GATE_PASSED' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class LoopIterationTask(Task):
    """Phase 5: Loop iteration - appends conditional token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        counter = stage.context.get("counter", 0)

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        # Conditional logic - different token based on counter
        if counter == 2:
            token = f"::PHASE5_LOOP_IF_{counter}"
        else:
            token = f"::PHASE5_LOOP_ELSE_{counter}"

        result = subprocess.run(f"printf '{token}' >> '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class FinalizeTask(Task):
    """Phase 6: Appends final token and reads file content."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        # Append final token
        subprocess.run(f"printf '::PHASE6_END' >> '{output_file}'", shell=True, capture_output=True, text=True)

        # Read the entire file content
        result = subprocess.run(f"cat '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success(outputs={"final_result": result.stdout.strip()})
        else:
            return TaskResult.terminal(error=f"Read failed: {result.stderr}")


# =============================================================================
# Task Registration
# =============================================================================


def register_v2_tasks(registry: TaskRegistry) -> None:
    """Register all V2 workflow tasks."""
    registry.register("v2_setup", SetupTask)
    registry.register("v2_retry", RetryTask)
    registry.register("v2_timeout", TimeoutTask)
    registry.register("v2_compensation", CompensationTask)
    registry.register("v2_event_emitter", EventEmitterTask)
    registry.register("v2_event_receiver", EventReceiverTask)
    registry.register("v2_join_gate", JoinGateTask)
    registry.register("v2_loop_iteration", LoopIterationTask)
    registry.register("v2_finalize", FinalizeTask)


# =============================================================================
# Workflow Definition
# =============================================================================


def create_v2_workflow() -> Workflow:
    """Create the V2 golden standard workflow."""
    return Workflow.create(
        application="golden-standard-v2",
        name="V2 Golden Standard Workflow",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="v2_setup",
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
                        implementing_class="v2_retry",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B: Timeout
            StageExecution(
                ref_id="2b",
                type="timeout",
                name="Branch B: Timeout",
                requisite_stage_ref_ids={"1"},
                context={"continuePipelineOnFailure": True},
                tasks=[
                    TaskExecution.create(
                        name="Timeout Task",
                        implementing_class="v2_timeout",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B Compensation
            StageExecution(
                ref_id="2b_comp",
                type="compensation",
                name="Branch B: Compensation",
                requisite_stage_ref_ids={"2b"},
                tasks=[
                    TaskExecution.create(
                        name="Compensation Task",
                        implementing_class="v2_compensation",
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
                        implementing_class="v2_event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch D: Event Receiver
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch D: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="v2_event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Join Gate
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Join Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Join Gate Task",
                        implementing_class="v2_join_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 1
            StageExecution(
                ref_id="5a",
                type="loop",
                name="Phase 5: Loop Iteration 1",
                requisite_stage_ref_ids={"4"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 2
            StageExecution(
                ref_id="5b",
                type="loop",
                name="Phase 5: Loop Iteration 2",
                requisite_stage_ref_ids={"5a"},
                context={"counter": 2},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 3
            StageExecution(
                ref_id="5c",
                type="loop",
                name="Phase 5: Loop Iteration 3",
                requisite_stage_ref_ids={"5b"},
                context={"counter": 3},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
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
                        implementing_class="v2_finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )
