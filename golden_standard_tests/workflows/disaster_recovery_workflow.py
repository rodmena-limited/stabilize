"""Disaster Recovery Golden Standard Workflow - File-Based Output.

Based on highway-workflow-engine/demo/disaster_recovery_simulation.py

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
    output_file = os.path.join(temp_dir, "golden_dr_output.txt")
    retry_flag = os.path.join(temp_dir, "golden_dr_retry.flag")
    return output_file, retry_flag


# =============================================================================
# Custom Task Implementations - Write to Files
# =============================================================================


class DRSetupTask(Task):
    """Phase 1: Setup - creates output file with header and first token."""

    def execute(self, stage: StageExecution) -> TaskResult:
        output_file, retry_flag = get_temp_files()

        # Clean up and write header + first token
        # Use printf with format string to avoid --- being interpreted as option
        result = subprocess.run(
            f"rm -f '{output_file}' '{retry_flag}' && "
            f"printf '%s\\n%s' '--- FINAL VERIFIABLE OUTPUT ---' 'WORKFLOW_START' > '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            return TaskResult.success(outputs={"output_file": output_file, "retry_flag": retry_flag})
        else:
            return TaskResult.terminal(error=f"Setup failed: {result.stderr}")


class DRRetryTask(RetryableTask):
    """Branch A: Tests retry mechanism.

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
        return timedelta(milliseconds=200)

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        retry_flag = context.get("retry_flag")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        stage_id = stage.id
        attempts = DRRetryTask._attempts.get(stage_id, 0) + 1
        DRRetryTask._attempts[stage_id] = attempts

        if attempts < 2:
            subprocess.run(f"touch '{retry_flag}'", shell=True)
            return TaskResult.running()

        result = subprocess.run(
            f"printf '\\n::BRANCH_A_SUCCESS' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRTimeoutTask(RetryableTask):
    """Branch B: Tests timeout mechanism.

    Timing: ~300ms timeout (completes second among parallel branches)
    """

    def get_timeout(self) -> timedelta:
        return timedelta(milliseconds=300)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()

    def on_timeout(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue(error="Task timed out as expected", outputs={"timeout_occurred": True})


class DRCompensationTask(Task):
    """Branch B Compensation: Appends compensation token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        timeout_occurred = context.get("timeout_occurred", False)

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        if not timeout_occurred:
            token = "\\n::ERROR_NO_TIMEOUT"
        else:
            token = "\\n::BRANCH_B_COMPENSATION_SUCCESS"

        result = subprocess.run(f"printf '{token}' >> '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DREventEmitterTask(RetryableTask):
    """Branch C: Produces event payload for receiver.

    Timing: ~500ms (so event chain completes last among parallel branches)
    """

    _emitter_attempts: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset_emitter(cls) -> None:
        cls._emitter_attempts = {}

    def get_timeout(self) -> timedelta:
        return timedelta(seconds=30)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=500)

    def execute(self, stage: StageExecution) -> TaskResult:
        stage_id = stage.id
        attempts = DREventEmitterTask._emitter_attempts.get(stage_id, 0) + 1
        DREventEmitterTask._emitter_attempts[stage_id] = attempts

        if attempts < 2:
            return TaskResult.running()

        return TaskResult.success(outputs={"event_payload": "EVENT_DATA_12345"})


class DREventReceiverTask(Task):
    """Branch C: Verifies event propagation and appends token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        event_payload = context.get("event_payload", "")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        if event_payload == "EVENT_DATA_12345":
            token = "\\n::BRANCH_C_EVENT_RECEIVED"
        else:
            token = f"\\n::ERROR_NO_EVENT_{event_payload}"

        result = subprocess.run(f"printf '{token}' >> '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRSyncGateTask(Task):
    """Phase 4: Appends sync gate token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\n::SYNC_GATE_PASSED' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRForeachProcessTask(Task):
    """Foreach: Process item - appends 'Processing {item}...' to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        item = stage.context.get("item", "unknown")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\nProcessing {item}...' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRForeachValidateTask(Task):
    """Foreach: Validate item - appends 'Validating {item}...' to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        item = stage.context.get("item", "unknown")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\nValidating {item}...' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRSwitchTask(Task):
    """Switch: Appends switch case token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\n::SWITCH_CASE_ANIMALS' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRSwitchJoinTask(Task):
    """Switch join: Appends switch joined token to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\n::SWITCH_JOINED' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRWhileLoopTask(Task):
    """While loop: Appends 'While loop iteration {counter}' to file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")
        counter = stage.context.get("counter", 0)

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        result = subprocess.run(
            f"printf '\\nWhile loop iteration {counter}' >> '{output_file}'", shell=True, capture_output=True, text=True
        )

        if result.returncode == 0:
            return TaskResult.success()
        else:
            return TaskResult.terminal(error=f"Write failed: {result.stderr}")


class DRFinalizeTask(Task):
    """Phase 8: Appends final token and reads file content."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)
        output_file = context.get("output_file")

        if not output_file:
            return TaskResult.terminal(error="output_file not in context")

        # Append final token
        subprocess.run(f"printf '\\n::WORKFLOW_END' >> '{output_file}'", shell=True, capture_output=True, text=True)

        # Read the entire file content
        result = subprocess.run(f"cat '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success(outputs={"final_result": result.stdout.strip()})
        else:
            return TaskResult.terminal(error=f"Read failed: {result.stderr}")


# =============================================================================
# Task Registration
# =============================================================================


def register_dr_tasks(registry: TaskRegistry) -> None:
    """Register all Disaster Recovery workflow tasks."""
    registry.register("dr_setup", DRSetupTask)
    registry.register("dr_retry", DRRetryTask)
    registry.register("dr_timeout", DRTimeoutTask)
    registry.register("dr_compensation", DRCompensationTask)
    registry.register("dr_event_emitter", DREventEmitterTask)
    registry.register("dr_event_receiver", DREventReceiverTask)
    registry.register("dr_sync_gate", DRSyncGateTask)
    registry.register("dr_foreach_process", DRForeachProcessTask)
    registry.register("dr_foreach_validate", DRForeachValidateTask)
    registry.register("dr_switch", DRSwitchTask)
    registry.register("dr_switch_join", DRSwitchJoinTask)
    registry.register("dr_while_loop", DRWhileLoopTask)
    registry.register("dr_finalize", DRFinalizeTask)


# =============================================================================
# Workflow Definition
# =============================================================================


def create_disaster_recovery_workflow() -> Workflow:
    """Create the Disaster Recovery golden standard workflow."""
    return Workflow.create(
        application="golden-standard-disaster-recovery",
        name="Disaster Recovery Golden Standard Workflow",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="dr_setup",
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
                        implementing_class="dr_retry",
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
                        implementing_class="dr_timeout",
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
                        implementing_class="dr_compensation",
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
                        implementing_class="dr_event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch C: Event Receiver
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch C: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="dr_event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Sync Gate
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Sync Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Sync Gate Task",
                        implementing_class="dr_sync_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_A.log process
            StageExecution(
                ref_id="5a",
                type="foreach",
                name="Foreach: Process file_A.log",
                requisite_stage_ref_ids={"4"},
                context={"item": "file_A.log"},
                tasks=[
                    TaskExecution.create(
                        name="Process Task",
                        implementing_class="dr_foreach_process",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_A.log validate
            StageExecution(
                ref_id="5b",
                type="foreach",
                name="Foreach: Validate file_A.log",
                requisite_stage_ref_ids={"5a"},
                context={"item": "file_A.log"},
                tasks=[
                    TaskExecution.create(
                        name="Validate Task",
                        implementing_class="dr_foreach_validate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_B.log process
            StageExecution(
                ref_id="5c",
                type="foreach",
                name="Foreach: Process file_B.log",
                requisite_stage_ref_ids={"5b"},
                context={"item": "file_B.log"},
                tasks=[
                    TaskExecution.create(
                        name="Process Task",
                        implementing_class="dr_foreach_process",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_B.log validate
            StageExecution(
                ref_id="5d",
                type="foreach",
                name="Foreach: Validate file_B.log",
                requisite_stage_ref_ids={"5c"},
                context={"item": "file_B.log"},
                tasks=[
                    TaskExecution.create(
                        name="Validate Task",
                        implementing_class="dr_foreach_validate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Switch
            StageExecution(
                ref_id="6",
                type="switch",
                name="Phase 6: Switch on Category",
                requisite_stage_ref_ids={"5d"},
                tasks=[
                    TaskExecution.create(
                        name="Switch Task",
                        implementing_class="dr_switch",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Switch Join
            StageExecution(
                ref_id="6_join",
                type="switch_join",
                name="Phase 6: Switch Join",
                requisite_stage_ref_ids={"6"},
                tasks=[
                    TaskExecution.create(
                        name="Switch Join Task",
                        implementing_class="dr_switch_join",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 7: While Loop - iteration 0
            StageExecution(
                ref_id="7a",
                type="while",
                name="Phase 7: While Loop Iteration 0",
                requisite_stage_ref_ids={"6_join"},
                context={"counter": 0},
                tasks=[
                    TaskExecution.create(
                        name="While Loop Task",
                        implementing_class="dr_while_loop",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 7: While Loop - iteration 1
            StageExecution(
                ref_id="7b",
                type="while",
                name="Phase 7: While Loop Iteration 1",
                requisite_stage_ref_ids={"7a"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="While Loop Task",
                        implementing_class="dr_while_loop",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 8: Finalize
            StageExecution(
                ref_id="8",
                type="finalize",
                name="Phase 8: Finalize",
                requisite_stage_ref_ids={"7b"},
                tasks=[
                    TaskExecution.create(
                        name="Finalize Task",
                        implementing_class="dr_finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )
