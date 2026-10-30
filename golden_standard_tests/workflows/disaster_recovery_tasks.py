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
    TaskResult,
)
from stabilize.tasks.interface import RetryableTask

# =============================================================================
# File paths - created once per test run
# =============================================================================


def get_temp_files(execution_id: str) -> tuple[str, str]:
    """Get temp file paths for output and retry flag, unique per execution."""
    temp_dir = tempfile.gettempdir()
    output_file = os.path.join(temp_dir, f"golden_dr_output_{execution_id}.txt")
    retry_flag = os.path.join(temp_dir, f"golden_dr_retry_{execution_id}.flag")
    return output_file, retry_flag


# =============================================================================
# Custom Task Implementations - Write to Files
# =============================================================================


class DRSetupTask(Task):
    """Phase 1: Setup - creates output file with header and first token."""

    def execute(self, stage: StageExecution) -> TaskResult:
        output_file, retry_flag = get_temp_files(stage.execution.id)

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
            f"printf '\\n::BRANCH_A_SUCCESS' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\n::SYNC_GATE_PASSED' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\nProcessing {item}...' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\nValidating {item}...' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\n::SWITCH_CASE_ANIMALS' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\n::SWITCH_JOINED' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
            f"printf '\\nWhile loop iteration {counter}' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
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
        subprocess.run(
            f"printf '\\n::WORKFLOW_END' >> '{output_file}'",
            shell=True,
            capture_output=True,
            text=True,
        )

        # Read the entire file content
        result = subprocess.run(f"cat '{output_file}'", shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return TaskResult.success(outputs={"final_result": result.stdout.strip()})
        else:
            return TaskResult.terminal(error=f"Read failed: {result.stderr}")
