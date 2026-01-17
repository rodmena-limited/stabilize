"""
Process-isolated task execution.

This module provides a mechanism to execute tasks in separate processes.
This protects the main worker process from:
1. Segmentation faults (e.g., in C extensions)
2. Memory leaks or OOM kills
3. Global interpreter lock (GIL) contention
4. Stuck tasks (force kill capability)

Usage:
    executor = ProcessIsolatedTaskExecutor(timeout_seconds=300)
    result = executor.execute(task, stage)
"""

from __future__ import annotations

import logging
import multiprocessing
import traceback
from dataclasses import dataclass
from typing import Any

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


@dataclass
class ProcessResult:
    """Result returned from the worker process."""

    success: bool
    result: TaskResult | None = None
    error: str | None = None
    traceback: str | None = None


def _worker_wrapper(
    task: Task,
    stage_data: dict[str, Any],
    queue: multiprocessing.Queue,
) -> None:
    """
    Worker function that runs in the separate process.

    We pass raw data instead of full StageExecution object to avoid
    pickling issues with complex objects attached to stage (like execution ref).
    We reconstruct a minimal StageExecution for the task.
    """
    try:
        # Reconstruct minimal stage context
        stage = StageExecution(
            id=stage_data["id"],
            ref_id=stage_data["ref_id"],
            type=stage_data["type"],
            name=stage_data["name"],
            status=WorkflowStatus[stage_data["status"]],
            context=stage_data["context"],
            outputs=stage_data["outputs"],
        )

        # Execute task
        result = task.execute(stage)

        # Send back success
        queue.put(ProcessResult(success=True, result=result))

    except Exception as e:
        # Send back failure
        queue.put(ProcessResult(success=False, error=str(e), traceback=traceback.format_exc()))


class ProcessIsolatedTaskExecutor:
    """Executes tasks in isolated processes."""

    def __init__(self, timeout_seconds: float = 300.0) -> None:
        self.timeout_seconds = timeout_seconds

    def execute(self, task: Task, stage: StageExecution) -> TaskResult:
        """
        Execute the task in a separate process.

        Args:
            task: The task instance to execute
            stage: The stage execution context

        Returns:
            The TaskResult from the task

        Raises:
            TimeoutError: If execution exceeds timeout
            RuntimeError: If process crashes or returns invalid result
        """
        # Prepare stage data for pickling (avoid circular refs in full objects)
        stage_data = {
            "id": stage.id,
            "ref_id": stage.ref_id,
            "type": stage.type,
            "name": stage.name,
            "status": stage.status.name,
            "context": stage.context,
            "outputs": stage.outputs,
        }

        # Use Spawn context for safety (default on macOS/Windows, safer on Linux)
        ctx = multiprocessing.get_context("spawn")
        queue = ctx.Queue()

        process = ctx.Process(
            target=_worker_wrapper,
            args=(task, stage_data, queue),
        )

        process.start()

        try:
            # Wait for result with timeout
            # We add a small buffer to the process join timeout to allow queue put
            process.join(timeout=self.timeout_seconds)

            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                return TaskResult.terminal(error=f"Task timed out after {self.timeout_seconds}s (Process enforced)")

            if process.exitcode != 0:
                # Process crashed (segfault, OOM, etc.)
                return TaskResult.terminal(error=f"Worker process crashed with exit code {process.exitcode}")

            # Check queue for result
            if queue.empty():
                return TaskResult.terminal(error="Worker process finished but returned no result")

            result_wrapper: ProcessResult = queue.get()

            if result_wrapper.success and result_wrapper.result:
                return result_wrapper.result
            else:
                # Exception in task execution
                error_msg = result_wrapper.error or "Unknown error"
                logger.error(f"Task process failed: {error_msg}\n{result_wrapper.traceback}")
                # We return terminal, but handlers might convert to retry if transient
                # For now, let's wrap it in a logic that allows the handler to decide?
                # Actually, RunTaskHandler catches exceptions. We should re-raise
                # if we want RunTaskHandler's standard logic to apply.
                raise RuntimeError(result_wrapper.error)

        finally:
            # Cleanup
            if process.is_alive():
                try:
                    process.terminate()
                except Exception:
                    pass
            queue.close()
