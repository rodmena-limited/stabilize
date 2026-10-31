"""
Process-isolated task execution.

This module provides a mechanism to execute tasks in separate processes.
This protects the main worker process from:
1. Segmentation faults (e.g., in C extensions)
2. Memory leaks or OOM kills
3. Global interpreter lock (GIL) contention
4. Stuck tasks (force kill capability)

Usage:
    executor = ProcessIsolatedTaskExecutor(timeout_seconds=14400)
    result = executor.execute(task, stage)
"""

from __future__ import annotations

import logging
import multiprocessing
import pickle
import traceback
from dataclasses import dataclass
from typing import Any

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


IN_WORKER_PROCESS = False


@dataclass
class ProcessResult:
    """Result returned from the worker process."""

    success: bool
    result: TaskResult | None = None
    error: str | None = None
    traceback: str | None = None
    exception: BaseException | None = None
    exception_type: str | None = None


def _worker_wrapper(
    task: Task,
    stage_data: dict[str, Any],
    queue: multiprocessing.Queue[ProcessResult],
) -> None:
    """
    Worker function that runs in the separate process.

    We pass raw data instead of full StageExecution object to avoid
    pickling issues with complex objects attached to stage (like execution ref).
    We reconstruct a minimal StageExecution for the task.
    """
    global IN_WORKER_PROCESS
    IN_WORKER_PROCESS = True
    try:
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
        text = traceback.format_exc()
        try:
            pickle.dumps(e)
            carried: BaseException | None = e
        except Exception:
            carried = None
        queue.put(
            ProcessResult(
                success=False, error=str(e), traceback=text, exception=carried, exception_type=type(e).__name__
            )
        )


class ProcessIsolatedTaskExecutor:
    """Executes tasks in isolated processes."""

    @staticmethod
    def _on_timeout(task: Task, stage: StageExecution, seconds: float) -> TaskResult | None:
        hook = getattr(task, "on_timeout", None)
        if hook is None:
            return None
        try:
            result = hook(stage)
        except Exception as exc:
            logger.error("on_timeout raised for %s after %.1fs: %s", type(task).__name__, seconds, exc)
            return None
        return result if isinstance(result, TaskResult) else None

    def __init__(self, timeout_seconds: float = 14400.0) -> None:
        """Initialize executor with timeout.

        Args:
            timeout_seconds: Default timeout for task execution (4 hours for long-running workflows)
        """
        self.timeout_seconds = timeout_seconds

    def execute(self, task: Task, stage: StageExecution, timeout_seconds: float | None = None) -> TaskResult:
        """
        Execute the task in a separate process.

        Args:
            task: The task instance to execute
            stage: The stage execution context
            timeout_seconds: Optional timeout override. If not provided, uses instance default.

        Returns:
            The TaskResult from the task

        Raises:
            TimeoutError: If execution exceeds timeout
            RuntimeError: If process crashes or returns invalid result
        """
        # Use provided timeout or fall back to instance default
        effective_timeout = timeout_seconds if timeout_seconds is not None else self.timeout_seconds

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
            # Drain the result BEFORE joining. A child putting a large result
            # blocks on the pipe until the parent reads it; joining first
            # deadlocks until the timeout and misreports the task as timed
            # out (then kills a healthy child).
            import queue as queue_module

            try:
                result_wrapper: ProcessResult = queue.get(timeout=effective_timeout)
            except queue_module.Empty:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        process.kill()
                    hooked = self._on_timeout(task, stage, effective_timeout)
                    if hooked is not None:
                        return hooked
                    return TaskResult.terminal(error=f"Task timed out after {effective_timeout}s (Process enforced)")
                # Process died without producing a result
                process.join(timeout=5)
                if process.exitcode != 0:
                    return TaskResult.terminal(error=f"Worker process crashed with exit code {process.exitcode}")
                return TaskResult.terminal(error="Worker process finished but returned no result")

            # Result received; the child exits right after the put.
            process.join(timeout=10)

            if result_wrapper.success and result_wrapper.result:
                return result_wrapper.result
            else:
                # Exception in task execution
                error_msg = result_wrapper.error or "Unknown error"
                logger.error("Task process failed: %s\n%s", error_msg, result_wrapper.traceback)
                # We return terminal, but handlers might convert to retry if transient
                # For now, let's wrap it in a logic that allows the handler to decide?
                # Actually, RunTaskHandler catches exceptions. We should re-raise
                # if we want RunTaskHandler's standard logic to apply.
                if result_wrapper.exception is not None:
                    raise result_wrapper.exception
                kind = result_wrapper.exception_type or "Exception"
                raise RuntimeError(f"{kind}: {result_wrapper.error or 'Unknown error'}")

        finally:
            # Cleanup
            if process.is_alive():
                try:
                    process.terminate()
                except Exception:
                    pass
            queue.close()
