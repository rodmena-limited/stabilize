"""
RunTaskHandler - executes tasks.

This is the handler that actually runs task implementations.
It handles execution, retries, timeouts, and result processing.

Uses bulkman for bulkhead pattern (per-task-type isolation) and
resilient_circuit for circuit breaker protection.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from resilient_circuit import ExponentialDelay

from stabilize.errors import (
    TaskTimeoutError,
    TransientVerificationError,
    VerificationError,
    is_transient,
)
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.run_task.error import (
    complete_with_error,
    handle_cancellation,
    handle_exception,
)
from stabilize.handlers.run_task.execution import execute_with_timeout, resolve_task
from stabilize.handlers.run_task.result import get_backoff_period, process_result
from stabilize.handlers.run_task.verification import verify_task_outputs
from stabilize.metrics import Timer
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.transaction import TransactionHelper
from stabilize.queue.messages import CompleteTask, PauseTask, RunTask
from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.circuits import WorkflowCircuitFactory
from stabilize.resilience.config import HandlerConfig, ResilienceConfig
from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor
from stabilize.resilience.timeouts import TimeoutManager
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


class RunTaskHandler(StabilizeHandler[RunTask]):
    """
    Handler for RunTask messages.

    This is where tasks are actually executed. The handler:
    1. Resolves the task implementation
    2. Checks for cancellation/pause
    3. Checks for timeout
    4. Executes the task
    5. Processes the result
    """

    # Class-level lock to track tasks currently being executed (prevents duplicate execution)
    # Maps task_id -> start_time (monotonic) for staleness detection
    _executing_tasks: dict[str, float] = {}
    _executing_lock = threading.Lock()

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        task_registry: TaskRegistry,
        retry_delay: timedelta | None = None,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)
        self.task_registry = task_registry
        self.txn_helper = TransactionHelper(repository, queue)
        self.timeout_manager = TimeoutManager(self.handler_config.default_task_timeout_seconds)

        # Create backoff calculator from config
        self._task_backoff = ExponentialDelay(
            min_delay=timedelta(milliseconds=self.handler_config.task_backoff_min_delay_ms),
            max_delay=timedelta(milliseconds=self.handler_config.task_backoff_max_delay_ms),
            factor=int(self.handler_config.concurrency_backoff_factor),
            jitter=self.handler_config.concurrency_jitter,
        )

        # Check isolation mode
        self.isolation_mode = os.environ.get("STABILIZE_ISOLATION_MODE", "thread").lower()
        self.process_executor = ProcessIsolatedTaskExecutor() if self.isolation_mode == "process" else None

        # Initialize resilience components with defaults if not provided
        if bulkhead_manager is None or circuit_factory is None:
            config = ResilienceConfig.from_env()
            self.bulkhead_manager = bulkhead_manager or TaskBulkheadManager(config)
            self.circuit_factory = circuit_factory or WorkflowCircuitFactory(config)
        else:
            self.bulkhead_manager = bulkhead_manager
            self.circuit_factory = circuit_factory

    @property
    def message_type(self) -> type[RunTask]:
        return RunTask

    def handle(self, message: RunTask) -> None:
        """Handle the RunTask message."""

        def on_task(stage: StageExecution, task_model: TaskExecution) -> None:
            execution = stage.execution

            # IDEMPOTENCY CHECK: Only run tasks that are in RUNNING state
            if task_model.status != WorkflowStatus.RUNNING:
                if task_model.status.is_complete:
                    logger.debug(
                        "Ignoring RunTask for %s - already completed with status %s",
                        task_model.name,
                        task_model.status,
                    )
                else:
                    logger.warning(
                        "Ignoring RunTask for %s - unexpected status %s (expected RUNNING)",
                        task_model.name,
                        task_model.status,
                    )
                # Mark message as processed to prevent redelivery
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(message.message_id)
                return

            # Resolve task implementation
            try:
                task = resolve_task(message.task_type, task_model, self.task_registry)
            except TaskNotFoundError as e:
                logger.error("Task type not found: %s", message.task_type)
                complete_with_error(
                    stage,
                    task_model,
                    message,
                    str(e),
                    self.repository,
                    self.txn_helper,
                    self.retry_on_concurrency_error,
                )
                return

            # Check execution state
            if execution.is_canceled:
                handle_cancellation(
                    stage,
                    task_model,
                    task,
                    message,
                    self.repository,
                    self.txn_helper,
                    self.retry_on_concurrency_error,
                )
                return

            if execution.status.is_complete:
                # Atomic: mark message processed + push CompleteTask
                self.txn_helper.execute_atomic(
                    source_message=message,
                    messages_to_push=[
                        (
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.CANCELED,
                            ),
                            None,
                        )
                    ],
                    handler_name="RunTask",
                )
                return

            if execution.status == WorkflowStatus.PAUSED:
                # Atomic: mark message processed + push PauseTask
                self.txn_helper.execute_atomic(
                    source_message=message,
                    messages_to_push=[
                        (
                            PauseTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                            ),
                            None,
                        )
                    ],
                    handler_name="RunTask",
                )
                return

            # Check for manual skip
            if stage.context.get("manualSkip"):
                # Atomic: mark message processed + push CompleteTask
                self.txn_helper.execute_atomic(
                    source_message=message,
                    messages_to_push=[
                        (
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.SKIPPED,
                            ),
                            None,
                        )
                    ],
                    handler_name="RunTask",
                )
                return

            # Stage-level elapsed time check
            stage_timeout_ms = stage.context.get("stageTimeoutMs")
            if stage_timeout_ms is not None and stage.start_time:
                stage_elapsed_ms = self.current_time_millis() - stage.start_time
                if stage_elapsed_ms > stage_timeout_ms:
                    logger.info(
                        "Stage %s exceeded stage timeout (%dms > %dms)",
                        stage.name,
                        stage_elapsed_ms,
                        stage_timeout_ms,
                    )
                    complete_with_error(
                        stage,
                        task_model,
                        message,
                        f"Stage exceeded timeout ({stage_elapsed_ms}ms > {stage_timeout_ms}ms)",
                        self.repository,
                        self.txn_helper,
                        self.retry_on_concurrency_error,
                    )
                    return

            # Total-lifecycle timeout for RetryableTask (execute_with_timeout only caps single calls)
            if isinstance(task, RetryableTask) and task_model.start_time:
                elapsed_ms = self.current_time_millis() - task_model.start_time
                allowed_ms = task.get_dynamic_timeout(stage).total_seconds() * 1000
                if elapsed_ms > allowed_ms:
                    logger.info(
                        "RetryableTask %s exceeded total timeout (%dms > %dms)",
                        task_model.name,
                        elapsed_ms,
                        int(allowed_ms),
                    )
                    timeout_result = task.on_timeout(stage) if hasattr(task, "on_timeout") else None
                    if timeout_result is None:
                        complete_with_error(
                            stage,
                            task_model,
                            message,
                            f"Task exceeded total timeout ({elapsed_ms}ms > {int(allowed_ms)}ms)",
                            self.repository,
                            self.txn_helper,
                            self.retry_on_concurrency_error,
                        )
                    else:
                        self._process_result_safely(message.stage_id, message.task_id, timeout_result, message)
                    return

            # CONCURRENT EXECUTION CHECK: Prevent duplicate execution of same task
            stale_threshold_s = 3600
            with RunTaskHandler._executing_lock:
                existing_start = RunTaskHandler._executing_tasks.get(task_model.id)
                if existing_start is not None:
                    elapsed = time.monotonic() - existing_start
                    if elapsed < stale_threshold_s:
                        logger.debug(
                            "Ignoring duplicate RunTask for %s - already executing (%.1fs)",
                            task_model.name,
                            elapsed,
                        )
                        return
                    else:
                        logger.warning(
                            "Stale execution lock for task %s (%.1fs > %ds), allowing re-execution",
                            task_model.name,
                            elapsed,
                            stale_threshold_s,
                        )
                RunTaskHandler._executing_tasks[task_model.id] = time.monotonic()

            # Execute the task with timeout enforcement
            try:
                timeout = self.timeout_manager.get_task_timeout(stage, task)
                with Timer(
                    "task_execution_seconds",
                    task_type=message.task_type,
                    task_name=task_model.name,
                ):
                    result = execute_with_timeout(
                        task,
                        stage,
                        timeout,
                        message,
                        self.bulkhead_manager,
                        self.circuit_factory,
                        self.process_executor,
                    )

                # Verify outputs before processing/persisting
                verify_task_outputs(stage, result, self.task_registry)

                self._process_result_safely(message.stage_id, message.task_id, result, message)
            except TaskTimeoutError as e:
                logger.info("Task %s timed out: %s", task_model.name, e)
                timeout_result = task.on_timeout(stage) if hasattr(task, "on_timeout") else None
                if timeout_result is None:
                    complete_with_error(
                        stage,
                        task_model,
                        message,
                        str(e),
                        self.repository,
                        self.txn_helper,
                        self.retry_on_concurrency_error,
                    )
                else:
                    self._process_result_safely(message.stage_id, message.task_id, timeout_result, message)
            except TransientVerificationError as e:
                logger.info(
                    "Verification pending for task %s, will retry: %s",
                    task_model.name,
                    e,
                )
                handle_exception(
                    stage,
                    task_model,
                    task,
                    message,
                    e,
                    self.repository,
                    self.txn_helper,
                    self._get_backoff_period,
                    self.retry_on_concurrency_error,
                )
            except VerificationError as e:
                logger.error("Verification failed for task %s: %s", task_model.name, e)
                complete_with_error(
                    stage,
                    task_model,
                    message,
                    str(e),
                    self.repository,
                    self.txn_helper,
                    self.retry_on_concurrency_error,
                )
            except Exception as e:
                if is_transient(e):
                    logger.debug(
                        "Transient error executing task %s: %s",
                        task_model.name,
                        e,
                    )
                else:
                    logger.error(
                        "Error executing task %s: %s",
                        task_model.name,
                        e,
                        exc_info=True,
                    )
                handle_exception(
                    stage,
                    task_model,
                    task,
                    message,
                    e,
                    self.repository,
                    self.txn_helper,
                    self._get_backoff_period,
                    self.retry_on_concurrency_error,
                )
            finally:
                # Release the execution lock so other messages can be processed
                with RunTaskHandler._executing_lock:
                    RunTaskHandler._executing_tasks.pop(task_model.id, None)

        self.with_task(message, on_task)

    def _process_result_safely(
        self,
        stage_id: str,
        task_id: str,
        result: TaskResult,
        message: RunTask,
    ) -> None:
        """Process result with retry on concurrency error."""

        def do_process() -> None:
            # Always reload stage to get latest version
            stage = self.repository.retrieve_stage(stage_id)
            if not stage:
                logger.error("Stage %s not found processing result", stage_id)
                return

            # Find task model
            task_model = next((t for t in stage.tasks if t.id == task_id), None)
            if not task_model:
                logger.error("Task %s not found in stage %s", task_id, stage_id)
                return

            process_result(
                stage,
                task_model,
                result,
                message,
                self.txn_helper,
                self._get_backoff_period,
            )

        self.retry_on_concurrency_error(do_process, f"processing result for task {task_id}")

    def _get_backoff_period(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
        attempt: int = 1,
    ) -> timedelta:
        """Calculate backoff period for retry."""
        return get_backoff_period(
            stage,
            task_model,
            message,
            attempt,
            self.task_registry,
            self._task_backoff,
            self.current_time_millis,
        )
