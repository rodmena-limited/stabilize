"""Task execution logic with timeout and resilience."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.resilience.executor import execute_with_resilience
from stabilize.tasks.registry import TaskNotFoundError

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.queue.messages import RunTask
    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor
    from stabilize.tasks.interface import Task
    from stabilize.tasks.registry import TaskRegistry
    from stabilize.tasks.result import TaskResult


def resolve_task(
    task_type: str,
    task_model: TaskExecution,
    task_registry: TaskRegistry,
) -> Task:
    """Resolve the task implementation.

    Args:
        task_type: The task type name
        task_model: The task execution model
        task_registry: Registry to look up task implementations

    Returns:
        The resolved task implementation

    Raises:
        TaskNotFoundError: If task type is not found
    """
    # Try by class name first
    try:
        return task_registry.get_by_class(task_model.implementing_class)
    except TaskNotFoundError:
        pass

    # Try by type name
    return task_registry.get(task_type)


def execute_with_timeout(
    task: Task,
    stage: StageExecution,
    timeout: timedelta,
    message: RunTask,
    bulkhead_manager: TaskBulkheadManager,
    circuit_factory: WorkflowCircuitFactory,
    process_executor: ProcessIsolatedTaskExecutor | None = None,
) -> TaskResult:
    """Execute a task with timeout enforcement via bulkhead.

    Uses bulkhead for thread pool management and circuit breaker for
    failure protection. Runs the task through the appropriate bulkhead
    based on task type.

    Args:
        task: The task to execute
        stage: The stage execution context
        timeout: Maximum time allowed for execution
        message: The RunTask message (for task type info)
        bulkhead_manager: Manager for task bulkheads
        circuit_factory: Factory for circuit breakers
        process_executor: Optional process executor for isolation

    Returns:
        The task result

    Raises:
        TaskTimeoutError: If the task exceeds the timeout
        TransientError: If bulkhead is full or circuit is open
    """
    task_name = getattr(task, "name", type(task).__name__)
    execution_id = stage.execution.id if stage.execution else None

    # Get circuit breaker for this workflow + task type
    circuit = circuit_factory.get_circuit(
        workflow_execution_id=execution_id or "unknown",
        task_type=message.task_type,
    )

    # Define the execution function (process-isolated or direct)
    def execute_task(s: StageExecution) -> TaskResult:
        if process_executor:
            # Pass timeout directly to avoid race conditions with concurrent tasks
            return process_executor.execute(task, s, timeout_seconds=timeout.total_seconds())
        return task.execute(s)

    # Execute through bulkhead with circuit breaker protection
    return execute_with_resilience(
        bulkhead_manager=bulkhead_manager,
        circuit=circuit,
        task_type=message.task_type,
        func=execute_task,
        func_args=(stage,),
        timeout=timeout.total_seconds(),
        task_name=task_name,
        stage_id=stage.id,
        execution_id=execution_id,
    )
