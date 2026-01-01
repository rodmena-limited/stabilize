"""Integration tests for pipeline execution."""

from typing import Any

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.persistence.memory import InMemoryWorkflowStore
from stabilize.queue.queue import InMemoryQueue


class SuccessTask(Task):
    """A task that always succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"success": True})


class FailTask(Task):
    """A task that always fails."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("Task failed intentionally")


class CounterTask(Task):
    """A task that increments a counter."""

    counter: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        CounterTask.counter += 1
        return TaskResult.success(outputs={"count": CounterTask.counter})


def setup_stabilize() -> tuple[
    InMemoryQueue,
    InMemoryWorkflowStore,
    QueueProcessor,
    Orchestrator,
]:
    """Set up a complete pipeline runner for testing."""
    queue = InMemoryQueue()
    repository = InMemoryWorkflowStore()
    task_registry = TaskRegistry()

    # Register test tasks
    task_registry.register("success", SuccessTask)
    task_registry.register("fail", FailTask)
    task_registry.register("counter", CounterTask)

    # Create processor
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

    return queue, repository, processor, runner


def test_simple_pipeline_execution() -> None:
    """Test a simple single-stage pipeline."""
    queue, repository, processor, runner = setup_stabilize()

    # Create a simple pipeline with one stage
    execution = Workflow.create(
        application="test",
        name="Simple Pipeline",
        stages=[
            StageExecution(
                ref_id="1",
                type="test",
                name="Test Stage",
                tasks=[
                    TaskExecution.create(
                        name="Success Task",
                        implementing_class="success",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    # Store and start
    repository.store(execution)
    runner.start(execution)

    # Process all messages
    processor.process_all(timeout=5.0)

    # Retrieve and verify
    result = repository.retrieve(execution.id)
    assert result.status == WorkflowStatus.SUCCEEDED
    assert result.stages[0].status == WorkflowStatus.SUCCEEDED


def test_linear_pipeline_execution() -> None:
    """Test a linear pipeline with multiple stages."""
    queue, repository, processor, runner = setup_stabilize()
    CounterTask.counter = 0

    # Create a linear pipeline: A -> B -> C
    execution = Workflow.create(
        application="test",
        name="Linear Pipeline",
        stages=[
            StageExecution(
                ref_id="1",
                type="test",
                name="Stage A",
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="test",
                name="Stage B",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="3",
                type="test",
                name="Stage C",
                requisite_stage_ref_ids={"2"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    repository.store(execution)
    runner.start(execution)
    processor.process_all(timeout=10.0)

    result = repository.retrieve(execution.id)
    assert result.status == WorkflowStatus.SUCCEEDED

    # Verify all stages succeeded
    for stage in result.stages:
        assert stage.status == WorkflowStatus.SUCCEEDED

    # Verify counter was incremented 3 times
    assert CounterTask.counter == 3


def test_parallel_stages() -> None:
    """Test parallel stage execution."""
    queue, repository, processor, runner = setup_stabilize()
    CounterTask.counter = 0

    # Create a diamond: A -> [B, C] -> D
    execution = Workflow.create(
        application="test",
        name="Parallel Pipeline",
        stages=[
            StageExecution(
                ref_id="a",
                type="test",
                name="Stage A",
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="b",
                type="test",
                name="Stage B",
                requisite_stage_ref_ids={"a"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="c",
                type="test",
                name="Stage C",
                requisite_stage_ref_ids={"a"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="d",
                type="test",
                name="Stage D",
                requisite_stage_ref_ids={"b", "c"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    repository.store(execution)
    runner.start(execution)
    processor.process_all(timeout=10.0)

    result = repository.retrieve(execution.id)
    assert result.status == WorkflowStatus.SUCCEEDED

    # Verify all 4 stages succeeded
    for stage in result.stages:
        assert stage.status == WorkflowStatus.SUCCEEDED

    # Verify all 4 tasks ran
    assert CounterTask.counter == 4


def test_stage_failure() -> None:
    """Test that stage failure terminates the pipeline."""
    queue, repository, processor, runner = setup_stabilize()

    execution = Workflow.create(
        application="test",
        name="Failing Pipeline",
        stages=[
            StageExecution(
                ref_id="1",
                type="test",
                name="Failing Stage",
                tasks=[
                    TaskExecution.create(
                        name="Fail Task",
                        implementing_class="fail",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="test",
                name="Never Reached",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Success Task",
                        implementing_class="success",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    repository.store(execution)
    runner.start(execution)
    processor.process_all(timeout=10.0)

    result = repository.retrieve(execution.id)
    assert result.status == WorkflowStatus.TERMINAL

    # First stage should be terminal
    assert result.stages[0].status == WorkflowStatus.TERMINAL

    # Second stage should not have started
    assert result.stages[1].status == WorkflowStatus.NOT_STARTED
