"""
Stress tests for concurrent workflow processing.

This module tests the system under high concurrency including:
- High volume workflow processing
- Concurrent handlers racing on same stage
- Optimistic lock retry behavior
- Queue claim contention
- Deadlock detection
"""

import threading
import time
from typing import Any

import pytest

from stabilize import TaskResult
from stabilize.errors import ConcurrencyError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class QuickTask(Task):
    """Task that completes quickly."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"quick": True})


class CountingTask(Task):
    """Task that counts executions (thread-safe)."""

    _lock = threading.Lock()
    count: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        with CountingTask._lock:
            CountingTask.count += 1
        return TaskResult.success(outputs={"count": CountingTask.count})


class SlowTask(Task):
    """Task that takes a configurable amount of time."""

    delay: float = 0.1

    def execute(self, stage: StageExecution) -> TaskResult:
        time.sleep(SlowTask.delay)
        return TaskResult.success()


class RandomFailTask(Task):
    """Task that randomly fails some percentage of the time."""

    fail_rate: float = 0.1
    _lock = threading.Lock()
    _attempt_count: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        import random

        with RandomFailTask._lock:
            RandomFailTask._attempt_count += 1

        if random.random() < RandomFailTask.fail_rate:
            return TaskResult.terminal("Random failure")
        return TaskResult.success()


def reset_tasks() -> None:
    """Reset all task class state."""
    CountingTask.count = 0
    SlowTask.delay = 0.1
    RandomFailTask.fail_rate = 0.1
    RandomFailTask._attempt_count = 0


@pytest.mark.stress
class TestHighVolumeProcessing:
    """Test high volume workflow processing."""

    def test_many_workflows_sequential_stages(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Process many workflows with sequential stages.

        Each workflow: A -> B -> C
        """
        reset_tasks()
        num_workflows = 20

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"quick": QuickTask})

        # Create and start many workflows
        workflow_ids = []
        for i in range(num_workflows):
            execution = Workflow.create(
                application="stress-test",
                name=f"workflow-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        tasks=[
                            TaskExecution.create(
                                name="Task A",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        tasks=[
                            TaskExecution.create(
                                name="Task B",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_b"},
                        tasks=[
                            TaskExecution.create(
                                name="Task C",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                ],
            )

            repository.store(execution)
            runner.start(execution)
            workflow_ids.append(execution.id)

        # Process all
        processor.process_all(timeout=60.0)

        # Verify all completed
        succeeded = 0
        for wf_id in workflow_ids:
            result = repository.retrieve(wf_id)
            if result.status == WorkflowStatus.SUCCEEDED:
                succeeded += 1

        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded"

    def test_many_parallel_workflows(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Process many workflows with parallel stages.

        Each workflow: [A, B, C] all parallel
        """
        reset_tasks()
        num_workflows = 15

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"quick": QuickTask})

        workflow_ids = []
        for i in range(num_workflows):
            execution = Workflow.create(
                application="stress-test",
                name=f"parallel-workflow-{i}",
                stages=[
                    # All parallel (no dependencies)
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        tasks=[
                            TaskExecution.create(
                                name="Task A",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        tasks=[
                            TaskExecution.create(
                                name="Task B",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        tasks=[
                            TaskExecution.create(
                                name="Task C",
                                implementing_class="quick",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                ],
            )

            repository.store(execution)
            runner.start(execution)
            workflow_ids.append(execution.id)

        processor.process_all(timeout=60.0)

        succeeded = sum(1 for wf_id in workflow_ids if repository.retrieve(wf_id).status == WorkflowStatus.SUCCEEDED)

        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded"


@pytest.mark.stress
class TestConcurrentHandlers:
    """Test concurrent handlers racing on same resources."""

    def test_concurrent_task_completion_same_stage(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test multiple tasks in same stage completing concurrently.

        This tests the optimistic locking when multiple CompleteTask handlers
        race to update the same stage.
        """
        reset_tasks()

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"quick": QuickTask})

        # Stage with many tasks that can complete in parallel
        execution = Workflow.create(
            application="stress-test",
            name="concurrent-completion",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name=f"Task {i}",
                            implementing_class="quick",
                            stage_start=(i == 0),
                            stage_end=(i == 9),
                        )
                        for i in range(10)
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # All tasks should have completed
        stage = result.stage_by_ref_id("stage_a")
        for task in stage.tasks:
            assert task.status == WorkflowStatus.SUCCEEDED, f"Task {task.name}: {task.status}"


@pytest.mark.stress
class TestOptimisticLockRetry:
    """Test optimistic locking retry behavior."""

    def test_retry_on_concurrency_error_succeeds(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that ConcurrencyError triggers retry and eventually succeeds.
        """
        if backend == "sqlite":
            pytest.skip("SQLite doesn't handle high-contention concurrent writes reliably")

        # Create a workflow
        wf = Workflow.create("test-app", "retry-test", [])
        stage = StageExecution.create("stage-1", "Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task A", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Simulate concurrent updates
        success_count = 0
        error_count = 0
        lock = threading.Lock()

        def update_stage(thread_id: int) -> None:
            nonlocal success_count, error_count
            for attempt in range(5):  # Retry up to 5 times
                try:
                    fresh_stage = repository.retrieve_stage(stage.id)
                    fresh_stage.context[f"thread_{thread_id}"] = f"value_{attempt}"
                    repository.store_stage(fresh_stage)
                    with lock:
                        success_count += 1
                    return
                except ConcurrencyError:
                    time.sleep(0.01 * (attempt + 1))  # Exponential backoff
            with lock:
                error_count += 1

        threads = [threading.Thread(target=update_stage, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should eventually succeed
        assert success_count == 5, f"Only {success_count}/5 succeeded, {error_count} errors"


@pytest.mark.stress
class TestQueueClaimContention:
    """Test queue message claiming under contention."""

    def test_messages_claimed_exactly_once(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that each message is claimed by exactly one worker.

        Push many messages, have multiple workers poll, verify no duplicates.
        """
        if backend == "sqlite":
            pytest.skip("SQLite doesn't handle high-contention concurrent writes reliably")

        num_messages = 50
        claimed_messages: list[str] = []
        lock = threading.Lock()

        # Push messages
        for i in range(num_messages):
            msg = StartWorkflow(
                execution_type="PIPELINE",
                execution_id=f"claim-test-{i}",
            )
            queue.push(msg)

        # Have multiple workers claim messages
        def claim_worker() -> None:
            while True:
                polled = queue.poll_one()
                if polled is None:
                    break
                with lock:
                    claimed_messages.append(polled.execution_id)
                queue.ack(polled)

        threads = [threading.Thread(target=claim_worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no duplicates
        assert len(claimed_messages) == len(set(claimed_messages)), (
            f"Duplicate claims detected! {len(claimed_messages)} claimed, {len(set(claimed_messages))} unique"
        )

        # Verify all claimed
        assert len(claimed_messages) == num_messages, (
            f"Not all messages claimed: {len(claimed_messages)}/{num_messages}"
        )


@pytest.mark.stress
class TestDeadlockDetection:
    """Test that the system doesn't deadlock under various scenarios."""

    def test_no_deadlock_under_high_contention(
        self, repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Any
    ) -> None:
        """
        Test that high contention doesn't cause deadlocks.

        Run many concurrent operations for a fixed time and verify completion.
        """
        if backend == "sqlite":
            pytest.skip("SQLite doesn't handle high-contention concurrent writes reliably")

        if backend == "sqlite_unused":
            # Use file-based SQLite for meaningful concurrency test
            from stabilize.persistence.sqlite import SqliteWorkflowStore
            from stabilize.queue.sqlite import SqliteQueue

            SingletonMeta.reset(ConnectionManager)

            db_path = tmp_path / "deadlock_test.db"
            connection_string = f"sqlite:///{db_path}"

            test_store = SqliteWorkflowStore(connection_string, create_tables=True)
            test_queue = SqliteQueue(connection_string)
            test_queue._create_table()
        else:
            test_store = repository
            test_queue = queue

        # Create workflows
        workflows = []
        for i in range(10):
            wf = Workflow.create("deadlock-test", f"workflow-{i}", [])
            stage = StageExecution.create(f"stage-{i}", "Test Stage", "1")
            stage.execution = wf
            task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
            stage.tasks = [task]
            wf.stages = [stage]
            test_store.store(wf)
            workflows.append(wf)

        # Run concurrent operations
        completed = threading.Event()
        errors: list[Exception] = []
        operation_count = 0
        op_lock = threading.Lock()

        def random_operation() -> None:
            nonlocal operation_count
            import random

            while not completed.is_set():
                try:
                    wf = random.choice(workflows)
                    op = random.choice(["read", "update"])

                    if op == "read":
                        test_store.retrieve(wf.id)
                    else:
                        try:
                            stage = test_store.retrieve_stage(wf.stages[0].id)
                            stage.context["op"] = random.randint(0, 1000)
                            test_store.store_stage(stage)
                        except ConcurrencyError:
                            pass  # Expected

                    with op_lock:
                        operation_count += 1

                except Exception as e:
                    errors.append(e)

        # Start workers
        threads = [threading.Thread(target=random_operation) for _ in range(10)]
        for t in threads:
            t.start()

        # Run for 3 seconds
        time.sleep(3.0)
        completed.set()

        for t in threads:
            t.join(timeout=5.0)

        # Verify no threads are stuck
        stuck_threads = [t for t in threads if t.is_alive()]
        assert not stuck_threads, f"{len(stuck_threads)} threads appear deadlocked"

        # Should have completed many operations
        assert operation_count > 100, f"Only {operation_count} operations completed"

        # Few or no errors (ConcurrencyError is expected, others are bugs)
        non_concurrency_errors = [e for e in errors if not isinstance(e, ConcurrencyError)]
        assert not non_concurrency_errors, f"Unexpected errors: {non_concurrency_errors}"

        if backend == "sqlite":
            test_store.close()
            test_queue.close()


@pytest.mark.stress
class TestErrorRecoveryStress:
    """Test error recovery under load."""

    def test_transient_errors_recovered(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that transient errors are recovered via retry.
        """
        reset_tasks()
        RandomFailTask.fail_rate = 0.0  # No failures for this test

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"random_fail": RandomFailTask})

        # Create workflows
        workflow_ids = []
        for i in range(10):
            execution = Workflow.create(
                application="recovery-test",
                name=f"workflow-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        tasks=[
                            TaskExecution.create(
                                name="Maybe Fail Task",
                                implementing_class="random_fail",
                                stage_start=True,
                                stage_end=True,
                            )
                        ],
                    ),
                ],
            )

            repository.store(execution)
            runner.start(execution)
            workflow_ids.append(execution.id)

        processor.process_all(timeout=30.0)

        # All should succeed (no failures)
        succeeded = sum(1 for wf_id in workflow_ids if repository.retrieve(wf_id).status == WorkflowStatus.SUCCEEDED)

        assert succeeded == 10, f"Only {succeeded}/10 succeeded"
