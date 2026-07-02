"""Tests for finalizer registry and cleanup callbacks."""

import threading
import time

from stabilize.models.status import WorkflowStatus

import pytest

from stabilize.finalizers import (
    FinalizerRegistry,
    FinalizerResult,
    get_finalizer_registry,
    reset_finalizer_registry,
)


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Reset global registry before each test."""
    reset_finalizer_registry()
    yield
    reset_finalizer_registry()


class TestFinalizerResult:
    """Tests for FinalizerResult dataclass."""

    def test_successful_result(self) -> None:
        """Successful result has success=True and no error."""
        result = FinalizerResult(
            name="cleanup",
            success=True,
            duration_ms=50.0,
        )

        assert result.success
        assert result.error is None

    def test_failed_result(self) -> None:
        """Failed result has success=False and error message."""
        result = FinalizerResult(
            name="cleanup",
            success=False,
            error="Connection refused",
            duration_ms=100.0,
        )

        assert not result.success
        assert result.error == "Connection refused"


class TestFinalizerRegistry:
    """Tests for FinalizerRegistry."""

    def test_register_and_execute(self) -> None:
        """Can register and execute a finalizer."""
        registry = FinalizerRegistry()
        executed = []

        registry.register(
            stage_id="stage-1",
            finalizer_name="cleanup",
            callback=lambda: executed.append("done"),
        )

        results = registry.execute("stage-1")

        assert len(results) == 1
        assert results[0].success
        assert results[0].name == "cleanup"
        assert executed == ["done"]

    def test_multiple_finalizers_for_stage(self) -> None:
        """Multiple finalizers for same stage all execute."""
        registry = FinalizerRegistry()
        executed = []

        registry.register("stage-1", "cleanup1", lambda: executed.append(1))
        registry.register("stage-1", "cleanup2", lambda: executed.append(2))
        registry.register("stage-1", "cleanup3", lambda: executed.append(3))

        results = registry.execute("stage-1")

        assert len(results) == 3
        assert all(r.success for r in results)
        assert 1 in executed
        assert 2 in executed
        assert 3 in executed

    def test_execute_removes_finalizers(self) -> None:
        """Executing finalizers removes them from registry."""
        registry = FinalizerRegistry()
        registry.register("stage-1", "cleanup", lambda: None)

        assert registry.pending_count() == 1

        registry.execute("stage-1")

        assert registry.pending_count() == 0
        assert "stage-1" not in registry.pending()

    def test_execute_nonexistent_stage(self) -> None:
        """Executing finalizers for unknown stage returns empty list."""
        registry = FinalizerRegistry()
        results = registry.execute("unknown-stage")

        assert results == []

    def test_failed_finalizer_captured(self) -> None:
        """Failed finalizer has error captured in result."""
        registry = FinalizerRegistry()

        def failing_cleanup() -> None:
            raise RuntimeError("Cleanup failed!")

        registry.register("stage-1", "failing", failing_cleanup)

        results = registry.execute("stage-1")

        assert len(results) == 1
        assert not results[0].success
        assert "Cleanup failed" in (results[0].error or "")

    def test_timeout_captured(self) -> None:
        """Timed out finalizer is marked as failed."""
        registry = FinalizerRegistry()

        def slow_cleanup() -> None:
            time.sleep(5)

        registry.register("stage-1", "slow", slow_cleanup)

        # Execute with very short timeout
        results = registry.execute("stage-1", timeout=0.1)

        assert len(results) == 1
        assert not results[0].success
        assert "Timeout" in (results[0].error or "")

    def test_pending_returns_stage_ids(self) -> None:
        """pending() returns list of stage IDs with unexecuted finalizers."""
        registry = FinalizerRegistry()

        registry.register("stage-1", "cleanup1", lambda: None)
        registry.register("stage-2", "cleanup2", lambda: None)
        registry.register("stage-3", "cleanup3", lambda: None)

        pending = registry.pending()

        assert len(pending) == 3
        assert "stage-1" in pending
        assert "stage-2" in pending
        assert "stage-3" in pending

    def test_pending_count(self) -> None:
        """pending_count() returns total number of pending finalizers."""
        registry = FinalizerRegistry()

        registry.register("stage-1", "cleanup1", lambda: None)
        registry.register("stage-1", "cleanup2", lambda: None)
        registry.register("stage-2", "cleanup3", lambda: None)

        assert registry.pending_count() == 3

    def test_clear_removes_all_without_executing(self) -> None:
        """clear() removes all finalizers without executing them."""
        registry = FinalizerRegistry()
        executed = []

        registry.register("stage-1", "cleanup", lambda: executed.append(1))

        registry.clear()

        assert registry.pending_count() == 0
        assert executed == []  # Not executed

    def test_execute_all_pending(self) -> None:
        """execute_all_pending() runs finalizers for all stages."""
        registry = FinalizerRegistry()
        executed = []

        registry.register("stage-1", "cleanup1", lambda: executed.append(1))
        registry.register("stage-2", "cleanup2", lambda: executed.append(2))

        results = registry.execute_all_pending()

        assert len(results) == 2
        assert "stage-1" in results
        assert "stage-2" in results
        assert 1 in executed
        assert 2 in executed
        assert registry.pending_count() == 0

    def test_thread_safety(self) -> None:
        """Registry is thread-safe for concurrent operations."""
        registry = FinalizerRegistry()
        results: list[list[FinalizerResult]] = []

        def register_and_execute(stage_num: int) -> None:
            stage_id = f"stage-{stage_num}"
            registry.register(stage_id, "cleanup", lambda: None)
            time.sleep(0.01)  # Small delay
            result = registry.execute(stage_id)
            results.append(result)

        threads = [threading.Thread(target=register_and_execute, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should have completed successfully
        assert len(results) == 10
        for result_list in results:
            assert len(result_list) == 1
            assert result_list[0].success


class TestGlobalRegistry:
    """Tests for global registry functions."""

    def test_get_returns_same_instance(self) -> None:
        """get_finalizer_registry() returns same instance."""
        registry1 = get_finalizer_registry()
        registry2 = get_finalizer_registry()

        assert registry1 is registry2

    def test_reset_creates_new_instance(self) -> None:
        """reset_finalizer_registry() allows new instance to be created."""
        registry1 = get_finalizer_registry()
        reset_finalizer_registry()
        registry2 = get_finalizer_registry()

        assert registry1 is not registry2


class TestHungFinalizerTimeout:
    """Audit finding A1-8: the per-finalizer timeout must actually be
    enforced — a hung finalizer previously blocked _execute_one forever in
    the ThreadPoolExecutor context-manager join, stalling graceful shutdown."""

    def test_hung_finalizer_returns_promptly_as_failed(self) -> None:
        from stabilize.finalizers import FinalizerRegistry

        registry = FinalizerRegistry()
        hang = threading.Event()
        registry.register("stage-hung", "hung-cleanup", lambda: hang.wait(10.0))

        start = time.monotonic()
        results = registry.execute("stage-hung", timeout=0.2)
        elapsed = time.monotonic() - start

        try:
            assert elapsed < 2.0, (
                f"execute() blocked {elapsed:.1f}s on a hung finalizer despite a 0.2s timeout"
            )
            assert len(results) == 1
            assert results[0].success is False
            assert "Timeout" in (results[0].error or "")
        finally:
            hang.set()  # release the abandoned worker thread


class TestFinalizersRunOnStageTerminalStates:
    """Audit finding A3-18: the registry documents execution 'when stages
    enter terminal states', but the engine only executed finalizers in the
    run-task error path and at process shutdown — successful completion and
    cancellation leaked registered resources for the life of the process."""

    def _make_env(self, tmp_path):
        from stabilize.persistence.connection import ConnectionManager, SingletonMeta
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        SingletonMeta.reset(ConnectionManager)
        url = f"sqlite:///{tmp_path}/finalizer_terminal.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url)
        queue._create_table()
        return store, queue

    def _make_stage(self, store, task_status):
        from stabilize.models.stage import StageExecution
        from stabilize.models.task import TaskExecution
        from stabilize.models.workflow import Workflow

        task = TaskExecution.create(
            name="t", implementing_class="noop", stage_start=True, stage_end=True
        )
        task.status = task_status
        stage = StageExecution(ref_id="s", name="S", tasks=[task])
        stage.status = WorkflowStatus.RUNNING
        execution = Workflow.create(application="t", name="w", stages=[stage])
        execution.status = WorkflowStatus.RUNNING
        store.store(execution)
        return execution, stage

    def test_finalizers_run_when_stage_completes_successfully(self, tmp_path) -> None:
        from stabilize.handlers import CompleteStageHandler
        from stabilize.queue.messages import CompleteStage

        reset_finalizer_registry()
        store, queue = self._make_env(tmp_path)
        execution, stage = self._make_stage(store, WorkflowStatus.SUCCEEDED)

        ran: list[str] = []
        get_finalizer_registry().register(stage.id, "release", lambda: ran.append("release"))

        CompleteStageHandler(queue, store).handle(
            CompleteStage(
                execution_type="PIPELINE",
                execution_id=execution.id,
                stage_id=stage.id,
                message_id="fin-1",
            )
        )
        assert ran == ["release"], (
            "finalizer not executed when the stage completed successfully"
        )
        reset_finalizer_registry()

    def test_finalizers_run_when_stage_is_canceled(self, tmp_path) -> None:
        from stabilize.handlers import CancelStageHandler
        from stabilize.queue.messages import CancelStage

        reset_finalizer_registry()
        store, queue = self._make_env(tmp_path)
        execution, stage = self._make_stage(store, WorkflowStatus.RUNNING)

        ran: list[str] = []
        get_finalizer_registry().register(stage.id, "release", lambda: ran.append("release"))

        CancelStageHandler(queue, store).handle(
            CancelStage(
                execution_type="PIPELINE",
                execution_id=execution.id,
                stage_id=stage.id,
                message_id="fin-2",
            )
        )
        assert ran == ["release"], "finalizer not executed when the stage was canceled"
        reset_finalizer_registry()
