"""Tests for cooperative task cancellation.

Covers the primitive (CancellationToken), the task_id registry, the per-thread
contextvar binding, cross-thread signaling, and the real execution path (a
cooperative task running through the processor).
"""

from __future__ import annotations

import threading
import time

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.cancellation import (
    CancellationToken,
    TaskCancelledError,
    bind_current_token,
    cancel_task,
    current_cancellation_token,
    get_token,
    is_cancellation_requested,
    raise_if_cancellation_requested,
    register_token,
    reset_current_token,
    unregister_token,
)
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult


class TestCancellationToken:
    def test_starts_uncancelled(self) -> None:
        token = CancellationToken()
        assert token.is_cancelled is False
        token.raise_if_cancelled()  # no raise

    def test_cancel_sets_flag_and_raises(self) -> None:
        token = CancellationToken()
        token.cancel()
        assert token.is_cancelled is True
        with pytest.raises(TaskCancelledError):
            token.raise_if_cancelled()

    def test_cancel_is_idempotent(self) -> None:
        token = CancellationToken()
        token.cancel()
        token.cancel()
        assert token.is_cancelled is True


class TestRegistry:
    def test_register_get_unregister(self) -> None:
        token = register_token("t1")
        assert get_token("t1") is token
        unregister_token("t1")
        assert get_token("t1") is None

    def test_cancel_task_signals_token(self) -> None:
        token = register_token("t2")
        assert cancel_task("t2") is True
        assert token.is_cancelled is True

    def test_cancel_task_missing_returns_false(self) -> None:
        assert cancel_task("does-not-exist") is False


class TestContextVarBinding:
    def test_unbound_is_not_cancelled(self) -> None:
        assert current_cancellation_token() is None
        assert is_cancellation_requested() is False
        raise_if_cancellation_requested()  # no raise

    def test_bound_token_reflected(self) -> None:
        token = register_token("t3")
        handle = bind_current_token(get_token("t3"))
        try:
            assert current_cancellation_token() is token
            assert is_cancellation_requested() is False
            cancel_task("t3")
            assert is_cancellation_requested() is True
            with pytest.raises(TaskCancelledError):
                raise_if_cancellation_requested()
        finally:
            reset_current_token(handle)
        # After reset, no token is bound on this thread.
        assert current_cancellation_token() is None

    def test_cross_thread_signal(self) -> None:
        """A worker thread bound to a token sees cancellation from another thread."""
        register_token("worker-task")
        started = threading.Event()
        stopped = threading.Event()

        def worker() -> None:
            handle = bind_current_token(get_token("worker-task"))
            try:
                started.set()
                # Loop until cooperatively cancelled.
                for _ in range(2000):
                    if is_cancellation_requested():
                        stopped.set()
                        return
                    time.sleep(0.005)
            finally:
                reset_current_token(handle)

        t = threading.Thread(target=worker)
        t.start()
        try:
            assert started.wait(2.0)
            assert not stopped.is_set()  # still running
            assert cancel_task("worker-task") is True
            assert stopped.wait(2.0)  # noticed cancellation
        finally:
            t.join(2.0)
            unregister_token("worker-task")


class _CooperativeTask(Task):
    """A task that records whether a cancellation token was bound during execution."""

    saw_token: bool = False

    def execute(self, stage: StageExecution) -> TaskResult:
        # The engine should have bound a token for the duration of execution.
        _CooperativeTask.saw_token = current_cancellation_token() is not None
        if is_cancellation_requested():
            return TaskResult.terminal("cancelled")
        return TaskResult.success(outputs={"ok": True})


class TestExecutionPathBinding:
    """The real RunTask execution path binds a token (no-op for normal tasks)."""

    def test_token_bound_during_normal_execution(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        from tests.conftest import setup_stabilize

        _CooperativeTask.saw_token = False
        processor, orchestrator, _ = setup_stabilize(
            repository, queue, extra_tasks={"coop": _CooperativeTask}
        )

        workflow = Workflow.create(
            application="cancel-app",
            name="coop-wf",
            stages=[
                StageExecution(
                    ref_id="s1",
                    type="coop",
                    name="Coop Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Coop Task",
                            implementing_class="coop",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )
        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        from stabilize.models.status import WorkflowStatus

        assert result.status == WorkflowStatus.SUCCEEDED
        # The token was bound while the task executed.
        assert _CooperativeTask.saw_token is True
