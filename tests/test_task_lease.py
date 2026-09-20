"""Tests for the opt-in distributed task lease.

Covers acquire/release/steal semantics on both backends, that the feature is
disabled by default, and that enabling it does not break normal execution.
"""

from __future__ import annotations

import time

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.task_lease import TaskLeaseManager
from stabilize.queue import Queue


class TestTaskLeaseManager:
    def test_acquire_then_other_owner_blocked(self, repository: WorkflowStore, backend: str) -> None:
        a = TaskLeaseManager(repository, owner="A")
        b = TaskLeaseManager(repository, owner="B")

        assert a.acquire("task-1") is True
        # B cannot acquire while A holds a non-expired lease.
        assert b.acquire("task-1") is False

        a.release("task-1")
        # After release, B can acquire.
        assert b.acquire("task-1") is True
        b.release("task-1")

    def test_reacquire_by_same_owner(self, repository: WorkflowStore, backend: str) -> None:
        a = TaskLeaseManager(repository, owner="A")
        assert a.acquire("task-2") is True
        # Re-entrant: the same owner can re-acquire its own lease.
        assert a.acquire("task-2") is True
        a.release("task-2")

    def test_expired_lease_can_be_stolen(self, repository: WorkflowStore, backend: str) -> None:
        short = TaskLeaseManager(repository, owner="A", ttl_seconds=0.05)
        other = TaskLeaseManager(repository, owner="B")

        assert short.acquire("task-3") is True
        time.sleep(0.12)  # let the lease expire
        # B can steal the expired lease.
        assert other.acquire("task-3") is True
        other.release("task-3")

    def test_release_only_affects_own_lease(self, repository: WorkflowStore, backend: str) -> None:
        a = TaskLeaseManager(repository, owner="A")
        b = TaskLeaseManager(repository, owner="B")
        assert a.acquire("task-4") is True
        # B releasing does nothing (A still holds it).
        b.release("task-4")
        assert b.acquire("task-4") is False
        a.release("task-4")


class TestRunTaskHandlerLeaseWiring:
    def test_disabled_by_default(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        from stabilize.handlers import RunTaskHandler
        from stabilize.tasks.registry import TaskRegistry

        handler = RunTaskHandler(queue, repository, TaskRegistry())
        assert handler.task_lease is None

    def test_enabled_via_env(
        self, repository: WorkflowStore, queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from stabilize.handlers import RunTaskHandler
        from stabilize.tasks.registry import TaskRegistry

        monkeypatch.setenv("STABILIZE_TASK_LEASE", "1")
        handler = RunTaskHandler(queue, repository, TaskRegistry())
        assert handler.task_lease is not None

    def test_normal_execution_unaffected_with_lease_enabled(
        self, repository: WorkflowStore, queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end: a workflow still succeeds with the lease feature enabled."""
        monkeypatch.setenv("STABILIZE_TASK_LEASE", "1")
        from tests.conftest import setup_stabilize

        processor, orchestrator, _ = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="lease-app",
            name="lease-wf",
            stages=[
                StageExecution(
                    ref_id="s1",
                    type="success",
                    name="Stage 1",
                    tasks=[
                        TaskExecution.create(
                            name="Task 1",
                            implementing_class="success",
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
        assert result.status == WorkflowStatus.SUCCEEDED


class TestLeaseFailsClosed:
    """STABILIZE_TASK_LEASE must provide leasing or refuse; never silently disable it."""

    def test_initialisation_failure_refuses(
        self, repository: WorkflowStore, queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from stabilize.handlers import RunTaskHandler
        from stabilize.persistence import task_lease as task_lease_module
        from stabilize.persistence.task_lease import TaskLeaseUnavailableError
        from stabilize.tasks.registry import TaskRegistry

        class Unavailable:
            def __init__(self, *args: object, **kwargs: object) -> None:
                raise RuntimeError("InsufficientPrivilege: permission denied for schema public")

        monkeypatch.setenv("STABILIZE_TASK_LEASE", "1")
        monkeypatch.setattr(task_lease_module, "TaskLeaseManager", Unavailable)

        with pytest.raises(TaskLeaseUnavailableError) as excinfo:
            RunTaskHandler(queue, repository, TaskRegistry())

        assert "double execution" in str(excinfo.value)
        assert "InsufficientPrivilege" in str(excinfo.value)

    def test_unparseable_ttl_refuses(
        self, repository: WorkflowStore, queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The old catch was bare `except Exception`, so a bad TTL disabled leasing too."""
        from stabilize.handlers import RunTaskHandler
        from stabilize.persistence.task_lease import TaskLeaseUnavailableError
        from stabilize.tasks.registry import TaskRegistry

        monkeypatch.setenv("STABILIZE_TASK_LEASE", "1")
        monkeypatch.setenv("STABILIZE_TASK_LEASE_TTL_SECONDS", "not-a-number")

        with pytest.raises(TaskLeaseUnavailableError):
            RunTaskHandler(queue, repository, TaskRegistry())

    def test_control_leasing_still_works_when_available(
        self, repository: WorkflowStore, queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without this, a handler that never leases would pass the two tests above."""
        from stabilize.handlers import RunTaskHandler
        from stabilize.persistence.task_lease import TaskLeaseManager
        from stabilize.tasks.registry import TaskRegistry

        monkeypatch.setenv("STABILIZE_TASK_LEASE", "1")
        handler = RunTaskHandler(queue, repository, TaskRegistry())

        assert isinstance(handler.task_lease, TaskLeaseManager)
