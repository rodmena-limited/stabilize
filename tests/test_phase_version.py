"""Tests for phase-version state tracking and snapshots."""

import pytest

from stabilize.models.snapshot import (
    StageStateSnapshot,
    TaskStateSnapshot,
    _freeze_dict,
)
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution


class TestFreezeDict:
    """Tests for _freeze_dict utility."""

    def test_empty_dict_returns_empty_tuple(self) -> None:
        """Empty dict returns empty tuple."""
        result = _freeze_dict({})
        assert result == ()

    def test_dict_converted_to_sorted_tuples(self) -> None:
        """Dict items converted to sorted tuple of tuples."""
        result = _freeze_dict({"b": 2, "a": 1, "c": 3})
        assert result == (("a", 1), ("b", 2), ("c", 3))

    def test_nested_values_preserved(self) -> None:
        """Nested values are preserved (not deep frozen)."""
        result = _freeze_dict({"key": [1, 2, 3]})
        assert result == (("key", [1, 2, 3]),)


class TestStageStateSnapshot:
    """Tests for StageStateSnapshot frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """Snapshot is immutable."""
        snapshot = StageStateSnapshot(
            id="test-id",
            ref_id="test-ref",
            status=WorkflowStatus.RUNNING,
            version=1,
            context=(),
            outputs=(),
        )

        with pytest.raises(Exception):
            snapshot.version = 2  # type: ignore

    def test_phase_version_property(self) -> None:
        """phase_version returns (status_name, version) tuple."""
        snapshot = StageStateSnapshot(
            id="test-id",
            ref_id="test-ref",
            status=WorkflowStatus.RUNNING,
            version=5,
            context=(),
            outputs=(),
        )

        assert snapshot.phase_version == ("RUNNING", 5)

    def test_context_dict_method(self) -> None:
        """context_dict() returns mutable dict from frozen tuple."""
        snapshot = StageStateSnapshot(
            id="test-id",
            ref_id="test-ref",
            status=WorkflowStatus.RUNNING,
            version=1,
            context=(("key1", "value1"), ("key2", "value2")),
            outputs=(),
        )

        result = snapshot.context_dict()
        assert result == {"key1": "value1", "key2": "value2"}
        assert isinstance(result, dict)

    def test_outputs_dict_method(self) -> None:
        """outputs_dict() returns mutable dict from frozen tuple."""
        snapshot = StageStateSnapshot(
            id="test-id",
            ref_id="test-ref",
            status=WorkflowStatus.SUCCEEDED,
            version=1,
            context=(),
            outputs=(("output1", 100), ("output2", 200)),
        )

        result = snapshot.outputs_dict()
        assert result == {"output1": 100, "output2": 200}


class TestTaskStateSnapshot:
    """Tests for TaskStateSnapshot frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """Snapshot is immutable."""
        snapshot = TaskStateSnapshot(
            id="task-id",
            name="test-task",
            status=WorkflowStatus.RUNNING,
            version=1,
        )

        with pytest.raises(Exception):
            snapshot.name = "changed"  # type: ignore

    def test_phase_version_property(self) -> None:
        """phase_version returns (status_name, version) tuple."""
        snapshot = TaskStateSnapshot(
            id="task-id",
            name="test-task",
            status=WorkflowStatus.SUCCEEDED,
            version=3,
        )

        assert snapshot.phase_version == ("SUCCEEDED", 3)

    def test_exception_dict_method(self) -> None:
        """exception_dict() returns mutable dict from frozen tuple."""
        snapshot = TaskStateSnapshot(
            id="task-id",
            name="test-task",
            status=WorkflowStatus.TERMINAL,
            version=1,
            exception_details=(("error", "Something failed"),),
        )

        result = snapshot.exception_dict()
        assert result == {"error": "Something failed"}


class TestStageExecutionPhaseVersion:
    """Tests for phase_version and state_snapshot() on StageExecution."""

    def test_phase_version_property(self) -> None:
        """StageExecution.phase_version returns (status_name, version)."""
        stage = StageExecution(
            name="test",
            type="test",
            ref_id="test-ref",
            status=WorkflowStatus.RUNNING,
            version=7,
        )

        assert stage.phase_version == ("RUNNING", 7)

    def test_state_snapshot_returns_frozen_copy(self) -> None:
        """state_snapshot() returns frozen StageStateSnapshot."""
        stage = StageExecution(
            name="test",
            type="test",
            ref_id="test-ref",
            status=WorkflowStatus.SUCCEEDED,
            version=3,
            context={"input": "value"},
            outputs={"result": 42},
        )

        snapshot = stage.state_snapshot()

        assert isinstance(snapshot, StageStateSnapshot)
        assert snapshot.id == stage.id
        assert snapshot.ref_id == stage.ref_id
        assert snapshot.status == stage.status
        assert snapshot.version == stage.version
        assert snapshot.context_dict() == {"input": "value"}
        assert snapshot.outputs_dict() == {"result": 42}

    def test_snapshot_not_affected_by_stage_changes(self) -> None:
        """Snapshot is independent of subsequent stage changes."""
        stage = StageExecution(
            name="test",
            type="test",
            ref_id="test-ref",
            status=WorkflowStatus.RUNNING,
            version=1,
            context={"key": "original"},
        )

        snapshot = stage.state_snapshot()

        # Modify stage after snapshot
        stage.status = WorkflowStatus.SUCCEEDED
        stage.version = 2
        stage.context["key"] = "changed"

        # Snapshot should still have original values
        assert snapshot.status == WorkflowStatus.RUNNING
        assert snapshot.version == 1
        assert snapshot.context_dict()["key"] == "original"


class TestTaskExecutionPhaseVersion:
    """Tests for phase_version and state_snapshot() on TaskExecution."""

    def test_phase_version_property(self) -> None:
        """TaskExecution.phase_version returns (status_name, version)."""
        task = TaskExecution(
            name="test-task",
            status=WorkflowStatus.RUNNING,
            version=4,
        )

        assert task.phase_version == ("RUNNING", 4)

    def test_state_snapshot_returns_frozen_copy(self) -> None:
        """state_snapshot() returns frozen TaskStateSnapshot."""
        task = TaskExecution(
            name="test-task",
            status=WorkflowStatus.SUCCEEDED,
            version=2,
            implementing_class="test.TestTask",
        )

        snapshot = task.state_snapshot()

        assert isinstance(snapshot, TaskStateSnapshot)
        assert snapshot.id == task.id
        assert snapshot.name == task.name
        assert snapshot.status == task.status
        assert snapshot.version == task.version
        assert snapshot.implementing_class == "test.TestTask"
