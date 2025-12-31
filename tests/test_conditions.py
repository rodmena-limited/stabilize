"""Tests for the structured conditions system."""

from stabilize.conditions import (
    Condition,
    ConditionReason,
    ConditionSet,
    ConditionType,
)


class TestCondition:
    """Tests for Condition class."""

    def test_ready_condition_true(self) -> None:
        """Test creating a ready condition with True status."""
        condition = Condition.ready(
            status=True,
            reason=ConditionReason.TASKS_SUCCEEDED,
            message="All tasks completed",
        )
        assert condition.type == ConditionType.READY
        assert condition.status is True
        assert condition.reason == ConditionReason.TASKS_SUCCEEDED
        assert condition.message == "All tasks completed"

    def test_ready_condition_false(self) -> None:
        """Test creating a ready condition with False status."""
        condition = Condition.ready(
            status=False,
            reason=ConditionReason.IN_PROGRESS,
            message="Still processing",
        )
        assert condition.status is False

    def test_progressing_condition(self) -> None:
        """Test creating a progressing condition."""
        condition = Condition.progressing(
            status=True,
            reason=ConditionReason.IN_PROGRESS,
        )
        assert condition.type == ConditionType.PROGRESSING
        assert condition.status is True

    def test_verified_condition(self) -> None:
        """Test creating a verified condition."""
        condition = Condition.verified(
            status=True,
            reason=ConditionReason.VERIFICATION_PASSED,
            message="All checks passed",
        )
        assert condition.type == ConditionType.VERIFIED
        assert condition.status is True

    def test_failed_condition(self) -> None:
        """Test creating a failed condition."""
        condition = Condition.failed(
            reason=ConditionReason.TASK_FAILED,
            message="Task execution failed",
        )
        assert condition.type == ConditionType.FAILED
        assert condition.status is True  # Failed always has status=True
        assert condition.message == "Task execution failed"

    def test_config_valid_condition(self) -> None:
        """Test creating a config valid condition."""
        condition = Condition.config_valid(status=True)
        assert condition.type == ConditionType.CONFIG_VALID
        assert condition.reason == ConditionReason.CONFIG_VALID

    def test_string_type_conversion(self) -> None:
        """Test that string types are converted to enums when possible."""
        condition = Condition(
            type="Ready",
            status=True,
            reason="TasksSucceeded",
        )
        assert condition.type == ConditionType.READY
        assert condition.reason == ConditionReason.TASKS_SUCCEEDED

    def test_custom_string_type(self) -> None:
        """Test that custom string types are preserved."""
        condition = Condition(
            type="CustomType",
            status=True,
            reason="CustomReason",
        )
        assert condition.type == "CustomType"
        assert condition.reason == "CustomReason"

    def test_update_status_changes_timestamp(self) -> None:
        """Test that updating status changes the transition time."""
        original = Condition.ready(True, ConditionReason.TASKS_SUCCEEDED)
        original_time = original.last_transition_time

        # Small delay to ensure different timestamp
        import time

        time.sleep(0.01)

        updated = original.update(status=False)
        assert updated.status is False
        assert updated.last_transition_time > original_time

    def test_update_same_status_preserves_timestamp(self) -> None:
        """Test that updating without status change preserves timestamp."""
        original = Condition.ready(True, ConditionReason.TASKS_SUCCEEDED)
        original_time = original.last_transition_time

        updated = original.update(message="New message")
        assert updated.message == "New message"
        assert updated.last_transition_time == original_time

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        condition = Condition.ready(
            status=True,
            reason=ConditionReason.TASKS_SUCCEEDED,
            message="Done",
        )
        d = condition.to_dict()
        assert d["type"] == "Ready"
        assert d["status"] is True
        assert d["reason"] == "TasksSucceeded"
        assert d["message"] == "Done"
        assert "lastTransitionTime" in d
        assert "observedGeneration" in d

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "type": "Ready",
            "status": True,
            "reason": "TasksSucceeded",
            "message": "All good",
            "lastTransitionTime": "2024-01-15T10:30:00",
            "observedGeneration": 3,
        }
        condition = Condition.from_dict(data)
        assert condition.type == ConditionType.READY
        assert condition.status is True
        assert condition.reason == ConditionReason.TASKS_SUCCEEDED
        assert condition.observed_generation == 3

    def test_observed_generation_increments(self) -> None:
        """Test that observed generation increments on update."""
        original = Condition.ready(True, ConditionReason.TASKS_SUCCEEDED)
        assert original.observed_generation == 0

        updated = original.update(status=False)
        assert updated.observed_generation == 1


class TestConditionSet:
    """Tests for ConditionSet class."""

    def test_empty_set(self) -> None:
        """Test empty condition set."""
        conditions = ConditionSet()
        assert len(conditions) == 0
        assert not conditions.is_ready
        assert not conditions.is_progressing

    def test_set_condition(self) -> None:
        """Test setting a condition."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        assert len(conditions) == 1
        assert conditions.is_ready

    def test_get_condition(self) -> None:
        """Test getting a condition by type."""
        conditions = ConditionSet()
        ready = Condition.ready(True, ConditionReason.TASKS_SUCCEEDED, "Done")
        conditions.set(ready)

        retrieved = conditions.get(ConditionType.READY)
        assert retrieved is not None
        assert retrieved.message == "Done"

    def test_get_missing_condition(self) -> None:
        """Test getting a condition that doesn't exist."""
        conditions = ConditionSet()
        assert conditions.get(ConditionType.READY) is None

    def test_update_existing_condition(self) -> None:
        """Test that setting a condition of same type updates it."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED, "First"))
        conditions.set(Condition.ready(False, ConditionReason.IN_PROGRESS, "Second"))

        assert len(conditions) == 1
        ready = conditions.get(ConditionType.READY)
        assert ready is not None
        assert ready.message == "Second"
        assert ready.status is False

    def test_remove_condition(self) -> None:
        """Test removing a condition."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        conditions.remove(ConditionType.READY)
        assert len(conditions) == 0

    def test_all_conditions(self) -> None:
        """Test getting all conditions."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        conditions.set(Condition.progressing(False, ConditionReason.STAGE_COMPLETED))

        all_conds = conditions.all()
        assert len(all_conds) == 2

    def test_clear_conditions(self) -> None:
        """Test clearing all conditions."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        conditions.set(Condition.progressing(False, ConditionReason.STAGE_COMPLETED))
        conditions.clear()
        assert len(conditions) == 0

    def test_is_ready_property(self) -> None:
        """Test is_ready property."""
        conditions = ConditionSet()
        assert not conditions.is_ready

        conditions.set(Condition.ready(False, ConditionReason.IN_PROGRESS))
        assert not conditions.is_ready

        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        assert conditions.is_ready

    def test_is_verified_property(self) -> None:
        """Test is_verified property."""
        conditions = ConditionSet()
        assert not conditions.is_verified

        conditions.set(Condition.verified(True, ConditionReason.VERIFICATION_PASSED))
        assert conditions.is_verified

    def test_has_failed_property(self) -> None:
        """Test has_failed property."""
        conditions = ConditionSet()
        assert not conditions.has_failed

        conditions.set(Condition.failed(ConditionReason.TASK_FAILED, "Error"))
        assert conditions.has_failed

    def test_is_config_valid_default(self) -> None:
        """Test is_config_valid defaults to True when not set."""
        conditions = ConditionSet()
        assert conditions.is_config_valid  # Default to valid

    def test_contains(self) -> None:
        """Test __contains__ method."""
        conditions = ConditionSet()
        assert ConditionType.READY not in conditions

        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        assert ConditionType.READY in conditions

    def test_iteration(self) -> None:
        """Test iterating over conditions."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        conditions.set(Condition.progressing(False, ConditionReason.STAGE_COMPLETED))

        types = [c.type for c in conditions]
        assert ConditionType.READY in types
        assert ConditionType.PROGRESSING in types

    def test_to_list(self) -> None:
        """Test serialization to list."""
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, ConditionReason.TASKS_SUCCEEDED))
        conditions.set(Condition.progressing(False, ConditionReason.STAGE_COMPLETED))

        lst = conditions.to_list()
        assert len(lst) == 2
        assert all(isinstance(item, dict) for item in lst)

    def test_from_list(self) -> None:
        """Test deserialization from list."""
        data = [
            {"type": "Ready", "status": True, "reason": "TasksSucceeded", "message": ""},
            {"type": "Progressing", "status": False, "reason": "StageCompleted", "message": ""},
        ]
        conditions = ConditionSet.from_list(data)
        assert len(conditions) == 2
        assert conditions.is_ready
        assert not conditions.is_progressing

    def test_init_with_conditions(self) -> None:
        """Test initialization with existing conditions."""
        initial = [
            Condition.ready(True, ConditionReason.TASKS_SUCCEEDED),
            Condition.progressing(False, ConditionReason.STAGE_COMPLETED),
        ]
        conditions = ConditionSet(initial)
        assert len(conditions) == 2
        assert conditions.is_ready
