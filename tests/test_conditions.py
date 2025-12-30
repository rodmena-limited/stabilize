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
