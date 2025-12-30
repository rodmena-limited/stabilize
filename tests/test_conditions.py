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
