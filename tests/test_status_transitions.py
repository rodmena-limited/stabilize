"""Tests for state transition validation.

These tests ensure that state transitions are properly validated to prevent
invalid workflow states.
"""

import pytest

from stabilize.models.status import (
    VALID_TRANSITIONS,
    InvalidStateTransitionError,
    WorkflowStatus,
    can_transition,
    validate_transition,
)


class TestCanTransition:
    """Tests for the can_transition function."""

    def test_same_state_always_valid(self) -> None:
        """Same state transition should always be valid (idempotent)."""
        for status in WorkflowStatus:
            assert can_transition(status, status)

    def test_not_started_to_running(self) -> None:
        """NOT_STARTED -> RUNNING is valid."""
        assert can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING)

    def test_not_started_to_canceled(self) -> None:
        """NOT_STARTED -> CANCELED is valid."""
        assert can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.CANCELED)

    def test_not_started_to_skipped(self) -> None:
        """NOT_STARTED -> SKIPPED is valid."""
        assert can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.SKIPPED)

    def test_running_to_succeeded(self) -> None:
        """RUNNING -> SUCCEEDED is valid."""
        assert can_transition(WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)

    def test_running_to_failed_continue(self) -> None:
        """RUNNING -> FAILED_CONTINUE is valid."""
        assert can_transition(WorkflowStatus.RUNNING, WorkflowStatus.FAILED_CONTINUE)

    def test_running_to_terminal(self) -> None:
        """RUNNING -> TERMINAL is valid."""
        assert can_transition(WorkflowStatus.RUNNING, WorkflowStatus.TERMINAL)

    def test_running_to_canceled(self) -> None:
        """RUNNING -> CANCELED is valid."""
        assert can_transition(WorkflowStatus.RUNNING, WorkflowStatus.CANCELED)

    def test_running_to_paused(self) -> None:
        """RUNNING -> PAUSED is valid."""
        assert can_transition(WorkflowStatus.RUNNING, WorkflowStatus.PAUSED)

    def test_paused_to_running(self) -> None:
        """PAUSED -> RUNNING is valid (resume)."""
        assert can_transition(WorkflowStatus.PAUSED, WorkflowStatus.RUNNING)

    def test_paused_to_canceled(self) -> None:
        """PAUSED -> CANCELED is valid."""
        assert can_transition(WorkflowStatus.PAUSED, WorkflowStatus.CANCELED)


class TestInvalidTransitions:
    """Tests for invalid state transitions."""

    def test_succeeded_cannot_transition(self) -> None:
        """SUCCEEDED is terminal - no transitions allowed."""
        for target in WorkflowStatus:
            if target != WorkflowStatus.SUCCEEDED:
                assert not can_transition(WorkflowStatus.SUCCEEDED, target)

    def test_terminal_cannot_transition(self) -> None:
        """TERMINAL is terminal - no transitions allowed."""
        for target in WorkflowStatus:
            if target != WorkflowStatus.TERMINAL:
                assert not can_transition(WorkflowStatus.TERMINAL, target)

    def test_canceled_cannot_transition(self) -> None:
        """CANCELED is terminal - no transitions allowed."""
        for target in WorkflowStatus:
            if target != WorkflowStatus.CANCELED:
                assert not can_transition(WorkflowStatus.CANCELED, target)

    def test_stopped_cannot_transition(self) -> None:
        """STOPPED is terminal - no transitions allowed."""
        for target in WorkflowStatus:
            if target != WorkflowStatus.STOPPED:
                assert not can_transition(WorkflowStatus.STOPPED, target)

    def test_skipped_cannot_transition(self) -> None:
        """SKIPPED is terminal - no transitions allowed."""
        for target in WorkflowStatus:
            if target != WorkflowStatus.SKIPPED:
                assert not can_transition(WorkflowStatus.SKIPPED, target)

    def test_not_started_to_succeeded_invalid(self) -> None:
        """NOT_STARTED -> SUCCEEDED is invalid (must go through RUNNING)."""
        assert not can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.SUCCEEDED)

    def test_not_started_to_terminal_invalid(self) -> None:
        """NOT_STARTED -> TERMINAL is invalid (must go through RUNNING)."""
        assert not can_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.TERMINAL)


class TestValidateTransition:
    """Tests for the validate_transition function."""

    def test_valid_transition_no_error(self) -> None:
        """Valid transitions should not raise."""
        # Should not raise
        validate_transition(WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING)
        validate_transition(WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)
        validate_transition(WorkflowStatus.PAUSED, WorkflowStatus.RUNNING)

    def test_invalid_transition_raises(self) -> None:
        """Invalid transitions should raise InvalidStateTransitionError."""
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            validate_transition(WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING)

        error = exc_info.value
        assert error.current == WorkflowStatus.SUCCEEDED
        assert error.target == WorkflowStatus.RUNNING

    def test_error_includes_entity_info(self) -> None:
        """Error should include entity type and ID if provided."""
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            validate_transition(
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.RUNNING,
                entity_type="workflow",
                entity_id="wf-123",
            )

        error = exc_info.value
        assert error.entity_type == "workflow"
        assert error.entity_id == "wf-123"
        assert "workflow" in str(error)
        assert "wf-123" in str(error)

    def test_same_state_no_error(self) -> None:
        """Same state transition should not raise (idempotent)."""
        validate_transition(WorkflowStatus.SUCCEEDED, WorkflowStatus.SUCCEEDED)


class TestValidTransitionsCompleteness:
    """Tests to ensure VALID_TRANSITIONS covers all states."""

    def test_all_states_have_transitions_defined(self) -> None:
        """All workflow states should have transition rules defined."""
        for status in WorkflowStatus:
            assert status in VALID_TRANSITIONS, f"Missing transitions for {status}"

    def test_terminal_states_have_empty_transitions(self) -> None:
        """Terminal states should have no valid outgoing transitions."""
        terminal_states = {
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED_CONTINUE,
            WorkflowStatus.TERMINAL,
            WorkflowStatus.CANCELED,
            WorkflowStatus.STOPPED,
            WorkflowStatus.SKIPPED,
        }
        for status in terminal_states:
            assert len(VALID_TRANSITIONS[status]) == 0, f"{status} should have no transitions"
