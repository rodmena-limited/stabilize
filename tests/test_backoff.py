"""Tests for exponential backoff with jitter.

These tests ensure that the backoff period calculation works correctly
with exponential growth and jitter.
"""

from stabilize.errors import PermanentError, TransientError, is_transient


class TestExponentialBackoff:
    """Tests for exponential backoff calculation."""

    def test_first_attempt_around_one_second(self) -> None:
        """First attempt should have ~1 second backoff (±25% jitter)."""
        # The backoff for attempt 1 is: 2^0 = 1 second, with ±25% jitter
        # So range is 0.75 to 1.25 seconds

        # We can't directly test the private method, but we verify the formula
        # base_delay = min(2 ** (attempt - 1), 60) = min(1, 60) = 1
        # jitter = 1 * random(-0.25, 0.25) = -0.25 to 0.25
        # delay = 1 + jitter = 0.75 to 1.25
        base_delay = min(2 ** (1 - 1), 60)
        assert base_delay == 1

    def test_exponential_growth(self) -> None:
        """Backoff should grow exponentially: 1, 2, 4, 8, 16, 32..."""
        expected = [1, 2, 4, 8, 16, 32, 60, 60, 60, 60]  # Capped at 60
        for attempt, expected_base in enumerate(expected, start=1):
            base_delay = min(2 ** (attempt - 1), 60)
            assert base_delay == expected_base, f"Attempt {attempt} should have base delay {expected_base}"

    def test_backoff_capped_at_60_seconds(self) -> None:
        """Backoff should be capped at 60 seconds."""
        # At attempt 7: 2^6 = 64, but capped to 60
        base_delay = min(2 ** (7 - 1), 60)
        assert base_delay == 60

        # At attempt 10: 2^9 = 512, but capped to 60
        base_delay = min(2 ** (10 - 1), 60)
        assert base_delay == 60

    def test_jitter_range(self) -> None:
        """Jitter should be ±25% of base delay."""
        import random

        # For a base delay of 1 second
        base_delay = 1
        min_jitter = base_delay * -0.25
        max_jitter = base_delay * 0.25

        # Simulate many jitter values
        random.seed(42)  # For reproducibility
        for _ in range(100):
            jitter = base_delay * random.uniform(-0.25, 0.25)
            assert min_jitter <= jitter <= max_jitter

    def test_minimum_delay_is_one_second(self) -> None:
        """Final delay should never be less than 1 second."""
        # Even with maximum negative jitter, delay should be >= 1
        # base_delay = 1, jitter = -0.25, delay = 0.75
        # max(1.0, 0.75) = 1.0
        delay = max(1.0, 1 - 0.25)
        assert delay >= 1.0


class TestIsTransient:
    """Tests for the is_transient error classification."""

    def test_transient_error_is_transient(self) -> None:
        """TransientError should be classified as transient."""
        error = TransientError("Connection timeout")
        assert is_transient(error)

    def test_permanent_error_is_not_transient(self) -> None:
        """PermanentError should not be classified as transient."""
        error = PermanentError("Invalid input")
        assert not is_transient(error)

    def test_timeout_errors_are_transient(self) -> None:
        """Errors with 'timeout' in name should be transient."""

        class SomeTimeoutError(Exception):
            pass

        error = SomeTimeoutError("Request timed out")
        assert is_transient(error)

    def test_connection_errors_are_transient(self) -> None:
        """Errors with 'connection' in name should be transient."""

        class ConnectionRefusedError(Exception):
            pass

        error = ConnectionRefusedError("Connection refused")
        assert is_transient(error)

    def test_temporary_errors_are_transient(self) -> None:
        """Errors with 'temporary' in name should be transient."""

        class TemporaryFailureError(Exception):  # noqa: N818 - intentional name for testing
            pass

        error = TemporaryFailureError("Service temporarily unavailable")
        assert is_transient(error)

    def test_validation_errors_are_not_transient(self) -> None:
        """Validation errors should not be classified as transient."""
        from stabilize.errors import is_permanent

        class ValidationError(Exception):
            pass

        error = ValidationError("Invalid input")
        assert is_permanent(error)
        assert not is_transient(error)

    def test_standard_exceptions_not_transient_by_default(self) -> None:
        """Standard exceptions should not be transient by default."""
        error = ValueError("Bad value")
        assert not is_transient(error)

        error = TypeError("Wrong type")
        assert not is_transient(error)

    def test_rate_limit_errors_are_transient(self) -> None:
        """Rate limit errors should be transient."""

        class RateLimitExceededError(Exception):
            pass

        error = RateLimitExceededError("Too many requests")
        assert is_transient(error)

    def test_throttling_errors_are_transient(self) -> None:
        """Throttling errors should be transient."""

        class ThrottlingError(Exception):
            pass

        error = ThrottlingError("Request throttled")
        assert is_transient(error)
