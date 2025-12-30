from datetime import UTC, datetime
from stabilize.models.stage import StageExecution
from stabilize.verification import (
    CallableVerifier,
    OutputVerifier,
    Verifier,
    VerifyResult,
    VerifyStatus,
)

class TestVerifyResult:
    """Tests for VerifyResult class."""

    def test_ok_result(self) -> None:
        """Test creating a successful result."""
        result = VerifyResult.ok("All good")
        assert result.status == VerifyStatus.OK
        assert result.message == "All good"
        assert result.is_ok
        assert not result.is_retry
        assert not result.is_failed
        assert result.is_terminal

    def test_ok_default_message(self) -> None:
        """Test OK result with default message."""
        result = VerifyResult.ok()
        assert result.message == "Verification passed"

    def test_retry_result(self) -> None:
        """Test creating a retry result."""
        result = VerifyResult.retry("Still waiting", {"attempt": 1})
        assert result.status == VerifyStatus.RETRY
        assert result.message == "Still waiting"
        assert result.details == {"attempt": 1}
        assert result.is_retry
        assert not result.is_ok
        assert not result.is_failed
        assert not result.is_terminal
