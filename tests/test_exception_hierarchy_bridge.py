"""
Repro tests for the split exception hierarchies (audit finding A3-16).

stabilize exported TWO unrelated StabilizeError classes: the assertion-helper
hierarchy (stabilize.exceptions / stabilize.assertions, re-exported as
stabilize.StabilizeError) and the engine hierarchy (stabilize.errors). A user
writing `except stabilize.StabilizeError` did NOT catch engine-raised errors
(TransientError, ConcurrencyError, verification failures) — the most natural
error-handling code silently failed. Same story for VerificationError.
"""

import stabilize
from stabilize.errors import ConcurrencyError, PermanentError, TransientError
from stabilize.errors import StabilizeError as EngineStabilizeError
from stabilize.errors.verification import VerificationError as EngineVerificationError


class TestUnifiedHierarchy:
    def test_engine_errors_are_caught_by_public_stabilize_error(self) -> None:
        for exc in (
            TransientError("t"),
            PermanentError("p"),
            ConcurrencyError("c"),
            EngineStabilizeError("s"),
            EngineVerificationError("v"),
        ):
            assert isinstance(exc, stabilize.StabilizeError), (
                f"`except stabilize.StabilizeError` misses engine-raised "
                f"{type(exc).__name__}"
            )

    def test_engine_verification_error_caught_by_public_verification_error(self) -> None:
        assert isinstance(EngineVerificationError("v"), stabilize.VerificationError), (
            "`except stabilize.VerificationError` misses the VerificationError "
            "the engine actually raises during output verification"
        )

    def test_assertion_errors_unchanged(self) -> None:
        """The assertion-helper classes keep their existing relationships."""
        from stabilize.assertions import ConfigError, ContextError, OutputError

        assert isinstance(ConfigError("c"), stabilize.StabilizeFatalError)
        assert isinstance(ContextError("c", key="k"), stabilize.StabilizeFatalError)
        assert isinstance(OutputError("o"), stabilize.StabilizeExpectedError)
        assert isinstance(ConfigError("c"), stabilize.StabilizeError)

    def test_engine_error_attributes_preserved(self) -> None:
        err = EngineVerificationError("boom", details={"k": "v"})
        assert err.details == {"k": "v"}
        assert err.code == 600

    def test_engine_errors_not_conflated_with_assertion_subtypes(self) -> None:
        """Bridging must not make engine errors instances of the assertion
        FATAL branch (which would change user-side semantics)."""
        assert not isinstance(TransientError("t"), stabilize.StabilizeFatalError)
        assert not isinstance(TransientError("t"), stabilize.StabilizeExpectedError)
