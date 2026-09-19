"""A postgresql:// URL requests circuit state shared across instances.
Substituting process-local state must not be silent."""

from __future__ import annotations

import logging

import pytest

from stabilize.resilience import circuits
from stabilize.resilience.circuits import CircuitStorageUnavailableError


@pytest.fixture
def unavailable_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    import resilient_circuit.storage as rcs

    class Unavailable:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("SchemaNotReady: breaker table missing or drifted")

    monkeypatch.setattr(rcs, "PostgresStorage", Unavailable)
    monkeypatch.delenv("STABILIZE_CIRCUIT_STORAGE_STRICT", raising=False)


class TestDegradationIsLoud:
    def test_known_positive_a_working_selection_is_reported(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            storage = circuits._create_storage("sqlite:///x")
        assert type(storage).__name__ == "InMemoryStorage"
        assert caplog.text

    def test_failure_logs_at_error(
        self, unavailable_postgres: None, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            circuits._create_storage("postgresql://u:p@h/db")
        assert any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_error_names_the_consequence(
        self, unavailable_postgres: None, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            circuits._create_storage("postgresql://u:p@h/db")
        assert "PROCESS-LOCAL" in caplog.text

    def test_does_not_claim_postgres_when_construction_failed(
        self, unavailable_postgres: None, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            circuits._create_storage("postgresql://u:p@h/db")
        assert "Using PostgreSQL storage for circuit breakers" not in caplog.text


class TestStrictModeFailsClosed:
    @pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE"])
    def test_strict_raises(
        self, unavailable_postgres: None, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", value)
        with pytest.raises(CircuitStorageUnavailableError):
            circuits._create_storage("postgresql://u:p@h/db")

    def test_unset_still_degrades(self, unavailable_postgres: None) -> None:
        """The release direction: without opting in, behaviour is unchanged."""
        storage = circuits._create_storage("postgresql://u:p@h/db")
        assert type(storage).__name__ == "InMemoryStorage"

    def test_strict_does_not_break_the_working_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", "1")
        storage = circuits._create_storage("sqlite:///x")
        assert type(storage).__name__ == "InMemoryStorage"
