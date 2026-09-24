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
    def test_known_positive_a_working_selection_is_reported(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            storage = circuits._create_storage("sqlite:///x")
        assert type(storage).__name__ == "InMemoryStorage"
        assert caplog.text

    def test_failure_logs_at_error(self, unavailable_postgres: None, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG):
            circuits._create_storage("postgresql://u:p@h/db")
        assert any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_error_names_the_consequence(self, unavailable_postgres: None, caplog: pytest.LogCaptureFixture) -> None:
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
    def test_strict_raises(self, unavailable_postgres: None, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", value)
        with pytest.raises(CircuitStorageUnavailableError):
            circuits._create_storage("postgresql://u:p@h/db")

    def test_unset_still_degrades(self, unavailable_postgres: None) -> None:
        """The release direction: without opting in, behaviour is unchanged."""
        storage = circuits._create_storage("postgresql://u:p@h/db")
        assert type(storage).__name__ == "InMemoryStorage"

    def test_strict_does_not_break_the_working_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", "1")
        storage = circuits._create_storage("sqlite:///x")
        assert type(storage).__name__ == "InMemoryStorage"


class TestDsnClassification:
    """A PostgreSQL DSN misread as 'no database' silently selects
    process-local circuit state."""

    @pytest.fixture
    def spy(self, monkeypatch: pytest.MonkeyPatch) -> list[object]:
        import resilient_circuit.storage as rcs

        attempted: list[object] = []

        class Spy:
            def __init__(self, *args: object, **kwargs: object) -> None:
                attempted.append(kwargs.get("connection_string"))

        monkeypatch.setattr(rcs, "PostgresStorage", Spy)
        return attempted

    @pytest.mark.parametrize(
        "url",
        [
            "postgresql://u:p@h/db",
            "postgresql+psycopg://u:p@h/db",
            "postgres://u:p@h/db",
            "POSTGRESQL://u:p@h/db",
            "  postgresql://u:p@h/db  ",
            "host=h dbname=d user=u",
            "service=mysvc",
        ],
    )
    def test_postgres_forms_reach_the_postgres_branch(self, spy: list[object], url: str) -> None:
        circuits._create_storage(url)
        assert spy, f"{url!r} did not reach the PostgreSQL branch"

    @pytest.mark.parametrize("url", ["HOST=h DBNAME=d", "postgresql+asyncpg://u:p@h/db"])
    def test_postgres_forms_libpq_cannot_parse_are_reported_not_treated_as_no_database(
        self, spy: list[object], url: str, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("STABILIZE_CIRCUIT_STORAGE_STRICT", raising=False)
        caplog.set_level(logging.INFO)
        circuits._create_storage(url)
        assert not spy
        messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any("storage unavailable" in m and "could not be parsed" in m for m in messages), messages
        assert not any("no PostgreSQL DSN configured" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("url", ["sqlite:///x", "sqlite:///:memory:", None, "", "   "])
    def test_non_postgres_forms_do_not(self, spy: list[object], url: str | None) -> None:
        """Without this the test above passes by routing everything to PostgreSQL."""
        circuits._create_storage(url)
        assert not spy, f"{url!r} wrongly reached the PostgreSQL branch"

    def test_postgres_url_is_passed_through_intact(self, spy: list[object]) -> None:
        circuits._create_storage("postgresql://u:p@h/db?sslmode=verify-full")
        assert spy[0] == "postgresql://u:p@h/db?sslmode=verify-full"

    def test_in_memory_log_no_longer_claims_sqlite_for_every_case(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            circuits._create_storage(None)
        assert "SQLite or no database" not in caplog.text
        assert "process-local" in caplog.text
