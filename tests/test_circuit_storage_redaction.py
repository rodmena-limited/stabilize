"""Circuit-breaker storage never logs or raises a DSN password (#56)."""

from __future__ import annotations

import logging
import traceback
from typing import Any

import pytest
from resilient_circuit.storage import InMemoryStorage

from stabilize.resilience import circuits
from stabilize.resilience.circuits import CircuitStorageUnavailableError, _create_storage

SECRET = "Zq7SentinelPw9xK"
DRIVER_QUALIFIED = f"postgresql+asyncpg://u:{SECRET}@127.0.0.1:1/db"


def test_detector_can_see_the_secret() -> None:
    assert SECRET in DRIVER_QUALIFIED


def test_unparseable_dsn_degrades_without_logging_the_password(caplog: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STABILIZE_CIRCUIT_STORAGE_STRICT", raising=False)
    caplog.set_level(logging.DEBUG)
    storage = _create_storage(DRIVER_QUALIFIED)
    assert isinstance(storage, InMemoryStorage)
    messages = [record.getMessage() for record in caplog.records]
    assert any("circuit breaker storage unavailable" in m for m in messages)
    assert not any(SECRET in m for m in messages)
    assert any("u:***@" in m for m in messages)


def test_strict_mode_raises_without_the_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", "1")
    with pytest.raises(CircuitStorageUnavailableError) as raised:
        _create_storage(DRIVER_QUALIFIED)
    assert SECRET not in str(raised.value)
    assert SECRET not in "".join(traceback.format_exception(raised.value))


def test_resilient_circuit_never_receives_the_unparseable_string(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[str] = []

    class RecordingStorage:
        def __init__(self, connection_string: str) -> None:
            received.append(connection_string)

    import resilient_circuit.storage as rc_storage

    monkeypatch.delenv("STABILIZE_CIRCUIT_STORAGE_STRICT", raising=False)
    monkeypatch.setattr(rc_storage, "PostgresStorage", RecordingStorage)
    _create_storage(DRIVER_QUALIFIED)
    assert received == []

    well_formed = "postgresql://u:p@127.0.0.1:1/db?sslmode=disable"
    storage = _create_storage(well_formed)
    assert isinstance(storage, RecordingStorage)
    assert received == [well_formed]


def test_import_failure_keeps_its_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_STORAGE_STRICT", "1")
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "resilient_circuit.storage" and args and args[2] and "PostgresStorage" in args[2]:
            raise ImportError("no postgres storage")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    with pytest.raises(CircuitStorageUnavailableError) as raised:
        circuits._create_storage("postgresql://u:p@127.0.0.1:1/db")
    assert isinstance(raised.value.__cause__, ImportError)
