"""A connection string libpq cannot parse fails before any pool exists, without leaking its password (#53)."""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

import pytest

from stabilize.persistence import connection as conn_mod
from stabilize.persistence.connection import ConnectionManager, SingletonMeta

SECRET = "Zq7SentinelPw9xK"
MALFORMED = [
    f"postgresql+psycopg://u:{SECRET}@127.0.0.1:1/db",
    f"postgres!!!://u:{SECRET}@127.0.0.1:1/db",
]


@pytest.fixture
def manager() -> Any:
    SingletonMeta.reset(ConnectionManager)
    yield ConnectionManager()
    SingletonMeta.reset(ConnectionManager)


class _RecordingPool:
    created: list[str] = []

    def __init__(self, conninfo: str, **kwargs: Any) -> None:
        _RecordingPool.created.append(conninfo)

    def close(self) -> None:
        pass


@pytest.mark.parametrize("dsn", MALFORMED)
def test_detector_can_see_the_secret(dsn: str) -> None:
    assert SECRET in dsn


@pytest.mark.parametrize("dsn", MALFORMED)
def test_malformed_dsn_raises_without_the_password(manager: Any, dsn: str, caplog: Any) -> None:
    caplog.set_level(logging.DEBUG)
    started = time.monotonic()
    with pytest.raises(ValueError, match="could not be parsed") as raised:
        manager.get_postgres_pool(dsn)
    elapsed = time.monotonic() - started

    assert SECRET not in str(raised.value)
    assert SECRET not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert not any(SECRET in record.getMessage() for record in caplog.records)
    assert elapsed < 5.0


@pytest.mark.parametrize("dsn", MALFORMED)
def test_malformed_dsn_creates_no_pool_and_no_hold(manager: Any, dsn: str) -> None:
    with pytest.raises(ValueError):
        manager.get_postgres_pool(dsn)
    assert manager._postgres_pools == {}
    assert manager._postgres_holders == {}


def test_queue_construction_surfaces_the_redacted_error(manager: Any) -> None:
    from stabilize.queue import PostgresQueue

    with pytest.raises(ValueError, match="could not be parsed") as raised:
        PostgresQueue(MALFORMED[0], table_name="queue_messages")
    assert SECRET not in str(raised.value)
    assert "u:***@" in str(raised.value)


def test_parseable_dsn_still_creates_a_pool(manager: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg_pool

    _RecordingPool.created = []
    monkeypatch.setattr(psycopg_pool, "ConnectionPool", _RecordingPool)
    dsn = "postgresql://u:p@127.0.0.1:1/db?sslmode=disable&application_name=probe"
    pool = manager.get_postgres_pool(dsn)
    assert isinstance(pool, _RecordingPool)
    assert _RecordingPool.created == [dsn]
    manager.release_postgres_pool(pool)


def test_keyword_form_is_accepted(manager: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg_pool

    _RecordingPool.created = []
    monkeypatch.setattr(psycopg_pool, "ConnectionPool", _RecordingPool)
    dsn = "host=127.0.0.1 port=1 dbname=db user=u password=p"
    pool = manager.get_postgres_pool(dsn)
    assert _RecordingPool.created == [dsn]
    manager.release_postgres_pool(pool)


def test_the_guard_is_the_libpq_parser() -> None:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    with pytest.raises(psycopg.ProgrammingError) as raised:
        conninfo_to_dict(MALFORMED[0])
    assert SECRET in str(raised.value)
    with pytest.raises(ValueError):
        conn_mod._require_parseable_conninfo(MALFORMED[0])
