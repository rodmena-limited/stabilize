"""Tickets #18, #19, #22: pool options passthrough, holder-counted pool
lifetime, and a bounded is_healthy()."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.pool_options import (
    DEFAULT_HEALTH_TIMEOUT_SECONDS,
    PoolOptions,
)

DSN = "postgresql://u:p@127.0.0.1:1/never-reachable"


class _FakePool:
    """Records how psycopg_pool would have been constructed."""

    instances: list[_FakePool] = []

    def __init__(self, conninfo: str, **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.closed = False
        _FakePool.instances.append(self)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> ConnectionManager:
    _FakePool.instances.clear()
    SingletonMeta.reset(ConnectionManager)

    import psycopg_pool

    monkeypatch.setattr(psycopg_pool, "ConnectionPool", _FakePool)
    created = ConnectionManager()
    yield created
    SingletonMeta.reset(ConnectionManager)


class TestPoolOptionsPassthrough:
    """#18 — the ConnectionManager must accept caller-supplied options."""

    def test_known_positive_defaults_produce_a_pool(self, manager: ConnectionManager) -> None:
        manager.get_postgres_pool(DSN)
        assert len(_FakePool.instances) == 1

    def test_default_pool_sets_no_statement_or_lock_timeout(
        self, manager: ConnectionManager
    ) -> None:
        """The documented default: server settings are inherited."""
        manager.get_postgres_pool(DSN)
        kwargs = _FakePool.instances[0].kwargs["kwargs"]
        assert "options" not in kwargs

    def test_connect_kwargs_reach_the_driver(self, manager: ConnectionManager) -> None:
        options = PoolOptions(
            connect_kwargs={"options": "-c statement_timeout=8000 -c lock_timeout=4000"}
        )
        manager.get_postgres_pool(DSN, options=options)
        kwargs = _FakePool.instances[0].kwargs["kwargs"]
        assert kwargs["options"] == "-c statement_timeout=8000 -c lock_timeout=4000"
        assert "row_factory" in kwargs

    def test_configure_callback_reaches_the_pool(self, manager: ConnectionManager) -> None:
        def configure(conn: Any) -> None:  # pragma: no cover - identity only
            pass

        manager.get_postgres_pool(DSN, options=PoolOptions(configure=configure))
        assert _FakePool.instances[0].kwargs["configure"] is configure

    def test_acquire_timeout_reaches_the_pool(self, manager: ConnectionManager) -> None:
        manager.get_postgres_pool(DSN, options=PoolOptions(acquire_timeout=2.5))
        assert _FakePool.instances[0].kwargs["timeout"] == 2.5

    def test_sizes_are_honoured(self, manager: ConnectionManager) -> None:
        manager.get_postgres_pool(DSN, min_size=2, max_size=4)
        assert _FakePool.instances[0].kwargs["min_size"] == 2
        assert _FakePool.instances[0].kwargs["max_size"] == 4

    def test_different_options_do_not_silently_share_a_pool(
        self, manager: ConnectionManager
    ) -> None:
        a = manager.get_postgres_pool(DSN, options=PoolOptions(acquire_timeout=1.0))
        b = manager.get_postgres_pool(DSN, options=PoolOptions(acquire_timeout=9.0))
        assert a is not b

    def test_identical_options_do_share(self, manager: ConnectionManager) -> None:
        """Without this, the test above passes by never sharing at all."""
        a = manager.get_postgres_pool(DSN)
        b = manager.get_postgres_pool(DSN)
        assert a is b


class TestPoolHolderLifetime:
    """#19 — one holder closing must not close another holder's pool."""

    def test_second_holder_survives_first_close(self, manager: ConnectionManager) -> None:
        store_pool = manager.get_postgres_pool(DSN)
        queue_pool = manager.get_postgres_pool(DSN)
        assert store_pool is queue_pool

        manager.close_postgres_pool(DSN)
        assert not store_pool.closed, "closing one holder closed the shared pool"

    def test_pool_closes_when_the_last_holder_releases(
        self, manager: ConnectionManager
    ) -> None:
        """Both directions: it must actually close, or nothing is ever reclaimed."""
        pool = manager.get_postgres_pool(DSN)
        manager.get_postgres_pool(DSN)

        manager.close_postgres_pool(DSN)
        manager.close_postgres_pool(DSN)
        assert pool.closed

    def test_reacquire_after_full_release_builds_a_new_pool(
        self, manager: ConnectionManager
    ) -> None:
        first = manager.get_postgres_pool(DSN)
        manager.close_postgres_pool(DSN)
        second = manager.get_postgres_pool(DSN)
        assert second is not first

    def test_close_all_closes_everything(self, manager: ConnectionManager) -> None:
        pool = manager.get_postgres_pool(DSN)
        manager.get_postgres_pool(DSN)
        manager.close_all()
        assert pool.closed

    def test_holder_counting_is_thread_safe(self, manager: ConnectionManager) -> None:
        pools: list[Any] = []
        barrier = threading.Barrier(8)

        def acquire() -> None:
            barrier.wait()
            pools.append(manager.get_postgres_pool(DSN))

        threads = [threading.Thread(target=acquire) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len({id(p) for p in pools}) == 1, "concurrent acquire built more than one pool"
        for _ in range(7):
            manager.close_postgres_pool(DSN)
        assert not pools[0].closed
        manager.close_postgres_pool(DSN)
        assert pools[0].closed


class TestIsHealthyIsBounded:
    """#22 — assert the LATENCY, not just the boolean.

    A test asserting only `is_healthy() is False` passes at 30s and at 0.5s
    alike, which is exactly the assertion that hid this.
    """

    def test_default_health_timeout_is_well_under_a_probe_budget(self) -> None:
        assert DEFAULT_HEALTH_TIMEOUT_SECONDS <= 5.0

    def test_unreachable_database_answers_false_quickly(self) -> None:
        pytest.importorskip("psycopg_pool")
        from stabilize.persistence.postgres.store import PostgresWorkflowStore

        SingletonMeta.reset(ConnectionManager)
        try:
            store = PostgresWorkflowStore(
                "postgresql://u:p@127.0.0.1:1/nope",
                options=PoolOptions(min_size=0, max_size=1, acquire_timeout=1.0),
                health_timeout=1.0,
            )
            started = time.monotonic()
            healthy = store.is_healthy()
            elapsed = time.monotonic() - started

            assert healthy is False
            assert elapsed < 5.0, f"is_healthy() took {elapsed:.1f}s on the failure path"
        finally:
            SingletonMeta.reset(ConnectionManager)
