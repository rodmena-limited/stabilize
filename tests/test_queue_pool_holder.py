"""Pool holder accounting for PostgresQueue, PostgresWorkflowStore and ConnectionManager."""

from __future__ import annotations

from typing import Any

import pytest

from stabilize.persistence import connection as conn_mod


class _FakePool:
    """Stands in for psycopg_pool.ConnectionPool."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def manager(monkeypatch: pytest.MonkeyPatch):
    """A clean ConnectionManager whose pools are recorders, not real pools."""
    import psycopg_pool

    monkeypatch.setattr(psycopg_pool, "ConnectionPool", _FakePool)
    conn_mod.SingletonMeta._instances.pop(conn_mod.ConnectionManager, None)
    m = conn_mod.get_connection_manager()
    yield m
    conn_mod.SingletonMeta._instances.pop(conn_mod.ConnectionManager, None)


DSN = "postgresql://u:p@localhost:5432/db"


def test_the_holder_table_can_be_read(manager) -> None:
    """Known-positive: without this, every count below could be vacuously zero."""
    manager.get_postgres_pool(DSN)
    assert sum(manager._postgres_holders.values()) == 1


def test_repeated_acquisition_does_increment(manager) -> None:
    """The manager's accounting is per-call. This is the behaviour the queue must not rely on."""
    for _ in range(4):
        manager.get_postgres_pool(DSN)
    assert sum(manager._postgres_holders.values()) == 4


def test_queue_holds_its_pool_exactly_once(manager) -> None:
    from stabilize.queue.postgres import PostgresQueue

    q = PostgresQueue(DSN, table_name="queue_messages")
    after_construction = sum(manager._postgres_holders.values())

    for _ in range(25):
        q._get_pool()

    after_operations = sum(manager._postgres_holders.values())

    assert after_construction == 1, f"construction took {after_construction} holds"
    assert after_operations == 1, (
        f"25 operations took {after_operations} holds; a queue must not register "
        "a new holder per operation or close() can never release the pool"
    )


def test_one_close_releases_the_pool(manager) -> None:
    from stabilize.queue.postgres import PostgresQueue

    q = PostgresQueue(DSN, table_name="queue_messages")
    pool = q._get_pool()
    for _ in range(25):
        q._get_pool()

    q.close()

    assert sum(manager._postgres_holders.values()) == 0, "holders remain after close()"
    assert not manager._postgres_pools, "the pool was not removed from the manager"
    assert pool.closed is True, "the pool object was never closed"


def test_a_store_sharing_the_dsn_is_not_closed_by_the_queue(manager) -> None:
    """The holder count exists so one owner cannot close another's pool.

    This is the regression the per-operation acquisition was masking, and it
    must still hold after the fix.
    """
    from stabilize.queue.postgres import PostgresQueue

    other_owner = manager.get_postgres_pool(DSN)
    q = PostgresQueue(DSN, table_name="queue_messages")

    q.close()

    assert manager._postgres_pools, "the queue's close() closed another owner's pool"
    assert other_owner.closed is False


def test_a_second_close_does_not_release_another_owners_hold(manager) -> None:
    from stabilize.queue.postgres import PostgresQueue

    other_owner = manager.get_postgres_pool(DSN)
    q = PostgresQueue(DSN, table_name="queue_messages")

    q.close()
    q.close()

    assert sum(manager._postgres_holders.values()) == 1
    assert other_owner.closed is False


def test_closing_a_queue_does_not_close_a_store_with_different_options(manager) -> None:
    from stabilize.persistence.pool_options import PoolOptions
    from stabilize.persistence.postgres.store import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    store = PostgresWorkflowStore(DSN, options=PoolOptions(min_size=1, max_size=3))
    q = PostgresQueue(DSN, table_name="queue_messages")
    assert len(manager._postgres_pools) == 2

    q.close()

    assert store._pool.closed is False
    assert len(manager._postgres_pools) == 1


def test_a_store_releases_exactly_its_own_hold_once(manager) -> None:
    from stabilize.persistence.postgres.store import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    store = PostgresWorkflowStore(DSN)
    q = PostgresQueue(DSN, table_name="queue_messages")
    shared = q._get_pool()
    assert store._pool is shared
    assert sum(manager._postgres_holders.values()) == 2

    store.close()
    store.close()

    assert sum(manager._postgres_holders.values()) == 1
    assert shared.closed is False

    q.close()

    assert not manager._postgres_pools
    assert shared.closed is True


def test_releasing_an_unknown_pool_changes_nothing(manager) -> None:
    held = manager.get_postgres_pool(DSN)

    manager.release_postgres_pool(_FakePool())

    assert sum(manager._postgres_holders.values()) == 1
    assert held.closed is False
