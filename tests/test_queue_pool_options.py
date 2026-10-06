from __future__ import annotations

from stabilize import PostgresQueue, PostgresWorkflowStore
from stabilize.persistence.pool_options import PoolOptions


def test_postgres_queue_and_store_share_one_pool_with_caller_options(postgres_url: str) -> None:
    options = PoolOptions(min_size=1, max_size=4)
    store = PostgresWorkflowStore(postgres_url, options=options)
    queue = PostgresQueue(postgres_url, options=options)
    try:
        assert queue._pool is store._pool
        assert queue._pool.max_size == 4
        assert queue.size() >= 0
    finally:
        queue.close()
        store.close()


def test_postgres_queue_without_options_keeps_the_default_pool(postgres_url: str) -> None:
    store = PostgresWorkflowStore(postgres_url)
    queue = PostgresQueue(postgres_url)
    try:
        assert queue._pool is store._pool
        assert queue._pool.max_size == 15
    finally:
        queue.close()
        store.close()
