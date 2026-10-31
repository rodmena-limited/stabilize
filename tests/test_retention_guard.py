from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from stabilize.persistence.store import WorkflowStore
from stabilize.queue.messages import StartWorkflow


def _queue(repository: WorkflowStore, table: str) -> Any:
    from stabilize.persistence.factory import create_queue

    url = repository.connection_string
    if url.startswith("postgres") and table != "queue_messages":
        import psycopg

        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(f"DROP TABLE IF EXISTS {table}_dlq")
            conn.execute(f"CREATE TABLE {table} (LIKE queue_messages INCLUDING ALL)")
            conn.execute(f"CREATE TABLE {table}_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
    q = create_queue(url, table_name=table, lock_duration=timedelta(minutes=5))
    create = getattr(q, "_create_table", None)
    if callable(create):
        create()
    q.clear()
    return q


def _sweep(repository: WorkflowStore, queue: Any) -> int:
    return repository.cleanup_old_processed_messages(
        max_age_hours=0, table_name=queue.table_name, live=queue.live_delivery_keys
    )


def test_guarded_retention_keeps_queued_and_releases_acked(file_repository: WorkflowStore) -> None:
    repository = file_repository
    queue = _queue(repository, "queue_messages")
    for i in range(2):
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id=f"wf-{i}"))
    held = queue.poll_one()
    done = queue.poll_one()
    assert held is not None and done is not None
    queue.ack(done)
    foreign = "other_queue:1"
    for key in (held.message_id, done.message_id, foreign):
        repository.mark_message_processed(key, handler_type="test", execution_id="x")
    time.sleep(0.05)

    removed = _sweep(repository, queue)
    assert repository.is_message_processed(held.message_id), "record of a message still in the queue was removed"
    assert not repository.is_message_processed(done.message_id), "record of an acked message was kept"
    assert repository.is_message_processed(foreign), "record of another queue table was removed"
    assert removed >= 1

    queue.ack(held)
    removed = _sweep(repository, queue)
    assert not repository.is_message_processed(held.message_id), "record kept after its message was acked"
    assert repository.is_message_processed(foreign)
    assert removed >= 1
    queue.close()


def test_guarded_retention_on_a_named_table_touches_only_that_table(file_repository: WorkflowStore) -> None:
    repository = file_repository
    queue = _queue(repository, "guard_q")
    queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="wf-a"))
    msg = queue.poll_one()
    assert msg is not None and msg.message_id.startswith("guard_q:")
    queue.ack(msg)
    for key in (msg.message_id, "7", "guard_qx:1"):
        repository.mark_message_processed(key, handler_type="test", execution_id="x")
    time.sleep(0.05)
    removed = _sweep(repository, queue)
    assert not repository.is_message_processed(msg.message_id)
    assert repository.is_message_processed("7"), "default-table record removed by a guard_q sweep"
    assert repository.is_message_processed("guard_qx:1"), "prefix match leaked to a table with a longer name"
    assert removed >= 1
    queue.close()


def test_prune_cli_fails_closed_when_the_queue_table_is_missing(tmp_path: Any) -> None:
    import subprocess
    import sys

    from stabilize.persistence.sqlite import SqliteWorkflowStore

    url = f"sqlite:///{tmp_path}/store.db"
    store = SqliteWorkflowStore(connection_string=url, create_tables=True)
    store.mark_message_processed("missing_q:12", handler_type="test", execution_id="x")
    time.sleep(0.05)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "stabilize.cli.main",
            "prune-processed",
            "--db-url",
            url,
            "--older-than-hours",
            "0",
            "--queue-table",
            "missing_q",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode != 0, f"prune-processed succeeded without a readable queue table: {result.stdout}"
    assert store.is_message_processed("missing_q:12"), "a record was deleted although its queue could not be checked"
    store.close()
