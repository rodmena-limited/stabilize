from __future__ import annotations

from pathlib import Path

from stabilize import QueueProcessor, SqliteQueue, SqliteWorkflowStore


def test_store_create_tables_provides_the_default_dead_letter_table(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path}/app.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    conn = queue._get_connection()
    conn.execute(
        "INSERT INTO queue_messages (message_id, message_type, payload, deliver_at, attempts, max_attempts) "
        "VALUES ('poison-1', 'NoSuchMessageType', '{}', datetime('now', '-1 second'), 0, 10)"
    )
    conn.commit()
    QueueProcessor(queue, store=store).process_all(timeout=2.0)
    assert queue.dlq_size() == 1
    assert queue.size() == 0
