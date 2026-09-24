"""A queue row that cannot be decoded is quarantined in the DLQ, never raised to the poller (#60).

A malformed row cannot be produced through the queue API, which only writes
messages it can serialise, so each test seeds the row directly and then
observes the outcome through poll_one, list_dlq, replay_dlq and process_all.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from stabilize import QueueProcessor
from stabilize.queue import Queue
from stabilize.queue.messages import MESSAGE_TYPES, StartWorkflow, WorkflowLevel

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


@dataclass
class FutureMessage(WorkflowLevel):
    pass


def _seed(queue: Queue, backend: str, message_type: str, payload: Any, message_id: str) -> None:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    if backend == "sqlite":
        conn = queue._get_connection()  # type: ignore[attr-defined]
        conn.execute(
            "INSERT INTO queue_messages (message_id, message_type, payload, deliver_at, attempts) "
            "VALUES (?, ?, ?, '2000-01-01T00:00:00', 0)",
            (message_id, message_type, text),
        )
        conn.commit()
        return
    import psycopg

    with psycopg.connect(queue.connection_string) as conn:  # type: ignore[attr-defined]
        conn.execute(
            "INSERT INTO queue_messages (message_id, message_type, payload, deliver_at, attempts) "
            "VALUES (%s, %s, %s::jsonb, NOW() - interval '1 hour', 0)",
            (message_id, message_type, text),
        )


POISON = [
    ("unknown-type", "NoSuchMessageType", {"execution_id": "e1"}, "Unknown message type"),
    ("bad-enum", "CompleteTask", {"execution_id": "e1", "stage_id": "s", "task_id": "t", "status": "NOPE"}, "NOPE"),
    ("contract", "StartWorkflow", {"execution_id": 123}, "field contract"),
    ("not-an-object", "StartWorkflow", [1, 2], "not an object"),
    ("json-scalar", "StartWorkflow", "\"just a string\"", "not an object"),
]


@pytest.mark.parametrize("case,message_type,payload,reason", POISON, ids=[p[0] for p in POISON])
def test_poison_row_is_quarantined_and_the_queue_keeps_flowing(
    queue: Queue, backend: str, case: str, message_type: str, payload: Any, reason: str
) -> None:
    queue.clear_dlq()  # type: ignore[attr-defined]
    _seed(queue, backend, message_type, payload, f"poison-{case}")
    queue.push(StartWorkflow(execution_id="valid-after-poison"))

    assert queue.poll_one() is None
    follow_on = queue.poll_one()
    assert isinstance(follow_on, StartWorkflow)
    assert follow_on.execution_id == "valid-after-poison"

    dead = queue.list_dlq()  # type: ignore[attr-defined]
    assert [(d["message_type"], d["message_id"]) for d in dead] == [(message_type, f"poison-{case}")]
    assert dead[0]["error"].startswith("Deserialization failed: ")
    assert reason in dead[0]["error"]
    stored = dead[0]["payload"]
    expected = json.loads(payload) if isinstance(payload, str) else payload
    assert (json.loads(stored) if backend == "sqlite" else stored) == expected

    assert queue.replay_dlq(dead[0]["id"]) is True  # type: ignore[attr-defined]
    assert queue.poll_one() is None
    again = queue.list_dlq()  # type: ignore[attr-defined]
    assert [(d["message_type"], d["payload"]) for d in again] == [(message_type, stored)]


def test_process_all_does_not_raise_on_a_poison_row(queue: Queue, backend: str, repository: Any) -> None:
    queue.clear_dlq()  # type: ignore[attr-defined]
    _seed(queue, backend, "NoSuchMessageType", {"execution_id": "e1"}, "poison-sync")
    QueueProcessor(queue, store=repository).process_all(timeout=5.0)
    assert [d["message_id"] for d in queue.list_dlq()] == ["poison-sync"]  # type: ignore[attr-defined]


def test_a_type_unknown_to_this_version_replays_once_it_is_known(
    queue: Queue, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue.clear_dlq()  # type: ignore[attr-defined]
    _seed(queue, backend, "FutureMessage", {"execution_id": "from-newer-worker"}, "future-1")
    assert queue.poll_one() is None
    [entry] = queue.list_dlq()  # type: ignore[attr-defined]

    monkeypatch.setitem(MESSAGE_TYPES, "FutureMessage", FutureMessage)
    assert queue.replay_dlq(entry["id"]) is True  # type: ignore[attr-defined]

    replayed = queue.poll_one()
    assert isinstance(replayed, FutureMessage)
    assert replayed.execution_id == "from-newer-worker"
    assert queue.list_dlq() == []  # type: ignore[attr-defined]
