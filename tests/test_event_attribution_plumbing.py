"""Plumbing that Phase 2 event wiring depends on (tickets #28/#29 follow-up).

These pin the contract, not end-to-end attribution: nothing records an event
during signal handling yet, so what is asserted here is that the actor reaches
the event context and that the new message field is wire-compatible in both
directions.
"""

from __future__ import annotations

import json

from stabilize.events.recorder.context import get_event_metadata, set_event_context
from stabilize.queue.messages import SignalStage
from stabilize.queue.sqlite.serialization import deserialize_message


def test_event_context_carries_actor_and_causation() -> None:
    set_event_context(correlation_id="wf-1", causation_id="ev-0", actor="alice")
    metadata = get_event_metadata(source_handler="SignalStage")

    assert metadata.actor == "alice"
    assert metadata.causation_id == "ev-0"
    assert metadata.correlation_id == "wf-1"


def test_event_context_defaults_to_system() -> None:
    """Control: an unattributed action must still record as system."""
    set_event_context(correlation_id="wf-1")
    metadata = get_event_metadata(source_handler="StartStage")

    assert metadata.actor == "system"
    assert metadata.causation_id is None


def test_signal_stage_carries_user() -> None:
    message = SignalStage(
        execution_type="PIPELINE",
        execution_id="e",
        stage_id="s",
        signal_name="approve",
        user="alice",
    )
    assert message.user == "alice"


def test_signal_stage_user_defaults_empty() -> None:
    """Control: the field is additive, so existing construction is unchanged."""
    message = SignalStage(
        execution_type="PIPELINE",
        execution_id="e",
        stage_id="s",
        signal_name="approve",
    )
    assert message.user == ""


def test_signal_stage_payload_without_user_still_deserializes() -> None:
    """A message enqueued by an older build must still load."""
    legacy = json.dumps(
        {
            "execution_type": "PIPELINE",
            "execution_id": "e",
            "stage_id": "s",
            "signal_name": "approve",
            "signal_data": {"k": 1},
            "persistent": True,
        }
    )
    message = deserialize_message("SignalStage", legacy)

    assert isinstance(message, SignalStage)
    assert message.user == ""
    assert message.signal_data == {"k": 1}


def test_approve_puts_user_on_the_message_not_in_signal_data() -> None:
    """The user must not leak into signal_data, which tasks expose as outputs."""
    from stabilize.hitl import approve

    pushed: list[SignalStage] = []

    class _Queue:
        def push(self, message, delay=0):  # noqa: ANN001, ANN201
            pushed.append(message)

    approve(_Queue(), execution_id="e", stage_id="s", data={"note": "ok"}, user="alice")

    assert len(pushed) == 1
    assert pushed[0].user == "alice"
    assert pushed[0].signal_data == {"note": "ok"}


def test_execute_atomic_runs_during_txn_hook() -> None:
    from stabilize.persistence.transaction import TransactionHelper

    calls: list[str] = []

    class _Txn:
        is_atomic = True

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *exc):  # noqa: ANN002, ANN204
            return False

        def store_stage(self, stage):  # noqa: ANN001, ANN201
            calls.append("store")

        def mark_message_processed(self, **kwargs):  # noqa: ANN003, ANN201
            calls.append("mark")

        def push_message(self, message, delay=0):  # noqa: ANN001, ANN201
            calls.append("push")

    class _Repo:
        def transaction(self, queue):  # noqa: ANN001, ANN201
            return _Txn()

    helper = TransactionHelper(_Repo(), queue=None)
    helper.execute_atomic(
        messages_to_push=[("msg", None)],
        during_txn=lambda: calls.append("during"),
    )

    assert calls == ["during", "push"]
