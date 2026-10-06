from __future__ import annotations

from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.transaction import TransactionHelper
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow


def _source(message_id: str) -> StartWorkflow:
    message = StartWorkflow(execution_type="workflow", execution_id="w")
    message.message_id = message_id
    return message


def test_atomic_effects_are_rolled_back_for_an_already_processed_source(
    repository: WorkflowStore, queue: Queue
) -> None:
    repository.mark_message_processed(message_id="dup-1", handler_type="StartWorkflow", execution_id="w")
    TransactionHelper(repository, queue).execute_atomic(
        source_message=_source("dup-1"),
        messages_to_push=[(StartWorkflow(execution_type="workflow", execution_id="follow-up"), None)],
        handler_name="Test",
    )
    assert queue.size() == 0


def test_atomic_effects_commit_for_a_new_source(repository: WorkflowStore, queue: Queue) -> None:
    TransactionHelper(repository, queue).execute_atomic(
        source_message=_source("new-1"),
        messages_to_push=[(StartWorkflow(execution_type="workflow", execution_id="follow-up"), None)],
        handler_name="Test",
    )
    assert queue.size() == 1
    assert repository.is_message_processed("new-1")
