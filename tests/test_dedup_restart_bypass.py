"""
Repro tests for the durable-dedup bypass (audit finding A1-1).

The two-tier dedup gates the durable ``processed_messages`` check behind an
in-memory bloom filter. The bloom is empty after a process restart and is
cleared by fill-ratio/age rotation, so a bloom negative proves nothing across
those boundaries — yet ``_handle_message`` skipped the DB check on a negative,
re-executing already-processed messages after a crash/restart or rotation.
"""

from typing import Any

import pytest

from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.dedup import BloomDeduplicator, get_deduplicator, reset_deduplicator
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.sqlite import SqliteQueue


@pytest.fixture()
def sqlite_env(tmp_path: Any):
    SingletonMeta.reset(ConnectionManager)
    reset_deduplicator()
    connection_string = f"sqlite:///{tmp_path}/dedup_restart.db"
    store = SqliteWorkflowStore(connection_string, create_tables=True)
    queue = SqliteQueue(connection_string)
    queue._create_table()
    yield store, queue
    reset_deduplicator()
    SingletonMeta.reset(ConnectionManager)


def _processor_with_counting_handler(store: Any, queue: Any) -> tuple[QueueProcessor, list[Any]]:
    processor = QueueProcessor(queue, store=store)
    calls: list[Any] = []
    processor.register_handler_func(StartWorkflow, lambda m: calls.append(m))
    return processor, calls


class TestDedupSurvivesBloomLoss:
    def test_processed_message_not_reexecuted_after_restart(self, sqlite_env: Any) -> None:
        """A message marked processed before a crash must not re-execute after
        restart, even though the fresh (empty) bloom has never seen it."""
        store, queue = sqlite_env
        store.mark_message_processed(
            "msg-restart-1", handler_type="StartWorkflow", execution_id="e1"
        )

        # Simulate process restart: brand-new global bloom filter.
        reset_deduplicator()

        processor, calls = _processor_with_counting_handler(store, queue)
        msg = StartWorkflow(
            execution_type="PIPELINE", execution_id="e1", message_id="msg-restart-1"
        )
        processor._handle_message(msg)

        assert calls == [], "already-processed message re-executed after restart"

    def test_processed_message_not_reexecuted_after_bloom_rotation(self, sqlite_env: Any) -> None:
        """Redelivery after the bloom is rotated (fill/age reset) must still
        be deduplicated by the durable store."""
        store, queue = sqlite_env
        processor, calls = _processor_with_counting_handler(store, queue)
        msg = StartWorkflow(
            execution_type="PIPELINE", execution_id="e1", message_id="msg-rotate-1"
        )

        processor._handle_message(msg)
        assert len(calls) == 1, "first delivery should process"

        get_deduplicator().reset()  # mid-run rotation clears the bloom

        processor._handle_message(msg)  # redelivery (e.g. crash before ack)
        assert len(calls) == 1, "already-processed message re-executed after rotation"


class TestBloomAuthority:
    def test_fresh_bloom_is_not_authoritative(self) -> None:
        dedup = BloomDeduplicator(expected_items=100)
        assert dedup.authoritative is False

    def test_hydration_grants_authority_and_reset_revokes_it(self) -> None:
        dedup = BloomDeduplicator(expected_items=100)
        dedup.hydrate(["a", "b"])
        assert dedup.authoritative is True
        assert dedup.maybe_seen("a") and dedup.maybe_seen("b")
        dedup.reset()
        assert dedup.authoritative is False

    def test_processor_hydrates_from_store(self, sqlite_env: Any) -> None:
        """Constructing a processor over a store with processed rows must
        restore the bloom fast-path soundly (hydrated + authoritative)."""
        store, queue = sqlite_env
        store.mark_message_processed("msg-hydrate-1", handler_type="X", execution_id="e1")
        reset_deduplicator()  # restart

        QueueProcessor(queue, store=store)
        dedup = get_deduplicator()
        assert dedup.authoritative is True
        assert dedup.maybe_seen("msg-hydrate-1")

    def test_opt_in_negative_cache_skips_db_but_stays_correct(self, sqlite_env: Any) -> None:
        """With dedup_trust_negative_cache=True and a hydrated bloom, fresh
        messages skip the durable lookup while processed ones still dedup."""
        store, queue = sqlite_env
        store.mark_message_processed("msg-old-1", handler_type="X", execution_id="e1")
        reset_deduplicator()

        from stabilize.queue.processor.config import QueueProcessorConfig

        config = QueueProcessorConfig(dedup_trust_negative_cache=True)
        processor = QueueProcessor(queue, config=config, store=store)
        calls: list[Any] = []
        processor.register_handler_func(StartWorkflow, lambda m: calls.append(m))

        db_lookups: list[str] = []
        original = store.is_message_processed

        def counting_is_processed(message_id: str) -> bool:
            db_lookups.append(message_id)
            return original(message_id)

        store.is_message_processed = counting_is_processed  # type: ignore[method-assign]

        # Processed pre-restart: bloom positive -> DB confirms -> skipped.
        processor._handle_message(
            StartWorkflow(execution_type="PIPELINE", execution_id="e1", message_id="msg-old-1")
        )
        assert calls == []
        assert "msg-old-1" in db_lookups

        # Fresh message: authoritative bloom negative -> no DB lookup, processed.
        db_lookups.clear()
        processor._handle_message(
            StartWorkflow(execution_type="PIPELINE", execution_id="e1", message_id="msg-new-1")
        )
        assert len(calls) == 1
        assert db_lookups == []
