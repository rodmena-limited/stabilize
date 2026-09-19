"""Ticket #21: the synchronous path starts no lock heartbeat, so a short lease
is silent double execution waiting to happen. It must be loud."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from stabilize.queue.processor.lease_guard import (
    DEFAULT_MIN_SYNC_LEASE,
    check_sync_lease,
    lease_duration,
)


class _Queue:
    def __init__(self, duration: timedelta | None) -> None:
        if duration is not None:
            self.lock_duration = duration


class TestCheckSyncLease:
    def test_short_lease_produces_a_warning(self) -> None:
        warning = check_sync_lease(_Queue(timedelta(seconds=60)))
        assert warning is not None
        assert "60s" in warning

    def test_warning_names_the_queue_and_the_consequence(self) -> None:
        warning = check_sync_lease(_Queue(timedelta(seconds=60)))
        assert "_Queue" in warning
        assert "execute it AGAIN" in warning

    def test_long_lease_produces_no_warning(self) -> None:
        """Both directions: a guard that always warns is noise, not a signal."""
        assert check_sync_lease(_Queue(timedelta(minutes=30))) is None

    def test_lease_exactly_at_the_minimum_is_accepted(self) -> None:
        assert check_sync_lease(_Queue(DEFAULT_MIN_SYNC_LEASE)) is None

    def test_queue_without_a_lease_is_not_warned_about(self) -> None:
        assert check_sync_lease(_Queue(None)) is None

    def test_threshold_is_configurable(self) -> None:
        queue = _Queue(timedelta(minutes=10))
        assert check_sync_lease(queue) is None
        assert check_sync_lease(queue, minimum=timedelta(minutes=30)) is not None

    def test_lease_duration_reads_the_attribute(self) -> None:
        assert lease_duration(_Queue(timedelta(seconds=5))) == timedelta(seconds=5)
        assert lease_duration(_Queue(None)) is None


class TestProcessorWarnsOnce:
    def test_synchronous_poll_warns_and_only_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from stabilize.queue.processor.processor import QueueProcessor
        from stabilize.queue.sqlite import SqliteQueue

        queue = SqliteQueue(connection_string="sqlite:///:memory:", table_name="queue_messages")
        queue._create_table()
        queue.lock_duration = timedelta(seconds=60)
        processor = QueueProcessor(queue)

        with caplog.at_level(logging.WARNING):
            processor.process_one()
            processor.process_one()
            processor.process_one()

        hits = [r for r in caplog.records if "SYNCHRONOUS path" in r.getMessage()]
        assert len(hits) == 1, f"expected exactly one warning, got {len(hits)}"

    def test_adequate_lease_produces_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Without this, the test above passes by warning unconditionally."""
        from stabilize.queue.processor.processor import QueueProcessor
        from stabilize.queue.sqlite import SqliteQueue

        queue = SqliteQueue(connection_string="sqlite:///:memory:", table_name="queue_messages")
        queue._create_table()
        queue.lock_duration = timedelta(minutes=30)
        processor = QueueProcessor(queue)

        with caplog.at_level(logging.WARNING):
            processor.process_one()

        hits = [r for r in caplog.records if "SYNCHRONOUS path" in r.getMessage()]
        assert hits == []
