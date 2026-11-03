"""Retry exhaustion on lock contention logs WARNING and names the redelivery; ERROR is for terminal outcomes (#108)."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pytest
from resilient_circuit import ExponentialDelay, RetryWithBackoffPolicy

import stabilize.persistence.transaction as transaction_module
from stabilize.errors import ConcurrencyError
from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.transaction import TransactionHelper, is_deadlock_error
from stabilize.queue import Queue

DEADLOCK_MAX_RETRIES = getattr(transaction_module, "DEADLOCK_MAX_RETRIES", 5)


def test_exhausted_contention_retries_log_a_warning_and_re_raise(
    repository: WorkflowStore, queue: Queue, backend: str, caplog: pytest.LogCaptureFixture, monkeypatch: Any
) -> None:
    quick = RetryWithBackoffPolicy(
        max_retries=DEADLOCK_MAX_RETRIES,
        backoff=ExponentialDelay(min_delay=timedelta(milliseconds=1), max_delay=timedelta(milliseconds=5), factor=2),
        should_handle=is_deadlock_error,
    )
    monkeypatch.setattr(transaction_module, "DEADLOCK_RETRY_POLICY", quick)
    helper = TransactionHelper(repository, queue)
    attempts = {"n": 0}

    def contended(txn: Any) -> None:
        attempts["n"] += 1
        raise ConcurrencyError("stage row held by a cancel")

    with caplog.at_level(logging.DEBUG, logger="stabilize.persistence.transaction"):
        with pytest.raises(ConcurrencyError):
            helper.execute_atomic(handler_name="RunTask", within=contended)

    assert attempts["n"] == DEADLOCK_MAX_RETRIES + 1, f"expected the full retry budget, saw {attempts['n']} attempts"
    records = [r for r in caplog.records if "lock contention" in r.getMessage()]
    assert records, "the exhaustion was not logged"
    assert [r.levelno for r in records] == [logging.WARNING], [r.levelname for r in records]
    assert "RunTask" in records[0].getMessage() and "redeliver" in records[0].getMessage()
