from __future__ import annotations

import time

from stabilize.persistence.store import WorkflowStore


def _mark(repository: WorkflowStore, n: int, prefix: str) -> list[str]:
    ids = [f"{prefix}-{i}" for i in range(n)]
    for mid in ids:
        repository.mark_message_processed(message_id=mid, handler_type="RunTask", execution_id=None)
    time.sleep(0.05)
    return ids


def test_cleanup_accepts_float_hours_on_every_backend(repository: WorkflowStore) -> None:
    ids = _mark(repository, 3, "float")
    try:
        removed = repository.cleanup_old_processed_messages(max_age_hours=0.0)
    except Exception as exc:
        raise AssertionError(
            f"C38 cleanup_old_processed_messages(max_age_hours=0.0) raised {type(exc).__name__}: {exc}"
        ) from exc
    assert removed >= 3
    assert not any(repository.is_message_processed(i) for i in ids)


def test_cleanup_accepts_int_hours(repository: WorkflowStore) -> None:
    ids = _mark(repository, 2, "int")
    repository.cleanup_old_processed_messages(max_age_hours=168)
    assert all(repository.is_message_processed(i) for i in ids)


def test_cleanup_in_batches_returns_the_total(repository: WorkflowStore) -> None:
    ids = _mark(repository, 5, "batch")
    try:
        removed = repository.cleanup_old_processed_messages(max_age_hours=0.0, batch_size=2)
    except TypeError as exc:
        raise AssertionError(f"C3 cleanup_old_processed_messages has no batch_size: {exc}") from exc
    assert removed >= 5
    assert not any(repository.is_message_processed(i) for i in ids)


def test_cleanup_respects_max_rows(repository: WorkflowStore) -> None:
    _mark(repository, 5, "cap")
    try:
        removed = repository.cleanup_old_processed_messages(max_age_hours=0.0, batch_size=2, max_rows=3)
    except TypeError as exc:
        raise AssertionError(f"C3 cleanup_old_processed_messages has no max_rows: {exc}") from exc
    assert 3 <= removed <= 4
