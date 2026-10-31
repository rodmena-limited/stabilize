"""A missing workflow is one StabilizeError class from every store and every import path (#104)."""

from __future__ import annotations

from typing import Any

import pytest

import stabilize
from stabilize.errors import PermanentError, TransientError, WorkflowError
from stabilize.errors import WorkflowNotFoundError as ModelNotFound
from stabilize.persistence import WorkflowNotFoundError as StoreNotFound

MISSING = "01MISSINGWORKFLOW000000000"


def test_persistence_and_errors_export_one_not_found_class() -> None:
    assert StoreNotFound is ModelNotFound
    assert issubclass(StoreNotFound, stabilize.StabilizeError)
    assert issubclass(StoreNotFound, stabilize.errors.StabilizeError)


def test_a_missing_workflow_is_caught_as_a_stabilize_error(repository: Any) -> None:
    with pytest.raises(stabilize.StabilizeError) as caught:
        repository.retrieve(MISSING)
    assert isinstance(caught.value, ModelNotFound)
    assert caught.value.execution_id == MISSING
    assert f"Execution not found: {MISSING}" in str(caught.value)


def test_the_class_still_builds_from_a_positional_id() -> None:
    error = StoreNotFound("abc")
    assert error.execution_id == "abc"
    assert str(error).startswith("Execution not found: abc")


def test_engine_errors_are_caught_by_the_top_level_base() -> None:
    for cls in (TransientError, PermanentError, WorkflowError, ModelNotFound):
        assert issubclass(cls, stabilize.StabilizeError), cls
