"""Run engine threads and executor jobs in the context of the code that started them."""

from __future__ import annotations

import contextvars
import functools
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

P = ParamSpec("P")
T = TypeVar("T")


def in_context(fn: Callable[P, T]) -> Callable[P, T]:
    """Return *fn* bound to a snapshot of the caller's contextvars, entered afresh on every call."""
    captured = contextvars.copy_context()

    @functools.wraps(fn)
    def run(*args: P.args, **kwargs: P.kwargs) -> T:
        return captured.copy().run(fn, *args, **kwargs)

    return run
