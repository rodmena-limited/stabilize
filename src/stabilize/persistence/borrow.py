"""What query functions receive in place of a pool, so every connection comes from the owner's borrow path."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any


class EngineBorrower:
    """Wraps an owner's ``_borrow`` so code written against ``pool.connection()`` keeps working and stays admitted."""

    def __init__(self, borrow: Callable[[], AbstractContextManager[Any]]) -> None:
        self._borrow = borrow

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self._borrow() as conn:
            yield conn
