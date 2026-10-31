"""Walking an exception's causes: __cause__, StabilizeError.cause, then __context__ when nothing else explains it."""

from __future__ import annotations

from collections.abc import Iterator


def walk_causes(error: BaseException) -> Iterator[BaseException]:
    """Yield *error* and what caused it, outermost first, each exception once.

    The next link is ``__cause__`` when set, else a ``cause`` attribute (StabilizeError),
    else ``__context__`` unless the raise suppressed it (``raise ... from None``).
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = next_cause(current)


def next_cause(error: BaseException) -> BaseException | None:
    return next_link(error)[0]


def next_link(error: BaseException) -> tuple[BaseException | None, bool]:
    """The next exception in the chain and whether it is only an implicit ``__context__``."""
    cause = getattr(error, "__cause__", None)
    if isinstance(cause, BaseException):
        return cause, False
    attached = getattr(error, "cause", None)
    if isinstance(attached, BaseException):
        return attached, False
    if cause is None and not getattr(error, "__suppress_context__", False):
        context = getattr(error, "__context__", None)
        if isinstance(context, BaseException):
            return context, True
    return None, False
