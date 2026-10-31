"""Shared pieces of event purge: the bound a purge is given and the refusal when a subscription is behind."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

DEFAULT_PURGE_BATCH = 10_000


class SubscriptionBehindError(RuntimeError):
    """An enabled durable subscription still has undelivered events inside the purge bound."""

    def __init__(self, subscriptions: list[str], bound: str) -> None:
        self.subscriptions = list(subscriptions)
        self.bound = bound
        names = ", ".join(self.subscriptions)
        super().__init__(
            f"enabled subscription(s) {names} still have undelivered events {bound}; "
            "let them catch up, disable or delete them, or pass ignore_subscriptions=True"
        )


def describe_bound(before_sequence: int | None, before: datetime | None) -> str:
    parts = []
    if before_sequence is not None:
        parts.append(f"below sequence {before_sequence}")
    if before is not None:
        parts.append(f"before {before.isoformat()}")
    return " and ".join(parts)


def bound_clauses(
    before_sequence: int | None,
    before: datetime | None,
    style: str,
    alias: str = "",
) -> tuple[list[str], dict[str, Any]]:
    """The WHERE fragments and parameters for a purge bound, in the backend's placeholder style."""
    if before_sequence is None and before is None:
        raise ValueError("purge_events needs before_sequence or before")
    if before is not None and before.tzinfo is None:
        raise ValueError("purge_events needs an aware datetime for before")
    if style == "postgres":
        seq_param, ts_param = "%(purge_seq)s", "%(purge_ts)s"
    elif style == "sqlite":
        seq_param, ts_param = ":purge_seq", ":purge_ts"
    else:
        raise ValueError(f"unknown placeholder style {style!r}")
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if before_sequence is not None:
        clauses.append(f"{alias}sequence < {seq_param}")
        params["purge_seq"] = int(before_sequence)
    if before is not None:
        clauses.append(f"{alias}timestamp < {ts_param}")
        params["purge_ts"] = before if style == "postgres" else before.astimezone(UTC).isoformat()
    return clauses, params
