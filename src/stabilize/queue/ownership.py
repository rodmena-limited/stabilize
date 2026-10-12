"""Which queue table a workflow's messages belong to."""

from __future__ import annotations

from typing import Any

DEFAULT_TABLE = "queue_messages"


def normalize_table(name: str | None) -> str:
    if not name:
        return DEFAULT_TABLE
    return name.strip().strip('"').rsplit(".", 1)[-1].strip('"') or DEFAULT_TABLE


def queue_table_name(queue: Any) -> str:
    return normalize_table(getattr(queue, "table_name", None))


def workflow_queue_table(workflow: Any) -> str:
    return normalize_table(getattr(workflow, "queue_table", None))
