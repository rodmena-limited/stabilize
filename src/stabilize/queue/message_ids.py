"""Delivery keys: how a queue row is named in processed_messages.

A message polled from the table named queue_messages keeps its row id as its
key, as in every release before 0.33.0; a message from any other table is
"<table>:<row id>", so two tables with independent id sequences never share a
key in the store-wide processed_messages table.
"""

from __future__ import annotations

from stabilize.queue.ownership import DEFAULT_TABLE, normalize_table


def delivery_key(table_name: str, row_id: int) -> str:
    table = normalize_table(table_name)
    if table == DEFAULT_TABLE:
        return str(row_id)
    return f"{table}:{row_id}"


def row_id(message_id: str | int) -> int:
    """The queue row id inside a delivery key; raises ValueError when there is none."""
    return int(str(message_id).rsplit(":", 1)[-1])


def own_row_ids(table_name: str, keys: list[str]) -> dict[int, str]:
    """Row id -> key for the keys that name a row of table_name."""
    owned: dict[int, str] = {}
    for key in keys:
        try:
            rid = row_id(key)
        except ValueError:
            continue
        if delivery_key(table_name, rid) == key:
            owned[rid] = key
    return owned


def owner_prefix(table_name: str) -> str | None:
    """None for the default table, whose keys are bare row ids; otherwise the "<table>:" key prefix."""
    table = normalize_table(table_name)
    return None if table == DEFAULT_TABLE else f"{table}:"
