"""Delivery keys: how a queue row is named in processed_messages.

A message polled from the table named queue_messages keeps its row id as its
key, as in every release before 1.0.0; a message from any other table is
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
