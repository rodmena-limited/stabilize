"""Deterministic merge of ancestor outputs.

Both persistence backends previously carried this algorithm verbatim, seeding a
topological sort from a ``set``. Python randomises string hashing per process, so
sibling order — and therefore which ancestor won a last-write-wins key collision
— varied by process. Two workers could hand the same stage different inputs for
the same stored state.

Ordering is now stable. Stability is not the same as correctness: when two
ancestors that are not ordered relative to each other write the same key with
different values, no tie-break is more right than another, so the collision is
reported rather than silently resolved.
"""

from __future__ import annotations

import logging
import os
from collections import deque
from typing import Any

logger = logging.getLogger(__name__)

STRICT_ENV = "STABILIZE_MERGE_STRICT"


class AmbiguousOutputMergeError(Exception):
    """Two unordered ancestors disagree on the value of the same output key."""


def _strict() -> bool:
    return os.environ.get(STRICT_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def topological_order(
    ancestors: set[str],
    requisites_of: dict[str, set[str]],
) -> list[str]:
    """Kahn's algorithm over the ancestor subgraph, in a stable order.

    Ties are broken by ref_id so the result does not depend on set iteration
    order, which varies with the process hash seed.
    """
    in_degree = {aid: 0 for aid in ancestors}
    graph: dict[str, list[str]] = {aid: [] for aid in ancestors}

    for aid in sorted(ancestors):
        for req in sorted(requisites_of.get(aid, set())):
            if req in ancestors:
                graph[req].append(aid)
                in_degree[aid] += 1

    queue = deque(sorted(aid for aid in ancestors if in_degree[aid] == 0))
    ordered: list[str] = []
    while queue:
        current = queue.popleft()
        ordered.append(current)
        ready = []
        for successor in graph[current]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                ready.append(successor)
        for successor in sorted(ready):
            queue.append(successor)

    return ordered


def _reaches(graph: dict[str, list[str]], source: str, target: str) -> bool:
    """Whether target is downstream of source within the ancestor subgraph."""
    seen = {source}
    queue = deque([source])
    while queue:
        current = queue.popleft()
        for successor in graph.get(current, ()):
            if successor == target:
                return True
            if successor not in seen:
                seen.add(successor)
                queue.append(successor)
    return False


def merge_ancestor_outputs(
    ordered_ancestors: list[str],
    outputs_of: dict[str, dict[str, Any]],
    requisites_of: dict[str, set[str]],
    *,
    stage_ref_id: str = "",
) -> dict[str, Any]:
    """Merge ancestor outputs in a stable order, reporting real ambiguity.

    Lists concatenate; everything else is last-write-wins along the topological
    order. A collision between two ancestors with no path between them is
    ambiguous by construction: reported at WARNING, or raised when
    ``STABILIZE_MERGE_STRICT`` is set.
    """
    in_scope = set(ordered_ancestors)
    graph: dict[str, list[str]] = {aid: [] for aid in in_scope}
    for aid in in_scope:
        for req in requisites_of.get(aid, set()):
            if req in in_scope:
                graph[req].append(aid)

    merged: dict[str, Any] = {}
    written_by: dict[str, str] = {}

    for aid in ordered_ancestors:
        for key, value in outputs_of.get(aid, {}).items():
            previous_writer = written_by.get(key)

            if key in merged and isinstance(merged[key], list) and isinstance(value, list):
                existing = merged[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
                written_by[key] = aid
                continue

            if (
                previous_writer is not None
                and merged.get(key) != value
                and not _reaches(graph, previous_writer, aid)
                and not _reaches(graph, aid, previous_writer)
            ):
                message = (
                    f"Ambiguous output merge for stage {stage_ref_id!r}: "
                    f"ancestors {previous_writer!r} and {aid!r} are unordered "
                    f"relative to each other and both set {key!r} "
                    f"({merged.get(key)!r} vs {value!r}). "
                    f"{aid!r} wins by ref_id ordering, which is a tie-break "
                    f"convention and not a semantic."
                )
                if _strict():
                    raise AmbiguousOutputMergeError(message)
                logger.warning("%s", message)

            merged[key] = value
            written_by[key] = aid

    return merged
