"""Emission control for refused WCP-24 persistent signals.

A refusal fires once per arriving signal, so logging every one reproduces at
the logging layer the unbounded growth the refusal exists to prevent. This
tracker emits the first refusal for a stage at WARNING, then only at
exponentially spaced counts, so a stage costs O(log n) lines rather than O(n).
"""

from __future__ import annotations

from collections import OrderedDict

MAX_TRACKED_STAGES = 1024
MAX_EMISSION_GAP = 100_000


class RefusalTracker:
    """Counts refusals per stage and decides which ones are worth emitting."""

    def __init__(self, max_tracked: int = MAX_TRACKED_STAGES) -> None:
        self._counts: OrderedDict[tuple[str, str], int] = OrderedDict()
        self._max_tracked = max_tracked

    def record(self, execution_id: str, stage_ref_id: str) -> tuple[int, bool]:
        """Record a refusal.

        Returns:
            The running count for this stage, and whether it should be emitted
            at WARNING.
        """
        key = (execution_id, stage_ref_id)
        count = self._counts.get(key, 0) + 1
        self._counts[key] = count
        self._counts.move_to_end(key)

        while len(self._counts) > self._max_tracked:
            self._counts.popitem(last=False)

        return count, _is_emission_point(count)

    def reset(self) -> None:
        self._counts.clear()


def _is_emission_point(count: int) -> bool:
    if count % MAX_EMISSION_GAP == 0:
        return True
    if count == 1:
        return True
    while count % 10 == 0:
        count //= 10
        if count == 1:
            return True
    return False
