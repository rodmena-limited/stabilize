"""How long a failed delivery waits: the configured delay, doubled per store failure up to a cap."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

STORE_RETRY_MAX_DELAY = timedelta(minutes=5)


def redelivery_delay(base: timedelta, message: Any, store_unavailable: bool) -> timedelta:
    if not store_unavailable:
        return base
    retries = int(getattr(message, "store_retries", 0) or 0)
    grown: timedelta = base * (2 ** min(retries, 16))
    cap: timedelta = max(STORE_RETRY_MAX_DELAY, base)
    return min(cap, grown)
