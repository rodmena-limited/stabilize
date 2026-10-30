from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path


def host_ok() -> bool:
    """vm-1's admission rule: load under 8 and 6 GiB available. REDPROOF_HOST_GATE=off skips it (CI)."""
    if os.environ.get("REDPROOF_HOST_GATE") == "off":
        return True
    load = float(Path("/proc/loadavg").read_text().split()[0])
    avail = 0
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            avail = int(line.split()[1]) // 1024
    return load < 8 and avail >= 6144


def wait_host(log: Callable[[str], None] = print) -> None:
    waited = 0
    while not host_ok():
        if waited % 300 == 0:
            log(f"waiting {waited} s for the host: load >= 8 or MemAvailable < 6 GiB")
            log("REDPROOF_HOST_GATE=off skips this wait")
        time.sleep(20)
        waited += 20
