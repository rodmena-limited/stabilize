"""QueueProcessor.stop(wait=True) returns within a bound and reports what is still running (#66).

0.31.0 joined the poll and recovery threads with a timeout but called
ThreadPoolExecutor.shutdown(wait=True) with none, so one blocked handler held
stop() -- and the lifecycle manager's advertised shutdown_timeout -- open
forever.

  A  a handler blocked indefinitely: stop(wait=True, timeout=1) returns within
     3 s and reports 1 handler still running
  B  CONTROL (release direction): a handler that finishes in 0.5 s is waited
     for: stop(wait=True, timeout=10) returns after it, reporting 0
  C  LifecycleManager(shutdown_timeout=1) with a blocked handler returns
     within 5 s

    python audit/evaluations/probe_processor_stop_bounded.py
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import sqlite_backend  # noqa: E402

from stabilize import QueueProcessor  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402
from stabilize.queue.processor.config import QueueProcessorConfig  # noqa: E402

RELEASE = threading.Event()
ENTERED = threading.Event()


def _processor(queue, hold: float | None) -> QueueProcessor:
    p = QueueProcessor(queue, config=QueueProcessorConfig(poll_frequency_ms=10, enable_deduplication=False))

    def handler(message: StartWorkflow) -> None:
        ENTERED.set()
        if hold is None:
            RELEASE.wait()
        else:
            time.sleep(hold)

    p.register_handler_func(StartWorkflow, handler)
    return p


def _run(hold: float | None, stop) -> tuple[float, object]:
    ENTERED.clear()
    with sqlite_backend() as (_, queue):
        p = _processor(queue, hold)
        queue.push(StartWorkflow(execution_type="workflow", execution_id="x"))
        p.start()
        ENTERED.wait(10)
        box: dict[str, object] = {}

        def call() -> None:
            t0 = time.monotonic()
            box["value"] = stop(p)
            box["took"] = time.monotonic() - t0

        t = threading.Thread(target=call, daemon=True)
        t.start()
        t.join(30)
        RELEASE.set()
        time.sleep(0.2)
        RELEASE.clear()
        return float(box.get("took", 999.0)), box.get("value", "HUNG")


def main() -> int:
    from stabilize.lifecycle import LifecycleManager

    failures = []
    took, value = _run(None, lambda p: p.stop(wait=True, timeout=1.0))
    ok = took < 3.0 and value == 1
    print(f"[{'PASS' if ok else 'FAIL'}] A blocked handler: stop returned after {took:.1f}s reporting {value}")
    failures += [] if ok else ["A"]

    took, value = _run(0.5, lambda p: p.stop(wait=True, timeout=10.0))
    ok = 0.3 <= took < 5.0 and value == 0
    print(f"[{'PASS' if ok else 'FAIL'}] B finishing handler: stop waited {took:.1f}s reporting {value}")
    failures += [] if ok else ["B"]

    def via_manager(p):
        m = LifecycleManager(shutdown_timeout=1.0, graceful_shutdown_delay=0.0)
        m.register_processor(p)
        m._shutdown()
        return p.active_count

    took, value = _run(None, via_manager)
    ok = took < 5.0
    print(f"[{'PASS' if ok else 'FAIL'}] C LifecycleManager(1s): returned after {took:.1f}s (active={value})")
    failures += [] if ok else ["C"]

    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — stop() is bounded on a blocked handler and still waits for one that finishes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
