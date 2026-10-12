"""QueueProcessor(handler_config=...) configures every handler it registers.

QueueProcessor documents handler_config as the handler configuration and, when
given a store and a task registry, registers the default handlers. A handler
that ignores it falls back to HandlerConfig.from_env(), so a caller's explicit
setting (max_stage_wait_retries, handler_retry_delay_seconds, default_page_size,
the ConcurrencyError retry policy) silently does not apply.

This probe reads each registered handler's handler_config attribute: the only
place the setting lives once the processor is built. It is internal state,
used here because no handler exposes its configuration otherwise.

  A  every registered handler with a handler_config uses the passed object
  B  CONTROL RunTaskHandler uses the passed object

    python audit/evaluations/probe_handler_config_reaches_handlers.py
"""

from __future__ import annotations

import logging
import sys
import tempfile

logging.basicConfig(level=logging.CRITICAL)

from stabilize import QueueProcessor, SqliteQueue, SqliteWorkflowStore, TaskRegistry  # noqa: E402
from stabilize.queue.messages import RunTask  # noqa: E402
from stabilize.resilience.config import HandlerConfig  # noqa: E402


def main() -> int:
    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/probe.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        cfg = HandlerConfig(max_stage_wait_retries=7, handler_retry_delay_seconds=1.5, default_page_size=3)
        proc = QueueProcessor(queue, store=store, task_registry=TaskRegistry(), handler_config=cfg)
        ignoring = sorted(
            type(h).__name__
            for h in proc._handlers.values()
            if hasattr(h, "handler_config") and h.handler_config is not cfg
        )
        run_task_ok = proc._handlers[RunTask].handler_config is cfg
        queue.close()
        store.close()
    a = not ignoring
    print(f"[{'PASS' if a else 'FAIL'}] sqlite A every handler uses the passed config: ignoring={ignoring}")
    print(f"[{'PASS' if run_task_ok else 'FAIL'}] sqlite B CONTROL RunTaskHandler uses the passed config")
    if a and run_task_ok:
        print("VERDICT: PASS — handler_config reaches every handler")
        return 0
    print("VERDICT: FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
