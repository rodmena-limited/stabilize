Resilience
==========

Stabilize is designed to survive failures.

Optimistic Locking
------------------

The database layer uses a ``version`` column to detect concurrent modifications. If two workers try to update the same task, one will fail with ``ConcurrencyError``, which is automatically caught and retried.

Atomic Deduplication
--------------------

Message processing is idempotent. The engine atomically checks if a message was processed *within the same transaction* as the state update. This guarantees "exactly-once" semantics for state transitions.

Circuit Breakers
----------------

Circuit breakers prevent cascading failures when external services are down.

Bulkheads
---------

Bulkheads isolate task types (e.g., HTTP vs Shell) to prevent resource exhaustion in one area from affecting others.

Crash Recovery
--------------

On startup, ``WorkflowRecovery`` scans for "stuck" workflows (e.g., process crash during execution) and re-queues them safely.
