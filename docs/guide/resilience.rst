Resilience
==========

Stabilize is designed to survive failures through optimistic locking, atomic deduplication, and crash recovery.

Optimistic Locking
------------------

The database layer uses a ``version`` column to detect concurrent modifications. Every stage and task tracks its version, and updates include a version check:

.. code-block:: sql

    UPDATE stage_executions SET
        status = :status,
        version = version + 1
    WHERE id = :id AND version = :expected_version

If the ``WHERE`` clause fails (another process modified the row), a ``ConcurrencyError`` is raised:

.. code-block:: python

    from stabilize.errors import ConcurrencyError

    try:
        repository.store_stage(stage)
    except ConcurrencyError:
        # Another process modified this stage
        # Handle accordingly (retry, ignore, or fail)
        pass

StartStage Race Condition
~~~~~~~~~~~~~~~~~~~~~~~~~

A common race condition occurs when multiple upstream stages complete simultaneously and all try to start the same downstream stage:

.. code-block:: text

        A         B
         \       /
          \     /
           \   /
             C    <- Both A and B complete, both send StartStage for C

The ``StartStageHandler`` safely handles this by catching ``ConcurrencyError``:

.. code-block:: python

    try:
        with self.repository.transaction(self.queue) as txn:
            txn.store_stage(stage)
            txn.push_message(StartTask(...))
    except ConcurrencyError:
        # Another handler already started this stage - safe to ignore
        logger.debug("Stage already started by another handler")
        return

This is safe because the stage is already being processed by another handler, and message deduplication prevents duplicate task execution.

Atomic Deduplication
--------------------

Message processing is idempotent. The engine atomically checks if a message was processed *within the same transaction* as the state update. This guarantees "exactly-once" semantics for state transitions.

Each message has a unique ``message_id``, and processed messages are tracked in the ``processed_messages`` table:

.. code-block:: python

    with repository.transaction(queue) as txn:
        txn.store_stage(stage)
        txn.mark_message_processed(
            message_id=message.message_id,
            handler_type="StartStage",
            execution_id=message.execution_id,
        )
        txn.push_message(next_message)

If the transaction rolls back, the message is not marked as processed and will be retried.

Circuit Breakers
----------------

Circuit breakers prevent cascading failures when external services are down. After a threshold of failures, the circuit opens and subsequent calls fail fast without attempting the operation.

Bulkheads
---------

Bulkheads isolate task types (e.g., HTTP vs Shell) to prevent resource exhaustion in one area from affecting others. Each task type can have its own thread pool or connection limits.

Crash Recovery
--------------

On startup, ``WorkflowRecovery`` scans for "stuck" workflows (e.g., process crash during execution) and re-queues them safely:

.. code-block:: python

    from stabilize.recovery import WorkflowRecovery, recover_on_startup

    # At application startup
    recovery = WorkflowRecovery(store, queue)
    results = recovery.recover_pending_workflows()

    # Or use the convenience function
    recover_on_startup(store, queue)

Recovery automatically:

*   Finds workflows in ``RUNNING`` or ``NOT_STARTED`` state
*   Re-queues their current stages for continuation
*   Uses idempotency to prevent duplicate work

Key Files
---------

*   ``src/stabilize/handlers/start_stage.py`` - ConcurrencyError handling
*   ``src/stabilize/persistence/store.py`` - Optimistic locking implementation
*   ``src/stabilize/recovery.py`` - Crash recovery logic
*   ``src/stabilize/errors.py`` - ConcurrencyError definition
