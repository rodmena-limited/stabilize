Resilience
==========

Stabilize is designed to survive failures through optimistic locking, atomic deduplication, crash recovery, circuit breakers, bulkheads, and configuration versioning.

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

Phase-Aware Optimistic Locking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For stronger guarantees, Stabilize supports phase-aware locking via ``expected_phase``:

.. code-block:: python

    # Update only if stage is still RUNNING
    repository.store_stage(
        stage,
        expected_phase="RUNNING"
    )

This prevents state transitions from stale handlers. The ``phase_version`` property combines status and version:

.. code-block:: python

    stage = get_stage(stage_id)
    phase, version = stage.phase_version  # ("RUNNING", 3)

State Snapshots
~~~~~~~~~~~~~~~

For read-only access to state, use frozen snapshots:

.. code-block:: python

    from stabilize.models.snapshot import StageStateSnapshot

    # Get immutable copy
    snapshot = stage.state_snapshot()

    # snapshot.status, snapshot.version, snapshot.context, snapshot.outputs
    # All are read-only

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

Bloom Filter Deduplication
~~~~~~~~~~~~~~~~~~~~~~~~~~

For high-throughput scenarios, Stabilize uses a probabilistic bloom filter as a first-pass deduplication check:

.. code-block:: python

    from stabilize.queue.dedup import BloomDeduplicator, get_deduplicator

    # Get the global deduplicator (singleton)
    dedup = get_deduplicator(
        expected_items=100_000,
        false_positive_rate=0.001
    )

    # Check if message might have been seen
    if dedup.maybe_seen(message_id):
        # Possible duplicate - fall back to DB check
        if db.is_message_processed(message_id):
            return  # Skip duplicate

    # Process message...

    # Mark as seen for future fast-path checks
    dedup.mark_seen(message_id)

The bloom filter guarantees:

*   **No false negatives**: If it says "not seen", it's definitely new
*   **Low false positives**: ~0.1% chance of unnecessary DB lookups
*   **Thread-safe**: Safe for concurrent access

Monitor filter health:

.. code-block:: python

    dedup.fill_ratio        # 0.0 to 1.0 (reset when > 0.7)
    dedup.items_added       # Count of items marked
    dedup.size_bytes        # Memory usage
    dedup.should_reset()    # True if filter is getting full

Circuit Breakers
----------------

Circuit breakers prevent cascading failures when external services are down. After a threshold of failures, the circuit opens and subsequent calls fail fast without attempting the operation.

Stabilize provides per-workflow, per-task-type circuit breakers:

.. code-block:: python

    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.resilience.config import ResilienceConfig

    config = ResilienceConfig(
        circuit_failure_threshold=Fraction(5, 10),  # 50% failure rate
        circuit_cooldown_seconds=30.0,              # Wait 30s before retry
    )

    factory = WorkflowCircuitFactory(config)

    # Get circuit for a specific workflow and task type
    circuit = factory.get_circuit("workflow_123", "http")

Circuit breaker states:

*   **CLOSED**: Normal operation, requests pass through
*   **OPEN**: Failures exceeded threshold, requests fail immediately
*   **HALF-OPEN**: After cooldown, one request allowed to test recovery

When a circuit opens, a ``TransientError`` is raised:

.. code-block:: python

    from stabilize.errors import TransientError

    try:
        result = execute_with_resilience(...)
    except TransientError as e:
        if "Circuit breaker open" in str(e):
            # Back off and retry later
            pass

Bulkheads
---------

Bulkheads isolate task types (e.g., HTTP vs Shell) to prevent resource exhaustion in one area from affecting others. Each task type has its own thread pool with independent limits.

.. code-block:: python

    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.config import ResilienceConfig

    config = ResilienceConfig()
    manager = TaskBulkheadManager(config)

    # Execute with bulkhead isolation
    result = manager.execute_with_timeout(
        task_type="http",
        func=make_request,
        url=api_url,
        timeout=30.0
    )

    if result.success:
        response = result.result
    else:
        handle_error(result.error)

Default bulkhead limits per task type:

.. list-table::
   :header-rows: 1

   * - Task Type
     - Max Concurrent
     - Max Queue Size
   * - shell
     - 5
     - 20
   * - python
     - 3
     - 20
   * - http
     - 10
     - 20
   * - docker
     - 3
     - 20
   * - ssh
     - 5
     - 20

Customize via environment variables:

.. code-block:: bash

    export STABILIZE_BULKHEAD_HTTP_MAX_CONCURRENT=20
    export STABILIZE_BULKHEAD_SHELL_MAX_CONCURRENT=10

Monitor bulkhead health:

.. code-block:: python

    stats = manager.get_all_stats()
    # {'shell': {'active': 2, 'queued': 0, ...}, 'http': {...}}

Finalizers and Cleanup
----------------------

Tasks can register cleanup callbacks (finalizers) that execute when a stage enters a terminal state:

.. code-block:: python

    from stabilize.tasks.interface import Task
    from stabilize.finalizers import get_finalizer_registry

    class MyTask(Task):
        def execute(self, stage):
            # Register cleanup
            registry = get_finalizer_registry()
            registry.register(
                stage.id,
                "cleanup_temp_files",
                lambda: cleanup_temp_files(stage)
            )

            # Do work...
            return TaskResult.success()

        def on_cleanup(self, stage):
            """Called automatically on terminal state."""
            cleanup_resources(stage)

Finalizers are:

*   **Guaranteed to run**: Even on process crash (on restart)
*   **Timeout-protected**: Each finalizer has a 30s timeout
*   **Logged**: Results are tracked for debugging

View pending finalizers:

.. code-block:: python

    registry = get_finalizer_registry()
    pending = registry.pending()  # List of stage IDs with pending cleanup

Configuration Versioning
------------------------

Stabilize configurations are immutable and versioned. This ensures consistent behavior and enables config drift detection.

All config classes are frozen dataclasses:

.. code-block:: python

    from stabilize.resilience.config import (
        ResilienceConfig,
        HandlerConfig,
        BulkheadConfig
    )

    # Configs are immutable
    config = HandlerConfig()
    config.max_workers = 20  # Raises FrozenInstanceError!

    # Each config has a fingerprint
    fingerprint = config.config_fingerprint()  # e.g., "a1b2c3d4e5f67890"

Workflow stores the config version used at start:

.. code-block:: python

    workflow = Workflow(
        application="my_app",
        name="pipeline",
        config_version=config.config_fingerprint()
    )

This enables:

*   **Drift detection**: Compare current vs. stored config
*   **Reproducibility**: Know exact config used for any workflow
*   **Audit trails**: Track config changes over time

Load config from environment:

.. code-block:: python

    # Reads STABILIZE_* environment variables
    config = ResilienceConfig.from_env()

    # Or with custom values
    config = ResilienceConfig(
        database_url="postgresql://user:pass@host/db",
        circuit_failure_threshold=Fraction(3, 10),
        circuit_cooldown_seconds=60.0,
    )

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
*   Executes pending finalizers for crashed stages

DAG Readiness Evaluation
------------------------

Stabilize uses a pure function to evaluate stage readiness:

.. code-block:: python

    from stabilize.dag.readiness import (
        evaluate_readiness,
        PredicatePhase,
        ReadinessResult
    )

    result = evaluate_readiness(
        stage=stage,
        upstream_stages=upstream_stages,
        jump_bypass=False
    )

    if result.phase == PredicatePhase.READY:
        # All upstreams complete, proceed
        start_stage(stage)
    elif result.phase == PredicatePhase.NOT_READY:
        # Upstreams still running, re-queue
        requeue_message()
    elif result.phase == PredicatePhase.SKIP:
        # Conditions not met (upstream failed/canceled)
        skip_stage(stage, reason=result.reason)

The ``ReadinessResult`` includes:

*   ``phase``: READY, NOT_READY, SKIP, or UNDEFINED
*   ``reason``: Human-readable explanation
*   ``failed_upstream_ids``: List of upstream stages that failed

Key Files
---------

*   ``src/stabilize/handlers/start_stage.py`` - ConcurrencyError handling
*   ``src/stabilize/persistence/store.py`` - Optimistic locking implementation
*   ``src/stabilize/recovery.py`` - Crash recovery logic
*   ``src/stabilize/errors.py`` - ConcurrencyError definition
*   ``src/stabilize/resilience/circuits.py`` - Circuit breaker factory
*   ``src/stabilize/resilience/bulkheads.py`` - Bulkhead manager
*   ``src/stabilize/resilience/config.py`` - Frozen configuration
*   ``src/stabilize/queue/dedup.py`` - Bloom filter deduplication
*   ``src/stabilize/dag/readiness.py`` - DAG readiness evaluation
*   ``src/stabilize/finalizers.py`` - Cleanup callback registry
