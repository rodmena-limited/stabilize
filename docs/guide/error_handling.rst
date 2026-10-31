Error Handling
==============

Stabilize provides a structured error hierarchy with semantic error codes, chain traversal, and automatic classification.

Error Hierarchy
---------------

All Stabilize exceptions inherit from a two-tier hierarchy:

.. code-block:: text

    StabilizeBaseException          # Root - bypasses default handlers
    └── StabilizeError              # Standard errors - caught and handled
        ├── TransientError          # Retryable (network, timeout, 5xx)
        │   └── ConcurrencyError    # Optimistic lock failures
        ├── PermanentError          # Non-retryable (auth, validation, 4xx)
        ├── RecoveryError           # Crash recovery failed
        ├── ConfigurationError      # Invalid configuration
        ├── TaskError               # Task execution failed
        │   ├── TaskTimeoutError    # Task exceeded timeout
        │   └── TaskNotFoundError   # Task implementation not found
        ├── WorkflowError           # Workflow-level issues
        │   └── WorkflowNotFoundError
        ├── StageError              # Stage-level issues
        ├── QueueError              # Queue operations failed
        │   └── DeadLetterError     # Message moved to DLQ
        └── VerificationError       # Stage verification failed
            └── TransientVerificationError  # Retry verification

Transient vs Permanent Errors
-----------------------------

The distinction between transient and permanent errors determines retry behavior:

**Transient Errors** (will be retried):

.. code-block:: python

    from stabilize.errors import TransientError

    # Network timeout - retry with backoff
    raise TransientError(
        "Connection timed out",
        retry_after=30,  # Hint: wait 30 seconds
        context_update={"processed_items": 50}  # Preserve progress
    )

**Permanent Errors** (moved to Dead Letter Queue):

.. code-block:: python

    from stabilize.errors import PermanentError

    # Validation failure - don't retry
    raise PermanentError(
        "Invalid input format",
        code=400
    )

Check error type programmatically:

.. code-block:: python

    from stabilize.errors import is_transient, is_permanent

    try:
        result = task.execute(stage)
    except Exception as e:
        if is_transient(e):
            # Schedule retry with backoff
            pass
        elif is_permanent(e):
            # Move to DLQ, alert operator
            pass

What counts as transient
~~~~~~~~~~~~~~~~~~~~~~~~

``is_transient`` looks at the exception and then at what caused it: ``__cause__``,
then a ``StabilizeError``'s ``cause``, then ``__context__`` when the raise did not
suppress it. Each exception is visited once, so a cycle ends the walk. The first
exception the rules can classify decides. A ``ValueError`` raised while handling a
``TransientError`` is therefore permanent.

The rules are applied in this order to each exception:

1. An explicit ``TransientError`` or ``PermanentError`` decides.
2. The standard library's connection and timeout errors, and the matching ``errno``
   values, are transient.
3. The PostgreSQL SQLSTATEs 40001, 40P01, 55P03, 53300, 57P03, 08001 and 08004 are
   transient. With ``STABILIZE_RETRY_ON_SHUTDOWN=1``, 57P01, 57P02 and 57014 (server
   shutdown and query cancellation) are transient too.
4. The class-name patterns from 0.33 apply last. ``validation``, ``authentication``,
   ``notfound`` and ``invalid`` in the class name mean permanent. ``timeout``,
   ``connection``, ``unavailable``, ``retry`` and ``throttl`` mean transient. Permanent
   wins when both match.

An error no rule names is neither transient nor permanent. The task path treats it as
permanent.

``retry_after`` on a transient error is a floor. The retry is due no earlier than that
many seconds after the failure, whatever the task's own backoff or the attempt backoff
would have chosen.

Store outages inside the engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The engine's own store calls fail differently from a task. When ``StartStage`` or
``CompleteStage`` cannot reach the database (connection loss, pool exhaustion, a server
shutting down or recovering, a cached plan invalidated by a migration), the handler
re-raises and the message is redelivered. Nothing is written about the stage. Before
0.34 the same failure was recorded as a planning error and the stage ended ``TERMINAL``.
``stabilize.errors.is_store_unavailable`` is the predicate, shared with the path that
holds a finished task's result for the redelivery.

The redelivery spends a store-retry budget. ``reschedule(..., store_unavailable=True)``
adds one to the row's ``store_retries`` and sets ``store_retry_since`` the first time. While
that stamp is younger than the queue's ``store_retry_budget`` (30 minutes by default, an
argument of ``PostgresQueue`` and ``SqliteQueue``) the message stays deliverable past
``max_attempts``. The processor waits ``retry_delay`` doubled per store failure, five
minutes at most, between deliveries. A message at or past ``max_attempts`` is moved to the
dead-letter queue only once its budget is spent, with the cause
``store_unavailable_budget_exhausted``. A failure that is not a store outage resets both
columns and the attempt rule applies again.

The circuit breaker counts task exceptions, transient and permanent alike, because each
one is a call the task made and lost. A redelivery caused by a store outage is not a
task call and never counts.

Queue Messages That Cannot Be Decoded
-------------------------------------

A queue row whose type is unknown to this version, whose enum values do not
exist, or whose fields fail the message contract is never retried and never
raised to the poller. ``poll_one`` moves it to the dead letter queue with the
reason and returns ``None``, and polling continues with the next message. The
payload is kept exactly as stored.

The DLQ entry's ``error`` starts with ``Deserialization failed:`` followed by
the message type and the cause. What happens next is the operator's decision:

.. code-block:: python

    for entry in queue.list_dlq():
        print(entry["id"], entry["message_type"], entry["error"])

    queue.replay_dlq(entry_id)   # back onto the queue, attempts reset

During a rolling upgrade an older worker can meet a message type that only the
newer version defines. That message is quarantined rather than lost; replay it
once every worker runs the newer version.

Error Codes
-----------

Stabilize uses semantic ``ErrorCode`` values for programmatic error handling:

.. code-block:: python

    from stabilize.error_codes import ErrorCode

    # Available error codes
    ErrorCode.UNKNOWN                   # Unclassified error
    ErrorCode.RESOURCE_EXHAUSTED        # Memory, disk, quota exceeded
    ErrorCode.TASK_TIMEOUT              # Task exceeded timeout
    ErrorCode.TASK_NOT_FOUND            # Task implementation missing
    ErrorCode.UPSTREAM_DEPENDENCY_FAILED  # Upstream stage failed
    ErrorCode.CONFIGURATION_INVALID     # Bad configuration
    ErrorCode.CONCURRENCY_CONFLICT      # Optimistic lock failure
    ErrorCode.AUTHENTICATION_FAILED     # Auth credentials invalid
    ErrorCode.VALIDATION_FAILED         # Input validation failed
    ErrorCode.NETWORK_ERROR             # Network/connection issues
    ErrorCode.CIRCUIT_OPEN              # Circuit breaker tripped
    ErrorCode.BULKHEAD_FULL             # Resource pool exhausted
    ErrorCode.VERIFICATION_FAILED       # Stage verification failed
    ErrorCode.USER_CODE_ERROR           # User task raised exception
    ErrorCode.SYSTEM_ERROR              # Internal system error
    ErrorCode.RECOVERY_FAILED           # Crash recovery failed

Each exception carries its error code:

.. code-block:: python

    from stabilize.errors import TaskTimeoutError

    try:
        execute_task(stage)
    except TaskTimeoutError as e:
        print(e.error_code)  # ErrorCode.TASK_TIMEOUT
        print(e.error_code.value)  # "TASK_TIMEOUT"

Override the default error code:

.. code-block:: python

    raise TransientError(
        "Rate limited by API",
        error_code=ErrorCode.RESOURCE_EXHAUSTED
    )

Error Chain Traversal
---------------------

When errors are wrapped (e.g., by bulkheads or retries), use chain traversal to find the root cause:

.. code-block:: python

    from stabilize.error_codes import error_chain, find_in_chain, classify_error

    try:
        execute_with_resilience(...)
    except Exception as e:
        # Get full chain from root to leaf
        chain = error_chain(e)
        for err in chain:
            print(f"  - {type(err).__name__}: {err}")

        # Find specific error type in chain
        timeout = find_in_chain(e, TaskTimeoutError)
        if timeout:
            print(f"Task {timeout.task_name} timed out")

        # Auto-classify any exception
        code = classify_error(e)  # Returns ErrorCode

Example chain:

.. code-block:: text

    BulkheadError: Execution failed
      - RuntimeError: Task execution failed
        - TaskTimeoutError: Shell command timed out after 60s

Error Truncation
----------------

Large error messages are automatically truncated before storage to prevent database bloat:

.. code-block:: python

    from stabilize.errors import truncate_error

    # Truncate to 100KB with UTF-8 aware boundary handling
    safe_message = truncate_error(large_error_message)

    # Customize max size
    safe_message = truncate_error(message, max_bytes=50_000)

    # Result includes marker when truncated
    # "Error details here... [TRUNCATED]"

The truncation:

*   Respects UTF-8 character boundaries
*   Appends ``[TRUNCATED]`` marker when shortened
*   Default limit is 100KB (configurable)

Custom Task Errors
------------------

Create rich error context for debugging:

.. code-block:: python

    from stabilize.errors import TaskError
    from stabilize.error_codes import ErrorCode

    raise TaskError(
        "Failed to process batch",
        task_name="batch_processor",
        stage_id=stage.id,
        execution_id=stage.execution_id,
        error_code=ErrorCode.USER_CODE_ERROR,
        details={
            "batch_id": "batch_123",
            "failed_item": 42,
            "reason": "Invalid format"
        }
    )

Error Classification
--------------------

The ``classify_error`` function maps any exception to an ``ErrorCode``:

.. code-block:: python

    from stabilize.error_codes import classify_error

    # Maps based on exception type and name patterns
    classify_error(TimeoutError(...))      # TASK_TIMEOUT
    classify_error(ConnectionError(...))   # NETWORK_ERROR
    classify_error(PermissionError(...))   # AUTHENTICATION_FAILED
    classify_error(ValueError(...))        # VALIDATION_FAILED

Classification rules (in order):

1. Check if it's a Stabilize exception with explicit ``error_code``
2. Check exception type name for patterns (timeout, connection, auth, etc.)
3. Fall back to ``ErrorCode.UNKNOWN``

Best Practices
--------------

1. **Always use the right error type**:

   .. code-block:: python

       # Good: Semantic error type
       raise TransientError("API rate limited", retry_after=60)

       # Bad: Generic exception
       raise Exception("API rate limited")

2. **Include context in errors**:

   .. code-block:: python

       raise TaskError(
           "Failed to fetch user",
           details={"user_id": user_id, "api_response": response.status_code}
       )

3. **Preserve progress across retries**:

   .. code-block:: python

       raise TransientError(
           "Batch processing interrupted",
           context_update={"last_processed_index": i}
       )

4. **Use error codes for routing**:

   .. code-block:: python

       if e.error_code == ErrorCode.AUTHENTICATION_FAILED:
           alert_security_team(e)
       elif e.error_code == ErrorCode.RESOURCE_EXHAUSTED:
           scale_up_resources()

Key Files
---------

*   ``src/stabilize/errors.py`` - Exception hierarchy and utilities
*   ``src/stabilize/error_codes.py`` - ErrorCode enum and classification
*   ``src/stabilize/handlers/run_task/error.py`` - Error handling in task execution

Errors During Synchronous Draining
----------------------------------

``QueueProcessor.process_one()`` and ``process_all()`` handle messages on the
calling thread. When a handler raises, the message is rescheduled by
``config.retry_delay`` (15 s by default) and the exception is **re-raised to the
caller**; ``process_all()`` stops at that point. With ``raise_on_error=False`` the
drain logs the error and carries on, and ``processor.last_drain`` reports the
number of messages processed, the number of handler errors and why the drain
stopped. The message is delivered again by a later call once the delay has
passed. ``processor.start()`` runs the same handling in the background and logs
the error instead of raising it.

When nothing is ready, ``process_all()`` asks the queue when the next message is
due (``Queue.next_ready_in()``) and sleeps until then, at most one second at a
time. When the next message is due after the remaining budget the drain returns,
so a cron that drains every minute spends milliseconds on a queue holding one
timer due tomorrow.

A task that completed while the store was unavailable (for example a statement
timeout or a pool timeout while saving its result) is not executed again: its
result is held and saved by the redelivery.

``process_one()`` renews the message lease while the handler runs, with the same
heartbeat as ``processor.start()``. A handler that outlives ``lock_duration`` therefore
keeps its message out of other consumers' polls. The short-lease warning is logged
only when ``enable_lock_heartbeat`` is off or the queue cannot extend a lock.

A handler marks the delivered message processed in the transaction that pushes its
successors. When a redelivered message reaches a handler whose mark is already
committed, the mark raises ``DuplicateDeliveryError`` inside that transaction, the
transaction rolls back, and both drain paths acknowledge the message without pushing
again. A handler that still pushes on the queue's own connection, or in a transaction
that does not mark the delivered message, is recorded on
``processor.delivery_defects`` and logged at ERROR. With ``STABILIZE_STRICT_DELIVERY=1``
in the environment, ``process_all()`` raises ``DeliveryContractError`` at the end of a
drain that recorded one.

To drain through transient errors in a script or a test:

.. code-block:: python

    processor.config.retry_delay = timedelta(milliseconds=50)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            processor.process_all(timeout=1.0)
        except Exception:
            logger.warning("handler error; the message was rescheduled", exc_info=True)
        if store.retrieve(workflow.id).status.is_complete:
            break

