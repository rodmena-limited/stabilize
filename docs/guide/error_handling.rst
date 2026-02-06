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
