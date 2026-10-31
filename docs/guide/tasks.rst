Tasks
=====

Tasks are the building blocks of Stabilize workflows.

The Task Interface
------------------

All tasks must implement the ``Task`` interface:

.. code-block:: python

    from stabilize import Task, TaskResult, StageExecution

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Access inputs
            data = stage.context.get("data")
            
            # Do work...
            result = process(data)
            
            # Return success
            return TaskResult.success(outputs={"result": result})

Built-in Tasks
--------------

Stabilize comes with robust built-in tasks.

ShellTask
~~~~~~~~~

Executes shell commands. Safely handles stdout/stderr capturing and timeouts.

.. code-block:: python

    TaskExecution.create("shell", "shell")
    # Context: {"command": "ls -la", "cwd": "/tmp"}

PythonTask
~~~~~~~~~~

Executes Python code. Can run inline scripts or module functions.

.. code-block:: python

    TaskExecution.create("python", "python")
    # Context: {"script": "RESULT = {'value': 42}"}

HTTPTask
~~~~~~~~

Makes HTTP requests. Supports all methods, JSON, auth, and retries.

.. code-block:: python

    TaskExecution.create("http", "http")
    # Context: {"url": "https://api.com", "method": "POST", "json": {...}}

Request and response keys share names. The task reads its request from the
context keys ``url``, ``method``, ``headers``, ``body`` and ``json``, and writes
its response to the outputs ``status_code``, ``headers``, ``body`` and
``body_json``. Because a stage's context is hydrated with its ancestors' outputs
before it runs (see :doc:`data_flow`), an HTTP stage downstream of another HTTP
stage inherits the upstream *response* headers and body as its own *request*
headers and body unless it sets them itself. A fan-in stage after two HTTP
stages has sent an upstream ``content-length`` and had the connection reset
(Provenance, 0.34.2). Declare ``headers`` and ``body`` on every HTTP stage that
follows another, or drop inherited keys your stage does not declare. 0.35 adds
``response_headers``, ``response_body`` and ``response_status`` as the
canonical output names (#106).

DockerTask
~~~~~~~~~~

Runs Docker containers. Supports volumes, env vars, and resource limits.

.. code-block:: python

    TaskExecution.create("docker", "docker")
    # Context: {"action": "run", "image": "alpine", "command": "echo hi"}

Advanced Task Features
----------------------

Skippable Tasks
~~~~~~~~~~~~~~~

Implement ``SkippableTask`` to conditionally skip execution logic.

.. code-block:: python

    class MySkippable(SkippableTask):
        def is_enabled(self, stage: StageExecution) -> bool:
            return stage.context.get("feature_flag") == True

Retryable Tasks
~~~~~~~~~~~~~~~

Implement ``RetryableTask`` for polling or unreliable operations. The engine handles backoff and timeouts.

A retryable task has two limits:

*   ``get_timeout()`` (or ``get_dynamic_timeout(stage)``) is the **total** time the task may
    spend across all its executions, measured from its first start. When it is exceeded the
    engine calls ``on_timeout(stage)``: a returned ``TaskResult`` is processed as the task's
    result, and ``None`` completes the task as ``TERMINAL``.
*   ``get_execution_timeout(stage)`` bounds a **single** ``execute()`` call. It defaults to the
    total limit; override it to bound each poll separately. A call that exceeds it also goes
    through ``on_timeout(stage)``.

A ``TransientError`` raised by ``execute()`` is retried with backoff up to ``max_attempts``
(10) **consecutive** times; a ``RUNNING`` result resets the count.

.. code-block:: python

    class WaitForDeploy(RetryableTask):
        def get_timeout(self) -> timedelta:
            return timedelta(hours=2)

        def get_execution_timeout(self, stage: StageExecution) -> timedelta:
            return timedelta(seconds=30)

        def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
            return timedelta(seconds=10)

        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success() if deployed(stage) else TaskResult.running()

Timers
~~~~~~

``TimerTask`` waits until an absolute time. The stage context names either ``until``
(an ISO 8601 time with a UTC offset, or epoch seconds) or ``duration_seconds``, counted
from the task's first execution. The first execution records ``_timer_armed_at`` and
``_timer_due_at`` in the stage context and asks the engine to run it again at the due
time. The execution at or after the due time succeeds. A delivery before the due time,
after a crash or a redelivery, asks again for the due time. A pause does not move the
due time, and the task's lifetime is the timer's length plus five minutes.

.. code-block:: python

    StageExecution(ref_id="cooling_off", type="timer", name="Cooling off",
                   context={"duration_seconds": 3600})
    StageExecution(ref_id="go_live", type="timer", name="Go live",
                   context={"until": "2026-11-19T09:00:00+00:00"})

``WaitTask`` is built on ``TimerTask``. It reads ``waitTime`` in seconds, keeps
``waitStartTime`` in the context and reports ``waitedSeconds`` in the outputs.

A ``RUNNING`` result from any ``RetryableTask`` may carry ``scheduled_time`` (an epoch
millisecond). The engine stores it on ``stage.scheduled_time`` while the task waits and
clears it when the task completes.

Task Cleanup
~~~~~~~~~~~~

Implement ``on_cleanup`` to release resources when a stage enters a terminal state:

.. code-block:: python

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Acquire resources
            temp_file = create_temp_file()
            stage.context["temp_file"] = temp_file

            # Do work...
            return TaskResult.success()

        def on_cleanup(self, stage: StageExecution) -> None:
            """Called automatically on terminal state (success, failure, or cancel)."""
            temp_file = stage.context.get("temp_file")
            if temp_file:
                cleanup_temp_file(temp_file)

The ``on_cleanup`` method is:

*   Called automatically when stage enters terminal state
*   Called even after process crash (on recovery)
*   Timeout-protected (30 seconds default)

For more complex cleanup, use the finalizer registry:

.. code-block:: python

    from stabilize.finalizers import get_finalizer_registry

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Register cleanup callback
            registry = get_finalizer_registry()
            registry.register(
                stage.id,
                "cleanup_external_resource",
                lambda: cleanup_external_api(stage.context["resource_id"])
            )

            # Do work...
            return TaskResult.success()
