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
