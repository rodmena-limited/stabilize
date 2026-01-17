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
