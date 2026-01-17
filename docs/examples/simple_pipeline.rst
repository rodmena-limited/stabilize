Simple Pipeline
===============

This example demonstrates a basic sequential pipeline using ``ShellTask``.

.. code-block:: python

    from stabilize import Workflow, StageExecution, TaskExecution

    workflow = Workflow.create(
        application="example",
        name="Simple Shell",
        stages=[
            StageExecution(
                ref_id="1",
                type="shell",
                name="List Files",
                context={"command": "ls -la"},
                tasks=[TaskExecution.create("Run ls", "shell", stage_start=True, stage_end=True)]
            )
        ]
    )

See ``examples/shell-example.py`` for the full runnable code.
