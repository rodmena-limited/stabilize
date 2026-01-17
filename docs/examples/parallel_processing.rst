Parallel Processing
===================

This example demonstrates parallel execution branches joining into a final aggregation stage.

.. code-block:: python

    #      Start
    #     /  |  \
    # Task1 Task2 Task3
    #      \  |  /
    #       Done

    workflow = Workflow.create(
        application="example",
        name="Parallel",
        stages=[
            StageExecution(ref_id="start", ...),
            StageExecution(ref_id="t1", requisite_stage_ref_ids={"start"}, ...),
            StageExecution(ref_id="t2", requisite_stage_ref_ids={"start"}, ...),
            StageExecution(ref_id="t3", requisite_stage_ref_ids={"start"}, ...),
            StageExecution(ref_id="done", requisite_stage_ref_ids={"t1", "t2", "t3"}, ...),
        ]
    )

See ``examples/python-example.py`` (Example 3) for the full runnable code.

