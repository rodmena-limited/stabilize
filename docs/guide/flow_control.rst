Flow Control
============

Parallel Execution
------------------

Stages run in parallel automatically when they depend on the same upstream stage (or no stage).

.. code-block:: python

    #      A
    #     / \
    #    B   C
    #     \ /
    #      D

    stages=[
        StageExecution(ref_id="A", ...),
        StageExecution(ref_id="B", requisite_stage_ref_ids={"A"}, ...),
        StageExecution(ref_id="C", requisite_stage_ref_ids={"A"}, ...),
        StageExecution(ref_id="D", requisite_stage_ref_ids={"B", "C"}, ...),
    ]

Synthetic Stages
----------------

Use ``StageDefinitionBuilder`` to inject dynamic stages at runtime.

*   **Before Stages**: Setup, Validation.
*   **After Stages**: Cleanup, Notification.
*   **On Failure**: Rollback, Alerting.

Concurrency Limits
------------------

Limit concurrent executions for a specific pipeline configuration.

.. code-block:: python

    config = {
        "limitConcurrent": True,
        "maxConcurrentExecutions": 5,
        "keepWaitingPipelines": True
    }

If the limit is reached, new executions enter ``BUFFERED`` state and are started automatically when slots free up.

