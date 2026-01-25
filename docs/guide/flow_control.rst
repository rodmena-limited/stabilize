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

Synthetic stages are dynamically injected stages that run before, after, or on failure of a parent stage. They're used for setup, cleanup, validation, and rollback.

SyntheticStageOwner Enum
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from stabilize.models.stage import SyntheticStageOwner

    SyntheticStageOwner.STAGE_BEFORE  # Runs before parent's tasks
    SyntheticStageOwner.STAGE_AFTER   # Runs after parent completes

Creating Synthetic Stages
~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``StageExecution.create_synthetic()`` to create synthetic stages:

.. code-block:: python

    from stabilize import StageExecution, TaskExecution
    from stabilize.models.stage import SyntheticStageOwner

    validation = StageExecution.create_synthetic(
        type="shell",
        name="Validate Configuration",
        parent=parent_stage,
        owner=SyntheticStageOwner.STAGE_BEFORE,
        context={"command": "validate-config.sh"},
        tasks=[
            TaskExecution.create("Validate", "shell", stage_start=True, stage_end=True)
        ],
    )

StageDefinitionBuilder
~~~~~~~~~~~~~~~~~~~~~~

Create custom builders to define synthetic stages for your stage types:

.. code-block:: python

    from stabilize import StageExecution, TaskExecution
    from stabilize.stages.builder import StageDefinitionBuilder
    from stabilize.dag.graph import StageGraphBuilder
    from stabilize.models.stage import SyntheticStageOwner

    class DeployStageBuilder(StageDefinitionBuilder):
        @property
        def type(self) -> str:
            return "deploy"

        def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
            return [
                TaskExecution.create(
                    name="Deploy Application",
                    implementing_class="shell",
                    stage_start=True,
                    stage_end=True,
                ),
            ]

        def before_stages(
            self,
            stage: StageExecution,
            graph: StageGraphBuilder,
        ) -> None:
            """Add validation stage that runs BEFORE deploy tasks."""
            validation = StageExecution.create_synthetic(
                type="shell",
                name="Validate Configuration",
                parent=stage,
                owner=SyntheticStageOwner.STAGE_BEFORE,
                context={"command": "validate-config.sh"},
                tasks=[
                    TaskExecution.create("Validate", "shell", stage_start=True, stage_end=True)
                ],
            )
            graph.add(validation)

        def after_stages(
            self,
            stage: StageExecution,
            graph: StageGraphBuilder,
        ) -> None:
            """Add notification stage that runs AFTER deploy succeeds."""
            notify = StageExecution.create_synthetic(
                type="http",
                name="Send Notification",
                parent=stage,
                owner=SyntheticStageOwner.STAGE_AFTER,
                context={"url": "https://hooks.slack.com/...", "method": "POST"},
                tasks=[
                    TaskExecution.create("Notify", "http", stage_start=True, stage_end=True)
                ],
            )
            graph.add(notify)

        def on_failure_stages(
            self,
            stage: StageExecution,
            graph: StageGraphBuilder,
        ) -> None:
            """Add rollback stage that runs ONLY if deploy fails."""
            rollback = StageExecution.create_synthetic(
                type="shell",
                name="Rollback Deployment",
                parent=stage,
                owner=SyntheticStageOwner.STAGE_AFTER,
                context={"command": "rollback.sh"},
                tasks=[
                    TaskExecution.create("Rollback", "shell", stage_start=True, stage_end=True)
                ],
            )
            graph.add(rollback)

Registering Builders
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from stabilize.stages.builder import register_builder, StageDefinitionBuilderFactory

    # Option 1: Use global factory
    register_builder(DeployStageBuilder())

    # Option 2: Create custom factory
    factory = StageDefinitionBuilderFactory()
    factory.register(DeployStageBuilder())

Execution Order
~~~~~~~~~~~~~~~

1. ``before_stages()`` synthetic stages execute first (in dependency order)
2. Parent stage's tasks execute
3. ``after_stages()`` synthetic stages execute (on success)
4. ``on_failure_stages()`` synthetic stages execute (on failure only)

The ``ContinueParentStageHandler`` manages transitions between synthetic stages and notifies the parent when all children complete.

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

Dynamic Routing
---------------

Dynamic routing allows tasks to redirect execution to a different stage based on runtime conditions. This enables patterns like retry loops, conditional branching, and error recovery flows.

TaskResult.jump_to()
~~~~~~~~~~~~~~~~~~~~

Use ``TaskResult.jump_to()`` to redirect execution to another stage:

.. code-block:: python

    from stabilize import Task, TaskResult, StageExecution

    class RouterTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            if stage.context.get("tests_passed"):
                return TaskResult.success()
            else:
                # Jump to another stage with context
                return TaskResult.jump_to(
                    "implement_stage",
                    context={"retry_reason": "tests failed"}
                )

The target stage is reset to ``NOT_STARTED`` and re-executed with the merged context.

Jump Count Limiting
~~~~~~~~~~~~~~~~~~~

To prevent infinite loops, jump count is tracked and limited:

- Default maximum: 10 jumps per execution
- Configurable via ``_max_jumps`` in execution context
- Jump history recorded in ``_jump_history`` for debugging

.. code-block:: python

    # Set custom max jumps
    execution = Workflow.create(
        ...,
        context={"_max_jumps": 5}
    )

Stateful Retries
~~~~~~~~~~~~~~~~

``TransientError`` supports preserving state across retry attempts with ``context_update``:

.. code-block:: python

    from stabilize import Task, TaskResult, TransientError

    class ProgressTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            processed = stage.context.get("processed_items", 0)
            try:
                new_processed = process_batch(processed)
                return TaskResult.success(outputs={"total": new_processed})
            except RateLimitError:
                # Preserve progress for next retry
                raise TransientError(
                    "Rate limited",
                    retry_after=30,
                    context_update={"processed_items": processed + 10}
                )

The ``context_update`` dict is merged into ``stage.context`` before the retry, allowing tasks to resume from where they left off.

Key Files
---------

*   ``src/stabilize/stages/builder.py`` - StageDefinitionBuilder and factory
*   ``src/stabilize/dag/graph.py`` - StageGraphBuilder
*   ``src/stabilize/handlers/continue_parent_stage.py`` - ContinueParentStageHandler
*   ``src/stabilize/handlers/jump_to_stage.py`` - JumpToStageHandler for dynamic routing
*   ``src/stabilize/models/stage.py`` - SyntheticStageOwner enum
*   ``src/stabilize/tasks/result.py`` - TaskResult with jump_to() factory method
*   ``src/stabilize/errors.py`` - TransientError with context_update parameter
