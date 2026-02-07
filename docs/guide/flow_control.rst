Flow Control
============

Stabilize implements all 43 Workflow Control-Flow Patterns (WCP) catalogued by
van der Aalst et al. This guide covers basic patterns first, then advanced
patterns introduced in v0.18.

.. contents:: On this page
   :local:
   :depth: 2

Basic Patterns
--------------

Sequence (WCP-1)
~~~~~~~~~~~~~~~~

Stages execute one after another via ``requisite_stage_ref_ids``:

.. code-block:: python

    stages=[
        StageExecution(ref_id="A", ...),
        StageExecution(ref_id="B", requisite_stage_ref_ids={"A"}, ...),
        StageExecution(ref_id="C", requisite_stage_ref_ids={"B"}, ...),
    ]

Parallel Split / AND-Split (WCP-2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Multiple stages depending on the same upstream run in parallel automatically:

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

Synchronization / AND-Join (WCP-3)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stage D above waits for **all** upstreams to complete before starting. This is
the default ``join_type=JoinType.AND``.

Exclusive Choice / XOR-Split (WCP-4)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``stageEnabled`` in stage context or ``TaskResult.jump_to()`` for dynamic
routing:

.. code-block:: python

    # Option 1: Conditional stage enablement
    StageExecution(
        ref_id="deploy_prod",
        context={"stageEnabled": {"type": "expression", "expression": "env == 'production'"}},
        ...
    )

    # Option 2: Dynamic routing via jump_to
    class RouterTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            if stage.context.get("tests_passed"):
                return TaskResult.jump_to("deploy_stage")
            else:
                return TaskResult.jump_to("fix_stage")

Simple Merge / XOR-Join (WCP-5)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A stage with a single ``requisite_stage_ref_id`` that serves as a merge point
when only one upstream branch is ever active.


Advanced Branching Patterns
---------------------------

OR-Split / Multi-Choice (WCP-6)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Diverge into multiple branches where **one or more** are chosen based on
conditions. Set ``split_type=SplitType.OR`` and provide per-downstream
conditions in ``split_conditions``:

.. code-block:: python

    from stabilize.models.stage import SplitType

    # Triage stage: dispatch police and/or ambulance and/or fire
    StageExecution(
        ref_id="triage",
        type="triage",
        name="Triage",
        split_type=SplitType.OR,
        split_conditions={
            "police": "emergency_type == 'crime' or emergency_type == 'accident'",
            "ambulance": "injury_severity > 0",
            "fire": "fire_detected == True",
        },
        ...
    ),
    StageExecution(ref_id="police", requisite_stage_ref_ids={"triage"}, ...),
    StageExecution(ref_id="ambulance", requisite_stage_ref_ids={"triage"}, ...),
    StageExecution(ref_id="fire", requisite_stage_ref_ids={"triage"}, ...),

Conditions are evaluated using the safe expression evaluator
(``stabilize.expressions.evaluate_expression``). Expressions can reference
values from both ``stage.context`` and ``stage.outputs``.

Supported expression syntax:

- Comparisons: ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``, ``in``, ``not in``
- Boolean: ``and``, ``or``, ``not``
- Literals: strings, numbers, ``True``, ``False``, ``None``
- Context lookups: ``key_name``, ``nested.key``, ``dict["key"]``

Branches with conditions that evaluate to ``False`` are automatically skipped.

OR-Join / Structured Synchronizing Merge (WCP-7)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Wait only for **activated** branches from a paired OR-split. Set
``join_type=JoinType.OR``. The OR-split records activated branch ref_ids in
the join stage's context under ``_activated_branches``:

.. code-block:: python

    from stabilize.models.stage import JoinType

    StageExecution(
        ref_id="merge",
        join_type=JoinType.OR,
        requisite_stage_ref_ids={"police", "ambulance", "fire"},
        ...
    )

The merge stage fires as soon as all **activated** upstreams complete. Skipped
branches are ignored.

Multi-Merge (WCP-8)
~~~~~~~~~~~~~~~~~~~~

Each upstream completion independently triggers the downstream stage — no
synchronization:

.. code-block:: python

    StageExecution(
        ref_id="quality_review",
        join_type=JoinType.MULTI_MERGE,
        requisite_stage_ref_ids={"foundations", "materials", "laborers"},
        ...
    )

Discriminator / 1-out-of-N Join (WCP-9)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fire on the **first** upstream completion, ignore the rest:

.. code-block:: python

    # Check breathing and check pulse run in parallel.
    # Start triage as soon as the first check completes.
    StageExecution(
        ref_id="triage",
        join_type=JoinType.DISCRIMINATOR,
        requisite_stage_ref_ids={"check_breathing", "check_pulse"},
        ...
    )

N-of-M Partial Join (WCP-30)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fire when N of M upstreams complete:

.. code-block:: python

    # 5 reviewers assigned, proceed after 3 respond
    StageExecution(
        ref_id="proceed",
        join_type=JoinType.N_OF_M,
        join_threshold=3,
        requisite_stage_ref_ids={"r1", "r2", "r3", "r4", "r5"},
        ...
    )


State-Based Patterns
--------------------

Deferred Choice (WCP-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~

A race between branches — the first to start execution wins, others are
cancelled. Group competing stages with ``deferred_choice_group``:

.. code-block:: python

    # Agent picks up complaint OR manager escalation timer fires
    StageExecution(
        ref_id="agent_contact",
        deferred_choice_group="complaint_response",
        ...
    ),
    StageExecution(
        ref_id="escalate_to_manager",
        deferred_choice_group="complaint_response",
        ...
    ),

When one stage in the group starts running, all siblings in the same group are
automatically cancelled.

Milestone Gating (WCP-18)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

An activity is only enabled when a milestone stage is in a required status. If
the milestone has already passed, the activity is skipped:

.. code-block:: python

    # Route change is only allowed while the ticket is RUNNING (not yet issued)
    StageExecution(
        ref_id="route_change",
        milestone_ref_id="issue_ticket",
        milestone_status="RUNNING",
        ...
    ),

Mutual Exclusion / Critical Section (WCP-17, 39, 40)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prevent concurrent execution of stages sharing a critical resource. Stages
with the same ``mutex_key`` cannot run simultaneously:

.. code-block:: python

    # Two branches both access a shared database
    StageExecution(
        ref_id="update_inventory",
        mutex_key="shared_db",
        ...
    ),
    StageExecution(
        ref_id="update_ledger",
        mutex_key="shared_db",
        ...
    ),

When a mutex-blocked stage tries to start, it is re-queued with a delay until
the lock holder completes.


Cancellation Patterns
---------------------

Cancel Task (WCP-19)
~~~~~~~~~~~~~~~~~~~~~

Cancel a running stage via ``CancelStage`` message. Tasks have an ``on_cancel()``
hook for cleanup.

Cancel Case (WCP-20)
~~~~~~~~~~~~~~~~~~~~~~

Cancel an entire workflow via ``CancelWorkflow`` message. Propagates
``CancelStage`` to all active stages.

Cancel Region (WCP-25)
~~~~~~~~~~~~~~~~~~~~~~~~

Cancel all stages in a named region at once. Tag stages with
``cancel_region``:

.. code-block:: python

    from stabilize.queue.messages import CancelRegion

    # Tag stages with a region name
    StageExecution(ref_id="access_evidence_1", cancel_region="evidence_access", ...),
    StageExecution(ref_id="access_evidence_2", cancel_region="evidence_access", ...),

    # Cancel all stages in the region
    queue.push(CancelRegion(
        execution_type="workflow",
        execution_id=workflow.id,
        region="evidence_access",
    ))


Trigger Patterns (Signals)
--------------------------

Stabilize supports external signal-based triggers with two semantics:

TaskResult.suspend()
~~~~~~~~~~~~~~~~~~~~~

A task can suspend itself to wait for an external signal:

.. code-block:: python

    from stabilize import Task, TaskResult

    class ApprovalTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Check if we've received the signal
            signal_name = stage.context.get("_signal_name")
            if signal_name == "approved":
                return TaskResult.success(
                    outputs={"approved_by": stage.context.get("_signal_data", {}).get("user")}
                )

            # Suspend and wait for signal
            return TaskResult.suspend()

Transient Trigger (WCP-23)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Send a signal to a stage. If the stage is not currently ``SUSPENDED``, the
signal is **discarded**:

.. code-block:: python

    from stabilize.queue.messages import SignalStage

    queue.push(SignalStage(
        execution_type="workflow",
        execution_id=workflow.id,
        stage_id=stage.id,
        signal_name="approved",
        signal_data={"user": "alice"},
        persistent=False,  # Transient — discarded if not ready
    ))

Persistent Trigger (WCP-24)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like a transient trigger, but the signal is **buffered** if the stage is not
yet suspended. Buffered signals are automatically consumed when the stage
enters ``SUSPENDED`` status:

.. code-block:: python

    queue.push(SignalStage(
        execution_type="workflow",
        execution_id=workflow.id,
        stage_id=stage.id,
        signal_name="data_ready",
        signal_data={"batch_id": 42},
        persistent=True,  # Buffered until stage is ready
    ))

Signal data is available in the resumed task via ``stage.context["_signal_name"]``
and ``stage.context["_signal_data"]``.


Iteration Patterns
------------------

Arbitrary Cycles (WCP-10)
~~~~~~~~~~~~~~~~~~~~~~~~~~

``TaskResult.jump_to()`` enables jumping to any stage (forward or backward).
The target stage is reset to ``NOT_STARTED``. Max 10 jumps by default to
prevent infinite loops.

Structured Loops (WCP-21)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``LoopBuilder`` for structured while and repeat-until patterns:

.. code-block:: python

    from stabilize.stages.loop_builder import LoopBuilder

    # While loop: check condition first, then execute body
    stages = LoopBuilder.while_loop(
        condition="iteration_count < max_iterations",
        body_stages=[stage_a, stage_b],
        loop_ref_prefix="retry_loop",
        max_iterations=100,
    )

    # Repeat-until loop: execute body first, then check condition
    stages = LoopBuilder.repeat_until(
        condition="tests_passed == True",
        body_stages=[stage_a, stage_b],
        loop_ref_prefix="test_loop",
    )

Recursion / Sub-Workflows (WCP-22)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``SubWorkflowTask`` to start a child workflow and wait for its completion:

.. code-block:: python

    from stabilize.tasks.sub_workflow import SubWorkflowTask

    registry.register("sub_workflow", SubWorkflowTask)

    StageExecution(
        ref_id="resolve_sub_defects",
        type="sub_workflow",
        context={
            "_sub_workflow_config": {
                "name": "Resolve Sub-Defect",
                "application": "defect-tracker",
                "stages": [...],
                "context": {"defect_id": "DEF-456"},
            },
        },
        ...
    )

Recursion depth is tracked via ``_recursion_depth`` (default max: 10).


Multiple Instance Patterns
--------------------------

Use ``MultiInstanceBuilder`` to create parallel instances of a stage.

Fixed Count (WCP-12, 13)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from stabilize.stages.multi_instance_builder import MultiInstanceBuilder

    parent = StageExecution(ref_id="review", type="review", name="Review", ...)

    # WCP-13: 6 reviewers, wait for all
    instance_stages = MultiInstanceBuilder.create_fixed(
        parent_stage=parent,
        count=6,
        instance_type="review",
        instance_name_prefix="Reviewer",
        sync_on_complete=True,
    )

    # WCP-12: Fire and forget (no synchronization)
    instance_stages = MultiInstanceBuilder.create_fixed(
        parent_stage=parent,
        count=3,
        sync_on_complete=False,
    )

Runtime Count from Context (WCP-14)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    parent = StageExecution(
        ref_id="review", type="review", name="Review",
        context={"num_reviewers": 4},
        ...
    )

    instance_stages = MultiInstanceBuilder.create_from_context(
        parent_stage=parent,
        count_key="num_reviewers",
    )

Collection-Based Instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    parent = StageExecution(
        ref_id="process_items", type="process", name="Process",
        context={"items": ["item_a", "item_b", "item_c"]},
        ...
    )

    instance_stages = MultiInstanceBuilder.create_from_collection(
        parent_stage=parent,
        collection_key="items",
        item_context_key="current_item",
    )

Dynamic Instances (WCP-15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instances can be added during execution via ``AddMultiInstance`` messages:

.. code-block:: python

    instance_stages = MultiInstanceBuilder.create_dynamic(
        parent_stage=parent,
        initial_count=2,
    )

    # Later, during execution:
    from stabilize.queue.messages import AddMultiInstance

    queue.push(AddMultiInstance(
        execution_type="workflow",
        execution_id=workflow.id,
        stage_id=parent_stage.id,
        instance_context={"item": "new_item"},
    ))

N-of-M with Multiple Instances (WCP-34)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Proceed after 3 of 5 reviewers respond
    instance_stages = MultiInstanceBuilder.create_fixed(
        parent_stage=parent,
        count=5,
        join_threshold=3,
        cancel_remaining=True,  # Cancel remaining after threshold
    )


Synthetic Stages
----------------

Synthetic stages are dynamically injected stages that run before, after, or on failure of a parent stage. They're used for setup, cleanup, validation, and rollback.

SyntheticStageOwner Enum
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from stabilize.models.stage import SyntheticStageOwner

    SyntheticStageOwner.STAGE_BEFORE  # Runs before parent's tasks
    SyntheticStageOwner.STAGE_AFTER   # Runs after parent completes

Creating Synthetic Stages
~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from stabilize.stages.builder import register_builder, StageDefinitionBuilderFactory

    # Option 1: Use global factory
    register_builder(DeployStageBuilder())

    # Option 2: Create custom factory
    factory = StageDefinitionBuilderFactory()
    factory.register(DeployStageBuilder())

Execution Order
~~~~~~~~~~~~~~~~

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


StageExecution Control-Flow Fields Reference
---------------------------------------------

All fields added to ``StageExecution`` for advanced control-flow patterns:

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Field
     - Type
     - Default
     - Description
   * - ``join_type``
     - ``JoinType``
     - ``AND``
     - Join semantics: AND, OR, MULTI_MERGE, DISCRIMINATOR, N_OF_M
   * - ``join_threshold``
     - ``int``
     - ``0``
     - For N_OF_M join: how many upstreams needed (0 = all)
   * - ``split_type``
     - ``SplitType``
     - ``AND``
     - Split semantics: AND (all downstream), OR (conditional)
   * - ``split_conditions``
     - ``dict[str, str]``
     - ``{}``
     - Map of downstream_ref_id to condition expression
   * - ``mi_config``
     - ``MultiInstanceConfig``
     - ``None``
     - Multi-instance configuration
   * - ``deferred_choice_group``
     - ``str``
     - ``None``
     - Group name for deferred choice race
   * - ``milestone_ref_id``
     - ``str``
     - ``None``
     - Ref ID of milestone stage to check
   * - ``milestone_status``
     - ``str``
     - ``None``
     - Required status name of milestone stage
   * - ``mutex_key``
     - ``str``
     - ``None``
     - Named mutex for critical section
   * - ``cancel_region``
     - ``str``
     - ``None``
     - Named region for group cancellation


Key Files
---------

*   ``src/stabilize/stages/builder.py`` - StageDefinitionBuilder and factory
*   ``src/stabilize/dag/graph.py`` - StageGraphBuilder
*   ``src/stabilize/dag/readiness.py`` - Readiness evaluation with join type dispatch
*   ``src/stabilize/expressions.py`` - Safe expression evaluator for conditions
*   ``src/stabilize/models/stage.py`` - JoinType, SplitType enums, all control-flow fields
*   ``src/stabilize/models/multi_instance.py`` - MultiInstanceConfig dataclass
*   ``src/stabilize/stages/multi_instance_builder.py`` - MultiInstanceBuilder
*   ``src/stabilize/stages/loop_builder.py`` - LoopBuilder for structured loops
*   ``src/stabilize/tasks/sub_workflow.py`` - SubWorkflowTask for recursion
*   ``src/stabilize/tasks/result.py`` - TaskResult with jump_to() and suspend()
*   ``src/stabilize/handlers/signal_stage.py`` - SignalStageHandler for triggers
*   ``src/stabilize/handlers/cancel_region.py`` - CancelRegionHandler
*   ``src/stabilize/handlers/add_multi_instance.py`` - AddMultiInstanceHandler
*   ``src/stabilize/handlers/continue_parent_stage.py`` - ContinueParentStageHandler
*   ``src/stabilize/handlers/jump_to_stage.py`` - JumpToStageHandler for dynamic routing
*   ``src/stabilize/errors.py`` - TransientError with context_update parameter
