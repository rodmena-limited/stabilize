"""V2 Golden Standard Workflow - File-Based Output.

Based on highway-workflow-engine/demo/v2.py

This workflow writes to an actual file (non-ACID medium) to test engine correctness.
Each task appends its token to the output file using shell commands.
The final file content must match the expected golden standard.

If the engine has bugs (wrong order, missing retries, broken timeout),
the file content will be wrong and the test will fail.
"""

from __future__ import annotations

from golden_standard_tests.workflows.v2_tasks import (
    CompensationTask,
    EventEmitterTask,
    EventReceiverTask,
    FinalizeTask,
    JoinGateTask,
    LoopIterationTask,
    RetryTask,
    SetupTask,
    TimeoutTask,
)
from stabilize import (
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)

# =============================================================================
# Task Registration
# =============================================================================


def register_v2_tasks(registry: TaskRegistry) -> None:
    """Register all V2 workflow tasks."""
    registry.register("v2_setup", SetupTask)
    registry.register("v2_retry", RetryTask)
    registry.register("v2_timeout", TimeoutTask)
    registry.register("v2_compensation", CompensationTask)
    registry.register("v2_event_emitter", EventEmitterTask)
    registry.register("v2_event_receiver", EventReceiverTask)
    registry.register("v2_join_gate", JoinGateTask)
    registry.register("v2_loop_iteration", LoopIterationTask)
    registry.register("v2_finalize", FinalizeTask)


# =============================================================================
# Workflow Definition
# =============================================================================


def create_v2_workflow() -> Workflow:
    """Create the V2 golden standard workflow."""
    return Workflow.create(
        application="golden-standard-v2",
        name="V2 Golden Standard Workflow",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="v2_setup",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch A: Retry
            StageExecution(
                ref_id="2a",
                type="retry",
                name="Branch A: Retry",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Retry Task",
                        implementing_class="v2_retry",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B: Timeout
            StageExecution(
                ref_id="2b",
                type="timeout",
                name="Branch B: Timeout",
                requisite_stage_ref_ids={"1"},
                context={"continuePipelineOnFailure": True},
                tasks=[
                    TaskExecution.create(
                        name="Timeout Task",
                        implementing_class="v2_timeout",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B Compensation
            StageExecution(
                ref_id="2b_comp",
                type="compensation",
                name="Branch B: Compensation",
                requisite_stage_ref_ids={"2b"},
                tasks=[
                    TaskExecution.create(
                        name="Compensation Task",
                        implementing_class="v2_compensation",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch C: Event Emitter
            StageExecution(
                ref_id="2c",
                type="event_emitter",
                name="Branch C: Event Emitter",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Event Emitter Task",
                        implementing_class="v2_event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch D: Event Receiver
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch D: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="v2_event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Join Gate
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Join Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Join Gate Task",
                        implementing_class="v2_join_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 1
            StageExecution(
                ref_id="5a",
                type="loop",
                name="Phase 5: Loop Iteration 1",
                requisite_stage_ref_ids={"4"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 2
            StageExecution(
                ref_id="5b",
                type="loop",
                name="Phase 5: Loop Iteration 2",
                requisite_stage_ref_ids={"5a"},
                context={"counter": 2},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iteration 3
            StageExecution(
                ref_id="5c",
                type="loop",
                name="Phase 5: Loop Iteration 3",
                requisite_stage_ref_ids={"5b"},
                context={"counter": 3},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="v2_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Finalize
            StageExecution(
                ref_id="6",
                type="finalize",
                name="Phase 6: Finalize",
                requisite_stage_ref_ids={"5c"},
                tasks=[
                    TaskExecution.create(
                        name="Finalize Task",
                        implementing_class="v2_finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )
