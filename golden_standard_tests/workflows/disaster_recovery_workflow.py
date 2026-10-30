"""Disaster Recovery Golden Standard Workflow - File-Based Output.

Based on highway-workflow-engine/demo/disaster_recovery_simulation.py

This workflow writes to an actual file (non-ACID medium) to test engine correctness.
Each task appends its token to the output file using shell commands.
The final file content must match the expected golden standard.

If the engine has bugs (wrong order, missing retries, broken timeout),
the file content will be wrong and the test will fail.
"""

from __future__ import annotations

from golden_standard_tests.workflows.disaster_recovery_tasks import (
    DRCompensationTask,
    DREventEmitterTask,
    DREventReceiverTask,
    DRFinalizeTask,
    DRForeachProcessTask,
    DRForeachValidateTask,
    DRRetryTask,
    DRSetupTask,
    DRSwitchJoinTask,
    DRSwitchTask,
    DRSyncGateTask,
    DRTimeoutTask,
    DRWhileLoopTask,
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


def register_dr_tasks(registry: TaskRegistry) -> None:
    """Register all Disaster Recovery workflow tasks."""
    registry.register("dr_setup", DRSetupTask)
    registry.register("dr_retry", DRRetryTask)
    registry.register("dr_timeout", DRTimeoutTask)
    registry.register("dr_compensation", DRCompensationTask)
    registry.register("dr_event_emitter", DREventEmitterTask)
    registry.register("dr_event_receiver", DREventReceiverTask)
    registry.register("dr_sync_gate", DRSyncGateTask)
    registry.register("dr_foreach_process", DRForeachProcessTask)
    registry.register("dr_foreach_validate", DRForeachValidateTask)
    registry.register("dr_switch", DRSwitchTask)
    registry.register("dr_switch_join", DRSwitchJoinTask)
    registry.register("dr_while_loop", DRWhileLoopTask)
    registry.register("dr_finalize", DRFinalizeTask)


# =============================================================================
# Workflow Definition
# =============================================================================


def create_disaster_recovery_workflow() -> Workflow:
    """Create the Disaster Recovery golden standard workflow."""
    return Workflow.create(
        application="golden-standard-disaster-recovery",
        name="Disaster Recovery Golden Standard Workflow",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="dr_setup",
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
                        implementing_class="dr_retry",
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
                        implementing_class="dr_timeout",
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
                        implementing_class="dr_compensation",
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
                        implementing_class="dr_event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch C: Event Receiver
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch C: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="dr_event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Sync Gate
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Sync Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Sync Gate Task",
                        implementing_class="dr_sync_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_A.log process
            StageExecution(
                ref_id="5a",
                type="foreach",
                name="Foreach: Process file_A.log",
                requisite_stage_ref_ids={"4"},
                context={"item": "file_A.log"},
                tasks=[
                    TaskExecution.create(
                        name="Process Task",
                        implementing_class="dr_foreach_process",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_A.log validate
            StageExecution(
                ref_id="5b",
                type="foreach",
                name="Foreach: Validate file_A.log",
                requisite_stage_ref_ids={"5a"},
                context={"item": "file_A.log"},
                tasks=[
                    TaskExecution.create(
                        name="Validate Task",
                        implementing_class="dr_foreach_validate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_B.log process
            StageExecution(
                ref_id="5c",
                type="foreach",
                name="Foreach: Process file_B.log",
                requisite_stage_ref_ids={"5b"},
                context={"item": "file_B.log"},
                tasks=[
                    TaskExecution.create(
                        name="Process Task",
                        implementing_class="dr_foreach_process",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Foreach - file_B.log validate
            StageExecution(
                ref_id="5d",
                type="foreach",
                name="Foreach: Validate file_B.log",
                requisite_stage_ref_ids={"5c"},
                context={"item": "file_B.log"},
                tasks=[
                    TaskExecution.create(
                        name="Validate Task",
                        implementing_class="dr_foreach_validate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Switch
            StageExecution(
                ref_id="6",
                type="switch",
                name="Phase 6: Switch on Category",
                requisite_stage_ref_ids={"5d"},
                tasks=[
                    TaskExecution.create(
                        name="Switch Task",
                        implementing_class="dr_switch",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Switch Join
            StageExecution(
                ref_id="6_join",
                type="switch_join",
                name="Phase 6: Switch Join",
                requisite_stage_ref_ids={"6"},
                tasks=[
                    TaskExecution.create(
                        name="Switch Join Task",
                        implementing_class="dr_switch_join",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 7: While Loop - iteration 0
            StageExecution(
                ref_id="7a",
                type="while",
                name="Phase 7: While Loop Iteration 0",
                requisite_stage_ref_ids={"6_join"},
                context={"counter": 0},
                tasks=[
                    TaskExecution.create(
                        name="While Loop Task",
                        implementing_class="dr_while_loop",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 7: While Loop - iteration 1
            StageExecution(
                ref_id="7b",
                type="while",
                name="Phase 7: While Loop Iteration 1",
                requisite_stage_ref_ids={"7a"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="While Loop Task",
                        implementing_class="dr_while_loop",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 8: Finalize
            StageExecution(
                ref_id="8",
                type="finalize",
                name="Phase 8: Finalize",
                requisite_stage_ref_ids={"7b"},
                tasks=[
                    TaskExecution.create(
                        name="Finalize Task",
                        implementing_class="dr_finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )
