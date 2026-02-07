from stabilize import TaskResult
from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class JumpTask(Task):
    target = None

    def execute(self, stage: StageExecution) -> TaskResult:
        if self.target:
            return TaskResult.jump_to(self.target)
        return TaskResult.success()


class RecordTask(Task):
    records = []

    def execute(self, stage: StageExecution) -> TaskResult:
        RecordTask.records.append(stage.ref_id)
        if stage.name:
            RecordTask.records.append(stage.name)
        return TaskResult.success()


def reset_tasks():
    JumpTask.target = None
    RecordTask.records = []


class TestJumpCorrectness:
    def test_forward_jump_skips_intermediate(self, repository: WorkflowStore, queue: Queue) -> None:
        """
        A -> B -> C
        A jumps to C.
        B should be SKIPPED.
        """
        reset_tasks()
        JumpTask.target = "stage_c"

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"jump": JumpTask, "record": RecordTask})

        execution = Workflow.create(
            application="test",
            name="Forward Jump",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Jump",
                            implementing_class="jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Record B",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_b"},
                    tasks=[
                        TaskExecution.create(
                            name="Record C",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        stage_b = result.stage_by_ref_id("stage_b")
        stage_c = result.stage_by_ref_id("stage_c")

        # Check B was skipped
        assert stage_b.status == WorkflowStatus.SKIPPED, f"Stage B status is {stage_b.status}, expected SKIPPED"

        # Check C ran
        assert stage_c.status == WorkflowStatus.SUCCEEDED
        assert "stage_c" in RecordTask.records

    def test_backward_jump_resets_intermediate(self, repository: WorkflowStore, queue: Queue) -> None:
        """
        A -> B -> C
        C jumps to B (once).
        Sequence should be: A, B, C, B, C
        """
        reset_tasks()

        # Conditional jump task
        class ConditionalJumpTask(Task):
            count = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                ConditionalJumpTask.count += 1
                RecordTask.records.append(stage.ref_id)
                if ConditionalJumpTask.count == 1:
                    return TaskResult.jump_to("stage_b")
                return TaskResult.success()

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"cond_jump": ConditionalJumpTask, "record": RecordTask}
        )

        execution = Workflow.create(
            application="test",
            name="Backward Jump",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Record A",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Record B",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_b"},
                    tasks=[
                        TaskExecution.create(
                            name="Cond Jump",
                            implementing_class="cond_jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        # Expected: A -> B -> C (jump) -> B -> C (finish)
        # Note: I modified RecordTask to append stage.name too, but let's just count refs
        # refs: a, b, c, b, c
        count_b = RecordTask.records.count("stage_b")
        count_c = RecordTask.records.count("stage_c")
        assert count_b == 2
        assert count_c == 2

    def test_diamond_skip(self, repository: WorkflowStore, queue: Queue) -> None:
        """
          /-> B ->\
        A          D
          \\-> C ->/

        A jumps to D.
        B and C should be SKIPPED.
        """
        reset_tasks()
        JumpTask.target = "stage_d"

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"jump": JumpTask, "record": RecordTask})

        execution = Workflow.create(
            application="test",
            name="Diamond Jump",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Jump",
                            implementing_class="jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Record B",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Record C",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    tasks=[
                        TaskExecution.create(
                            name="Record D",
                            implementing_class="record",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        stage_b = result.stage_by_ref_id("stage_b")
        stage_c = result.stage_by_ref_id("stage_c")
        stage_d = result.stage_by_ref_id("stage_d")

        assert stage_b.status == WorkflowStatus.SKIPPED
        assert stage_c.status == WorkflowStatus.SKIPPED
        assert stage_d.status == WorkflowStatus.SUCCEEDED

    def test_synthetic_stage_handling(self, repository: WorkflowStore, queue: Queue) -> None:
        """
        A -> B (has synthetic stages) -> C
        A jumps to C.
        B and its synthetic stages should be SKIPPED.
        """
        reset_tasks()
        JumpTask.target = "stage_c"

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"jump": JumpTask, "record": RecordTask})

        # Create execution manually to attach synthetic stages
        stage_a = StageExecution(
            ref_id="stage_a",
            name="Stage A",
            tasks=[TaskExecution.create(name="Jump", implementing_class="jump", stage_start=True, stage_end=True)],
        )
        stage_b = StageExecution(
            ref_id="stage_b",
            name="Stage B",
            requisite_stage_ref_ids={"stage_a"},
            tasks=[
                TaskExecution.create(name="Record B", implementing_class="record", stage_start=True, stage_end=True)
            ],
        )
        stage_c = StageExecution(
            ref_id="stage_c",
            name="Stage C",
            requisite_stage_ref_ids={"stage_b"},
            tasks=[
                TaskExecution.create(name="Record C", implementing_class="record", stage_start=True, stage_end=True)
            ],
        )

        # Add synthetic stages to B
        b_setup = StageExecution.create_synthetic(
            type="test", name="B Setup", parent=stage_b, owner=SyntheticStageOwner.STAGE_BEFORE
        )
        b_teardown = StageExecution.create_synthetic(
            type="test", name="B Teardown", parent=stage_b, owner=SyntheticStageOwner.STAGE_AFTER
        )

        # Create workflow
        execution = Workflow.create(application="test", name="Synthetic Jump", stages=[stage_a, stage_b, stage_c])

        # Manually add synthetic stages to execution since we didn't use builder properly
        b_setup._execution = execution
        b_teardown._execution = execution
        execution.stages.append(b_setup)
        execution.stages.append(b_teardown)

        # Connect synthetic stages: Setup -> B -> Teardown
        # Adjust requisites
        stage_b.requisite_stage_ref_ids.add(b_setup.ref_id)
        b_setup.requisite_stage_ref_ids = {"stage_a"}
        stage_b.requisite_stage_ref_ids = {b_setup.ref_id}
        b_teardown.requisite_stage_ref_ids = {"stage_b"}
        stage_c.requisite_stage_ref_ids = {b_teardown.ref_id}

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        s_b = result.stage_by_ref_id("stage_b")
        s_setup = next(s for s in result.stages if s.name == "B Setup")
        s_teardown = next(s for s in result.stages if s.name == "B Teardown")
        s_c = result.stage_by_ref_id("stage_c")

        # Verify A -> C jump skipped B and its synthetic stages
        assert s_b.status == WorkflowStatus.SKIPPED, "Stage B should be SKIPPED"
        assert s_setup.status == WorkflowStatus.SKIPPED, "B Setup should be SKIPPED"
        assert s_teardown.status == WorkflowStatus.SKIPPED, "B Teardown should be SKIPPED"
        assert s_c.status == WorkflowStatus.SUCCEEDED, "Stage C should be SUCCEEDED"

    def test_jump_to_stage_with_already_run_synthetic(self, repository: WorkflowStore, queue: Queue) -> None:
        """
        A -> B (has synthetic Before stage).
        1. Run flow. B and B_Before run.
        2. Jump to B.
        3. B_Before should be reset and run again.
        If not, B_Before start is ignored, and flow stops.
        """
        reset_tasks()

        # Simple task to trigger jump
        class TriggerJumpTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                if stage.context.get("do_jump"):
                    return TaskResult.jump_to("stage_b")
                return TaskResult.success()

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"trigger_jump": TriggerJumpTask, "record": RecordTask}
        )

        # Construct workflow: A -> B (has setup) -> C (jumps back to B)

        stage_a = StageExecution(
            ref_id="stage_a",
            name="Stage A",
            tasks=[
                TaskExecution.create(name="Record A", implementing_class="record", stage_start=True, stage_end=True)
            ],
        )

        stage_b = StageExecution(
            ref_id="stage_b",
            name="Stage B",
            requisite_stage_ref_ids={"stage_a"},
            tasks=[
                TaskExecution.create(name="Record B", implementing_class="record", stage_start=True, stage_end=True)
            ],
        )

        stage_c = StageExecution(
            ref_id="stage_c",
            name="Stage C",
            requisite_stage_ref_ids={"stage_b"},
            context={"do_jump": True, "_max_jumps": 1},
            tasks=[
                TaskExecution.create(
                    name="Jump Back",
                    implementing_class="trigger_jump",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        # Add synthetic stage to B
        b_setup = StageExecution.create_synthetic(
            type="test", name="B Setup", parent=stage_b, owner=SyntheticStageOwner.STAGE_BEFORE
        )
        b_setup.tasks = [
            TaskExecution.create(name="Record Setup", implementing_class="record", stage_start=True, stage_end=True)
        ]

        execution = Workflow.create(application="test", name="Synthetic Retry", stages=[stage_a, stage_b, stage_c])

        b_setup._execution = execution
        execution.stages.append(b_setup)

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        # Records: "Record A", "Record Setup", "Record B", ... then jump ... "Record Setup", "Record B"
        # Note: I modified RecordTask to append stage.name, but let's just count refs/names if easier.
        # But wait, I modified RecordTask to append ref_id AND name.
        # "stage_a", "Stage A", "stage_b", "B Setup", "stage_b", "Stage B"

        # Let's just check counts of "Stage B" (name) or "stage_b" (ref_id).
        # B Setup name is "B Setup".

        b_counts = RecordTask.records.count("Stage B")
        setup_counts = RecordTask.records.count("B Setup")

        assert b_counts == 2, f"Stage B should run twice, got {b_counts}. Records: {RecordTask.records}"
        assert setup_counts == 2, f"B Setup should run twice, got {setup_counts}. Records: {RecordTask.records}"
