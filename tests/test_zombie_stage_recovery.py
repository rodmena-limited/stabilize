"""
Repro test for the zombie-RUNNING-stage wedge (audit finding A1-3).

If a worker crashes after the StartStage claim transaction (stage committed
as RUNNING) but before the planning transaction (tasks persisted), recovery
re-queues StartStage and the handler detects the zombie — but then re-claims
with expected_phase="NOT_STARTED" while the row is RUNNING. The CAS raises
ConcurrencyError on every attempt, is swallowed as "duplicate claim", and the
workflow is permanently wedged.
"""

from typing import Any

from stabilize.handlers import StartStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartStage
from stabilize.stages.builder import StageDefinitionBuilder, get_default_factory


class _ZombieReplanDemoBuilder(StageDefinitionBuilder):
    """Builder whose tasks are planned at start time (not predefined)."""

    @property
    def type(self) -> str:
        return "zombie_replan_demo"

    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [
            TaskExecution.create(
                name="planned task",
                implementing_class="noop",
                stage_start=True,
                stage_end=True,
            )
        ]


def _drain(queue: Queue) -> list[Any]:
    messages = []
    while True:
        message = queue.poll_one()
        if message is None:
            break
        messages.append(message)
        queue.ack(message)
    return messages


class TestZombieStageRecovery:
    def test_zombie_running_stage_is_replanned_not_wedged(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        get_default_factory().register(_ZombieReplanDemoBuilder())

        # Simulate the post-crash state: stage claimed RUNNING, no tasks and
        # no synthetic stages persisted (planning never committed).
        stage = StageExecution(
            ref_id="z",
            type="zombie_replan_demo",
            name="Zombie",
            tasks=[],
        )
        stage.status = WorkflowStatus.RUNNING
        execution = Workflow.create(application="test", name="zombie test", stages=[stage])
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        handler = StartStageHandler(queue, repository)
        message = StartStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=stage.id,
            message_id="ss-zombie-1",
        )
        handler.handle(message)  # recovery redelivery

        fresh = repository.retrieve(execution.id)
        zombie = next(s for s in fresh.stages if s.ref_id == "z")
        assert len(zombie.tasks) >= 1, (
            "zombie stage was not re-planned: CAS with expected_phase="
            "NOT_STARTED can never succeed against a RUNNING row"
        )
        assert zombie.status == WorkflowStatus.RUNNING
        pushed = _drain(queue)
        assert pushed, "re-planned zombie stage pushed no start messages; workflow stays wedged"
