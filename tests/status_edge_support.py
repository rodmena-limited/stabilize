from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.tasks.interface import Task


class SuccessTask(Task):
    """Task that always succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()
