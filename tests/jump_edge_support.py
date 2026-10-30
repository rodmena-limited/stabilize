from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.tasks.interface import Task


class JumpToSelfTask(Task):
    """Task that jumps to itself for retry, up to max_retries times."""

    retry_count: int = 0
    max_retries: int = 2

    def execute(self, stage: StageExecution) -> TaskResult:
        JumpToSelfTask.retry_count += 1
        if JumpToSelfTask.retry_count <= JumpToSelfTask.max_retries:
            return TaskResult.jump_to(stage.ref_id)  # Jump to self
        return TaskResult.success(outputs={"final_retry_count": JumpToSelfTask.retry_count})


class MaxJumpsTask(Task):
    """Task that always jumps to test max jump limit."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.jump_to("stage_a")  # Always jump back to A


class JumpWithContextTask(Task):
    """Task that jumps with context data."""

    target: str = "stage_b"
    context_data: dict = {}
    output_data: dict = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.jump_to(
            JumpWithContextTask.target,
            context=JumpWithContextTask.context_data,
            outputs=JumpWithContextTask.output_data,
        )


class RecordContextTask(Task):
    """Task that records its stage context."""

    recorded_context: list = []

    def execute(self, stage: StageExecution) -> TaskResult:
        RecordContextTask.recorded_context.append(dict(stage.context))
        return TaskResult.success()


class FailThenSucceedTask(Task):
    """Task that fails first time, succeeds on retry."""

    attempt_count: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        FailThenSucceedTask.attempt_count += 1
        if FailThenSucceedTask.attempt_count == 1:
            return TaskResult.terminal("Intentional first failure")
        return TaskResult.success()


def reset_tasks() -> None:
    """Reset all task class state."""
    JumpToSelfTask.retry_count = 0
    JumpToSelfTask.max_retries = 2
    MaxJumpsTask.jump_count = 0
    JumpWithContextTask.target = "stage_b"
    JumpWithContextTask.context_data = {}
    JumpWithContextTask.output_data = {}
    RecordContextTask.recorded_context = []
    FailThenSucceedTask.attempt_count = 0
