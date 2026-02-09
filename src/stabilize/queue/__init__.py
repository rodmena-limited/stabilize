"""Message queue system for pipeline execution."""

from stabilize.queue.interface import Queue, QueuedMessage, QueueFullError
from stabilize.queue.messages import (
    CancelStage,
    CancelWorkflow,
    CompleteStage,
    CompleteTask,
    CompleteWorkflow,
    ContinueParentStage,
    InvalidStageId,
    InvalidTaskId,
    InvalidTaskType,
    InvalidWorkflowId,
    Message,
    PauseTask,
    RestartStage,
    ResumeStage,
    RunTask,
    SkipStage,
    StageLevel,
    StartStage,
    StartTask,
    StartWorkflow,
    TaskLevel,
    WorkflowLevel,
)
from stabilize.queue.postgres import PostgresQueue
from stabilize.queue.sqlite import SqliteQueue

__all__ = [
    # Message types
    "Message",
    "WorkflowLevel",
    "StageLevel",
    "TaskLevel",
    "StartWorkflow",
    "StartStage",
    "StartTask",
    "RunTask",
    "CompleteTask",
    "CompleteStage",
    "CompleteWorkflow",
    "CancelWorkflow",
    "CancelStage",
    "SkipStage",
    "PauseTask",
    "ResumeStage",
    "ContinueParentStage",
    "RestartStage",
    "InvalidWorkflowId",
    "InvalidStageId",
    "InvalidTaskId",
    "InvalidTaskType",
    # Queue implementations
    "Queue",
    "QueuedMessage",
    "QueueFullError",
    "PostgresQueue",
    "SqliteQueue",
]
