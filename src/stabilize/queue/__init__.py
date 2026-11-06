"""Message queue system for pipeline execution."""

from __future__ import annotations

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
    "CancelStage",
    "CancelWorkflow",
    "CompleteStage",
    "CompleteTask",
    "CompleteWorkflow",
    "ContinueParentStage",
    "InvalidStageId",
    "InvalidTaskId",
    "InvalidTaskType",
    "InvalidWorkflowId",
    # Message types
    "Message",
    "PauseTask",
    "PostgresQueue",
    # Queue implementations
    "Queue",
    "QueueFullError",
    "QueuedMessage",
    "RestartStage",
    "ResumeStage",
    "RunTask",
    "SkipStage",
    "SqliteQueue",
    "StageLevel",
    "StartStage",
    "StartTask",
    "StartWorkflow",
    "TaskLevel",
    "WorkflowLevel",
]
