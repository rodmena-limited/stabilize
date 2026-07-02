"""
Human-in-the-loop (HITL) helpers (agentic ergonomics).

LangGraph's ``interrupt`` pauses a graph and returns the human's value inline
on resume. Stabilize already has the durable machinery — ``TaskResult.suspend``
sets a stage SUSPENDED and ``SignalStage(persistent=True)`` buffers a signal
sent before the stage suspends, all surviving a crash via the atomic queue.
This module wraps that into a friendly, one-call API plus a builtin task:

- :class:`ApprovalTask` — a ready-made task that suspends until an
  ``approve``/``reject`` signal arrives, then routes accordingly.
- :func:`approve` / :func:`reject` / :func:`send_signal` — send the resume
  signal (with a payload) to a suspended stage via an orchestrator/queue.
- :func:`get_signal` — read the delivered signal name/data inside a task.

All additive: existing suspend/signal users are unaffected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.queue.messages import SignalStage
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.queue import Queue

# Canonical signal names for approvals.
APPROVE_SIGNAL = "approve"
REJECT_SIGNAL = "reject"


class ApprovalTask(Task):
    """A task that waits for a human approval signal, then routes on it.

    Behavior:
    - First execution (no signal yet): returns ``TaskResult.suspend()``; the
      stage becomes SUSPENDED and waits.
    - Resumed by an ``approve`` signal: succeeds, exposing the approver's
      payload in outputs under ``approval``.
    - Resumed by a ``reject`` signal: fails. By default this is terminal;
      set ``context['approval_reject_continues'] = True`` to continue the
      pipeline (FAILED_CONTINUE) instead.

    Register it and use it like any task::

        registry.register("approval", ApprovalTask)
        StageExecution(ref_id="gate", type="approval", tasks=[...])

    Send the decision with :func:`approve` / :func:`reject`.
    """

    aliases = ["human_approval", "hitl_approval"]

    def execute(self, stage: StageExecution) -> TaskResult:
        signal_name = stage.context.get("_signal_name")
        signal_data = stage.context.get("_signal_data", {}) or {}

        if signal_name == APPROVE_SIGNAL:
            return TaskResult.success(outputs={"approved": True, "approval": signal_data})
        if signal_name == REJECT_SIGNAL:
            if stage.context.get("approval_reject_continues", False):
                return TaskResult.failed_continue(
                    error="Approval rejected",
                    outputs={"approved": False, "approval": signal_data},
                )
            return TaskResult.terminal(
                error="Approval rejected",
                context={"approved": False, "approval": signal_data},
            )

        # No decision yet — suspend and wait for the signal.
        return TaskResult.suspend()


def send_signal(
    queue: Queue,
    execution_id: str,
    stage_id: str,
    signal_name: str,
    signal_data: dict[str, Any] | None = None,
    *,
    execution_type: str = "PIPELINE",
    persistent: bool = True,
) -> None:
    """Send a resume signal to a (possibly not-yet-)suspended stage.

    Args:
        queue: The workflow queue.
        execution_id: Workflow id.
        stage_id: The suspended stage's id.
        signal_name: Signal name the task checks for (e.g. "approve").
        signal_data: Payload delivered to the task as ``_signal_data``.
        execution_type: Workflow type value (default "PIPELINE").
        persistent: Buffer the signal if the stage has not suspended yet
            (WCP-24). True by default so an early approval is not lost.
    """
    queue.push(
        SignalStage(
            execution_type=execution_type,
            execution_id=execution_id,
            stage_id=stage_id,
            signal_name=signal_name,
            signal_data=signal_data or {},
            persistent=persistent,
        )
    )


def approve(
    queue: Queue,
    execution_id: str,
    stage_id: str,
    data: dict[str, Any] | None = None,
    *,
    execution_type: str = "PIPELINE",
) -> None:
    """Approve a stage waiting on an :class:`ApprovalTask`."""
    send_signal(
        queue,
        execution_id,
        stage_id,
        APPROVE_SIGNAL,
        data,
        execution_type=execution_type,
    )


def reject(
    queue: Queue,
    execution_id: str,
    stage_id: str,
    data: dict[str, Any] | None = None,
    *,
    execution_type: str = "PIPELINE",
) -> None:
    """Reject a stage waiting on an :class:`ApprovalTask`."""
    send_signal(
        queue,
        execution_id,
        stage_id,
        REJECT_SIGNAL,
        data,
        execution_type=execution_type,
    )


def get_signal(stage: StageExecution) -> tuple[str | None, dict[str, Any]]:
    """Return ``(signal_name, signal_data)`` delivered to a resumed stage.

    Use inside a task's ``execute`` to branch on which signal woke it.
    Returns ``(None, {})`` when the stage was not resumed by a signal.
    """
    return stage.context.get("_signal_name"), stage.context.get("_signal_data", {}) or {}
