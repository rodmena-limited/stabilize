"""Tasks implementing structured loops (WCP-21).

``LoopBuilder`` emits stages referencing these two tasks by name. Both drive the
existing ``jump_to`` mechanism rather than introducing a second control-flow
path, so the jump handler is unchanged.

Iteration is bounded by ``_loop_max_iterations`` rather than the engine's
generic jump budget: both tasks reset ``_jump_count`` on every jump they issue,
because exhausting the jump budget marks the stage TERMINAL and fails the
workflow, which is not how a loop should end.

Loop state travels in two directions. The condition (or, for repeat-until, the
entry) publishes the loop's variables as outputs so the body hydrates from them;
the loop-back carries them in its jump context, because the condition sits
upstream of the body and cannot otherwise observe what the body produced.

Nested loops are NOT yet supported. Two loops whose bodies share a variable name
give the inner loop-back two ancestors offering that name -- the inner body and
the outer condition -- and which one wins is decided by the ancestor merge,
whose order is not deterministic (issuedb #26). Until that is fixed, a nested
inner loop can observe the outer loop's stale value instead of its own.
"""

from __future__ import annotations

import ast
from typing import Any
from uuid import uuid4

from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.models.stage import StageExecution
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

_LITERALS = {"True", "False", "None", "true", "false", "null", "none"}

LOOP_CONDITION = "_loop_condition"
LOOP_MAX_ITERATIONS = "_loop_max_iterations"
LOOP_ITERATION = "_loop_iteration"
LOOP_TYPE = "_loop_type"
LOOP_TARGET = "_loop_target_ref_id"
LOOP_EXIT_REF = "_loop_exit_ref_id"
LOOP_EXIT = "_loop_exit"
LOOP_TOKEN = "_loop_token"
LOOP_EXHAUSTED = "_loop_exhausted"
JUMP_COUNT = "_jump_count"

DEFAULT_MAX_ITERATIONS = 100


def _identifiers(expression: str) -> set[str]:
    """Names an expression reads, excluding literals."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError:
        return set()
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} - _LITERALS


def _evaluation_context(stage: StageExecution) -> dict[str, Any]:
    """Stage context plus the aliases the documented conditions use."""
    context = dict(stage.context)
    iteration = context.get(LOOP_ITERATION, 0)
    maximum = context.get(LOOP_MAX_ITERATIONS, DEFAULT_MAX_ITERATIONS)
    context.setdefault("iteration_count", iteration)
    context.setdefault("loop_iteration", iteration)
    context.setdefault("iteration", iteration)
    context.setdefault("max_iterations", maximum)
    return context


def _exit_ref(stage: StageExecution) -> str:
    explicit = stage.context.get(LOOP_EXIT_REF)
    if explicit:
        return str(explicit)
    return stage.ref_id.removesuffix("_condition") + "_loopback"


def _loop_variables(stage: StageExecution) -> dict[str, Any]:
    """User-visible state to thread back to the condition on a loop-back.

    The condition stage sits upstream of the body, so the body's outputs can
    never reach it through the normal ancestor merge. The jump has to carry
    them. Internal bookkeeping (leading underscore) is excluded.
    """
    return {
        key: value
        for key, value in stage.context.items()
        if not key.startswith("_")
    }


class LoopConditionTask(Task):
    """Evaluates a loop's condition and either enters the body or exits.

    ``while`` loops check before the body; ``repeat_until`` loops check after it.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        context = stage.context
        expression = str(context.get(LOOP_CONDITION, "") or "")
        loop_type = str(context.get(LOOP_TYPE, "while"))
        maximum = int(context.get(LOOP_MAX_ITERATIONS, DEFAULT_MAX_ITERATIONS) or 0)

        # A counter only carries meaning when this entry came from the loop's own
        # jump. Re-entry from outside (an enclosing loop resetting these stages)
        # starts a fresh budget.
        token = context.get(LOOP_TOKEN)
        iteration = int(context.get(LOOP_ITERATION, 0) or 0) if token else 0

        if not expression:
            return TaskResult.terminal(
                error=f"loop '{stage.ref_id}': no {LOOP_CONDITION} in stage context"
            )

        evaluation_context = _evaluation_context(stage)
        missing = _identifiers(expression) - set(evaluation_context)
        if missing:
            return TaskResult.terminal(
                error=(
                    f"loop '{stage.ref_id}': undefined identifier(s) "
                    f"{sorted(missing)} in condition {expression!r}"
                )
            )

        try:
            keep_going = bool(evaluate_expression(expression, evaluation_context))
        except ExpressionError as exc:
            return TaskResult.terminal(
                error=f"loop '{stage.ref_id}': condition {expression!r} failed: {exc}"
            )

        exhausted = iteration >= maximum > 0

        if loop_type == "repeat_until":
            return self._repeat_until(stage, keep_going, iteration, maximum, exhausted)
        return self._while(stage, keep_going, iteration, maximum, exhausted)

    def _while(
        self,
        stage: StageExecution,
        keep_going: bool,
        iteration: int,
        maximum: int,
        exhausted: bool,
    ) -> TaskResult:
        if exhausted or not keep_going:
            return TaskResult.jump_to(
                _exit_ref(stage),
                context={
                    LOOP_EXIT: True,
                    LOOP_EXHAUSTED: exhausted,
                    LOOP_ITERATION: iteration,
                    LOOP_TOKEN: None,
                    JUMP_COUNT: 0,
                },
                outputs={"loop_iterations": iteration},
            )

        # Publish the loop variables as outputs so the body hydrates from them.
        # The body's own persisted copy is stale from the previous iteration.
        return TaskResult.success(
            context={LOOP_ITERATION: iteration, LOOP_TOKEN: None},
            outputs={**_loop_variables(stage), "loop_iteration": iteration},
        )

    def _repeat_until(
        self,
        stage: StageExecution,
        satisfied: bool,
        iteration: int,
        maximum: int,
        exhausted: bool,
    ) -> TaskResult:
        if satisfied:
            return TaskResult.success(
                context={LOOP_TOKEN: None},
                outputs={"loop_iterations": iteration + 1, "loop_exhausted": False},
            )

        if exhausted:
            return TaskResult.failed_continue(
                error=f"loop '{stage.ref_id}' exceeded max_iterations={maximum}",
                outputs={"loop_iterations": iteration + 1, "loop_exhausted": True},
            )

        return TaskResult.jump_to(
            str(stage.context.get(LOOP_TARGET, "")),
            context={
                **_loop_variables(stage),
                LOOP_ITERATION: iteration + 1,
                LOOP_TOKEN: uuid4().hex,
                JUMP_COUNT: 0,
            },
        )


class LoopEntryTask(Task):
    """Publishes a repeat-until loop's variables at the top of each iteration.

    The entry stage is the jump-back target, so the condition's jump lands its
    carried state here. Without publishing that state as outputs, the body
    hydrates from its own stale copy of the previous iteration.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        iteration = int(stage.context.get(LOOP_ITERATION, 0) or 0)
        return TaskResult.success(
            outputs={**_loop_variables(stage), "loop_iteration": iteration},
        )

class LoopBackTask(Task):


    """Closes a ``while`` loop: jumps back to the condition, or exits cleanly."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = stage.context
        iteration = int(context.get(LOOP_ITERATION, 0) or 0)

        if context.get(LOOP_EXIT):
            exhausted = bool(context.get(LOOP_EXHAUSTED))
            outputs = {"loop_iterations": iteration, "loop_exhausted": exhausted}
            maximum = context.get(LOOP_MAX_ITERATIONS, DEFAULT_MAX_ITERATIONS)
            if exhausted:
                # The loop gave up rather than converging. Continue past it so
                # downstream compensation can run, but record the failure.
                return TaskResult.failed_continue(
                    error=f"loop '{stage.ref_id}' exceeded max_iterations={maximum}",
                    outputs=outputs,
                )
            return TaskResult.success(context={LOOP_EXIT: False}, outputs=outputs)

        return TaskResult.jump_to(
            str(context.get(LOOP_TARGET, "")),
            context={
                **_loop_variables(stage),
                LOOP_ITERATION: iteration + 1,
                LOOP_TOKEN: uuid4().hex,
                JUMP_COUNT: 0,
            },
        )
