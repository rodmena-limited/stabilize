"""A stage that does nothing must say so, and must not be refused when it is legitimate.

`StageDefinitionBuilderFactory.get` falls back to `NoOpStageBuilder` for any
type it does not know (`stages/builder.py`). A stage whose caller relied on a
builder that was never registered therefore gets zero tasks and completes as
SUCCEEDED -- the workflow reports success having run nothing.

REFUSING unregistered types is NOT available as a fix. `stage.type` is a
free-form label: this repo alone uses "test" (118x), "python" (68x), "stage"
(42x) and "shell" (37x), none of them registered. A multi-instance PARENT is
also legitimately taskless -- its instances depend on it completing. Both are
structurally identical to the defect.

So the cure for a SILENT failure is to make it LOUD, following
STABILIZE_MERGE_STRICT's shape: warn by default, raise under
STABILIZE_STRICT_STAGE_TYPES.

  A  default: the empty unregistered stage WARNS, and still completes
  B  strict:  the same stage is REFUSED
  C  CONTROL: a registered builder produces tasks and does NOT warn
  D  CONTROL: explicit tasks + unregistered type does NOT warn
  E  CONTROL: a multi-instance parent (mi_config) does NOT warn

C-E are the false-positive controls. Without them a guard that warned on
everything would pass A and B.

    python audit/evaluations/probe_unknown_stage_type.py
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
)
from stabilize.models.multi_instance import MultiInstanceConfig  # noqa: E402
from stabilize.stages.builder import (  # noqa: E402
    STRICT_STAGE_TYPE_ENV,
    StageDefinitionBuilder,
    UnregisteredStageTypeError,
    get_default_factory,
    register_builder,
    report_empty_unregistered_stage,
)
from stabilize.tasks.shell import ShellTask  # noqa: E402

BUILDER_LOGGER = "stabilize.stages.builder"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class RegisteredBuilder(StageDefinitionBuilder):
    @property
    def type(self) -> str:
        return "probe_registered"

    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [TaskExecution.create("Built", "shell", stage_start=True, stage_end=True)]


def _setup():
    d = Path(tempfile.mkdtemp(prefix="probe-stage-type-"))
    dsn = f"sqlite:///{d}/p.db"
    store = SqliteWorkflowStore(dsn, create_tables=True)
    q = SqliteQueue(dsn, table_name="queue_messages")
    q._create_table()
    reg = TaskRegistry()
    reg.register("shell", ShellTask)
    return store, q, QueueProcessor(q, store=store, task_registry=reg)


def _run(stage: StageExecution, name: str) -> tuple[WorkflowStatus, StageExecution, list[str]]:
    """Run one stage, returning its outcome and any builder warnings it emitted."""
    cap = _Capture()
    lg = logging.getLogger(BUILDER_LOGGER)
    prev_level, prev_prop = lg.level, lg.propagate
    lg.setLevel(logging.WARNING)
    lg.propagate = False
    lg.addHandler(cap)
    try:
        store, q, proc = _setup()
        wf = Workflow.create(application="probe", name=name, stages=[stage])
        store.store(wf)
        Orchestrator(q).start(wf)
        proc.process_all(timeout=60.0)
        out = store.retrieve(wf.id)
        proc.stop(wait=True)
        store.close()
        return out.status, out.stages[0], list(cap.messages)
    finally:
        lg.removeHandler(cap)
        lg.setLevel(prev_level)
        lg.propagate = prev_prop


def main() -> int:
    results: list[tuple[str, bool, str]] = []
    register_builder(RegisteredBuilder())

    print("=== CONTROL C: a REGISTERED builder produces tasks and does NOT warn ===")
    print("    without this, 'warned' below could be a guard that fires on everything")
    st = StageExecution(ref_id="r", type="probe_registered", name="Registered",
                        context={"command": "true"})
    status_c, stage_c, warn_c = _run(st, "registered")
    print(f"    status={status_c}  tasks={len(stage_c.tasks)}  warnings={len(warn_c)}")
    results.append(("a registered builder yields tasks", len(stage_c.tasks) > 0, f"{len(stage_c.tasks)} tasks"))
    results.append(("a registered builder does NOT warn", not warn_c, f"{len(warn_c)} warning(s)"))

    print()
    print("=== A. DEFAULT: unregistered + no tasks WARNS, and still completes ===")
    st = StageExecution(ref_id="u", type="probe_NOT_registered", name="Unknown",
                        context={"command": "true"})
    status_a, stage_a, warn_a = _run(st, "unknown")
    print(f"    status={status_a}  tasks={len(stage_a.tasks)}  warnings={len(warn_a)}")
    if warn_a:
        print(f"    -> {warn_a[0][:150]}")
    results.append((
        "an empty unregistered stage is no longer SILENT",
        len(warn_a) == 1,
        f"{len(warn_a)} warning(s)",
    ))
    results.append((
        "and it is NOT broken by default (still completes)",
        status_a == WorkflowStatus.SUCCEEDED,
        str(status_a),
    ))
    results.append((
        "the warning names the remedy",
        bool(warn_a) and "register_builder" in warn_a[0],
        "mentions register_builder" if warn_a and "register_builder" in warn_a[0] else "does not",
    ))

    print()
    print("=== CONTROL D: explicit tasks + unregistered type does NOT warn ===")
    st = StageExecution(ref_id="e", type="probe_NOT_registered", name="Explicit",
                        context={"command": "true"},
                        tasks=[TaskExecution.create("Mine", "shell", stage_start=True, stage_end=True)])
    status_d, _, warn_d = _run(st, "explicit")
    print(f"    status={status_d}  warnings={len(warn_d)}")
    results.append(("explicit tasks are never warned about", not warn_d, f"{len(warn_d)} warning(s)"))

    print()
    print("=== CONTROL E: a multi-instance PARENT (mi_config) does NOT warn ===")
    print("    a taskless coordinator is legitimate; its instances depend on it")
    parent = StageExecution(ref_id="p", type="step", name="MI parent")
    parent.mi_config = MultiInstanceConfig(count=2)
    _, _, warn_e = _run(parent, "mi-parent")
    print(f"    warnings={len(warn_e)}")
    results.append(("a declared coordinator is not warned about", not warn_e, f"{len(warn_e)} warning(s)"))

    print()
    print("=== B. STRICT MODE: the same stage is REFUSED ===")
    os.environ[STRICT_STAGE_TYPE_ENV] = "1"
    try:
        raised = None
        try:
            report_empty_unregistered_stage(
                StageExecution(ref_id="s", type="probe_NOT_registered", name="Strict")
            )
        except UnregisteredStageTypeError as exc:
            raised = exc
        print(f"    strict raise: {type(raised).__name__ if raised else 'NOTHING RAISED'}")

        st = StageExecution(ref_id="sw", type="probe_NOT_registered", name="StrictWf",
                            context={"command": "true"})
        status_b, _, _ = _run(st, "strict-wf")
        print(f"    workflow under strict mode: {status_b}")
    finally:
        os.environ.pop(STRICT_STAGE_TYPE_ENV, None)

    results.append(("strict mode raises", isinstance(raised, UnregisteredStageTypeError), repr(raised)[:80]))
    results.append((
        "strict mode stops the workflow rather than succeeding silently",
        status_b != WorkflowStatus.SUCCEEDED,
        str(status_b),
    ))

    print()
    print("=== CONTROL: strict mode is OFF again (no leakage into later probes) ===")
    off = STRICT_STAGE_TYPE_ENV not in os.environ
    print(f"    {STRICT_STAGE_TYPE_ENV} unset: {off}")
    results.append(("strict flag does not leak", off, str(off)))
    print(f"    'noop' still registered: {get_default_factory().has('noop')}")

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    print("NOT COVERED, stated rather than implied: whether an unregistered type")
    print("also drops before/after/on-failure stages. A first attempt at that case")
    print("had its own CONTROL fail, so its negative would have proved nothing.")

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — an empty unregistered stage is loud, not silent ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
