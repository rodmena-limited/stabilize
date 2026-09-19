"""Audit probe: does a credential embedded in an HTTPTask URL survive into
persisted workflow state and into downstream stage context?

Drives a real workflow through the engine and reads the result back through
WorkflowStore.retrieve() -- the product's own interface, not a SQL query.

Run:  python audit/evaluations/probe_http_credential_persistence.py
"""

from __future__ import annotations

import json
import logging
import sys

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (
    HTTPTask,
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)

SECRET = "HTTPSECRETPW999"
CREDENTIAL_URL = f"http://probeuser:{SECRET}@127.0.0.1:1/resource"


def run_workflow() -> object:
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue)

    workflow = Workflow.create(
        application="audit-probe",
        name="credential in url",
        stages=[
            StageExecution(
                ref_id="1",
                type="http",
                name="Call with embedded credential",
                context={
                    "url": CREDENTIAL_URL,
                    "method": "GET",
                    "allow_private_urls": True,
                    "timeout": 2,
                    "continue_on_failure": True,
                },
                tasks=[
                    TaskExecution.create(
                        name="HTTP GET",
                        implementing_class="http",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)
    return store.retrieve(workflow.id)


def main() -> int:
    print("=" * 72)
    print("CONTROL: the assertion can see the secret when it is present")
    control = f"a string holding {SECRET}"
    if SECRET not in control:
        print("  >>> CONTROL FAILED: assertion is blind; results are vacuous")
        return 1
    print("  >>> control green")
    print()

    result = run_workflow()
    stage = result.stages[0]

    serialised = json.dumps(
        {
            "stage_context": stage.context,
            "stage_outputs": stage.outputs,
            "task_contexts": [getattr(t, "context", None) for t in stage.tasks],
        },
        default=str,
    )

    print("=" * 72)
    print("PERSISTED WORKFLOW STATE, read back via WorkflowStore.retrieve()")
    print(f"  stage.outputs keys : {sorted(stage.outputs)}")
    print(f"  outputs['url']     : {stage.outputs.get('url')!r}")
    print(f"  context['url']     : {stage.context.get('url')!r}")
    print(f"  context['error']   : {str(stage.context.get('error'))[:120]!r}")
    print()

    outputs_blob = json.dumps(stage.outputs, default=str)
    context_blob = json.dumps(stage.context, default=str)

    amplified = SECRET in outputs_blob
    print("CLAIM: the engine must not AMPLIFY an author-supplied credential into")
    print("       stage OUTPUTS, which propagate to downstream stages and the monitor.")
    if amplified:
        print(f"  >>> FAIL: credential {SECRET!r} present in stage.outputs")
    else:
        print("  >>> PASS: stage.outputs carries the redacted URL only")

    print()
    print("RESIDUAL, reported rather than asserted: stage.context still holds the")
    print("author's own URL verbatim, because it IS the stage definition the engine")
    print("must persist in order to execute or replay the stage. Redacting it would")
    print("break re-execution. Credentials belong in headers or `secrets`, not a URL.")
    print(f"  context['url'] contains the secret: {SECRET in context_blob}")

    print()
    print("=" * 72)
    print(f"RESULT: {'FAIL - credential amplified into outputs' if amplified else 'PASS'}")
    return 1 if amplified else 0


if __name__ == "__main__":
    sys.exit(main())
