# 41 — No contract validation at the deserialization boundary: a wrong-typed queue message constructs silently and detonates in a handler

Ticket: issuedb #41 · status in-progress · priority high

EARS SPEC:
- When a message is reconstructed from its persisted form, the engine shall validate every field against its declared type and shall reject a message whose fields do not conform.
- If a persisted message fails validation, then the engine shall route it to the dead-letter queue naming the field, the expected type and the received value, rather than constructing an object that fails later in a handler.
- When a stage or task row is loaded from the store, the engine shall validate the reconstructed model against its declared types.
- The engine shall validate at the boundary where untrusted or cross-version data enters, and shall not re-validate on every internal call.
- Where a field is a container (context, outputs), the engine shall validate the container's type and shall state explicitly that its contents are caller-owned and unvalidated.
- The validation layer shall add no more than 5% to the wall time of a 100-stage workflow on SQLite, measured before and after.

WHY THIS TICKET EXISTS. Issue 36's bidirectional manifest test found pydantic declared in [project].dependencies and imported nowhere. I read that as a phantom dependency and removed it. That was the wrong reading: the declaration is INTENT THAT WAS NEVER IMPLEMENTED, and the gap it points at is real. The removal is reverted; pydantic stays declared and carries an explicit DECLARED_PENDING_USE exemption in the manifest test naming this ticket.

MEASURED — THERE IS NO TYPE VALIDATION ANYWHERE IN THE ENGINE.

queue/messages.py:478-482, the entire deserialization contract:
    if type_name not in MESSAGE_TYPES:
        raise ValueError(f"Unknown message type: {type_name}")
    message_class = MESSAGE_TYPES[type_name]
    return message_class(**data)

A plain dataclass splat. Field NAMES are checked by Python; no field TYPE is. Demonstrated against the installed engine:

    create_message_from_dict('StartTask', {
        'execution_id': 12345, 'stage_id': ['not','a','string'], 'task_id': None,
        'attempts': 'three', 'created_at': 'not-a-datetime', 'retry_count': {'oops': True},
    })
    CONSTRUCTED WITHOUT COMPLAINT: StartTask
      execution_id  = 12345                     type=int
      stage_id      = ['not', 'a', 'string']    type=list
      attempts      = 'three'                   type=str
      created_at    = 'not-a-datetime'          type=str
      retry_count   = {'oops': True}            type=dict

    and then, in the retry path:
      attempts + 1 RAISES TypeError: can only concatenate str (not "int") to str

That last line is the shape of the defect. The bad value is accepted at the boundary and detonates later, inside a handler, attributed to whatever code touched it rather than to the deserialization that let it in. It surfaces as a poison message in the DLQ with a misleading stack.

The model layer has exactly three __post_init__ hooks across every model:
    models/workflow.py:147        wiring only (sets stage.execution)
    models/stage/stage.py:120     one business rule (join_threshold >= 0) plus
                                  _output_reducers round-tripping
    (no third type check anywhere)
Structural DAG validation DOES exist at submit time (duplicate_ref, self_edge, unknown_ref, cycles, added in 0.20.0) — that is graph shape, not field contracts. The two are complements, not substitutes.

WHY THE BOUNDARY MATTERS HERE SPECIFICALLY. The queue row was written by another process, and on a rolling deploy or a mixed-version fleet, by another VERSION. 0.27.0 already ships #28 precisely because an event written by a newer build could reach an older reader. The same exposure exists on the message queue and has no guard at all. JSONB context and outputs come back from PostgreSQL with whatever types the writer put in.

TECHNICAL PROBLEMS:
1. Structured data crosses a process and version boundary with no contract enforcement, so a violation is detected at use rather than at entry.
2. Errors are attributed to the wrong layer, which makes the DLQ misleading exactly when it is being read under pressure.
3. Container fields (context, outputs) are legitimately caller-owned and must NOT be schema-constrained; the boundary between engine-owned fields and caller-owned payload has never been stated.

SOLUTION DOMAINS:
- Boundary validation -> pydantic v2, already declared and already the house stack preference. TypeAdapter validates an existing dataclass without rewriting the model layer, which matters because these dataclasses carry behaviour (StageNavigationMixin, phase_version, cleanup) that a BaseModel rewrite would disturb.
- Parse, don't validate -> the boundary produces a typed object or an error; nothing downstream re-checks.
- Precedent in this repo: #28's tolerant event read path, which resolves an unknown EventType to UNKNOWN and preserves the raw string rather than raising through the whole query. A message-level failure should be per-message, not per-batch, for the same reason.

ALTERNATIVES:
- pydantic TypeAdapter over the EXISTING dataclasses at the boundary [CHOSEN: validates without rewriting models that carry behaviour; pydantic is already declared; v2 core is Rust and fast enough for the 5% budget] vs converting models to BaseModel [REJECTED: the models carry mixins, properties and mutation used throughout the handlers; a rewrite is a much larger blast radius for the same guarantee] vs hand-written __post_init__ type checks on 23 message classes [REJECTED: that is the hand-maintenance defect issue 36 exists to remove, and it will drift the first time a field is added] vs dataclasses + a typeguard-style decorator [REJECTED: another dependency for something a declared one already does].
- Failure handling: reject to DLQ naming field/expected/received [CHOSEN: a malformed message is not retryable, so retrying it burns the attempt budget to reach the same DLQ later] vs raise and let the existing retry path carry it [REJECTED: wastes max_attempts on a deterministic failure].
- Scope: message boundary FIRST, store rows second [CHOSEN: the queue is the cross-process/cross-version boundary and the one with a measured failure; store rows are written by the same version that reads them far more often].

EXPLICITLY OUT OF SCOPE: validating the CONTENTS of context and outputs. Those are caller-owned by design — PythonTask exposes context as INPUT and tasks write arbitrary JSON-serialisable outputs. The ticket validates that context IS a dict, not what is in it. Stating this is part of the work, because #37 is separately establishing the boundary between engine-written and caller-written keys in that same container.

ACCEPTANCE: the reproduction above must be shown to FAIL (message rejected, field named) after the fix and to PASS (constructed silently) before it, and the 5% wall-time budget measured on a 100-stage workflow in both configurations rather than asserted.
