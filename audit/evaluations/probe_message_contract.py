"""A persisted message must match its declared field types, or be refused here.

`create_message_from_dict` was a bare `message_class(**data)` splat over a plain
dataclass. Python checks field NAMES; nothing checked a field TYPE. So a queue
row carrying `attempts="three"` constructed without complaint and raised
`TypeError: can only concatenate str` far away in the retry path, reaching the
DLQ attributed to whatever code touched it rather than to the boundary that
admitted it.

The queue is a cross-process AND cross-version boundary — 0.27.0 shipped #28
precisely because an event written by a newer build can reach an older reader.

Both directions, because a boundary that refuses everything is as broken as one
that refuses nothing:

  A  a well-formed message is ACCEPTED and keeps its values
  B  wrong field types are REFUSED, naming the field
  C  the refusal happens at the boundary, not later in a handler
  D  caller-owned payload (context, outputs) is NOT schema-constrained

    python audit/evaluations/probe_message_contract.py
"""

from __future__ import annotations

import sys

from stabilize.queue import messages as messages_module
from stabilize.queue.messages import MESSAGE_TYPES, create_message_from_dict

ContractError = getattr(messages_module, "MessageContractError", None)


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    print("=== A. A WELL-FORMED MESSAGE IS ACCEPTED AND KEEPS ITS VALUES ===")
    print("    without this, a boundary that refused everything would pass B and C")
    good = create_message_from_dict(
        "StartTask",
        {"execution_id": "exec-1", "stage_id": "stage-1", "task_id": "task-1"},
    )
    kept = (
        good.execution_id == "exec-1"
        and good.stage_id == "stage-1"
        and good.task_id == "task-1"
    )
    print(f"    constructed {type(good).__name__}; values preserved: {kept}")
    results.append(("well-formed message accepted intact", kept, f"execution_id={good.execution_id!r}"))

    print()
    print("=== B. WRONG FIELD TYPES ARE REFUSED, WITH THE FIELD NAMED ===")
    bad = {
        "execution_id": 12345,
        "stage_id": ["not", "a", "string"],
        "attempts": "three",
    }
    try:
        built = create_message_from_dict("StartTask", bad)
        print(f"    ACCEPTED: attempts={built.attempts!r} ({type(built.attempts).__name__})")
        print("    -> the bad value will detonate later, inside a handler")
        results.append(("wrong types refused", False, f"accepted attempts={built.attempts!r}"))
        results.append(("refusal names the field", False, "no refusal"))
    except Exception as exc:
        correct_type = ContractError is not None and isinstance(exc, ContractError)
        names_field = "attempts" in str(exc)
        print(f"    RAISED {type(exc).__name__}")
        print(f"    names the offending field: {names_field}")
        results.append(("wrong types refused", correct_type, type(exc).__name__))
        results.append(("refusal names the field", names_field, str(exc)[:110]))

    print()
    print("=== C. THE REFUSAL IS AT THE BOUNDARY, NOT IN THE RETRY PATH ===")
    print("    the old failure was `attempts + 1` raising TypeError far downstream")
    try:
        built = create_message_from_dict("StartTask", {"execution_id": "e", "attempts": "three"})
        try:
            _ = built.attempts + 1
            late = False
        except TypeError:
            late = True
        print(f"    constructed, and arithmetic on attempts fails later: {late}")
        results.append(("no late detonation in the retry path", not late, "constructed with a str attempts"))
    except Exception as exc:
        print(f"    refused at construction: {type(exc).__name__}")
        results.append(("no late detonation in the retry path", True, type(exc).__name__))

    print()
    print("=== D. CALLER-OWNED PAYLOAD IS NOT SCHEMA-CONSTRAINED ===")
    print("    context and outputs hold arbitrary caller JSON, by design")
    payload = {"anything": [1, {"nested": True}], "_odd": None}
    accepted_payload = True
    detail = ""
    for type_name in ("StartStage", "CompleteStage"):
        if type_name not in MESSAGE_TYPES:
            continue
        try:
            create_message_from_dict(
                type_name, {"execution_id": "e", "stage_id": "s", "context": payload}
            )
        except TypeError:
            pass  # that message type has no context field; not a contract failure
        except Exception as exc:
            if ContractError is not None and isinstance(exc, ContractError):
                accepted_payload = False
                detail = f"{type_name}: {exc}"
    print(f"    arbitrary payload accepted: {accepted_payload}")
    results.append(("caller payload is not constrained", accepted_payload, detail or "arbitrary values accepted"))

    print()
    failures = 0
    for name, ok, info in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {info}")
        if not ok:
            failures += 1

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — the message boundary enforces its contract ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
