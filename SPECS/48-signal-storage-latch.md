# 48 — supports_signal_storage() latches False permanently

Ticket: #48 (high, bug). Shipped defective in 0.28.2 by this repo.

## EARS spec

- While signal storage has been found unusable, the PostgreSQL store shall
  re-check reachability after a cooldown rather than refuse permanently.
- When a previously-unusable `workflow_signals` table becomes reachable again,
  the store shall resume using it.
- If signal storage is unusable, then the store shall degrade to stage-context
  buffering and say so, without failing the workflow.

## Technical problems

1. A degradation decision cached with no expiry — a latch presented as a check.
2. Distinguishing a transient fault from a permanent one, without a re-probe
   storm on a hot path (`pending_signal_count` runs on every stage completion).

## Solution domains

Circuit-breaker half-open semantics: a negative answer carries an expiry, after
which one probe decides. Extracted, not invented — `resilient-circuit` is the
estate's implementation of this shape.

## Alternatives

- **Timestamped cooldown then re-probe — CHOSEN.** Smallest change that restores
  the release direction. A fixed module constant (30s) rather than a config knob,
  so there is one behaviour to reason about until evidence says otherwise.
- Never cache, probe every call — REJECTED: a round trip per stage completion.
- A `resilient-circuit` breaker around it — REJECTED: makes a degradation path
  depend on breaker storage, when the point of the path is to work when storage
  is unreliable.

## Verification

`audit/evaluations/probe_signal_storage_degrades.py` case E, one store:

    against the latching code   FAIL, 1 of 7
    against the fix             PASS, 8 checks
    before=False -> immediate=False (cooldown holds) -> after=True (expires)

A control asserts the role genuinely regained read access, so `after=False`
cannot be confused with a GRANT that never landed. The probe waits the real
cooldown rather than patching the constant.

## Lesson

Cases A-D used two different roles and tested the BLOCK direction twice while
reading as "both directions". A guard's release direction is only tested by
taking **one** instance through the whole cycle.
