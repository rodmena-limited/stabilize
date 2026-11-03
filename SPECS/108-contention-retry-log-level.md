# #108 — Lock-contention retry exhaustion logs WARNING, not ERROR; the dead-letter move keeps ERROR

Ticket: issuedb #108. Asked by casebound-api on 2026-11-03 (thread 01M6G13J3CBC35CCX7DJAPGZ1M): on 0.34.2 a run cancel's own row locks exhausted a RunTask's retries and `persistence/transaction.py` logged ERROR "Transaction failed after max retries due to lock contention: RunTask"; the run, its 17 stages and 17 tasks ended CANCELED with nothing queued, claimed, leased or waiting. Low priority; ships with the next release.

## EARS SPEC
- When a handler transaction exhausts its retries on 40001, 40P01 or `ConcurrencyError`, the engine shall log at WARNING, naming the handler and that the message is redelivered, and shall re-raise the original error unchanged.
- The error-handling path's exhaustion (`execute_atomic_critical`) shall keep its CRITICAL line, and the dead-letter move shall keep ERROR, because those are terminal outcomes.
- A test shall drive `execute_atomic` to exhaustion with a deadlock-class error and assert the record's level is WARNING and the error propagates; it shall be seen failing before the change.

## TECHNICAL PROBLEMS
1. Log level as a severity signal: a recoverable event, after which the processor redelivers the message, logged at ERROR pages operators for the designed path.

## SOLUTION DOMAINS
- The engine's redelivery design (WI-7: `ConcurrencyError` is in the store-unavailable set, so the message is redelivered with backoff and never ends a stage; a redelivered RunTask for a CANCELED task is dropped by `handlers/run_task/handler.py`) and its logging convention (ERROR for terminal outcomes, which the dead-letter move already follows).

## ALTERNATIVES
- WARNING with the redelivery named in the text [CHOSEN].
- Keep ERROR [REJECTED: the event is recovered by design; Casebound's run ended consistent].
- Suppress the line when the contention comes from a cancel [REJECTED: the retry loop cannot know the cause of contention].

## Synthesis notes
- `tests/test_contention_log_level.py` shrinks the backoff fifty-fold, drives the helper to exhaustion with `ConcurrencyError`, and asserts one WARNING record naming the handler and the redelivery, with the error propagating. Red on the 0.34.3 code (the record is ERROR).
