# 76 — Document that process_all/process_one re-raise handler errors

Ticket: #76 (0.32.1). Reported by tokengate-c934ae against 0.32.0.

## EARS spec

- The documentation of `QueueProcessor.process_one` and `process_all` shall state that a handler exception is rescheduled by `config.retry_delay` and re-raised to the caller, and that a result held under #63 is saved by the next delivery without re-executing the task.

## Synthesis

- Solution domain: documentation (docstrings, README, docs/guide/error_handling.rst).
- Alternatives: document the existing contract [CHOSEN] vs swallow handler errors in `process_all` [REJECTED: hides store outages from synchronous callers and changes behaviour for every handler error].

## Evidence

Behaviour confirmed unchanged since 0.31.0 (`process_one` reschedule-then-raise, 0.31.0 processor.py:472-473; now queue/processor/drain.py). tokengate re-ran its fault injection as a drain loop on published 0.32.0: SUCCEEDED, 1 execution, errors raised and then saved from the held result.
