# 88 — A task's exception is recorded with its own error code and text (C10)

Ticket: #88. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (round 1b, pipeline 2933): a Task raising ValueError and a RetryableTask raising KeyError were recorded with error_code BULKHEAD_FULL and error text "Execution failed: <message>"; classify_error on the raw exception gives UNKNOWN. Probe: probe_raw_exception_error_code.py. Peer v3 #11 (P22).

## EARS spec

- When a task raises, the recorded error_code shall equal classify_error of the task's own exception, and the recorded error text shall be the task's exception text without the bulkhead's "Execution failed:" wrapper.
- Where bulkman rejects a task because the bulkhead is full, the error_code shall remain BULKHEAD_FULL.
- A raw non-transient exception shall stay TERMINAL (unchanged contract); to retry, a task raises TransientError or returns RUNNING.

## Technical problems

1. Exception translation by an intermediate layer (bulkman wraps the task's exception in a generic BulkheadError with __cause__ set) hiding the cause from classification and from the operator.

## Solution domains

- Exception chaining: unwrap a transport/wrapper exception to its cause at the boundary where the wrapper adds nothing (PEP 3134 __cause__).
- bulkman 2.0.x: BulkheadError("Execution failed: ...") from the task exception for execution failures; a distinct rejection path for capacity.

## Alternatives

- Unwrap in resilience/executor.py: if result.error is exactly bulkman's BulkheadError and has a __cause__, raise the cause [CHOSEN] vs teach classify_error about BulkheadError causes [REJECTED: the TERMINAL text and is_transient still see the wrapper] vs catch in the task wrapper before bulkman [REJECTED: bulkman's own timeout / rejection semantics would be bypassed].
- Raise the cause without `from` [CHOSEN: `raise cause from wrapper` would make cause.__cause__ point back at the wrapper, and error_chain would loop] vs `raise ... from None` [REJECTED: drops the context the operator needs].

## Callers

- execute_with_timeout -> executor (resilience/executor.py): RunTaskHandler only. Exception consumers downstream: handle_exception (is_transient, classify_error, truncate_error) — all now see the task's exception; TransientError raised by a task becomes visible as itself (it was already found through error_chain).

## Die-here / two-at-once

- None: pure exception translation.
