# 66 — QueueProcessor.stop(wait=True) is bounded

Ticket: #66 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- When `stop(wait=True)` is called, the processor shall return within `timeout` (default `QueueProcessorConfig.shutdown_timeout_seconds` = 60 s) and return the number of handlers still running.
- `LifecycleManager` shall pass its remaining shutdown budget to `stop()`.

## Synthesis

- Alternatives: poll active_count to a deadline after a non-blocking executor shutdown [CHOSEN] vs cancel queued futures [REJECTED in a follow-up commit: cancelled futures never decrement active_count] vs kill threads [not possible in CPython].
- Assumption made in Farshid's place: default bound 60 s; `None` restores the unbounded wait.

## Evidence

`probe_processor_stop_bounded.py`: 0.31.0 LifecycleManager(shutdown_timeout=1) did not return within 30 s on a blocked handler; fixed 1.0 s; a handler finishing in 0.5 s is still waited for. `tests/test_processor_stop_bounded.py`.

## Second falsification pass

- A message polled while stop() shut the executor down incremented `_active_count`, then `submit()` raised; the count never came down -> undone and the message rescheduled.
- LifecycleManager calling `stop(timeout=...)` on a subclass with the old signature -> falls back to `stop(wait=False)`.
- Behaviour change documented: a plain `stop()` now returns after 60 s.
