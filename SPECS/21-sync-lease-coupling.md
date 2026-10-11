# Spec: synchronous processing on an unrenewed lease (#21)

## EARS requirements

- Where a processor has no lease-renewal path active, the engine shall warn
  when it polls a queue whose lock_duration is below a configured threshold,
  naming the queue and the duration.
- The PostgresQueue constructor documentation shall state that the default
  lock_duration assumes an active lease renewer.
- The warning shall be emitted once per processor, not once per poll.

## The defect

Lease renewal and the execution path are coupled, and only one is visible at
the call site. process_all() -> process_one() calls the handler directly: no
executor, no threads, and therefore no heartbeat, which is started only from
_submit_message on the asynchronous start()/poll-loop path. PostgresQueue's
lock_duration defaults to 60s. A synchronous embedder therefore gets a
60-second lease that nothing renews; a handler exceeding it makes the message
visible again mid-flight and a later poll re-runs it. Silent double execution,
no error on either side.

The defect is the invisible coupling, not the number.

## Alternatives considered

1. Warn once when a synchronous processor polls a short-leased queue. CHOSEN —
   catches it at the first tick rather than the first slow stage, costs nothing
   at runtime, and leaves the operator's sizing decision to the operator.
2. Start the heartbeat in process_one too. Rejected: reintroduces a thread per
   message into the path embedders chose specifically to avoid threads.
3. Raise the default. Rejected: does not fix the coupling and penalises
   redelivery after a crash.
4. Document only. That is what existed, and it did not work.

## Scope honesty

The first EARS clause in the ticket — "the engine shall not allow the message
to become visible while the handler is still running" — is NOT satisfied by
this change. Preventing it in the synchronous path requires either a renewal
thread (rejected above) or a lease sized above the worst-case handler, which is
the operator's call and cannot be inferred. What this change does is make the
condition detectable at the first poll instead of discoverable from a duplicate
side effect. That is a smaller claim than the clause and is recorded as such.

## Refactor carried in the same change

processor.py was 554 lines, already past the 550 hard cap, and adding to it
took it to 566. _start_lock_heartbeat (41 lines) moved to lease_guard.py, where
it sits beside the check that warns when it is absent. processor.py is now 529.
