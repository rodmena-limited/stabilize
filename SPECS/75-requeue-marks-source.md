# 75 — A re-queued RunTask marks its source processed atomically

Ticket: #75 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- When a RunTask handler re-queues the task (RUNNING result or TransientError retry), it shall mark the source message processed in the same transaction as the push.
- If the processor's post-handler processed mark fails, then the task shall not be executed again by a redelivery of the source.

## Synthesis

- Solution domain: transactional outbox / idempotent consumer (codebase pattern: `execute_atomic(source_message=...)` on every other terminal path).
- Alternatives: mark the source inside the re-queue transaction [CHOSEN] vs retrying the post-handler mark [REJECTED: still a window if the process dies] vs dedup on (task_id, retry_count) [REJECTED: a new key scheme for one path].

## Evidence

`probe_requeue_marks_source.py` (fault on the post-handler mark, 5 s follow-up, 1.5 s window): 0.31.0 executed the task twice and left two queued RunTasks on sqlite and postgres for both paths, 2/2 runs; fixed 1/1. `tests/test_requeue_marks_source.py`.
