# 91 — Residual audit items before 0.33.0: crash points, pause/resume/jump, recovery window, N3, N5, throughput, C2, soak exit

Ticket: #91. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

EARS SPEC:
- R1 (C16) If a processor process is killed (SIGKILL) after any handler's commit and before its ack, then the stabilize engine shall, on a second processor on the same PostgreSQL store, finish the workflow SUCCEEDED with each task's side effect executed exactly 1 time, for each of the 7 lifecycle message types.
- R2 (C18) When pause, resume or jump_to_stage is applied to a workflow whose stage is RUNNING, BUFFERED or NOT_STARTED, the stabilize engine shall reach a terminal workflow status within 30 s of resume (or of the jump) with no stage left RUNNING or NOT_STARTED that the graph would run.
- R3 (C30) If a RUNNING workflow started more than recovery_max_age_hours ago lost a message while its last stage or task activity is within recovery_max_age_hours, then the recovery sweep shall recover it; a workflow with no activity within the window shall still be skipped (both directions probed).
- R4 (N3) While a stage's before-stages are planned, if a zombie re-plan races the original, then the stabilize engine shall execute each before-stage task's side effect at most 1 time per attempt; otherwise N3 is recorded NOT REPRODUCED with the attempted interleavings.
- R5 (N5) When JumpToStage runs while another handler commits context to the same stage, the stabilize engine shall not discard the concurrently committed keys (0 lost keys over 50 trials), or the loss shall be reproduced and fixed.
- R6 (throughput) The stabilize engine 0.33.0 shall complete at least 95 % of the workflows 0.32.1 completes on the same 5-minute overload run on the same host (measured three times each, median), or the cause shall be identified and recorded.
- R7 (C2) If a message is redelivered after its processed_messages record would be due for retention, then the engine shall not handle it again; the probe backdates the record and redelivers.
- R8 (soak exit) The soak driver shall close its store and queue so the run ends with no PythonFinalizationError, or the error shall be shown to come from the driver, not the engine.

TECHNICAL PROBLEMS:
1. Crash consistency at commit/ack boundaries under a real process kill (R1).
2. Control-message state machines (pause/resume/jump) against every stage state (R2, R5).
3. Recovery eligibility window keyed on the wrong timestamp (R3).
4. Concurrent re-planning of synthetic stages (R4).
5. Performance regression attribution (R6).
6. Retention vs redelivery ordering (R7).

SOLUTION DOMAINS:
- Fault injection by SIGKILL at instrumented points (existing probe harness, _engine.py, both backends; PostgreSQL for the two-process case).
- Optimistic concurrency on stage version (existing store pattern) for R5.
- Recovery query predicate (existing recovery module) for R3.
- Profiling by A/B on the same host (soak driver, overload rate) for R6.

ALTERNATIVES:
- R3: last-activity predicate from stage/task timestamps [CANDIDATE] vs raising the default window [REJECTED: moves the cliff, does not remove it] vs documenting the limit [REJECTED unless the probe shows recovery of such a workflow is unsafe].
- R5: re-read and merge under the stage version check [CANDIDATE] vs a stage row lock [REJECTED: no row-lock pattern in the store API; version check is the established pattern].
- R1: SIGKILL at instrumented points via an env-driven hook in the probe's task/handler wrapper [CANDIDATE] vs timing-based kill [REJECTED: nondeterministic, cannot target the commit/ack window].

## Outcomes (2026-10-12)

| Item | Result | Evidence |
|---|---|---|
| R1 C16 SIGKILL after commit, before ack | NOT REPRODUCED | `audit/evaluations/probe_sigkill_before_ack.py`: 7 types x sqlite/postgres, executions 1 and SUCCEEDED, on HEAD and published 0.32.1; worker exit checked to be -SIGKILL. Red-proven: durable dedup check disabled -> RunTask executions=2 on both backends. Limit: duplicates observed through task executions only. |
| R2 C18 pause/resume/jump | CONFIRMED x3, fixed (d0941eb) | `tests/test_audit_control_matrix.py`: paused-before-start and paused-BUFFERED workflows stayed PAUSED forever after unpause; `store.pause()` rewrote SUCCEEDED to PAUSED; `store.cancel()` flagged SUCCEEDED as canceled. Red on HEAD and 0.32.1, both backends. Pause of a RUNNING stage and a jump into a RUNNING stage: green before and after. Monitor resume moved to `Orchestrator.unpause`. |
| R3 C30 recovery window | Documented limit (692a2bc) | `tests/test_recovery_window.py`: 30 h-old RUNNING workflow skipped at 24 h, recovered and SUCCEEDED at 72 h; each direction red on its own mutant. `docs/guide/stability.rst`. |
| R4 N3 zombie re-plan | CONFIRMED (wedge), fixed (9c85bea); class: duplicate after-stage execution CONFIRMED, fixed (391c1fa); AddMultiInstance instance loss CONFIRMED, fixed (6983b69) | `tests/test_audit_zombie_replan.py`, `tests/test_audit_multi_instance_crash.py`; all red on HEAD and 0.32.1, both backends. |
| R5 N5 jump lost update | CONFIRMED, fixed (de07526) | `tests/test_audit_jump_lost_update.py`, red on HEAD and 0.32.1. |
| R6 throughput | MET: 0.33.0 median 216 vs 0.32.1 median 204 SUCCEEDED by t=300 s (105.9 %) | interleaved 3 x (0.32.1, 0.33.0), 5 min at 4 wf/s, PostgreSQL 18, 8 workers; per run 215/199, 204/216, 179/228; host load 6.6-8.7 (red-proof baseline running alongside), so single runs vary by about 12 %. Observation: RSS 86 MB vs 68 MB, flat within each run. |
| R7 C2 retention vs redelivery | CONFIRMED, fixed (50c12d4) | `tests/test_audit_retention_redelivery.py` (prune-processed during an outage: RunTask executed twice; red on 433f187; 0.32.1 has no prune-processed, its store primitive shows the same at 85c7bde); `tests/test_retention_guard.py` both directions. |
| R8 soak exit | Driver defect, fixed (ca94c9b) | 433f187 gate soak logs end in PythonFinalizationError (driver never closed its pools); all 6 R6 runs (both versions, fixed driver) end with 0 PythonFinalizationError lines. |

Synthesis change (R7): the guard lives in the queue (`live_delivery_keys`) because store and queue may be in different databases; a store-side SQL join was rejected for that reason. `prune-processed` needs `--queue-db-url` when the queue is elsewhere (documented; cannot be detected).
