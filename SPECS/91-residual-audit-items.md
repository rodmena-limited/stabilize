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
