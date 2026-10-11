# 77 — Audit of 0.32.1 to a 1.0.0 stability bar; release 1.0.0

Ticket: #77. Input: ci-conductor-dd94c8 report #143 (thread 01M4ME7M81P310BRX51Y24HYN5) and v3 items #1-#12 (thread 01M4NQV92G8321SKGSN8N82A4C, evidence rodmena-limited/ci audit/evaluations/stabilize-v3 @ fd3e2e8). Operator decision 2026-10-11: the release ships as 1.0.0, tests and probes go red before green, full adversarial refutation audit.

## EARS spec

- The audit shall treat every claim (peer report #143 and v3 #1-#12, docs, SPECS, docstrings, CHANGELOG) as SUSPECTED until a probe reproduces it on published 0.32.1 in ci.rodmena.co.uk.
- When a claim is reproduced red on 0.32.1 for its stated reason, the audit shall mark it CONFIRMED, open a ticket with EARS spec + synthesis + callers + die-here/two-at-once table + class sweep, and sweep the codebase for its class.
- If a probe or regression test passes on 0.32.1, then it shall not be counted as evidence; the claim shall be re-diagnosed once and otherwise recorded as not reproduced.
- Every regression test and probe shall be committed before its fix and observed failing in CI (branch run, and falsify_tests.sh / falsify_previous.sh against published 0.32.1) before the fix commit.
- Each design shall be attacked by a fresh-context refuter before implementation, and the final diff shall be attacked again before release.
- Every guard (concurrency limit, retention, lease, retry bound) shall be tested in both directions: blocks when exceeded and releases when it should.
- The release shall be 1.0.0: public API inventoried and frozen with a written semver policy; upgrade from the 0.32.1 schema with in-flight work exercised; the suite run on Python 3.11 (declared floor) and 3.14; a sustained run of >= 30 min on PostgreSQL reported with throughput, queue depth, processed_messages growth, pool connections and RSS.
- The published 1.0.0 wheel shall be installed from PyPI in CI and pass the suite and the full probe harness before it is announced.
- The audit report shall rank findings by customer harm, separate CONFIRMED from SUSPECTED, list claims not reproduced, and close with what was exercised / not tested / unsure.


## Technical problems

1. Falsification of an execution engine's at-most-once / no-stranding / true-outcome guarantees under crash, concurrency, retention and multi-queue deployment.
2. Evidence discipline: red-before-green for tests and probes, with execution only in CI (vm-1 cease).
3. Release contract for 1.0.0 (API surface, upgrade path, interpreter floor).


## Solution domains

- mission-critical-audit skill (claim inventory, live reproduction, class sweeps, both-direction guards).
- Existing harness: audit/evaluations (run_all.sh, falsify_previous.sh, _backends.py), .rodmena/ci.yml.
- Semantic Versioning 2.0.0 for the 1.0.0 public API promise.


## Alternatives

- Scope: whole-engine audit with the peer report as input [CHOSEN] vs fixing the 13 reported items [REJECTED by the operator: a fix list cannot find the defect nobody reported].
- Release: one release 1.0.0 [CHOSEN] vs 0.33.0 then 1.0.0 [REJECTED: two estate-wide upgrades for one change set, release cadence memory].
- Evidence of red: commit order + falsify scripts against the published previous wheel [CHOSEN] vs red observed locally [REJECTED: vm-1 cease; and a local red is not reproducible by the reader].

## Evidence mechanics

- `audit/evaluations/expect/<version>.probes` — `probe|[FAIL] marker` lines; `falsify_previous.sh` (OLD_VERSION=...) requires each listed probe to exit non-zero on the published old release with that marker. An empty list fails the check.
- `audit/evaluations/expect/<version>.tests` — `nodeid|marker` lines; `falsify_tests.sh` requires each listed test to fail with pytest exit 1 (an assertion, not a collection error) and the marker in its output, against the published old wheel from site-packages.
- CI `baseline` job: the existing suite and probes against published 0.32.1, excluding the entries in the expect files, so old guarantees are re-established on the old release before the audit hunts new ones.
- CI `py311` job: the suite on the declared floor (3.11, managed CPython via uv).
- New tests go in new test files, so the baseline exclusion is by file.

## Claim inventory, findings, not-reproduced claims

Maintained in the ticket comments and below as the audit proceeds.
## Claim inventory (Phase 1) — every entry SUSPECTED until its probe goes red on published 0.32.1

Ranked by customer harm. "src" is the code read; "exp" is the disproving experiment.

### Tier 1 — a side effect runs that must not, or runs twice

- C0  After a task completes TERMINAL / STOPPED / CANCELED (or FAILED_CONTINUE converted from TERMINAL by continuePipelineOnFailure), no later task of the same stage starts. src: complete_task.py next_task(task) is unconditional; original_status is carried but never read; Orca's shouldCompleteStage only continues on SUCCEEDED or a task-returned FAILED_CONTINUE. exp: stage [fails, side_effect] → side_effect executions must be 0; control [ok, side_effect] = 1. Not in the peer report (peer P1 saw the FAILED_CONTINUE case).
- C27 While a handler runs longer than the queue lock, the lock is renewed for the whole run. src: lease_guard.beat returns on the first extend_lock exception; the task lease is off by default. exp: two processors; one extend_lock raises once mid-task; the second processor must not execute the task while the first is still in it.
- C1  Messages from two queue tables in one store are each handled (peer #10, P19 own-seq). src: message_id = str(row id), store-wide processed_messages. exp: queue_a and queue_b with own sequences; both id 1 → one silently skipped.
- C31 Recovery never pushes a second RunTask for a task whose message sits in another queue table (peer item C + #12). src: has_pending_message_for_task checks only the sweeping processor's table; recovery_application=None selects every application. exp: workflow of app B in flight on table B; unfiltered sweep on processor A → a RunTask lands in table A.
- C17 Two processors on one table execute each task once per attempt (default config, lease off). exp: N workflows × 2 processors; count do_execute per task.
- C16 Kill at each handler commit point → recovery resumes with no duplicate task execution and no stranding. exp: fault injection between commit and ack for each handler type.
- C4  limitConcurrent admits at most N RUNNING (peer #8). src: _should_queue counts RUNNING without a lock. exp: 8 concurrent StartWorkflow, limit 2, PG, two processors.
- C2  Retention never removes a dedup record whose message can still be delivered. src: rows outlive their processed mark only for lock_duration / reschedule delay. Expected NOT reproducible; documented as an argued negative unless a path is found.

### Tier 2 — work stranded, or a false outcome reported

- C9  A workflow is TERMINAL only if a stage failed (peer #6, P5). src: complete_workflow re-queues max_stage_wait_retries then TERMINAL, then CancelStage on the still-running leaf. exp: two leaves, MAX_STAGE_WAIT_RETRIES small, slow leaf.
- C8  A task skipped at StartTask completes its stage (peer #9, F19). src: start_task SKIPPED + CompleteTask(SKIPPED); complete_task ignores non-RUNNING. exp: [on, off] stage → RUNNING forever, also after 2 recovery sweeps.
- C5  BUFFERED work starts when a slot frees, oldest first (peer #8). src: recovery ignores BUFFERED; first-page-DESC then in-memory sort. exp: lose StartWaitingWorkflows; > page_size buffered.
- C6  A transactional push lands on the processor's own queue table (peer #1). src: sqlite/transaction.py hardcodes queue_messages. exp: SqliteQueue(table_name='q_b'), push via store.transaction(queue).
- C11 A raising user hook does not strand or repeat (peer #4, P17; class: on_timeout, on_cancel, is_enabled, on_cleanup). src: on_timeout outside try (handler.py ~277, ~354), on_cancel outside try (error.py:51), is_enabled outside try (start_task.py:98). exp: each hook raises → task completes TERMINAL once; hook called once.
- C12 Canceling a running task runs on_cancel and on_cleanup (peer #3, D). src: CancelStage marks tasks CANCELED; RunTask status guard returns before is_canceled. exp: cancel mid-RetryableTask, 6 trials.
- C29 continuePipelineOnFailure applies to every task failure. src: complete_with_error always TERMINAL (timeouts, task-not-found, verification failure, stageTimeoutMs); _mark_terminal uses failure_status. exp: task timeout in a stage with continuePipelineOnFailure → stage must be FAILED_CONTINUE and downstream must run.
- C10 A raw task exception is reported with its own error code (peer #11, P22). src: bulkman BulkheadError wrapper + name heuristics → BULKHEAD_FULL. exp: ValueError → error_code.
- C7  A recovered RunTask resolves the same task and bulkhead as the original (peer #2, P10, E). src: recovery task_type=stage.type, copied on every re-queue. exp: lose the RunTask; recover; record bulkhead name and task_type on later polls.
- C13 The skip decision is taken once, at StartTask, for every registered name form (peer #5, P20). src: StartTask uses registry.get(implementing_class); RunTask uses resolve_task; SkippableTask.execute re-checks. exp: FQCN-registered SkippableTask; is_enabled flip.
- C30 A RUNNING workflow older than recovery_max_age_hours with a lost message is recoverable. src: cutoff on start_time, not last activity (peer P8 measured). exp: 30 h old workflow, lost message. Possible outcome: documented limit, not a defect — decide in Phase 3.
- C32 A RUNNING stage whose tasks are all complete but whose CompleteStage was lost is recovered. src: recovery pushes StartStage, which ignores a RUNNING stage with tasks. exp: drop CompleteStage; sweep.

### Tier 3 — operability, contract, 1.0 promise

- C3  Retention on a 1 M-row backlog runs in bounded transactions and does not stop message processing (#143). src: single DELETE on the poll thread. exp: 1 M backdated rows; poll latency during the sweep.
- C14 Unfiltered recovery with several applications is visible (peer #12). Superseded in severity by C31.
- C15 Completed workflows can be purged through the product interface (peer #7b).
- C26 A workflow id longer than the column is refused with a clear error before any write (peer F11: StringDataRightTruncation).
- C18–C24 as in the plan (cancel/pause/signals/jump strandings, upgrade from 0.32.1 schema, security surfaces re-run, Python 3.11, sustained run, declared-but-false doc claims, public API freeze).

### Refuted by reading (no probe; recorded so they are not re-opened)

- C28 SQLite queue row-id reuse after deleting the max row: both queue schemas use INTEGER PRIMARY KEY AUTOINCREMENT; no code resets a sequence (no RESTART IDENTITY / sqlite_sequence / setval).
- Recovery duplicating an in-flight RunTask on ONE table: has_pending_message_for_task does not filter locked rows, so an in-flight message counts as pending.

## Falsification round 1 — CI pipeline 2930 (sha f52e1c4, falsify job 8484, published 0.32.1 from site-packages)

CONFIRMED (red for the stated reason, control green, both backends unless noted):
- C0  next task runs after TERMINAL / raise / STOPPED / CANCELED / continue-on-failure TERMINAL: side effect 1x in all 10 cases; control 1x as expected. Regression test red with the C0 marker, control test green.
- C27 (postgres) one extend_lock exception -> second process executes the running task: executions=2; control 1. SQLite did not reproduce (1).
- C1  (postgres) own id sequence per table -> WB NOT_STARTED, never run; shared-sequence control SUCCEEDED. SQLite masked by C6.
- C31 (postgres) unfiltered sweep pushed a RunTask for WB's in-flight task into queue_messages; filtered control did not. SQLite masked by C6.
- C8  StartTask skip wedges stage RUNNING ([on,off] and [off,on]); unchanged after 2 recovery sweeps; control SUCCEEDED.
- C6  SQLite txn push to queue_messages: workflow on q_b NOT_STARTED, 1 stray message on queue_messages.
- C4  limit 2 exceeded: peak RUNNING sqlite [5,6,2], postgres [4,4,4]; release direction failed in one postgres round (4/8 SUCCEEDED in 60 s).
- C5  lost StartWaitingWorkflows -> W2 BUFFERED after a recovery sweep; control SUCCEEDED.
- C11 on_timeout raising and is_enabled raising: hook called 10x, task stranded RUNNING / NOT_STARTED; control 1x TERMINAL.
- C12 cancel during polling: on_cancel 0, on_cleanup 0; control on_cleanup 1.
- C7  recovered poll ran in Bulkhead-stabilize_default_0; normal poll in Bulkhead-stabilize_job_0.
- C13 is_enabled evaluated 4x for 3 polls; dotted implementing_class skipped only at RunTask (start_time set).
- C33 lost CompleteTask -> task executed 2x after recovery (both backends).
- C32 lost CompleteStage -> workflow RUNNING, stage RUNNING with all tasks done.
- C34 lost CompleteWorkflow -> workflow RUNNING with every stage SUCCEEDED.
- C37 (by reading, probe added) QueueProcessor(handler_config=X) passes X to SignalStage and RunTask handlers only (queue/processor/mixins.py:102-125); every other handler uses HandlerConfig.from_env().

NOT REPRODUCED:
- C29 continuePipelineOnFailure on timeout / unregistered task: s FAILED_CONTINUE and downstream SUCCEEDED on both backends (CompleteStage applies failure_status). Probe kept as a green regression guard; claim closed.
- C5-FIFO at page size 2 passed, but the page size was not applied (C37). Re-run with STABILIZE_DEFAULT_PAGE_SIZE=2.
- C9 passed, but max_stage_wait_retries was not applied (C37). Re-run with STABILIZE_MAX_STAGE_WAIT_RETRIES=3.

PROBE DEFECTS (red for another reason; not findings):
- C10 read task_exception_details, which is not persisted; details live in stage.context["exception"]. Probe corrected.
- C3 stored the workflow twice (duplicate key) before measuring. Probe corrected.

REFUTED:
- C35 (apparent duplicate 'on' in postgres skip case C) was cross-case leakage in the shared probe database: recovery correctly healed case B's [off,on] workflow. Counter now keyed per workflow.

## Falsification round 1b — CI pipeline 2933 (sha 14a3068, falsify job 8500)

- C10 CONFIRMED: ValueError / KeyError recorded as BULKHEAD_FULL, text "Execution failed: ..." (#88).
- C37 CONFIRMED: 17 of 19 default handlers ignore QueueProcessor(handler_config=...) (#85).
- C27 CONFIRMED on both backends across rounds (postgres in 2930, sqlite in 2933): timing-sensitive; probe margins widened, verdict aggregated across backends.
- C9 reproduced (TERMINAL, slow leaf CANCELED) but its control was also red: get_handler_config() caches the environment per process. Probe now calls reset_handler_config(); re-run in round 1c (#86).
- FIFO (C5) still passed, under the same caching defect; re-run in round 1c.
- C3: no poll stall on PostgreSQL at 2 M rows; SQLite 1.95 s (bound 2.0 s); PostgreSQL backlog not drained in 120 s — diagnostics added (sweep log, direct timed cleanup) (#87).
- C17 NOT REPRODUCED: two processor processes, 20 workflows x 6 tasks, both backends: every task executed exactly once, every poll 3 times. Claim holds on 0.32.1; probe kept as a regression guard.
- C19 upgrade job: the old phase exited 1 by its own rule (two workflows finished before the others reached 2 polls); poll budget raised to 6.

## Phase 4 — design refutation (fresh-context refuter, read-only, 2026-10-11)

Breaks found in 9 of 11 designs (#78 #79 #80 #81 #82 #83 #84 #85 #86); #87 and #88 hold with conditions. Every amendment is recorded on its ticket and SPECS file under "Phase 4 design refutation". The most consequential:
- #79: retrying renewal without an owner check extends / clears / acks another worker's lease -> fence on attempts.
- #81: pre-upgrade bare-id records collide with future default-table ids -> advance queue_messages_id_seq; ownership written in StartWorkflow's own transaction.
- #82: purge leaves canceled workflows BUFFERED (recovery would loop); SQLite commits inside the admission section; advisory key must be stable across processes.
- #83: on_cancel can run twice; hooks must run after commit, outside the version-conflict retry.
- #84: task-less zombie stage would become false SUCCEEDED; marker must be keyed and cleared on every path; every completion producer must write it.
- #86: CancelStage never pushes CompleteWorkflow (strands a canceled workflow once the retry is removed); ContinueParentStage has the same false-TERMINAL wait.

New claims from Phase 4 (SUSPECTED until reproduced; round 2 probes):
- N1 REDIRECT without target wedges its stage — probe_redirect_without_target.py
- N2 recovery starts a parent's tasks while its before-stage runs — probe_synthetic_stage_paths.py A
- N3 zombie re-plan race duplicates before-stage work — SUSPECTED, no deterministic external interleaving yet
- N4 on_cleanup / finalizers repeat on every version conflict — probe_cleanup_once_on_conflict.py
- N5 JumpToStage overwrites concurrently written context — SUSPECTED, not probed
- N6 SystemExit/KeyboardInterrupt from a task: redelivered, re-executed, stranded — probe_base_exception_task.py
- N7 purge leaves BUFFERED + is_canceled; StartWorkflow canceled path no-op — probe_purge_cancels_buffered.py
- N8 FAILED_CONTINUE before-stage wedges its parent — probe_synthetic_stage_paths.py B
- B86-2 ContinueParentStage bounded wait fails a parent while a sibling before-stage runs — probe_synthetic_stage_paths.py C

## Falsification round 1c — CI pipeline 2935 (sha 387c944)

- C9 CONFIRMED with control green: budget 0.6 s -> TERMINAL, slow leaf CANCELED (both backends); budget 20 s control SUCCEEDED.
- C27 CONFIRMED (aggregated verdict).
- C38 NEW, CONFIRMED: PostgreSQL retention query raises UndefinedFunction make_interval(hours => double precision); the sweep has never removed a row on PostgreSQL (#87).
- C3: SQLite poll stall 2.06 s during a 500 k-row sweep (bound 2.0 s) — marginal red.
- C5-FIFO NOT REPRODUCED with page size 2 applied (order f0..f4 on both backends); the design orders explicitly anyway (cheap).
- C19 upgrade: in-flight work started under published 0.32.1 finished under the checkout; polls resumed from persisted counts (before+after = 6 for all 6 workflows).

## Falsification round 2 — CI pipeline 2936 (sha f89ca2b, falsify job 8522): FALSIFICATION CHECK PASSED, 21/21 probes red on published 0.32.1 for their stated reason, controls green

New CONFIRMED (from the Phase 4 refuter's suspected list): N1 (#78), N2 (#84), N4 and N6 (#83), N7 (#82), N8 and B86-2 (#86). C38 detail: int max_age_hours works on PostgreSQL (2 M rows in 1.43 s), float fails.
Still SUSPECTED, not reproduced: N3 (zombie re-plan race), N5 (JumpToStage context lost update).

## Phase 6 — refutation of the fix diff (fresh-context refuter, read-only, 2026-10-11)

Findings against c1725aa..5195655, all adopted (fixes in 60475c0, regression tests in e8f6717):
- F1 CERTAIN false outcome: CancelStage's new CompleteWorkflow push met the "any CANCELED stage" rule before the active-stage check; a deferred-choice loser or a cancel region canceled running branches. Fixed by waiting for active stages unless the workflow itself was canceled. Regression: test_canceling_a_sibling_does_not_cancel_a_running_branch.
- F2 CERTAIN duplicate side effect: on_cancel could run twice (RunTask before its commit, CancelStage after its own). Fixed: whichever commit wins the RUNNING task's cancellation (optimistic lock on the stage) is the only caller of on_cancel.
- F3 CERTAIN missed compensation: RunTask for an already-complete workflow never called on_cancel. Fixed.
- F4 CERTAIN stranding with recovery off: an orphan BUFFERED row blocked a config. Fixed: a wake-up is pushed when buffering behind older work with a free slot. Regression test.
- F5 CERTAIN broken upgrade: SQLite migration 4's ALTER autocommitted and was not idempotent. Fixed: migrations run in one BEGIN IMMEDIATE transaction.
- F6 POSSIBLE stall: second pooled connection under the advisory lock. Fixed: status read on the transaction's connection; promotion loads candidates outside the lock.
- F7 CERTAIN minor: promotion skipped start_time_expiry, the created event and audit. Fixed.
- F8 POSSIBLE false outcome: a stale recovery CompleteTask could complete a restarted run. Fixed: CompleteTask carries task_start_time from recovery and is ignored when it differs.
- F9 CERTAIN minor: gate error ignored continuePipelineOnFailure. Fixed. Regression test.
- F10 CERTAIN minor: a StartTask-skipped task's late CompleteTask was accepted after cancel. Fixed.
- F11 POSSIBLE minor compatibility: old one-argument cleanup signature, stores without admission support, alias-registered tasks in cancel hooks, heartbeat on unparsable ids. Fixed.
Attacks that found nothing: halt rule paths and redelivery; completion-record producers (RUNNING / transient / held paths push no CompleteTask); attempts fencing (every caller holds the polled message); default-table keys unchanged; nothing runs on the SQLite connection before lock_admission; no CompleteWorkflow/CancelStage loop; no import cycle; gate record persisted before RunTask; base-exception catch order; retention thread lifecycle; PostgreSQL migration additive; bulkman capacity/timeout/shutdown errors are typed and never unwrapped; all 19 handlers registered once.
Pipeline 2939 (5195655): lint failed (mypy **cfg typing in handlers/defaults.py, 3 ruff issues) — fixed in 60475c0.

## Release number

RELEASE NUMBER CORRECTION (operator, 2026-10-11): this release ships as 0.33.0, not 1.0.0. 0.33.0 is built to the 1.0 bar; the operator promotes it to 1.0.0 later. Version strings, CHANGELOG and docs/guide/stability.rst updated; ci-conductor told.
