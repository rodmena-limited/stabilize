# 82 — limitConcurrent admission is serialized and buffered work always starts (C4, C5)

Ticket: #82. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930). C4 block: 8 workflows, limit 2, 8 worker threads: peak RUNNING sqlite [5, 6, 2], postgres [4, 4, 4] over 3 rounds. C4 release: postgres round 2 finished 4 of 8 in 60 s (keep_waiting_pipelines=True). C5: StartWaitingWorkflows consumed unhandled -> W2 still BUFFERED after a recovery sweep; control SUCCEEDED. FIFO at page size 2: pending re-run (C37 hid the page size). Probe: probe_concurrency_limit.py. Peer v3 #8.

## EARS spec

- While N workflows of a pipeline_config_id with is_limit_concurrent are RUNNING, StartWorkflow shall set any further workflow of that config BUFFERED, with admission (count + status write) serialized per pipeline_config_id: PostgreSQL pg_advisory_xact_lock on the config id in the admitting transaction; SQLite BEGIN IMMEDIATE.
- When a RUNNING workflow of such a config completes, the BUFFERED workflows shall be admitted oldest first (by workflow id, a ULID) up to the free slots, under the same serialization.
- When a recovery sweep finds BUFFERED workflows for a config with free slots and no StartWaitingWorkflows queued, it shall push StartWaitingWorkflows for that config with purge_queue=False.
- Both directions shall be shown: peak RUNNING <= limit over 3 rounds of 8 concurrent starts, and all 8 SUCCEEDED in every round.
- keep_waiting_pipelines=False keeps its documented meaning (excess buffered workflows are canceled when a slot frees); recovery never purges.

## Technical problems

1. Admission control under concurrency: check-then-act across workers (classic TOCTOU on a counter).
2. Liveness: the wake-up is a single message; its loss leaves BUFFERED work forever.
3. Ordering of the waiting set across pages.

## Solution domains

- Admission control with a mutual-exclusion section keyed by the resource (advisory locks are PostgreSQL's documented tool for application-defined locks).
- Level-triggered reconciliation (recovery) instead of an edge-triggered wake-up only.

## Alternatives

- Serialization: advisory xact lock per config [CHOSEN] vs SELECT ... FOR UPDATE on the RUNNING rows [REJECTED: locks nothing when 0 rows are running, the race at the first admissions remains] vs a counter row per config [REJECTED: new table + migration, and the counter can drift from the real RUNNING set] vs optimistic retry [REJECTED: nothing detects an overshoot after the fact].
- Liveness: recovery re-issues StartWaitingWorkflows [CHOSEN] vs make CompleteWorkflow retry the wake-up [REJECTED: the message itself is what is lost].

## Callers (contract change)

- StartWorkflowHandler._should_queue and StartWaitingWorkflowsHandler admission become one store operation; both handlers are the only callers (grep "_should_queue(", "retrieve_by_pipeline_config_id(" in src/ to be listed in the implementation comment).

## Die-here / two-at-once

- Two StartWorkflow for one config at once: the advisory lock serializes them; the second sees the first's RUNNING.
- Die holding the advisory lock: the transaction aborts and the lock is released with it.
- StartWaitingWorkflows racing a StartWorkflow: both admit under the lock.
