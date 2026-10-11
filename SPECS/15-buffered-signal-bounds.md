# 15 — Unbounded `_buffered_signals` growth in `stage_executions.context`

Ticket: issuedb #15
Reported by: `ci-conductor-dd94c8` (ci.rodmena.co.uk) via AgentBus thread `01M2B83G80C4PPRRWSGZTM781T`
Status: released in stabilize 0.22.0 (tag v0.22.0, commit 1f3ea14, PyPI 2026-09-12)

## Incident

73,608 buffered signals in a single 3.5 MB `stage_executions.context` row on
ci.rodmena.co.uk (stabilize 0.21.1, PostgreSQL 18.4). Production distribution
supplied by the reporter:

| status | rows | total signals | max per row |
|---|---|---|---|
| CANCELED | 11 | 629,509 | 112,899 |
| TERMINAL | 28 | 629,463 | 112,901 |
| SUCCEEDED | 59 | 49 | 3 |
| NOT_STARTED | 3 | 4 | 2 |
| SUSPENDED | 4 | 0 | 0 |

39 rows in CANCELED or TERMINAL held 1,258,972 of 1,259,015 entries — 99.997%.
Every entry was `{"signal_data": {}, "signal_name": "ci.resume"}`, 46 bytes.

Secondary damage: PostgreSQL sent the uncompressed context as a bind parameter on
every state transition and logged it each time, producing a 3,210,658-byte single
log line on pg-nano-03, a host shared by six databases. Measured by infra-manager
from Prometheus: ~5.1 GiB of free space consumed in about 36 hours to 2026-09-06,
peaking at 1.66 GiB per 6-hour window (~6.6 GiB/day at the filesystem against a
~0.25 GiB/day baseline), floor of 14.46 GiB of 38 GB, 3.76 GiB reclaimed on
remediation. No other database on the host was degraded. Mitigated by
infra-manager with `log_parameter_max_length=2048` and log rotation.

A separate outage on that host on 2026-09-12 (OOM from `max_connections=200` with
`work_mem=8MB` on a 3.9 GB box) was an unrelated memory misconfiguration and is
**not** attributable to this defect. The two were initially conflated in this
project's reporting; infra-manager corrected it.

`pg_column_size(context)` read 93 kB against `octet_length(context::text)` of
5,405 kB — 58x TOAST compression, because the entries are identical. Any cap
sized from the on-disk figure would have been 58x too generous, so the cap is on
**array length**, never on bytes.

## EARS SPEC

- If a persistent `SignalStage` message targets a stage whose status is complete
  (`WorkflowStatus.is_complete`: SUCCEEDED, FAILED_CONTINUE, TERMINAL, CANCELED,
  STOPPED, SKIPPED), then the SignalStageHandler shall not buffer the signal.
- If a persistent signal is refused because its target stage is complete, then the
  SignalStageHandler shall mark the source message processed and record the
  refusal, so the message is neither reprocessed nor silently lost.
- The SignalStageHandler shall enforce a maximum buffered-signal count per stage,
  configurable via `STABILIZE_SIGNAL_BUFFER_MAX`, defaulting to 1000 entries.
- If buffering would exceed `STABILIZE_SIGNAL_BUFFER_MAX`, then the
  SignalStageHandler shall refuse the signal, move the source message to the
  dead-letter queue with reason `signal_buffer_full`, and leave the existing
  buffer unmodified.
- When a stage reaches a complete status, the engine shall remove the
  `_buffered_signals` key from that stage's context.
- The engine shall emit at most one WARNING per stage per decade of refusals
  (counts 1, 10, 100, 1000, ...), so refusal logging cannot reproduce at the
  logging layer the unbounded growth it exists to prevent.
- The engine shall additionally emit one WARNING per 100,000 refusals per stage,
  so the interval between emissions is bounded and an ongoing problem never
  becomes indistinguishable from a stopped one.
- If a caller names a status the engine does not define, then reclamation shall
  raise rather than match no rows, so a typo cannot report a successful cleanup
  of zero.
- The WorkflowStore shall expose `cleanup_buffered_signals(only_complete, statuses)`
  and `count_buffered_signal_stages(only_complete, statuses)`, and the CLI shall
  expose them as `stabilize prune-signals [--dry-run] [--include-active]
  [--status STATUS]`, so buffers already stranded in a database are reclaimable
  through the product's own interface.
- The existing WCP-23 (transient) and WCP-24 (persistent, consumed-on-suspend)
  behaviours shall remain unchanged for stages that are not complete and are
  below the cap.

## TECHNICAL PROBLEMS

1. **Admission control for undeliverable messages.** The fill predicate was
   `not SUSPENDED`, which conflates "not ready yet" (bufferable) with "will never
   be ready" (undeliverable). The only drain
   (`handlers/run_task/result.py`) runs on a suspend *edge*, so a complete stage
   can never reach one.
2. **Unbounded accumulation in a serialised document.** `_buffered_signals` is a
   list re-serialised into one JSONB value per append: O(n) per write, O(n) row
   size, no ceiling.
3. **Reclamation.** No lifecycle event removed entries when they became
   undeliverable, and nothing could reclaim entries already stranded.
4. **Emission rate.** Any per-event log on the refusal path fires at the signal
   rate and reproduces the incident at the logging layer.
5. **Storage shape (deferred).** The migrated-but-unwired `workflow_signals`
   table is the correct home for this data. Debt independent of the leak.

## SOLUTION DOMAINS

- Undeliverable-message handling → the dead-letter concept, already in this
  codebase (`queue/dlq.py`, migration `01KE4YJT2W8A6NBX5R7CQMJK84`). The queue is
  lease-based — `poll` locks the row, `ack` deletes it — so a handler can
  dead-letter the message it is holding, and `queue/sqlite/queue.py` already does
  exactly that for undeserialisable messages. Extracted, not invented.
- Bounded buffer with an explicit overflow policy → the classic bounded-queue
  concept. Commonality: a capacity and a policy. Variability: the policy and the
  capacity value.
- Terminal-state predicate → `WorkflowStatus.is_complete`, already the engine's
  answer to "can this make progress".
- Configuration → `HandlerConfig.from_env`, the established `STABILIZE_*` pattern.
- Reclamation → the existing store-maintenance pattern
  (`cleanup_completed_stage_claims`, `cleanup_old_processed_messages`).
- Log emission control → exponential (decade) sampling with a running count, the
  standard answer to a log line on an unbounded event path.

## ALTERNATIVES

**Concept 1 — where buffered signals live.**
- CHOSEN (this ticket): keep the list in stage context, bounded and reclaimed.
- REJECTED here, split to a follow-up: move to the `workflow_signals` table. The
  table exists in migration `01KGHWCP1M2QSDE3TN4GUBLV8A` with indexes, and a full
  SQLite implementation sits in `persistence/sqlite/signals.py`, but nothing
  imports either — confirmed by grep across `src/`, `tests/` and
  `golden_standard_tests/`, and independently by the reporter
  (`SELECT count(*) FROM workflow_signals` = 0 in production). Wiring it needs new
  `WorkflowStore` and `StoreTransaction` methods across three backends,
  transaction integration, SQLite table creation (`create_signals_table` is never
  called), and a read-both/write-new period for existing data. Decided by the
  quantified criterion: the cap and the terminal refusal satisfy "shall not exceed
  the cap under any arrival rate" completely, so the storage move buys zero
  additional leak protection at materially higher regression risk — and it would
  put an incident remediation on the same release as a hot-path refactor. Reporter
  agreed with the split.

**Concept 2 — overflow policy at the cap.**
- CHOSEN: reject-producer, routing the refused message to the DLQ.
- REJECTED: drop-oldest (ring buffer). WCP-24 promises persistence; silently
  discarding an accepted signal breaks that promise invisibly.
- REJECTED: log-only. A log line is not a recoverable artifact.
- KNOWN PROPERTY, accepted deliberately: DLQ growth on this path is not bounded —
  a producer hammering a live-but-never-suspending stage writes a DLQ row per
  overflow. Kept because it is row-per-message in a dedicated table with an
  existing `clear_dlq()`, is the shape Release 2 moves toward, and the reported
  incident does not touch this path at all.

**Concept 3 — refusal predicate.**
- CHOSEN: `WorkflowStatus.is_complete`.
- REJECTED: an explicit status allow-list in the handler — duplicates a predicate
  the model owns and would drift on the next status added.

**Concept 4 — refusal log level and rate.**
- CHOSEN: WARNING at counts 1, 10, 100, 1000, ... with the running total; DEBUG in
  between; tracker bounded to 1024 stages with LRU eviction.
- REJECTED: WARNING on every refusal. This was the original implementation and the
  reporter objected, correctly: at the incident's rate it is ~234 lines/min, which
  is a log-volume fix for a log-volume incident on the same shared disk. Measured:
  112,901 refusals cost 6 WARNING lines instead of 112,901.
- REJECTED: drop to INFO. The level was never the problem; an unbounded emission
  rate was. INFO hides a real producer defect in a stream nobody reads.
- REJECTED: first-only-at-WARNING then DEBUG (the reporter's simpler variant).
  Gets the alarm to a human once, but discards magnitude — and magnitude is what
  separates one stray signal from 13 days of a poller.

**Concept 5 — emission-gap floor.**
- CHOSEN: emit on every power of ten AND every multiple of 100,000.
- REJECTED: decades alone. The reporter observed that the gap grows geometrically;
  measured, 100,000 -> 1,000,000 is 104 days at the incident's 6/min, and the next
  gap is 1,040 days. Unbounded in exactly the dimension that matters while looking
  finite. Measured after: max gap 100,000 across 3M counts, incident volume still
  6 lines, 10M refusals cost 105 lines.

**Concept 6 — dry-run counting for the CLI.**
- CHOSEN: `count_buffered_signal_stages` on the store interface.
- REJECTED: backend-specific SQL in the CLI reaching into `store._pool`. Caught by
  mypy; it reaches past the abstraction and would have diverged per backend.

## VERIFICATION

Harness: `audit/evaluations/probe_buffered_signal_growth.py`, using the reporter's
exact 46-byte payload. Suite: `tests/test_signal_buffer_bounds.py`.

Red baseline, taken by stashing the handler and persistence changes:

```
[FAIL] complete stage refuses every persistent signal
       status=SUCCEEDED signals_sent=500 buffered=500 sibling_keys_intact=True
[FAIL] buffer stops at the cap and overflow is dead-lettered
       cap=100 buffered=499 dlq=0 expected_dlq=400 (cap admitted 500, refused 0)
[PASS] WCP-24 still resumes a stage from a buffered signal
[FAIL] cleanup reclaims a stranded buffer
       raised AttributeError: no attribute 'cleanup_buffered_signals'
VERDICT: 3 FAILING
```

Case 3 passes in both runs by design: it is the control, guarding the WCP-24
behaviour the reporter still depends on. A probe where everything fails before and
passes after says nothing about what was preserved.

Two assertions were vacuous when first written and were replaced before the green
run was trusted:
- the cleanup test asserted "sibling context keys survived" on a stage whose
  context had no sibling keys. It now seeds keys modelled on the reporter's real
  row (`env`, `head_sha`, `job_id`, `script`, `slug`, `steps`) and asserts each
  survives.
- the cap test asserted `buffered <= cap`, which passes even if buffering is
  entirely broken. It now asserts `dlq == n - cap` exactly, and asserts the
  injected `HandlerConfig` actually reached the handler.

That second assertion caught a real defect: `SignalStageHandler` was constructed
without `handler_config` in `queue/processor/mixins.py`, so an injected config was
silently ignored and only the environment variable worked. Fixed.

## Defects found while building

Both were on paths that had never been exercised, and both were found by
strengthening a check rather than by a failing feature.

1. **`SignalStageHandler` never received `handler_config`.** It was constructed as
   `SignalStageHandler(queue, store)` in `queue/processor/mixins.py`, so an
   injected `HandlerConfig` was silently ignored and only the environment variable
   worked. Found because the cap test asserted the config reached the handler
   instead of trusting that it had.

2. **`count_buffered_signal_stages` was broken on PostgreSQL.** It read `row[0]`
   against psycopg's `dict_row` factory, raising `KeyError: 0`. That is the
   function behind `prune-signals --dry-run` — the read-only command the reporter
   said they would run first, on the only backend they run. It passed mypy and
   ruff; its single caller was the CLI, and no test exercised counting under the
   postgres fixture until the `--status` tests were added. Fixed with the tolerant
   pattern already at `persistence/task_lease.py:147`.

The second is the sharpest instance of the rule: a check that has never run
against the real backend cannot go red, and this one did not.

## Reporter-side verification

`ci-conductor-dd94c8` captured a read-only per-row snapshot of all 105 affected
rows before this release existed — per-key md5 of every sibling value, plus
`job_id`, `head_sha` and `slug` in the clear:

| scope | rows |
|---|---|
| SUCCEEDED / TERMINAL / CANCELED (cleaned) | 98 |
| SUSPENDED / NOT_STARTED (spared) | 7 |

52 of the 105 rows carry a `secrets` key — an array of secret names resolved at
run time — which is why the sibling-survival assertion is load-bearing rather
than defensive. The 7 spared rows independently verify the `is_complete`
predicate against data this project did not construct.

## Out of scope, and explicitly not resolved by this ticket

`prune-signals` strips `_buffered_signals` and deliberately leaves every other
context key untouched. **Nothing in 0.22.0 addresses credentials in stage
context**, and the cleanup running successfully must not be read as having done so.

The Stabilize-side gap is tracked as ticket #17: stage context is persisted
verbatim with no redaction boundary (`persistence/postgres/converters.py:27`, and
the sqlite path is equivalent), the whole context is sent as a bind parameter on
every state transition so a statement log captures it, and
`tasks/http/task.py:131` documents `bearer_token` as a stage context key with a
worked example passing a literal token. A user following that documentation puts a
credential into a column this engine persists unredacted and logs in full.

That claim is "nothing stops a credential being persisted and logged", not
"credentials were exposed". During this work ci-conductor raised, and then
retracted, an alarm about live credentials in `context -> env` on 13 rows; the
values were RunFlow test-suite placeholders (a 64-character run of one repeated
character, the literal string `runflow`, a nil UUID, loopback DSNs) and
infra-manager's original all-clear was correct. This project relayed the alarm
onward without verifying it, which is recorded here because the ticket must not be
justified by a retracted incident — it stands on the source alone.
