# #46 — Bounded database round trips per stage

Ticket: issuedb #46. Released in 0.30.0.

## EARS spec

- The stabilize engine shall execute a single no-op stage on PostgreSQL within a documented number of SQL statements: 97 for one stage, and 87 for each additional stage.
- If a change raises either number, then the test suite shall fail.
- The processor shall not mark a message processed when that message's processed-mark was committed in the handler's own transaction.
- The processor shall still mark a message processed when the handler did not mark it.
- `process_all` shall count the queue only when no message was ready, and shall still stop only when the queue is empty.

## Breakdown (measured, recorder control = 1 for 1)

Before: 110 statements for one stage, marginal 97 per stage.
- `INSERT INTO processed_messages` ran 14 times for 7 messages.
- `SELECT COUNT(*) FROM queue_messages` ran 8 times.

After: 97, marginal 87. See `docs/guide/query_budget.rst`.

## Alternatives

- **Skip the processor's mark only for an id recorded by a committed transaction on this thread [CHOSEN]** vs deleting the processor's mark [REJECTED: `continue_parent_stage` and early-return paths do not mark in-transaction, and redelivery would re-run them] vs setting a flag when the handler calls mark [REJECTED: the flag would be set before commit, and a handler that swallowed a failed commit would suppress the only mark].
- **Count only when idle [CHOSEN]** vs stopping on the first empty poll [REJECTED: `process_all` would return while delayed messages are pending].
- Remaining costs (`store_stage` rewriting task rows, per-load synthetic-stage query, nine workflow reads) are not reduced here. Each changes persistence behaviour and needs its own red/green proof: ticket #54.

## Verification

- `tests/test_processed_mark_once.py`: a PostgreSQL one-stage workflow issues 7 processed inserts for 7 messages; HEAD issued 14. Unit cases cover a handler that marked, one that did not, and a stale id.
- `tests/test_query_budget.py`: counter control, 1-stage budget, marginal budget.
