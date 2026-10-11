# 81 — Several queue tables in one store - dedup key, SQLite transactional push, recovery ownership (C1, C6, C31)

Ticket: #81. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930). C1 (postgres): with q_b on its own id sequence, workflow WB on q_b was never started (its StartWorkflow skipped as a duplicate of queue_messages id 1); shared-sequence control SUCCEEDED. C6 (sqlite): a workflow started on SqliteQueue(table_name="q_b") stayed NOT_STARTED and its message landed on queue_messages. C31 (postgres): a processor on queue_messages with recovery_application=None pushed a RunTask for a task in flight on q_b into queue_messages; filtered control did not. Probes: probe_two_queue_tables.py, probe_sqlite_txn_queue_table.py, probe_recovery_cross_table.py. Peer report v3 #1, #10, #12, item C.

## EARS spec

- The processor shall record a message as processed under a key unique across every queue table in the store: str(row id) for the table named queue_messages, "<table>:<row id>" for any other table.
- When a message is pushed through store.transaction(queue) on SQLite, the store shall insert it into queue.table_name.
- When a workflow is started through Orchestrator(queue) or promoted by a processor, the store shall record queue.table_name as the workflow's queue table (NULL means queue_messages for rows written before 1.0.0).
- A recovery sweep shall only re-issue work for workflows whose queue table is the sweeping processor's queue table.
- If a recovery sweep with recovery_application=None sees workflows of more than one application on its table, then it shall log one warning per sweep naming the applications.
- The three probes shall pass on both backends; dedup of the default table shall be byte-for-byte unchanged, so messages in flight during an upgrade are not re-executed.

## Technical problems

1. Dedup key collision between independent id sequences in one store.
2. SQLite transactional push bound to a hard-coded table.
3. Recovery ownership: a sweep must know which workflows its queue carries, because its pending-message check sees only its own table.

## Solution domains

- Idempotent-consumer pattern: the dedup key must be unique in the dedup store's scope (here: store-wide), i.e. (source, id).
- Ownership recorded at creation (the same idea as a partition key): the workflow row says which queue serves it.
- Codebase: migretti migrations; queue/postgres.py and queue/sqlite/queue.py set message.message_id = str(id) at poll.

## Alternatives

- Dedup key: "<table>:<id>" for non-default tables, unchanged for queue_messages [CHOSEN: no re-execution of in-flight default-table messages across the upgrade] vs key on the row's UUID message_id column [REJECTED: ack/extend_lock/reschedule/DLQ use the row id; 18 mark_message_processed call sites] vs prefix every table [REJECTED: every processed record of the default table would stop matching at upgrade -> redelivered messages re-execute] vs require a shared sequence (peer's workaround) [REJECTED: a deployment convention the engine does not enforce].
- Recovery ownership: queue_table column on the workflow [CHOSEN] vs check has_pending in every queue table [REJECTED: the store does not know other processors' tables] vs refuse unfiltered recovery [REJECTED: breaks single-table multi-application deployments that rely on it] vs skip workflows whose tasks the registry cannot resolve [REJECTED: two registries can share task names, duplicates remain].

## Callers (contract change)

- message.message_id is now a delivery key, not a parseable int, on non-default tables. Every int(message.message_id) site moves to one parser: queue/postgres.py ack / extend_lock / reschedule, queue/sqlite/queue.py ack / extend_lock / reschedule, queue/dlq.py, queue/sqlite/dlq.py. Dedup consumers (mixins._handle_message, txn.mark_message_processed in handlers, committed_marks) only compare the string and stay correct. To be listed exactly with grep in the implementation comment.

## Die-here / two-at-once

- Upgrade with default-table messages in flight: keys unchanged, dedup intact. Non-default tables: records written before the upgrade under bare ids stop matching; a non-default-table message redelivered across the upgrade window can run once more (changelog caveat; only deployments with separate sequences, which were already dropping messages).
- Workflow created before the column exists: queue_table NULL -> treated as queue_messages; a non-default-table deployment must set it (changelog: one UPDATE per application, or recovery_application) — to be stated in the release notes.
