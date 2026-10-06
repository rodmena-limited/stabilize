# 72 — PostgreSQL queue ids are bigint

Ticket: #72 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- The PostgreSQL queue shall accept messages after more than 2^31-1 pushes over the database's lifetime.

## Synthesis

- Alternatives: widen to bigint [CHOSEN] vs CYCLE the sequence [REJECTED: id reuse collides with processed_messages marks keyed by those ids -> lost messages] vs uuid primary key [REJECTED: callers parse message_id as int].
- Migration 01M483DVR3DS980FHS8N754CWH (combined with #74 so the table is rewritten once). ALTER COLUMN TYPE takes ACCESS EXCLUSIVE; the queue table is normally small.

## Evidence

`probe_queue_id_capacity.py` (own throwaway database, sequence at 2147483645): 0.31.0 schema SequenceGeneratorLimitExceeded; migrated schema pushes, polls and dead-letters id 2147483648. Least-privilege probe still passes after the migration.
