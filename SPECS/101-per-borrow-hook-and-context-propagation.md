# 101 — One pool can serve many tenants: a per-borrow hook, a reset hook, and the caller's context carried into engine threads

Ticket: issuedb #101. Requested by casebound-audit (2026-10-30).

## Problem
Casebound sets its tenant with a GUC and wants one engine pool to serve many tenants. The engine borrows connections in 74 places (reads as well as transactions) and runs work on threads it creates itself, so a caller can neither run code on each borrowed connection nor make its own context (which tenant) visible to the engine's threads.

## EARS spec
- R1. Where PoolOptions.on_borrow is set, the engine shall call it with the connection each time it borrows one from a pool, before any engine statement on that borrow, for every borrow site (reads and transactions).
- R2. If on_borrow raises, then the engine shall return the connection to the pool and propagate the error, and shall not run its own statements on that connection.
- R3. Where PoolOptions.reset is set, the engine shall pass it to psycopg_pool.ConnectionPool(reset=...), so it runs when a connection is returned.
- R4. on_borrow and reset shall be part of the pool key.
- R5. The engine shall run every thread it starts and every callable it submits to an executor (queue processor poll, recovery, handlers, retention, lock heartbeat, event bus, subscriptions, finalizers, bulkheads) in a copy of the contextvars context of the code that started or submitted it.
- R6. With one pool, an on_borrow that sets a session GUC from a caller contextvar, a reset that clears it, restrictive RLS on the engine tables keyed on that GUC, and one QueueProcessor per tenant started under that tenant's context: each tenant's workflow shall reach SUCCEEDED, neither tenant shall see the other's rows (checked both ways), and the rows shall exist when RLS is bypassed.
- R7. Where on_borrow is not set, borrowing shall behave exactly as in 0.33.2.

## Technical problems
1. A hook on every borrow from a shared psycopg pool.
2. Carrying caller context into engine-created threads.
3. Multi-tenant RLS on one pool, including queue polling under the policy.

## Solution domains
- psycopg_pool: ConnectionPool.connection() as the single borrow path; `reset` callback on return.
- contextvars.copy_context().run for threads and executors (Python docs, contextvars).
- PostgreSQL row-level security with current_setting() policies.

## Alternatives
- A ConnectionPool subclass whose connection() runs on_borrow after the borrow [CHOSEN]: every engine borrow already goes through pool.connection(), so one place covers all 74 sites.
- Per-call hooks at store.transaction() and the queue only [REJECTED: misses reads; RLS applies to every read].
- psycopg_pool's check for this [REJECTED: check must leave the connection idle and is for health, not session state; #100 exposes it for that purpose].
- A transaction-start hook [REJECTED: psycopg has none; a borrow may span several transactions, so the documented pattern is a session-level setting on borrow plus reset on return].
- Tenant carried on the workflow with a pool per tenant [REJECTED: larger API change; one processor per tenant with context propagation covers the request].
