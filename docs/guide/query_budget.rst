Database round trips per stage
==============================

Running one no-op stage on PostgreSQL, from ``store()`` through
``process_all()``, issues **87** SQL statements. Each additional stage adds
**67**. ``tests/test_query_budget.py`` fails if either number rises, and
``audit/evaluations/measure_queries_per_stage.py`` prints the per-statement
breakdown shown below.

A stage moves through seven queue messages: StartWorkflow, StartStage,
StartTask, RunTask, CompleteTask, CompleteStage and CompleteWorkflow. Each
message is polled, checked against ``processed_messages``, handled, marked
processed and deleted, so about five statements per message are queue and
deduplication bookkeeping before the handler does any work.

One stage, by statement (0.31.0)
--------------------------------

.. code-block:: text

   9  SELECT * FROM pipeline_executions WHERE id = ...
   8  UPDATE queue_messages ... (poll; the last one finds the queue empty)
   7  INSERT INTO queue_messages ...
   7  SELECT 1 FROM processed_messages WHERE message_id = ...
   7  INSERT INTO processed_messages ...
   7  DELETE FROM queue_messages WHERE id = ...
   6  SELECT * FROM stage_executions WHERE id = ...
   6  SELECT * FROM stage_executions WHERE ... (ref_id = ANY(...) OR parent_stage_id = ...)
   6  SELECT * FROM task_executions WHERE stage_id = ...
   6  UPDATE stage_executions SET status = ...
   3  INSERT INTO task_executions ... (only tasks that changed)
  15  other statements, three or fewer each

0.31.0 reduced the one-stage count from 97 to 87 and each additional stage
from 87 to 67:

- ``store_stage`` ran ``SELECT id`` before every write to choose between UPDATE
  and INSERT. It now runs the optimistic-lock UPDATE first and looks for the
  row only when that UPDATE matched nothing, to tell a missing stage (INSERT)
  from a version or phase conflict (``ConcurrencyError``).
- ``store_stage`` rewrote every task row of the stage, bumping each task's
  version, on every write. It now writes only the tasks whose state differs
  from their row, the way an ORM flushes only dirty entities. Every task write
  goes through ``store_stage``, whose stage-version check runs first in the
  same transaction, so a conflicting writer is still rejected.
- ``retrieve_stage`` loaded synthetic children through ``get_synthetic_stages``,
  which borrowed a second pool connection while the first was held, and read
  the upstream stages in a separate statement. Both now come from one statement
  on the connection already held. On a pool of size one the old path could not
  complete (``PoolTimeout``); ``tests/test_retrieve_stage_single_connection.py``
  covers it.
- ``get_upstream_stages``, ``get_downstream_stages`` and
  ``get_synthetic_stages`` read the workflow row and every stage row of the
  workflow on each call, and attached the result through a weak reference only,
  so it was garbage-collected before the call returned. They no longer load it.
  The returned stages are detached, as they already were, and carry their tasks.

Each message now reads the workflow row once. RunTask reads the stage again
after the task has run, because a cancel or a signal can land while it runs.

0.30.0 reduced the one-stage count from 110 to 97:

- Each message was marked processed twice: inside the handler's transaction and
  again by the processor afterwards. The processor now skips its mark when the
  handler's committed transaction already recorded that message. A handler
  path that does not mark still gets the processor's mark.
- ``process_all`` counted the queue before every message. It now counts only
  when no message was ready, and still stops only when the queue is empty.
