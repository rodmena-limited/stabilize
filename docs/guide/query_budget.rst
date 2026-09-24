Database round trips per stage
==============================

Running one no-op stage on PostgreSQL, from ``store()`` through
``process_all()``, issues **97** SQL statements. Each additional stage adds
**87**. ``tests/test_query_budget.py`` fails if either number rises, and
``audit/evaluations/measure_queries_per_stage.py`` prints the per-statement
breakdown shown below.

A stage moves through seven queue messages: StartWorkflow, StartStage,
StartTask, RunTask, CompleteTask, CompleteStage and CompleteWorkflow. Each
message is polled, checked against ``processed_messages``, handled, marked
processed and deleted, so about five statements per message are queue and
deduplication bookkeeping before the handler does any work.

One stage, by statement (0.30.0)
--------------------------------

.. code-block:: text

   9  SELECT * FROM pipeline_executions WHERE id = ...
   8  UPDATE queue_messages ... (poll; the last one finds the queue empty)
   7  INSERT INTO task_executions ... (store_stage rewrites the stage's tasks)
   7  INSERT INTO queue_messages ...
   7  SELECT 1 FROM processed_messages WHERE message_id = ...
   7  INSERT INTO processed_messages ...
   7  DELETE FROM queue_messages WHERE id = ...
   7  SELECT * FROM stage_executions WHERE ... parent_stage_id = ... (synthetic stages)
   6  SELECT * FROM stage_executions WHERE id = ...
   6  SELECT * FROM task_executions WHERE stage_id = ...
   6  SELECT id FROM stage_executions WHERE id = ...
   6  UPDATE stage_executions SET status = ...
  14  other statements, three or fewer each

0.30.0 reduced the one-stage count from 110 to 97:

- Each message was marked processed twice: inside the handler's transaction and
  again by the processor afterwards. The processor now skips its mark when the
  handler's committed transaction already recorded that message. A handler
  path that does not mark still gets the processor's mark.
- ``process_all`` counted the queue before every message. It now counts only
  when no message was ready, and still stops only when the queue is empty.

Not yet reduced, and each needs its own change and proof: ``store_stage``
rewriting the task rows on every write, the synthetic-stage query on every
stage load, and the workflow row being read nine times.
