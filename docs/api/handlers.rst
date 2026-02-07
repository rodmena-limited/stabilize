Handlers
========

Stabilize uses a message-driven architecture where handlers process different message types
to drive workflow execution. All 15 default handlers are auto-registered by ``QueueProcessor``
when ``store`` and ``task_registry`` are provided to the constructor. Manual registration is
no longer needed for default handlers.

Core Handlers
-------------

.. automodule:: stabilize.handlers.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.start_workflow
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.start_waiting_workflows
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.start_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.skip_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.cancel_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.continue_parent_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.start_task
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.run_task
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.complete_task
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.complete_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.complete_workflow
   :members:
   :undoc-members:
   :show-inheritance:

Control-Flow Pattern Handlers
-----------------------------

These handlers support advanced workflow control-flow patterns (WCP 6-43).

.. automodule:: stabilize.handlers.signal_stage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.cancel_region
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: stabilize.handlers.add_multi_instance
   :members:
   :undoc-members:
   :show-inheritance:
