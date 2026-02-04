Handlers
========

Stabilize uses a message-driven architecture where handlers process different message types
to drive workflow execution. All 12 default handlers are auto-registered by ``QueueProcessor``
when ``store`` and ``task_registry`` are provided to the constructor. Manual registration is
no longer needed for default handlers.

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
