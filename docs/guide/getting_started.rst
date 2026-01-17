Getting Started
===============

Installation
------------

Stabilize requires Python 3.11 or later.

.. code-block:: bash

    pip install stabilize

For PostgreSQL support:

.. code-block:: bash

    pip install stabilize[postgres]

Quick Start
-----------

Create a simple "Hello World" workflow.

.. code-block:: python

    from stabilize import (
        Workflow, StageExecution, TaskExecution,
        SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator,
        Task, TaskResult, TaskRegistry,
        StartWorkflowHandler, StartStageHandler, StartTaskHandler,
        RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
        CompleteWorkflowHandler, StartWaitingWorkflowsHandler,
    )

    # 1. Define a Task
    class HelloTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            name = stage.context.get("name", "World")
            return TaskResult.success(outputs={"greeting": f"Hello, {name}!"})

    # 2. Setup Infrastructure
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    # 3. Register Handlers
    registry = TaskRegistry()
    registry.register("hello", HelloTask)

    processor = QueueProcessor(queue)
    for handler in [
        StartWorkflowHandler(queue, store),
        StartWaitingWorkflowsHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store, registry),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)

    # 4. Create & Run Workflow
    workflow = Workflow.create(
        application="demo",
        name="Hello",
        stages=[
            StageExecution(
                ref_id="1",
                type="hello",
                name="Greet",
                tasks=[TaskExecution.create("Run", "hello", stage_start=True, stage_end=True)],
                context={"name": "Stabilize"},
            )
        ]
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=5.0)

    # 5. Check Result
    result = store.retrieve(workflow.id)
    print(result.stages[0].outputs["greeting"])
    # Output: Hello, Stabilize!
