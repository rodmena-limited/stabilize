from stabilize import (
    Orchestrator,
    PythonTask,
    Queue,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStore,
)
from stabilize.events import (
    SqliteEventStore,
    configure_event_sourcing,
)


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with PythonTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("python", PythonTask)

    # Enable event sourcing — all handler events are recorded automatically
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    configure_event_sourcing(event_store)

    processor = QueueProcessor(queue, store=store, task_registry=task_registry)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# =============================================================================
# Example 3: Parallel Processing
# =============================================================================


def example_parallel_processing() -> None:
    """Process data in parallel branches."""
    print("\n" + "=" * 60)
    print("Example 3: Parallel Processing")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    #      Generate
    #     /    |    \
    #  Stats  Sort  Filter
    #     \    |    /
    #       Combine

    workflow = Workflow.create(
        application="python-example",
        name="Parallel Processing",
        stages=[
            # Generate
            StageExecution(
                ref_id="generate",
                type="python",
                name="Generate Numbers",
                context={
                    "script": """
import random
random.seed(123)
numbers = [random.randint(1, 1000) for _ in range(100)]
RESULT = numbers
print(f"Generated {len(numbers)} numbers")
""",
                },
                tasks=[
                    TaskExecution.create(
                        name="Generate",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel: Statistics
            StageExecution(
                ref_id="stats",
                type="python",
                name="Calculate Statistics",
                requisite_stage_ref_ids={"generate"},
                context={
                    "script": """
numbers = INPUT.get('numbers') or INPUT.get('result')
RESULT = {
    'count': len(numbers),
    'sum': sum(numbers),
    'min': min(numbers),
    'max': max(numbers),
    'avg': sum(numbers) / len(numbers),
}
print(f"Stats: min={RESULT['min']}, max={RESULT['max']}, avg={RESULT['avg']:.2f}")
""",
                    "inputs": {"numbers": []},
                },
                tasks=[
                    TaskExecution.create(
                        name="Stats",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel: Sort
            StageExecution(
                ref_id="sort",
                type="python",
                name="Sort Numbers",
                requisite_stage_ref_ids={"generate"},
                context={
                    "script": """
numbers = INPUT.get('numbers') or INPUT.get('result')
sorted_nums = sorted(numbers)
RESULT = {
    'sorted': sorted_nums,
    'median': sorted_nums[len(sorted_nums)//2],
}
print(f"Sorted {len(sorted_nums)} numbers, median={RESULT['median']}")
""",
                    "inputs": {"numbers": []},
                },
                tasks=[
                    TaskExecution.create(
                        name="Sort",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel: Filter
            StageExecution(
                ref_id="filter",
                type="python",
                name="Filter Numbers",
                requisite_stage_ref_ids={"generate"},
                context={
                    "script": """
numbers = INPUT.get('numbers') or INPUT.get('result')
threshold = 500
above = [n for n in numbers if n > threshold]
below = [n for n in numbers if n <= threshold]
RESULT = {
    'above_threshold': len(above),
    'below_threshold': len(below),
    'threshold': threshold,
}
print(f"Above {threshold}: {len(above)}, Below: {len(below)}")
""",
                    "inputs": {"numbers": []},
                },
                tasks=[
                    TaskExecution.create(
                        name="Filter",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Combine results
            StageExecution(
                ref_id="combine",
                type="python",
                name="Combine Results",
                requisite_stage_ref_ids={"stats", "sort", "filter"},
                context={
                    "script": """
RESULT = {
    'processing': 'complete',
    'branches': ['stats', 'sort', 'filter'],
    'summary': 'All parallel branches completed successfully'
}
print("Combined results from all branches")
""",
                },
                tasks=[
                    TaskExecution.create(
                        name="Combine",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")

    for stage in result.stages:
        script_result = stage.outputs.get("result", {})
        if isinstance(script_result, dict):
            # Show key metrics
            display = {k: v for k, v in script_result.items() if k != "sorted"}
            print(f"  {stage.name}: {display}")


# =============================================================================
# Example 4: Error Handling
# =============================================================================


def example_error_handling() -> None:
    """Demonstrate error handling in Python scripts."""
    print("\n" + "=" * 60)
    print("Example 4: Error Handling")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="python-example",
        name="Error Handling",
        stages=[
            # Stage 1: Validate input (succeeds)
            StageExecution(
                ref_id="1",
                type="python",
                name="Validate Input",
                context={
                    "script": """
data = INPUT.get('data', {})

if not isinstance(data, dict):
    raise ValueError("Data must be a dictionary")

required_fields = ['name', 'value']
missing = [f for f in required_fields if f not in data]

if missing:
    raise ValueError(f"Missing required fields: {missing}")

RESULT = {'valid': True, 'data': data}
print("Validation passed")
""",
                    "inputs": {"data": {"name": "test", "value": 42}},
                },
                tasks=[
                    TaskExecution.create(
                        name="Validate",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Stage 2: Process with try/except
            StageExecution(
                ref_id="2",
                type="python",
                name="Safe Processing",
                requisite_stage_ref_ids={"1"},
                context={
                    "script": """
try:
    # Simulate processing that might fail
    value = INPUT['value']
    result = 100 / value  # Would fail if value is 0

    RESULT = {
        'success': True,
        'result': result,
        'error': None
    }
    print(f"Processing succeeded: {result}")

except ZeroDivisionError as e:
    RESULT = {
        'success': False,
        'result': None,
        'error': str(e)
    }
    print(f"Processing failed: {e}")

except Exception as e:
    RESULT = {
        'success': False,
        'result': None,
        'error': f"Unexpected error: {e}"
    }
    print(f"Unexpected error: {e}")
""",
                    "inputs": {"value": 5},
                },
                tasks=[
                    TaskExecution.create(
                        name="Process",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")

    for stage in result.stages:
        script_result = stage.outputs.get("result", {})
        print(f"  {stage.name}: {script_result}")
