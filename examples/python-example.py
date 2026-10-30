#!/usr/bin/env python3
"""
Python Script Example - Demonstrates executing Python code with Stabilize.

This example shows how to use the built-in PythonTask for:
1. Running inline Python code
2. Executing Python script files
3. Calling module functions
4. Building data processing pipelines

The built-in PythonTask runs code in an isolated subprocess with:
- INPUT dict for accessing inputs from upstream stages
- RESULT variable for returning JSON-serializable outputs
- Full timeout and error handling

Run with:
    python examples/python-example.py
"""

import logging

logging.basicConfig(level=logging.ERROR)

from python_example_pipelines import example_error_handling, example_parallel_processing, setup_pipeline_runner

from stabilize import (
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    Workflow,
)


# =============================================================================
# Example 1: Simple Calculation
# =============================================================================


def example_simple_calculation() -> None:
    """Run a simple inline Python calculation."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Calculation")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="python-example",
        name="Simple Calculation",
        stages=[
            StageExecution(
                ref_id="1",
                type="python",
                name="Calculate Fibonacci",
                context={
                    "script": """
def fib(n):
    if n <= 1:
        return n
    return fib(n-1) + fib(n-2)

n = INPUT.get('n', 10)
RESULT = {
    'n': n,
    'fibonacci': fib(n),
    'sequence': [fib(i) for i in range(n+1)]
}
print(f"Fibonacci({n}) = {fib(n)}")
""",
                    "inputs": {"n": 10},
                },
                tasks=[
                    TaskExecution.create(
                        name="Run Python",
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
    script_result = result.stages[0].outputs.get("result", {})
    print(f"Result: Fibonacci({script_result.get('n')}) = {script_result.get('fibonacci')}")
    print(f"Sequence: {script_result.get('sequence')}")


# =============================================================================
# Example 2: Data Processing Pipeline
# =============================================================================


def example_data_pipeline() -> None:
    """Sequential data processing: generate -> transform -> validate."""
    print("\n" + "=" * 60)
    print("Example 2: Data Processing Pipeline")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="python-example",
        name="Data Pipeline",
        stages=[
            # Stage 1: Generate data
            StageExecution(
                ref_id="1",
                type="python",
                name="Generate Data",
                context={
                    "script": """
import random
random.seed(42)  # Reproducible

data = [
    {'id': i, 'value': random.randint(1, 100), 'name': f'item_{i}'}
    for i in range(10)
]
RESULT = data
print(f"Generated {len(data)} records")
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
            # Stage 2: Transform data
            StageExecution(
                ref_id="2",
                type="python",
                name="Transform Data",
                requisite_stage_ref_ids={"1"},
                context={
                    "script": """
data = INPUT.get('data') or INPUT.get('result')

# Transform: double values, uppercase names
transformed = [
    {
        'id': item['id'],
        'value': item['value'] * 2,
        'name': item['name'].upper(),
        'category': 'HIGH' if item['value'] > 50 else 'LOW'
    }
    for item in data
]
RESULT = transformed
print(f"Transformed {len(transformed)} records")
""",
                    "inputs": {"data": []},  # Will be populated from stage context
                },
                tasks=[
                    TaskExecution.create(
                        name="Transform",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Stage 3: Validate and summarize
            StageExecution(
                ref_id="3",
                type="python",
                name="Validate Data",
                requisite_stage_ref_ids={"2"},
                context={
                    "script": """
data = INPUT.get('data') or INPUT.get('result')

# Validation
errors = []
for item in data:
    if item['value'] < 0:
        errors.append(f"Negative value for {item['id']}")
    if not item['name']:
        errors.append(f"Empty name for {item['id']}")

# Summary
summary = {
    'total_records': len(data),
    'high_count': sum(1 for d in data if d.get('category') == 'HIGH'),
    'low_count': sum(1 for d in data if d.get('category') == 'LOW'),
    'total_value': sum(d['value'] for d in data),
    'avg_value': sum(d['value'] for d in data) / len(data) if data else 0,
    'errors': errors,
    'valid': len(errors) == 0
}
RESULT = summary
print(f"Validation: {'PASSED' if summary['valid'] else 'FAILED'}")
print(f"Total value: {summary['total_value']}")
""",
                    "inputs": {"data": []},
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
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")

    for stage in result.stages:
        print(f"\n{stage.name}:")
        script_result = stage.outputs.get("result")
        if isinstance(script_result, dict):
            for k, v in script_result.items():
                print(f"  {k}: {v}")
        elif isinstance(script_result, list):
            print(f"  {len(script_result)} items")
        else:
            print(f"  {script_result}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Python Script Examples")
    print("=" * 60)

    example_simple_calculation()
    example_data_pipeline()
    example_parallel_processing()
    example_error_handling()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
