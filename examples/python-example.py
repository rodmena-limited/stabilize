#!/usr/bin/env python3
"""
Python Script Example - Demonstrates executing Python code with Stabilize.

This example shows how to:
1. Create a custom Task that executes Python scripts
2. Run inline Python code or external script files
3. Pass inputs and capture outputs
4. Build data processing pipelines

Requirements:
    None (uses subprocess from standard library)

Run with:
    python examples/python-example.py
"""

import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import StageExecution, TaskExecution, Workflow
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.orchestrator import Orchestrator
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.queue import Queue
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.tasks.interface import Task
from stabilize.tasks.registry import TaskRegistry
from stabilize.tasks.result import TaskResult

# =============================================================================
# Custom Task: PythonTask
# =============================================================================


class PythonTask(Task):
    """
    Execute Python code.

    Context Parameters:
        script: Inline Python code to execute (string)
        script_file: Path to Python script file (alternative to script)
        args: Command line arguments as list (optional)
        inputs: Input variables as dict, available as INPUT in script (optional)
        python_path: Python interpreter path (default: current interpreter)
        timeout: Execution timeout in seconds (default: 60)

    Outputs:
        stdout: Standard output
        stderr: Standard error
        exit_code: Process exit code
        result: Value of RESULT variable if set in script

    Notes:
        - Scripts can access INPUT dict for inputs
        - Scripts should set RESULT variable for return value
        - RESULT must be JSON-serializable
    """

    # Wrapper template that handles INPUT/RESULT
    WRAPPER_TEMPLATE = """
import json
import sys

# Input data
try:
    INPUT = json.loads('''{inputs}''')
except json.JSONDecodeError:
    # Fallback for simple cases or empty
    INPUT = {{}}

# User script
{script}

# Output result if set
if 'RESULT' in dir():
    print("__RESULT_START__")
    print(json.dumps(RESULT))
    print("__RESULT_END__")
"""

    def execute(self, stage: StageExecution) -> TaskResult:
        script = stage.context.get("script")
        script_file = stage.context.get("script_file")
        args = stage.context.get("args", [])

        # Merge inputs with stage context (which contains upstream outputs)
        # 1. Start with stage context (excludes script, args, etc to keep it clean?)
        # 2. Update with explicit inputs
        base_context = {
            k: v
            for k, v in stage.context.items()
            if k not in ("script", "script_file", "args", "inputs", "python_path", "timeout")
        }
        explicit_inputs = stage.context.get("inputs", {})
        inputs = {**base_context, **explicit_inputs}

        python_path = stage.context.get("python_path", sys.executable)
        timeout = stage.context.get("timeout", 60)

        if not script and not script_file:
            return TaskResult.terminal(error="Either 'script' or 'script_file' must be specified")

        if script and script_file:
            return TaskResult.terminal(error="Cannot specify both 'script' and 'script_file'")

        # Handle script file
        if script_file:
            script_path = Path(script_file)
            if not script_path.exists():
                return TaskResult.terminal(error=f"Script file not found: {script_file}")
            script = script_path.read_text()

        # At this point, script is guaranteed to be a string (validated above)
        assert script is not None
        print(f"  [PythonTask] Executing script ({len(script)} chars)")

        # Create wrapped script
        wrapped_script = self.WRAPPER_TEMPLATE.format(
            inputs=json.dumps(inputs),
            script=script,
        )

        # Write to temp file and execute
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
            tmp.write(wrapped_script)
            tmp.flush()
            tmp_path = tmp.name

        try:
            cmd = [python_path, tmp_path] + list(args)
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            stdout = result.stdout
            stderr = result.stderr
            exit_code = result.returncode

            # Extract RESULT if present
            script_result = None
            if "__RESULT_START__" in stdout:
                start = stdout.index("__RESULT_START__") + len("__RESULT_START__\n")
                end = stdout.index("__RESULT_END__")
                result_json = stdout[start:end].strip()
                try:
                    script_result = json.loads(result_json)
                except json.JSONDecodeError:
                    script_result = result_json

                # Clean stdout
                stdout = (
                    stdout[: stdout.index("__RESULT_START__")]
                    + stdout[stdout.index("__RESULT_END__") + len("__RESULT_END__\n") :]
                ).strip()

            outputs = {
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": exit_code,
                "result": script_result,
            }

            if exit_code == 0:
                print(f"  [PythonTask] Success, result: {str(script_result)[:100]}")
                return TaskResult.success(outputs=outputs)
            else:
                print(f"  [PythonTask] Failed with exit code {exit_code}")
                if stderr:
                    print(f"  [PythonTask] Stderr: {stderr}")
                return TaskResult.terminal(
                    error=f"Script exited with code {exit_code}",
                    context=outputs,
                )

        except subprocess.TimeoutExpired:
            return TaskResult.terminal(error=f"Script timed out after {timeout}s")

        finally:
            Path(tmp_path).unlink(missing_ok=True)


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with PythonTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("python", PythonTask)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, task_registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


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
