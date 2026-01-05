# Stage Data Flow in Stabilize

This document explains how data flows between stages in a Stabilize workflow, including how to declare dependencies and access upstream outputs.

## Overview

Stabilize workflows consist of stages (nodes in a DAG) that can pass data to downstream stages. When a stage completes, its outputs are automatically made available to all dependent stages.

```
Stage A ──outputs──> Stage B ──outputs──> Stage C
              └────────────────outputs────────────┘
```

## Declaring Dependencies

Use `requisite_stage_ref_ids` to declare which stages must complete before a stage can start:

```python
from stabilize import StageExecution, TaskExecution

# Stage B depends on Stage A
StageExecution(
    ref_id="b",
    type="python",
    name="Process Data",
    requisite_stage_ref_ids={"a"},  # Must wait for stage "a" to complete
    context={...},
    tasks=[...],
)

# Stage C depends on multiple stages (join point)
StageExecution(
    ref_id="c",
    type="python",
    name="Combine Results",
    requisite_stage_ref_ids={"a", "b"},  # Waits for both "a" and "b"
    context={...},
    tasks=[...],
)
```

## Producing Outputs

### Using TaskResult

Tasks produce outputs by returning `TaskResult.success()` with an `outputs` dict:

```python
from stabilize import Task, TaskResult, StageExecution

class MyTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        # Do some work
        result = calculate_something()

        # Return outputs for downstream stages
        return TaskResult.success(outputs={
            "result": result,
            "count": 42,
            "items": ["a", "b", "c"],
        })
```

### Using PythonTask

With the built-in `PythonTask`, set the `RESULT` variable in your script:

```python
StageExecution(
    ref_id="calculate",
    type="python",
    name="Calculate Factorial",
    context={
        "script": """
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

n = INPUT.get("n", 5)
result = factorial(n)

# Set RESULT to make data available to downstream stages
RESULT = {
    "factorial": result,
    "input": n,
}
""",
        "inputs": {"n": 10},
    },
    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
)
```

### Using ShellTask

Shell tasks automatically capture stdout/stderr as outputs:

```python
StageExecution(
    ref_id="list_files",
    type="shell",
    name="List Files",
    context={"command": "ls -la /tmp"},
    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
)
# Outputs: {"stdout": "...", "stderr": "", "exit_code": 0}
```

## Accessing Upstream Outputs

When a stage starts, all ancestor outputs are automatically merged into its `context`. For PythonTask, these are available via the `INPUT` dict.

### Basic Access

```python
StageExecution(
    ref_id="process",
    type="python",
    name="Process",
    requisite_stage_ref_ids={"calculate"},  # Depends on "calculate" stage
    context={
        "script": """
# INPUT contains all outputs from upstream stages
upstream = INPUT.get("result", {})

# Access specific values
factorial = upstream.get("factorial")
original_input = upstream.get("input")

# Process and output
RESULT = {"doubled": factorial * 2}
""",
    },
    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
)
```

### Accessing Shell Output

```python
StageExecution(
    ref_id="parse",
    type="python",
    name="Parse Output",
    requisite_stage_ref_ids={"list_files"},
    context={
        "script": """
# Shell tasks put output in "stdout"
file_listing = INPUT.get("stdout", "")
lines = file_listing.strip().split("\\n")
RESULT = {"file_count": len(lines)}
""",
    },
    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
)
```

### Multiple Upstream Stages

When a stage depends on multiple upstream stages, all their outputs are merged:

```python
# Stage A outputs: {"a_result": 10}
# Stage B outputs: {"b_result": 20}

StageExecution(
    ref_id="combine",
    type="python",
    name="Combine",
    requisite_stage_ref_ids={"a", "b"},  # Depends on both
    context={
        "script": """
# Both outputs are available in INPUT
a_result = INPUT.get("a_result")  # 10
b_result = INPUT.get("b_result")  # 20

RESULT = {"sum": a_result + b_result}  # 30
""",
    },
    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
)
```

## Complete Example: Data Pipeline

```python
from stabilize import (
    Workflow, StageExecution, TaskExecution, WorkflowStatus,
    Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore,
    TaskRegistry, PythonTask,
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)

# Setup infrastructure
store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
queue._create_table()

registry = TaskRegistry()
registry.register("python", PythonTask)

processor = QueueProcessor(queue)
handlers = [
    StartWorkflowHandler(queue, store),
    StartStageHandler(queue, store),
    StartTaskHandler(queue, store),
    RunTaskHandler(queue, store, registry),
    CompleteTaskHandler(queue, store),
    CompleteStageHandler(queue, store),
    CompleteWorkflowHandler(queue, store),
]
for h in handlers:
    processor.register_handler(h)

orchestrator = Orchestrator(queue)

# Create workflow with data passing
workflow = Workflow.create(
    application="data-pipeline",
    name="Three-Stage Pipeline",
    stages=[
        # Stage 1: Generate data
        StageExecution(
            ref_id="generate",
            type="python",
            name="Generate Data",
            context={
                "script": """
import random
random.seed(42)
data = [random.randint(1, 100) for _ in range(10)]
RESULT = {"numbers": data, "count": len(data)}
print(f"Generated {len(data)} numbers")
""",
            },
            tasks=[TaskExecution.create("Generate", "python", stage_start=True, stage_end=True)],
        ),

        # Stage 2: Process data (depends on stage 1)
        StageExecution(
            ref_id="process",
            type="python",
            name="Process Data",
            requisite_stage_ref_ids={"generate"},
            context={
                "script": """
# Access output from generate stage
result = INPUT.get("result", {})
numbers = result.get("numbers", [])

# Calculate statistics
total = sum(numbers)
average = total / len(numbers) if numbers else 0

RESULT = {
    "total": total,
    "average": average,
    "min": min(numbers) if numbers else 0,
    "max": max(numbers) if numbers else 0,
}
print(f"Processed: sum={total}, avg={average:.2f}")
""",
            },
            tasks=[TaskExecution.create("Process", "python", stage_start=True, stage_end=True)],
        ),

        # Stage 3: Format report (depends on stage 2)
        StageExecution(
            ref_id="report",
            type="python",
            name="Generate Report",
            requisite_stage_ref_ids={"process"},
            context={
                "script": """
# Access output from process stage
stats = INPUT.get("result", {})

report = f'''
=== Data Report ===
Total: {stats.get("total")}
Average: {stats.get("average"):.2f}
Min: {stats.get("min")}
Max: {stats.get("max")}
'''
print(report)
RESULT = {"report": report}
""",
            },
            tasks=[TaskExecution.create("Report", "python", stage_start=True, stage_end=True)],
        ),
    ],
)

# Execute
store.store(workflow)
orchestrator.start(workflow)
processor.process_all(timeout=30.0)

# Check results
result = store.retrieve(workflow.id)
print(f"Workflow Status: {result.status}")

for stage in result.stages:
    print(f"\n{stage.name}:")
    if "result" in stage.outputs:
        print(f"  Result: {stage.outputs['result']}")
```

## Parallel Branches with Join

```
        ┌─── stats ───┐
        │             │
generate ─── sort ────── combine
        │             │
        └─── filter ──┘
```

```python
stages=[
    # Root stage
    StageExecution(
        ref_id="generate",
        type="python",
        name="Generate",
        context={"script": "RESULT = {'numbers': [1,2,3,4,5]}"},
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),

    # Parallel branch 1
    StageExecution(
        ref_id="stats",
        type="python",
        name="Calculate Stats",
        requisite_stage_ref_ids={"generate"},
        context={"script": """
numbers = INPUT.get('result', {}).get('numbers', [])
RESULT = {'sum': sum(numbers), 'avg': sum(numbers)/len(numbers)}
"""},
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),

    # Parallel branch 2
    StageExecution(
        ref_id="sort",
        type="python",
        name="Sort Numbers",
        requisite_stage_ref_ids={"generate"},
        context={"script": """
numbers = INPUT.get('result', {}).get('numbers', [])
RESULT = {'sorted': sorted(numbers)}
"""},
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),

    # Parallel branch 3
    StageExecution(
        ref_id="filter",
        type="python",
        name="Filter Numbers",
        requisite_stage_ref_ids={"generate"},
        context={"script": """
numbers = INPUT.get('result', {}).get('numbers', [])
RESULT = {'evens': [n for n in numbers if n % 2 == 0]}
"""},
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),

    # Join point - waits for all parallel branches
    StageExecution(
        ref_id="combine",
        type="python",
        name="Combine Results",
        requisite_stage_ref_ids={"stats", "sort", "filter"},  # Join!
        context={"script": """
# All three branch outputs are available
# Note: each branch's RESULT is under its own 'result' key
# You may need to access them by stage outputs structure
RESULT = {'status': 'all branches complete'}
"""},
        tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
    ),
]
```

## Output Key Conventions

| Task Type | Output Keys |
|-----------|-------------|
| **ShellTask** | `stdout`, `stderr`, `exit_code` |
| **PythonTask** | `stdout`, `stderr`, `exit_code`, `result` (from RESULT variable) |
| **HTTPTask** | `status_code`, `body`, `headers` |
| **DockerTask** | `stdout`, `stderr`, `exit_code`, `container_id` (if detached) |

## How It Works Internally

1. **Task completes** → `RunTaskHandler` stores outputs in `stage.outputs`
2. **Stage completes** → `CompleteStageHandler` triggers downstream stages via `start_next()`
3. **Downstream starts** → `StartStageHandler` calls `get_merged_ancestor_outputs()`
4. **Outputs merged** → All ancestor outputs merged into downstream stage's `context`
5. **Task executes** → PythonTask makes merged context available as `INPUT`

Key files:
- `src/stabilize/handlers/start_stage.py` - Output merging logic
- `src/stabilize/handlers/run_task.py` - Output capture
- `src/stabilize/persistence/memory.py` - `get_merged_ancestor_outputs()`
- `src/stabilize/context/stage_context.py` - Dynamic ancestor lookup

## Key Collision Warning

When multiple stages output to the same top-level key, only the last one (in topological order) survives. This commonly occurs with PythonTask since all RESULT values go under the `result` key.

```python
# PROBLEMATIC: Both stages output to "result" key
# Stage A: RESULT = {"a_value": 10}  -> outputs: {result: {"a_value": 10}}
# Stage B: RESULT = {"b_value": 20}  -> outputs: {result: {"b_value": 20}}
# Join stage only sees one result (the other is overwritten)
```

**Solutions:**
1. Use ShellTask for intermediate stages (outputs to `stdout` directly)
2. Use unique keys within each stage's RESULT
3. Chain stages sequentially so each only has one upstream

## Best Practices

1. **Use descriptive output keys** - Makes downstream access clear
2. **Document expected outputs** - Comment what each stage produces
3. **Handle missing keys gracefully** - Use `INPUT.get("key", default)`
4. **Keep outputs JSON-serializable** - Required for persistence
5. **Avoid output key collisions** - When joining parallel branches, be aware that PythonTask RESULT values go to the same `result` key
