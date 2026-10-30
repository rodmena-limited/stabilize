from llama_example_support import setup_pipeline_runner

from stabilize import (
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    Workflow,
)


# =============================================================================
# Example 4: Sequential Processing Pipeline
# =============================================================================


def example_processing_pipeline() -> None:
    """Chain LLM calls: summarize -> translate -> format."""
    print("\n" + "=" * 60)
    print("Example 4: Processing Pipeline")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    original_text = """
    Stabilize is a lightweight Python workflow execution engine with DAG-based
    stage orchestration. It supports message-driven execution, parallel and
    sequential stages, synthetic stages for lifecycle hooks, multiple persistence
    backends including PostgreSQL and SQLite, and a pluggable task system with
    retry and timeout support.
    """

    workflow = Workflow.create(
        application="llama-example",
        name="Processing Pipeline",
        stages=[
            # Step 1: Summarize
            StageExecution(
                ref_id="1",
                type="ollama",
                name="Summarize",
                context={
                    "system": "You are a technical writer. Summarize text into bullet points.",
                    "prompt": f"Summarize this into 3 key bullet points:\n\n{original_text}",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.3,
                    "max_tokens": 150,
                },
                tasks=[
                    TaskExecution.create(
                        name="Summarize",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 2: Extract keywords
            StageExecution(
                ref_id="2",
                type="ollama",
                name="Extract Keywords",
                requisite_stage_ref_ids={"1"},
                context={
                    "system": "Extract technical keywords. Output only a comma-separated list.",
                    "prompt": f"Extract 5 technical keywords from:\n\n{original_text}",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.2,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Keywords",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 3: Generate tagline
            StageExecution(
                ref_id="3",
                type="ollama",
                name="Generate Tagline",
                requisite_stage_ref_ids={"2"},
                context={
                    "system": "You are a marketing copywriter. Be concise and catchy.",
                    "prompt": f"Create a one-line tagline (max 10 words) for this product:\n\n{original_text}",
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.8,
                    "max_tokens": 30,
                },
                tasks=[
                    TaskExecution.create(
                        name="Tagline",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=300.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print("\nPipeline Results:")
    print("-" * 40)
    for stage in result.stages:
        print(f"\n{stage.name}:")
        response = stage.outputs.get("response", "N/A")
        print(f"  {response[:200]}")


# =============================================================================
# Example 5: Parallel Generation
# =============================================================================


def example_parallel_generation() -> None:
    """Generate multiple variations in parallel."""
    print("\n" + "=" * 60)
    print("Example 5: Parallel Generation")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    base_prompt = "Write a one-sentence description of a workflow engine"

    #      Start
    #     /  |  \
    # Formal Casual Technical
    #     \  |  /
    #     Compare

    workflow = Workflow.create(
        application="llama-example",
        name="Parallel Generation",
        stages=[
            StageExecution(
                ref_id="start",
                type="ollama",
                name="Setup",
                context={
                    "prompt": "Say 'Starting parallel generation' in exactly 3 words.",
                    "model": "qwen3-coder-next:cloud",
                    "max_tokens": 10,
                },
                tasks=[
                    TaskExecution.create(
                        name="Setup",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel variations
            StageExecution(
                ref_id="formal",
                type="ollama",
                name="Formal Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a formal, professional tone.",
                    "prompt": base_prompt,
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.3,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Formal",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="casual",
                type="ollama",
                name="Casual Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a casual, friendly tone.",
                    "prompt": base_prompt,
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.7,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Casual",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="technical",
                type="ollama",
                name="Technical Style",
                requisite_stage_ref_ids={"start"},
                context={
                    "system": "Write in a technical, precise tone for developers.",
                    "prompt": base_prompt,
                    "model": "qwen3-coder-next:cloud",
                    "temperature": 0.2,
                    "max_tokens": 50,
                },
                tasks=[
                    TaskExecution.create(
                        name="Technical",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Compare
            StageExecution(
                ref_id="compare",
                type="ollama",
                name="Compare Results",
                requisite_stage_ref_ids={"formal", "casual", "technical"},
                context={
                    "prompt": "Say 'Generation complete' in exactly 2 words.",
                    "model": "qwen3-coder-next:cloud",
                    "max_tokens": 10,
                },
                tasks=[
                    TaskExecution.create(
                        name="Compare",
                        implementing_class="ollama",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=300.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print("\nGenerated Variations:")
    print("-" * 40)

    for stage in result.stages:
        if stage.ref_id in ["formal", "casual", "technical"]:
            response = stage.outputs.get("response", "N/A")
            print(f"\n{stage.name}:")
            print(f"  {response[:150]}")
