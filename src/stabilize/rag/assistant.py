"""StabilizeRAG - RAG assistant for generating Stabilize pipelines."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import numpy as np

from .cache import CachedEmbedding, EmbeddingCache

if TYPE_CHECKING:
    from numpy.typing import NDArray


class ChunkDict(TypedDict):
    """Type for document chunk dictionary."""

    doc_id: str
    content: str
    chunk_index: int


# Load .env file if present (ragit does this too, but ensure it's loaded early)
try:
    from dotenv import load_dotenv

    _env_path = Path.cwd() / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass  # dotenv not required if env vars are set directly


class StabilizeRAG:
    """RAG assistant for generating Stabilize pipelines.

    Uses ragit for embeddings and LLM generation, with custom caching layer
    to persist embeddings in database.

    Configuration:
        - LLM generation uses ollama.com cloud (requires OLLAMA_API_KEY)
        - Embeddings use local Ollama (ollama.com doesn't support embeddings)

    Environment Variables:
        OLLAMA_API_KEY: Required API key for ollama.com cloud
        OLLAMA_BASE_URL: Override LLM URL (default: https://ollama.com)
        OLLAMA_EMBEDDING_URL: Override embedding URL (default: http://localhost:11434)
    """

    # Default URLs
    DEFAULT_LLM_URL = "https://ollama.com"
    DEFAULT_EMBEDDING_URL = "http://localhost:11434"

    # Default models
    DEFAULT_EMBEDDING_MODEL = "nomic-embed-text:latest"
    DEFAULT_LLM_MODEL = "qwen3-vl:235b"

    # Chunking defaults
    DEFAULT_CHUNK_SIZE = 512
    DEFAULT_CHUNK_OVERLAP = 100  # Increased overlap for better context continuity
    DEFAULT_TOP_K = 10  # Retrieve more context chunks for better accuracy

    def __init__(
        self,
        cache: EmbeddingCache,
        embedding_model: str | None = None,
        llm_model: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ):
        self.cache = cache
        self.embedding_model = embedding_model or self.DEFAULT_EMBEDDING_MODEL
        self.llm_model = llm_model or self.DEFAULT_LLM_MODEL
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        # Lazily initialized
        self._provider = None
        self._cached_embeddings: list[CachedEmbedding] | None = None
        self._embedding_matrix: NDArray[np.float64] | None = None

    def _get_provider(self) -> Any:
        """Get or create OllamaProvider with cloud LLM and local embeddings."""
        if self._provider is None:
            try:
                from ragit import OllamaProvider  # type: ignore[import-untyped]
            except ImportError as e:
                raise ImportError("RAG support requires: pip install stabilize[rag]") from e

            # Get configuration from environment or use defaults
            llm_url = os.environ.get("OLLAMA_BASE_URL", self.DEFAULT_LLM_URL)
            embedding_url = os.environ.get("OLLAMA_EMBEDDING_URL", self.DEFAULT_EMBEDDING_URL)
            api_key = os.environ.get("OLLAMA_API_KEY")

            # Validate API key if using cloud
            if "ollama.com" in llm_url and not api_key:
                raise RuntimeError(
                    "OLLAMA_API_KEY environment variable is required for ollama.com.\n"
                    "Set it in your .env file or export it:\n"
                    "  export OLLAMA_API_KEY=your_api_key"
                )

            self._provider = OllamaProvider(
                base_url=llm_url,
                embedding_url=embedding_url,
                api_key=api_key,
            )
        return self._provider

    def init(self, force: bool = False, additional_paths: list[str] | None = None) -> int:
        """Initialize embeddings from PROMPT_TEXT + examples/ + additional context.

        Args:
            force: If True, regenerate even if cache exists.
            additional_paths: Optional list of file/directory paths to include as
                additional training context.

        Returns:
            Number of embeddings cached.
        """
        if self.cache.is_initialized(self.embedding_model) and not force:
            print(f"Cache already initialized for {self.embedding_model}")
            return 0

        # Load documents
        documents = self._load_documents(additional_paths)
        if not documents:
            raise RuntimeError("No documents found to index")

        print(f"Loaded {len(documents)} documents")

        # Chunk documents
        chunks = self._chunk_documents(documents)
        print(f"Created {len(chunks)} chunks")

        # Generate embeddings
        print("Generating embeddings...")
        provider = self._get_provider()
        texts = [chunk["content"] for chunk in chunks]

        try:
            responses = provider.embed_batch(texts, self.embedding_model)
        except ConnectionError as e:
            embedding_url = os.environ.get("OLLAMA_EMBEDDING_URL", self.DEFAULT_EMBEDDING_URL)
            raise RuntimeError(
                f"Cannot connect to Ollama for embeddings at {embedding_url}\n\n"
                "Embeddings require a local Ollama instance (ollama.com doesn't support embeddings).\n\n"
                "To fix this:\n"
                "  1. Install Ollama: https://ollama.com/download\n"
                "  2. Start Ollama: ollama serve\n"
                "  3. Pull embedding model: ollama pull nomic-embed-text\n\n"
                "Or set OLLAMA_EMBEDDING_URL to point to your Ollama instance."
            ) from e

        # Build cached embeddings
        cached = []
        for i, (chunk, response) in enumerate(zip(chunks, responses)):
            cached.append(
                CachedEmbedding(
                    doc_id=chunk["doc_id"],
                    content=chunk["content"],
                    embedding=list(response.embedding),
                    embedding_model=self.embedding_model,
                    chunk_index=chunk["chunk_index"],
                )
            )

        # Store in cache
        self.cache.store(cached)
        print(f"Cached {len(cached)} embeddings")

        return len(cached)

    def generate(self, prompt: str, top_k: int | None = None, temperature: float = 0.3) -> str:
        """Generate pipeline code from natural language prompt.

        Args:
            prompt: Natural language description of desired pipeline.
            top_k: Number of context chunks to retrieve (default: 10).
            temperature: LLM temperature for generation.

        Returns:
            Generated Python code.
        """
        if top_k is None:
            top_k = self.DEFAULT_TOP_K

        if not self.cache.is_initialized(self.embedding_model):
            raise RuntimeError("Run 'stabilize rag init' first to initialize embeddings")

        # Load cached embeddings
        self._load_cache()

        # Get relevant context
        context = self._get_context(prompt, top_k=top_k)

        # Generate code
        system_prompt = """You are a Stabilize workflow engine expert.
Generate ONLY valid Python code that creates a working Stabilize pipeline.

CRITICAL RULES:
1. Use SIMPLIFIED imports from stabilize (not from sub-modules)
2. Use BUILT-IN tasks (ShellTask, PythonTask, DockerTask, HTTPTask) - do NOT define your own
3. For PythonTask, use context with "script" key containing inline Python code

=== IMPORTS (use these simplified imports) ===
from stabilize import (
    Workflow, StageExecution, TaskExecution, WorkflowStatus,
    Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore,
    TaskRegistry, ShellTask, PythonTask, DockerTask, HTTPTask,
    StartWorkflowHandler, StartStageHandler, StartTaskHandler,
    RunTaskHandler, CompleteTaskHandler, CompleteStageHandler,
    CompleteWorkflowHandler,
)

=== BUILT-IN TASKS (use these, do NOT define your own) ===
# ShellTask: context={"command": "echo hello"}
# PythonTask: context={"script": "RESULT = 1+1", "inputs": {...}}
# DockerTask: context={"action": "run", "image": "alpine", "command": "echo hi"}
# HTTPTask: context={"url": "https://api.example.com", "method": "GET"}

=== PYTHONTASK PATTERN (uses script/INPUT/RESULT) ===
StageExecution(
    ref_id="1",
    type="python",
    name="Calculate",
    context={
        "script": \"\"\"
result = sum(INPUT["numbers"])
RESULT = {"sum": result}
\"\"\",
        "inputs": {"numbers": [1, 2, 3]}
    },
    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
)

=== SETUP PATTERN ===
store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
queue._create_table()

registry = TaskRegistry()
registry.register("shell", ShellTask)
registry.register("python", PythonTask)
registry.register("docker", DockerTask)

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

=== EXECUTION ===
store.store(workflow)
orchestrator.start(workflow)
processor.process_all(timeout=30.0)
result = store.retrieve(workflow.id)

Output ONLY valid Python code. No markdown, no explanations."""

        provider = self._get_provider()
        response = provider.generate(
            prompt=f"""Based on the following reference documentation and examples:

{context}

Generate a complete, runnable Python script that: {prompt}

Remember: Output ONLY valid Python code, no markdown, no explanations.""",
            model=self.llm_model,
            system_prompt=system_prompt,
            temperature=temperature,
        )

        # Clean up response (remove any markdown if present)
        code: str = response.text.strip()
        if code.startswith("```python"):
            code = code[9:]
        if code.startswith("```"):
            code = code[3:]
        if code.endswith("```"):
            code = code[:-3]

        return code.strip()

    def _load_documents(self, additional_paths: list[str] | None = None) -> list[dict[str, str]]:
        """Load PROMPT_TEXT + bundled examples/*.py + additional context as documents.

        Args:
            additional_paths: Optional list of file/directory paths to include as
                additional training context.

        Returns:
            List of documents with 'id' and 'content' keys.
        """
        from stabilize.cli import PROMPT_TEXT

        docs = [{"id": "prompt_reference", "content": PROMPT_TEXT}]

        # Try bundled examples from package
        try:
            from importlib.resources import files

            examples_pkg = files("stabilize.examples")
            for item in examples_pkg.iterdir():
                if item.name.endswith(".py") and item.name != "__init__.py":
                    content = item.read_text()
                    docs.append({"id": item.name[:-3], "content": content})
        except (TypeError, FileNotFoundError, ModuleNotFoundError):
            pass

        # Fallback: try local examples/ directory (for development)
        examples_dir = Path(__file__).parent.parent.parent.parent / "examples"
        if examples_dir.exists():
            for py_file in examples_dir.glob("*.py"):
                if py_file.name == "__init__.py":
                    continue
                # Skip if already loaded from package
                doc_id = py_file.stem
                if any(d["id"] == doc_id for d in docs):
                    continue
                content = py_file.read_text()
                docs.append({"id": doc_id, "content": content})

        # Load additional context from user-provided paths
        if additional_paths:
            for path_str in additional_paths:
                path = Path(path_str)
                if path.is_file():
                    # Single file
                    try:
                        content = path.read_text()
                        docs.append({"id": f"additional:{path.name}", "content": content})
                    except Exception as e:
                        print(f"Warning: Could not read {path}: {e}")
                elif path.is_dir():
                    # Directory - load all .py files recursively
                    for py_file in path.rglob("*.py"):
                        if py_file.name == "__init__.py":
                            continue
                        try:
                            content = py_file.read_text()
                            rel_path = py_file.relative_to(path)
                            docs.append({"id": f"additional:{rel_path}", "content": content})
                        except Exception as e:
                            print(f"Warning: Could not read {py_file}: {e}")
                else:
                    print(f"Warning: Path does not exist: {path}")

        return docs

    def _chunk_documents(self, documents: list[dict[str, str]]) -> list[ChunkDict]:
        """Split documents into overlapping chunks."""
        try:
            from ragit import chunk_text
        except ImportError as e:
            raise ImportError("RAG support requires: pip install stabilize[rag]") from e

        all_chunks: list[ChunkDict] = []
        for doc in documents:
            chunks = chunk_text(
                doc["content"],
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                doc_id=doc["id"],
            )
            for i, chunk in enumerate(chunks):
                all_chunks.append(
                    ChunkDict(
                        doc_id=doc["id"],
                        content=chunk.content,
                        chunk_index=i,
                    )
                )

        return all_chunks

    def _load_cache(self) -> None:
        """Load embeddings from cache and build embedding matrix."""
        if self._cached_embeddings is not None:
            return

        self._cached_embeddings = self.cache.load(self.embedding_model)
        if not self._cached_embeddings:
            raise RuntimeError(f"No embeddings found for model {self.embedding_model}")

        # Build normalized embedding matrix for fast similarity search
        embeddings = [e.embedding for e in self._cached_embeddings]
        matrix = np.array(embeddings, dtype=np.float64)

        # Normalize for cosine similarity via dot product
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)  # Avoid division by zero
        self._embedding_matrix = matrix / norms

    def _get_context(self, query: str, top_k: int = 5) -> str:
        """Retrieve relevant context for a query."""
        if self._cached_embeddings is None or self._embedding_matrix is None:
            raise RuntimeError("Cache not loaded")

        # Get query embedding
        provider = self._get_provider()
        response = provider.embed(query, self.embedding_model)
        query_embedding = np.array(response.embedding, dtype=np.float64)

        # Normalize query
        query_norm = np.linalg.norm(query_embedding)
        if query_norm > 0:
            query_embedding = query_embedding / query_norm

        # Cosine similarity via dot product (embeddings are pre-normalized)
        similarities = self._embedding_matrix @ query_embedding

        # Get top-k indices
        if top_k >= len(similarities):
            top_indices = np.argsort(similarities)[::-1]
        else:
            # Use argpartition for O(n) partial sort
            top_indices = np.argpartition(similarities, -top_k)[-top_k:]
            top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]

        # Build context string
        context_parts = []
        for idx in top_indices:
            emb = self._cached_embeddings[idx]
            score = similarities[idx]
            context_parts.append(f"--- {emb.doc_id} (relevance: {score:.3f}) ---\n{emb.content}")

        return "\n\n".join(context_parts)
