"""StabilizeRAG - RAG assistant for generating Stabilize pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from .cache import CachedEmbedding, EmbeddingCache

if TYPE_CHECKING:
    from numpy.typing import NDArray


class StabilizeRAG:
    """RAG assistant for generating Stabilize pipelines.

    Uses ragit for embeddings and LLM generation, with custom caching layer
    to persist embeddings in database.
    """

    DEFAULT_EMBEDDING_MODEL = "nomic-embed-text:latest"
    DEFAULT_LLM_MODEL = "llama3.1:70b"
    DEFAULT_CHUNK_SIZE = 512
    DEFAULT_CHUNK_OVERLAP = 50

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

    def _get_provider(self):  # type: ignore[no-untyped-def]
        """Get or create OllamaProvider."""
        if self._provider is None:
            try:
                from ragit import OllamaProvider
            except ImportError as e:
                raise ImportError(
                    "RAG support requires: pip install stabilize[rag]"
                ) from e
            self._provider = OllamaProvider()
        return self._provider

    def init(self, force: bool = False) -> int:
        """Initialize embeddings from PROMPT_TEXT + examples/.

        Args:
            force: If True, regenerate even if cache exists.

        Returns:
            Number of embeddings cached.
        """
        if self.cache.is_initialized(self.embedding_model) and not force:
            print(f"Cache already initialized for {self.embedding_model}")
            return 0

        # Load documents
        documents = self._load_documents()
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

        responses = provider.embed_batch(texts, self.embedding_model)

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

    def generate(self, prompt: str, top_k: int = 5, temperature: float = 0.3) -> str:
        """Generate pipeline code from natural language prompt.

        Args:
            prompt: Natural language description of desired pipeline.
            top_k: Number of context chunks to retrieve.
            temperature: LLM temperature for generation.

        Returns:
            Generated Python code.
        """
        if not self.cache.is_initialized(self.embedding_model):
            raise RuntimeError(
                "Run 'stabilize rag init' first to initialize embeddings"
            )

        # Load cached embeddings
        self._load_cache()

        # Get relevant context
        context = self._get_context(prompt, top_k=top_k)

        # Generate code
        system_prompt = """You are a Stabilize workflow engine expert.
Generate ONLY valid Python code that creates a working Stabilize pipeline.
The code must be complete, runnable, and use the exact patterns from the context.
Do NOT include markdown formatting, explanations, or code fences.
Output ONLY the Python code."""

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
        code = response.text.strip()
        if code.startswith("```python"):
            code = code[9:]
        if code.startswith("```"):
            code = code[3:]
        if code.endswith("```"):
            code = code[:-3]

        return code.strip()

    def _load_documents(self) -> list[dict[str, str]]:
        """Load PROMPT_TEXT + bundled examples/*.py as documents."""
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

        return docs

    def _chunk_documents(
        self, documents: list[dict[str, str]]
    ) -> list[dict[str, str | int]]:
        """Split documents into overlapping chunks."""
        try:
            from ragit import chunk_text
        except ImportError as e:
            raise ImportError(
                "RAG support requires: pip install stabilize[rag]"
            ) from e

        all_chunks = []
        for doc in documents:
            chunks = chunk_text(
                doc["content"],
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                doc_id=doc["id"],
            )
            for i, chunk in enumerate(chunks):
                all_chunks.append(
                    {
                        "doc_id": doc["id"],
                        "content": chunk.content,
                        "chunk_index": i,
                    }
                )

        return all_chunks

    def _load_cache(self) -> None:
        """Load embeddings from cache and build embedding matrix."""
        if self._cached_embeddings is not None:
            return

        self._cached_embeddings = self.cache.load(self.embedding_model)
        if not self._cached_embeddings:
            raise RuntimeError(
                f"No embeddings found for model {self.embedding_model}"
            )

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
            context_parts.append(
                f"--- {emb.doc_id} (relevance: {score:.3f}) ---\n{emb.content}"
            )

        return "\n\n".join(context_parts)
