from __future__ import annotations
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict
import numpy as np
from .cache import CachedEmbedding, EmbeddingCache

class ChunkDict(TypedDict):
    """Type for document chunk dictionary."""
    doc_id: str
    content: str
    chunk_index: int

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
    DEFAULT_LLM_URL = 'https://ollama.com'
    DEFAULT_EMBEDDING_URL = 'http://localhost:11434'
    DEFAULT_EMBEDDING_MODEL = 'nomic-embed-text:latest'
    DEFAULT_LLM_MODEL = 'qwen3-vl:235b'
    DEFAULT_CHUNK_SIZE = 512
    DEFAULT_CHUNK_OVERLAP = 100
    DEFAULT_TOP_K = 10
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
