from __future__ import annotations
import json
import os
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

@dataclass
class CachedEmbedding:
    """A cached embedding with its metadata."""
    doc_id: str
    content: str
    embedding: list[float]
    embedding_model: str
    chunk_index: int

class EmbeddingCache(ABC):
    """Abstract interface for embedding cache."""

    def store(self, embeddings: list[CachedEmbedding]) -> None:
        """Store embeddings in cache."""
        ...

    def load(self, embedding_model: str) -> list[CachedEmbedding]:
        """Load embeddings from cache for a specific model."""
        ...

    def is_initialized(self, embedding_model: str) -> bool:
        """Check if cache has embeddings for the given model."""
        ...

    def clear(self) -> None:
        """Clear all cached embeddings."""
        ...

class SqliteEmbeddingCache(EmbeddingCache):
    """Store embeddings in SQLite database.

    Default location: ~/.stabilize/embeddings.db
    """
    def __init__(self, db_path: str | None = None):
        if db_path is None:
            cache_dir = Path.home() / ".stabilize"
            cache_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(cache_dir / "embeddings.db")

        self.db_path = db_path
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
