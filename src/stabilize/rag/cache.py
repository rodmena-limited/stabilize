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
