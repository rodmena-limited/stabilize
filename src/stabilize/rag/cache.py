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

    def _init_schema(self) -> None:
        """Initialize the database schema."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rag_embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    embedding TEXT NOT NULL,
                    embedding_model TEXT NOT NULL,
                    chunk_index INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(doc_id, chunk_index, embedding_model)
                )
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_rag_embeddings_model
                ON rag_embeddings(embedding_model)
            """
            )
            conn.commit()

    def store(self, embeddings: list[CachedEmbedding]) -> None:
        """Store embeddings in SQLite."""
        if not embeddings:
            return

        with self._get_connection() as conn:
            # Clear existing embeddings for this model
            model = embeddings[0].embedding_model
            conn.execute(
                "DELETE FROM rag_embeddings WHERE embedding_model = ?",
                (model,),
            )

            # Insert new embeddings
            for emb in embeddings:
                conn.execute(
                    """
                    INSERT INTO rag_embeddings
                    (doc_id, content, embedding, embedding_model, chunk_index)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        emb.doc_id,
                        emb.content,
                        json.dumps(emb.embedding),
                        emb.embedding_model,
                        emb.chunk_index,
                    ),
                )
            conn.commit()

    def load(self, embedding_model: str) -> list[CachedEmbedding]:
        """Load embeddings from SQLite."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT doc_id, content, embedding, embedding_model, chunk_index
                FROM rag_embeddings
                WHERE embedding_model = ?
                ORDER BY doc_id, chunk_index
                """,
                (embedding_model,),
            )
            results = []
            for row in cursor:
                results.append(
                    CachedEmbedding(
                        doc_id=row["doc_id"],
                        content=row["content"],
                        embedding=json.loads(row["embedding"]),
                        embedding_model=row["embedding_model"],
                        chunk_index=row["chunk_index"],
                    )
                )
            return results

    def is_initialized(self, embedding_model: str) -> bool:
        """Check if cache has embeddings for the given model."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM rag_embeddings WHERE embedding_model = ?",
                (embedding_model,),
            )
            row = cursor.fetchone()
            count: int = row[0] if row else 0
            return count > 0
