"""Embedding cache implementations for RAG."""

from __future__ import annotations

import json
import os
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import psycopg


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

    @abstractmethod
    def store(self, embeddings: list[CachedEmbedding]) -> None:
        """Store embeddings in cache."""
        ...

    @abstractmethod
    def load(self, embedding_model: str) -> list[CachedEmbedding]:
        """Load embeddings from cache for a specific model."""
        ...

    @abstractmethod
    def is_initialized(self, embedding_model: str) -> bool:
        """Check if cache has embeddings for the given model."""
        ...

    @abstractmethod
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

    def clear(self) -> None:
        """Clear all cached embeddings."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM rag_embeddings")
            conn.commit()


class PostgresEmbeddingCache(EmbeddingCache):
    """Store embeddings in PostgreSQL database.

    Uses the rag_embeddings table created by migration.
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._conn: psycopg.Connection[tuple[Any, ...]] | None = None

    def _get_connection(self) -> psycopg.Connection[tuple[Any, ...]]:
        """Get a database connection."""
        try:
            import psycopg as psycopg_module
        except ImportError as e:
            raise ImportError("PostgreSQL support requires: pip install stabilize[postgres]") from e

        if self._conn is None or self._conn.closed:
            self._conn = psycopg_module.connect(self.connection_string)
        return self._conn

    def store(self, embeddings: list[CachedEmbedding]) -> None:
        """Store embeddings in PostgreSQL."""
        if not embeddings:
            return

        conn = self._get_connection()
        with conn.cursor() as cur:
            # Clear existing embeddings for this model
            model = embeddings[0].embedding_model
            cur.execute(
                "DELETE FROM rag_embeddings WHERE embedding_model = %s",
                (model,),
            )

            # Insert new embeddings
            for emb in embeddings:
                cur.execute(
                    """
                    INSERT INTO rag_embeddings
                    (doc_id, content, embedding, embedding_model, chunk_index)
                    VALUES (%s, %s, %s, %s, %s)
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
        """Load embeddings from PostgreSQL."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, content, embedding, embedding_model, chunk_index
                FROM rag_embeddings
                WHERE embedding_model = %s
                ORDER BY doc_id, chunk_index
                """,
                (embedding_model,),
            )
            results = []
            for row in cur.fetchall():
                embedding_data = row[2]
                if isinstance(embedding_data, str):
                    embedding_data = json.loads(embedding_data)
                results.append(
                    CachedEmbedding(
                        doc_id=row[0],
                        content=row[1],
                        embedding=embedding_data,
                        embedding_model=row[3],
                        chunk_index=row[4],
                    )
                )
            return results

    def is_initialized(self, embedding_model: str) -> bool:
        """Check if cache has embeddings for the given model."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM rag_embeddings WHERE embedding_model = %s",
                (embedding_model,),
            )
            row = cur.fetchone()
            count: int = row[0] if row else 0
            return count > 0

    def clear(self) -> None:
        """Clear all cached embeddings."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rag_embeddings")
            conn.commit()


def get_cache(db_url: str | None = None) -> EmbeddingCache:
    """Get appropriate cache based on configuration.

    Priority:
    1. Explicit db_url parameter
    2. MG_DATABASE_URL environment variable (if postgres)
    3. Default SQLite in ~/.stabilize/embeddings.db
    """
    if db_url:
        if db_url.startswith("postgres"):
            return PostgresEmbeddingCache(db_url)
        else:
            return SqliteEmbeddingCache(db_url)

    # Check environment
    env_url = os.environ.get("MG_DATABASE_URL")
    if env_url and env_url.startswith("postgres"):
        return PostgresEmbeddingCache(env_url)

    # Default: SQLite
    return SqliteEmbeddingCache()
