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
