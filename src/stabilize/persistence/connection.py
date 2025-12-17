from __future__ import annotations
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

class SingletonMeta(type):
    """
    Thread-safe metaclass for singleton pattern.

    Ensures only one instance of a class exists, even when accessed
    from multiple threads simultaneously.
    """
    _instances: dict[type, Any] = {}
    _lock: threading.Lock = threading.Lock()
