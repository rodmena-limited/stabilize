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

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            with cls._lock:
                # Double-check locking pattern
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]

    def reset(mcs, cls: type) -> None:
        """Reset singleton instance (for testing)."""
        with mcs._lock:
            if cls in mcs._instances:
                instance = mcs._instances.pop(cls)
                if hasattr(instance, "close_all"):
                    instance.close_all()
