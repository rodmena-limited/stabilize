"""
Base class for message handlers.

This module provides the MessageHandler generic class used by
the queue processor to dispatch messages to appropriate handlers.
"""

from __future__ import annotations

from typing import Generic, TypeVar

from stabilize.queue.messages import Message

M = TypeVar("M", bound=Message)


class MessageHandler(Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

    @property
    def message_type(self) -> type[M]:
        """Return the type of message this handler processes."""
        raise NotImplementedError

    def handle(self, message: M) -> None:
        """
        Handle a message.

        Args:
            message: The message to handle
        """
        raise NotImplementedError
