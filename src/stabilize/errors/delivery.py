"""Errors of the message-delivery contract: one marking transaction per delivery, pushes beside it."""

from __future__ import annotations

from stabilize.errors.base import StabilizeError


class DuplicateDeliveryError(StabilizeError):
    """The delivered message was already marked processed, so this delivery's transaction rolled back."""

    code: int = 340

    def __init__(self, message_id: str, *, cause: Exception | None = None) -> None:
        super().__init__(f"message {message_id} was already processed by another delivery", cause=cause)
        self.message_id = message_id


class DeliveryContractError(StabilizeError):
    """A delivery committed a push outside the transaction that marked its message."""

    code: int = 341
