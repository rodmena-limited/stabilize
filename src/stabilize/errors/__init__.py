"""Stabilize error hierarchy.

All error classes are re-exported here for backward compatibility.
Import from ``stabilize.errors`` as before.
"""

from stabilize.errors.base import StabilizeBaseException, StabilizeError
from stabilize.errors.delivery import DeliveryContractError, DuplicateDeliveryError
from stabilize.errors.permanent import ConfigurationError, PermanentError, RecoveryError
from stabilize.errors.store_errors import is_store_unavailable
from stabilize.errors.task import TaskError, TaskNotFoundError, TaskTimeoutError
from stabilize.errors.transient import ConcurrencyError, TransientError
from stabilize.errors.utils import is_permanent, is_transient, truncate_error
from stabilize.errors.verification import TransientVerificationError, VerificationError
from stabilize.errors.workflow import (
    DeadLetterError,
    QueueError,
    StageError,
    WorkflowError,
    WorkflowNotFoundError,
)

__all__ = [
    "ConcurrencyError",
    "ConfigurationError",
    "DeadLetterError",
    "DeliveryContractError",
    "DuplicateDeliveryError",
    "DeliveryContractError",
    "DuplicateDeliveryError",
    "DeliveryContractError",
    "DuplicateDeliveryError",
    "PermanentError",
    "QueueError",
    "RecoveryError",
    "StabilizeBaseException",
    "StabilizeError",
    "StageError",
    "TaskError",
    "TaskNotFoundError",
    "TaskTimeoutError",
    "TransientError",
    "TransientVerificationError",
    "VerificationError",
    "WorkflowError",
    "WorkflowNotFoundError",
    "is_permanent",
    "is_store_unavailable",
    "is_transient",
    "truncate_error",
]
