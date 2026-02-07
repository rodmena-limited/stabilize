"""Task output verification logic."""

from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING, Any

from stabilize.errors import TransientVerificationError, VerificationError
from stabilize.tasks.registry import TaskNotFoundError
from stabilize.verification import (
    CallableVerifier,
    OutputVerifier,
    Verifier,
    VerifyResult,
    VerifyStatus,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.tasks.registry import TaskRegistry
    from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


def handle_verify_result(
    verify_result: VerifyResult,
    max_retries: int,
    retry_delay: float,
) -> None:
    """Handle verification result, raising appropriate exception if needed.

    Args:
        verify_result: The result from the verifier
        max_retries: Maximum retries for transient failures
        retry_delay: Delay between retries in seconds

    Raises:
        VerificationError: If verification failed permanently
        TransientVerificationError: If verification failed transiently
    """
    if verify_result.is_ok:
        return

    if verify_result.status == VerifyStatus.SKIPPED:
        # Verification was skipped - this is OK
        return

    if verify_result.is_retry:
        # Transient failure - raise retryable error
        # Include retry configuration in details for the handler
        details = verify_result.details.copy()
        details["max_retries"] = max_retries
        details["retry_delay_seconds"] = retry_delay
        raise TransientVerificationError(
            f"Verification pending retry: {verify_result.message}",
            details=details,
            retry_after=retry_delay,
        )

    # Permanent failure
    raise VerificationError(
        f"Output verification failed: {verify_result.message}",
        details=verify_result.details,
    )


def verify_task_outputs(
    stage: StageExecution,
    result: TaskResult,
    task_registry: TaskRegistry,
) -> None:
    """Verify task outputs against configured schema.

    Verification can return three terminal states:
    - OK: Verification passed, continue processing
    - FAILED: Permanent failure, raise VerificationError (terminal)
    - RETRY: Transient failure, raise TransientVerificationError (will be retried)

    The verifier's max_retries and retry_delay_seconds are used to configure
    retry behavior through the message processing system.

    Args:
        stage: The stage execution
        result: The task result containing outputs to verify
        task_registry: Registry for looking up custom verifiers

    Raises:
        VerificationError: If verification fails permanently
        TransientVerificationError: If verification fails transiently (will retry)
    """
    from stabilize.models.status import WorkflowStatus

    # Only verify success results
    if result.status != WorkflowStatus.SUCCEEDED:
        return

    verification_config = stage.context.get("verification")
    if not verification_config or not isinstance(verification_config, dict):
        return

    # Prepare a temporary stage with merged outputs for verification
    # We don't want to modify the actual stage object yet
    temp_stage = copy.copy(stage)
    temp_stage.outputs = stage.outputs.copy()
    if result.outputs and isinstance(result.outputs, dict):
        temp_stage.outputs.update(result.outputs)

    verifier_type = verification_config.get("type", "output")

    # Get retry configuration from config (with defaults)
    max_retries = verification_config.get("max_retries", 3)
    retry_delay = verification_config.get("retry_delay_seconds", 1.0)

    if verifier_type == "output":
        _verify_with_output_verifier(temp_stage, verification_config, max_retries, retry_delay)
    elif verifier_type == "callable":
        _verify_with_callable_verifier(temp_stage, verification_config, max_retries, retry_delay, task_registry)


def _verify_with_output_verifier(
    temp_stage: StageExecution,
    verification_config: dict[str, Any],
    max_retries: int,
    retry_delay: float,
) -> None:
    """Verify with OutputVerifier."""
    required_keys = verification_config.get("required_keys", [])
    type_checks_config = verification_config.get("type_checks", {})

    # Convert string type names to types
    type_map = {
        "str": str,
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "boolean": bool,
        "list": list,
        "array": list,
        "dict": dict,
        "object": dict,
    }
    type_checks: dict[str, type] = {}
    for k, v in type_checks_config.items():
        if v in type_map:
            type_checks[k] = type_map[v]

    output_verifier = OutputVerifier(required_keys=required_keys, type_checks=type_checks)
    verify_result = output_verifier.verify(temp_stage)

    handle_verify_result(verify_result, max_retries, retry_delay)


def _verify_with_callable_verifier(
    temp_stage: StageExecution,
    verification_config: dict[str, Any],
    max_retries: int,
    retry_delay: float,
    task_registry: TaskRegistry,
) -> None:
    """Verify with a custom callable verifier."""
    callable_name = verification_config.get("callable")
    if not callable_name:
        return

    # Try to get the verifier from the task registry
    try:
        verifier_or_callable = task_registry.get_verifier(callable_name)

        # Check if it's already a Verifier instance with its own retry settings
        if isinstance(verifier_or_callable, Verifier):
            # Use the verifier directly with its own retry settings
            verifier_instance = verifier_or_callable
            verify_result = verifier_instance.verify(temp_stage)
            handle_verify_result(
                verify_result,
                verifier_instance.max_retries,
                verifier_instance.retry_delay_seconds,
            )
        else:
            # Wrap callable with config defaults
            callable_verifier = CallableVerifier(
                verifier_or_callable,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
            verify_result = callable_verifier.verify(temp_stage)
            handle_verify_result(
                verify_result,
                callable_verifier.max_retries,
                callable_verifier.retry_delay_seconds,
            )
    except (TaskNotFoundError, AttributeError):
        # Verifier not found - log warning but don't fail
        logger.warning(
            "Callable verifier '%s' not found, skipping verification",
            callable_name,
        )
