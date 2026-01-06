"""
Resilience configuration for Stabilize.

Provides configuration for bulkheads and circuit breakers,
with support for loading from environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from fractions import Fraction


@dataclass
class BulkheadConfig:
    """Configuration for a single bulkhead."""

    max_concurrent: int = 5
    max_queue_size: int = 20
    timeout_seconds: float = 300.0


@dataclass
class ResilienceConfig:
    """
    Configuration for resilience patterns.

    Attributes:
        bulkheads: Per-task-type bulkhead configurations
        circuit_failure_threshold: Fraction of failures to trip circuit (e.g., 5/10 = 50%)
        circuit_cooldown_seconds: Seconds to wait before testing recovery
        database_url: Database connection string (from MG_DATABASE_URL)
    """

    bulkheads: dict[str, BulkheadConfig] = field(
        default_factory=lambda: {
            "shell": BulkheadConfig(max_concurrent=5),
            "python": BulkheadConfig(max_concurrent=3),
            "http": BulkheadConfig(max_concurrent=10),
            "docker": BulkheadConfig(max_concurrent=3),
            "ssh": BulkheadConfig(max_concurrent=5),
        }
    )
    circuit_failure_threshold: Fraction = field(default_factory=lambda: Fraction(5, 10))
    circuit_cooldown_seconds: float = 30.0
    database_url: str | None = None

    @classmethod
    def from_env(cls) -> ResilienceConfig:
        """
        Load configuration from environment variables.

        Environment variables:
            MG_DATABASE_URL: Database connection string
            STABILIZE_BULKHEAD_{TYPE}_MAX_CONCURRENT: Max concurrent for task type
            STABILIZE_CIRCUIT_FAILURE_THRESHOLD: Failure rate to trip (0.0-1.0)
            STABILIZE_CIRCUIT_COOLDOWN_SECONDS: Cooldown duration
        """
        # Load bulkhead configs from environment
        bulkheads = {
            "shell": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_SHELL_MAX_CONCURRENT", "5"))),
            "python": BulkheadConfig(
                max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_PYTHON_MAX_CONCURRENT", "3"))
            ),
            "http": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_HTTP_MAX_CONCURRENT", "10"))),
            "docker": BulkheadConfig(
                max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_DOCKER_MAX_CONCURRENT", "3"))
            ),
            "ssh": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_SSH_MAX_CONCURRENT", "5"))),
        }

        # Parse failure threshold as fraction
        threshold_float = float(os.environ.get("STABILIZE_CIRCUIT_FAILURE_THRESHOLD", "0.5"))
        # Convert to fraction (e.g., 0.5 -> 5/10)
        threshold_int = int(threshold_float * 10)
        circuit_failure_threshold = Fraction(threshold_int, 10)

        return cls(
            bulkheads=bulkheads,
            circuit_failure_threshold=circuit_failure_threshold,
            circuit_cooldown_seconds=float(os.environ.get("STABILIZE_CIRCUIT_COOLDOWN_SECONDS", "30")),
            database_url=os.environ.get("MG_DATABASE_URL"),
        )
