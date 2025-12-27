"""Configuration for Highway integration.

This module provides the HighwayConfig dataclass for configuring
the connection to Highway Workflow Engine.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any


@dataclass
class HighwayConfig:
    """Configuration for Highway API connection.

    Priority: stage.context > environment variables > defaults

    Attributes:
        api_endpoint: Highway API base URL
        api_key: API key for authentication (hw_k1_... format)
        poll_interval: Time between status polls
        timeout: Maximum time to wait for workflow completion
    """

    api_endpoint: str = field(
        default_factory=lambda: os.environ.get(
            "HIGHWAY_API_ENDPOINT",
            "https://highway.solutions",
        )
    )

    api_key: str = field(default_factory=lambda: os.environ.get("HIGHWAY_API_KEY", ""))

    poll_interval: timedelta = field(default_factory=lambda: timedelta(seconds=5))

    timeout: timedelta = field(default_factory=lambda: timedelta(minutes=30))

    @classmethod
    def from_stage_context(cls, context: dict[str, Any]) -> HighwayConfig:
        """Create config with stage context overrides.

        Stage context can override:
        - highway_api_endpoint: API base URL
        - highway_api_key: API key
        - highway_poll_interval_seconds: Poll interval in seconds
        - highway_timeout_seconds: Timeout in seconds

        Args:
            context: Stage context dictionary

        Returns:
            HighwayConfig with overrides applied
        """
        config = cls()

        if "highway_api_endpoint" in context:
            config.api_endpoint = context["highway_api_endpoint"]

        if "highway_api_key" in context:
            config.api_key = context["highway_api_key"]

        if "highway_poll_interval_seconds" in context:
            config.poll_interval = timedelta(seconds=context["highway_poll_interval_seconds"])

        if "highway_timeout_seconds" in context:
            config.timeout = timedelta(seconds=context["highway_timeout_seconds"])

        return config

    def validate(self) -> list[str]:
        """Validate configuration.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if not self.api_key:
            errors.append("HIGHWAY_API_KEY not configured. Set environment variable or stage context.")

        if not self.api_endpoint:
            errors.append("HIGHWAY_API_ENDPOINT not configured. Set environment variable or stage context.")

        return errors
