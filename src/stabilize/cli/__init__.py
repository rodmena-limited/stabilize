"""Stabilize CLI for database migrations and developer tools."""

from __future__ import annotations

from stabilize.cli.main import main
from stabilize.cli.prompt_text import PROMPT_TEXT

__all__ = ["PROMPT_TEXT", "main"]
