"""Errors built from another service's response never carry the credential this process sent (#59)."""

from __future__ import annotations

import json
import threading
import traceback
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from stabilize.redaction import redact_upstream_text

KEY = "ZZSENTINELKEY0000"


class _EchoHandler(BaseHTTPRequestHandler):
    status = 401

    def _reply(self) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length:
            self.rfile.read(length)
        body = json.dumps(
            {
                "error": "unauthorized",
                "headers_seen": {"Authorization": self.headers.get("Authorization")},
                "api_key_seen": (self.headers.get("Authorization") or "").removeprefix("Bearer "),
            }
        ).encode()
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        self._reply()

    def do_GET(self) -> None:
        self._reply()

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def echo_server() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), _EchoHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_the_upstream_really_echoes_the_key(echo_server: str) -> None:
    import urllib.error
    import urllib.request

    request = urllib.request.Request(echo_server, headers={"Authorization": f"Bearer {KEY}"})
    with pytest.raises(urllib.error.HTTPError) as raised:
        urllib.request.urlopen(request, timeout=5)
    assert KEY in raised.value.read().decode()


def test_llm_error_does_not_carry_the_api_key(echo_server: str) -> None:
    from stabilize.llm.client import ChatMessage, LLMClient, LLMError

    client = LLMClient(model="m", base_url=f"{echo_server}/v1", api_key=KEY)
    with pytest.raises(LLMError) as raised:
        client.chat([ChatMessage(role="user", content="hi")])
    assert KEY not in str(raised.value)
    assert KEY not in "".join(traceback.format_exception(raised.value))
    assert "401" in str(raised.value)
    assert "unauthorized" in str(raised.value)


@pytest.mark.parametrize("phase", ["submit", "poll"])
def test_highway_error_does_not_carry_the_api_key(echo_server: str, phase: str, caplog: Any) -> None:
    from stabilize.tasks.highway.config import HighwayConfig
    from stabilize.tasks.highway.task import HighwayTask

    config = HighwayConfig.from_stage_context({"highway_api_endpoint": echo_server, "highway_api_key": KEY})
    assert config.api_key == KEY
    task = HighwayTask()
    stage: Any = type(
        "Stage",
        (),
        {
            "context": {"highway_workflow_definition": {"name": "w"}},
            "id": "s1",
            "execution": type("Execution", (), {"id": "e1"})(),
        },
    )()
    caplog.set_level("DEBUG")
    try:
        result = (
            task._submit_workflow(stage, config) if phase == "submit" else task._poll_workflow("run-1", stage, config)
        )
    except Exception as exc:
        pytest.fail(f"{phase} raised instead of returning a result: {type(exc).__name__}")
    stored = json.dumps(result.context) + json.dumps(result.outputs)
    assert "authentication failed" in str(result.context.get("error"))
    assert KEY not in stored
    assert not any(KEY in r.getMessage() for r in caplog.records)


def test_known_secret_is_removed_anywhere() -> None:
    assert redact_upstream_text(f"x {KEY} y {KEY}", [KEY]) == "x *** y ***"


def test_credential_fields_are_masked_whoever_they_belong_to() -> None:
    text = redact_upstream_text('{"x-api-key": "someoneelsestoken", "authorization": "Basic Zm9vOmJhcg=="}', [])
    assert "someoneelsestoken" not in text
    assert "Zm9vOmJhcg==" not in text


def test_a_diagnostic_without_secrets_is_kept() -> None:
    message = '{"error": {"message": "model llama3 not found", "type": "invalid_request_error"}}'
    assert redact_upstream_text(message, [KEY]) == message


def test_upstream_text_is_capped() -> None:
    assert len(redact_upstream_text("a" * 5000, [])) < 600


def test_short_or_empty_secrets_are_ignored() -> None:
    assert redact_upstream_text("abc model", ["", None, "ab"]) == "abc model"
