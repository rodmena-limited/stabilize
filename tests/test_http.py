from __future__ import annotations
import base64
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, ClassVar
from unittest.mock import MagicMock
import pytest
from stabilize import HTTPTask, WorkflowStatus

def http_server():
    """Start a test HTTP server."""
    server = HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()

def task() -> HTTPTask:
    """Create HTTPTask instance."""
    return HTTPTask()

def stage(http_server: str) -> MagicMock:
    """Create mock stage with context."""
    mock = MagicMock()
    mock.context = {"url": http_server}
    return mock

class MockHTTPHandler(BaseHTTPRequestHandler):
    """Mock HTTP server handler for tests."""
    last_request: ClassVar[dict[str, Any]] = {}
    response_override: ClassVar[dict[str, Any] | None] = None

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress server logs during tests."""
        pass

    def _send_response(
        self,
        status: int = 200,
        body: bytes = b"",
        content_type: str = "text/plain",
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send HTTP response."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _store_request(self, body: bytes = b"") -> None:
        """Store request details for test inspection."""
        MockHTTPHandler.last_request = {
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers),
            "body": body,
        }

    def do_GET(self) -> None:
        """Handle GET requests."""
        self._store_request()

        if self.path == "/json":
            self._send_response(
                body=json.dumps({"message": "hello", "count": 42}).encode(),
                content_type="application/json",
            )
        elif self.path == "/error/404":
            self._send_response(status=404, body=b"Not Found")
        elif self.path == "/error/500":
            self._send_response(status=500, body=b"Internal Server Error")
        elif self.path == "/error/503":
            self._send_response(status=503, body=b"Service Unavailable")
        elif self.path == "/auth/basic":
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Basic "):
                self._send_response(body=b"Authorized")
            else:
                self._send_response(status=401, body=b"Unauthorized")
        elif self.path == "/auth/bearer":
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                self._send_response(body=auth.encode())
            else:
                self._send_response(status=401, body=b"Unauthorized")
        elif self.path == "/headers":
            response = json.dumps(dict(self.headers)).encode()
            self._send_response(body=response, content_type="application/json")
        elif self.path == "/large":
            # Return 1MB of data
            self._send_response(body=b"x" * (1024 * 1024))
        elif self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", f"http://{self.headers['Host']}/redirected")
            self.end_headers()
        elif self.path == "/redirected":
            self._send_response(body=b"Redirected!")
        elif self.path.startswith("/param/"):
            param = self.path.split("/param/")[1]
            self._send_response(body=f"param={param}".encode())
        else:
            self._send_response(body=b"OK")

    def do_POST(self) -> None:
        """Handle POST requests."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""
        self._store_request(body)

        if self.path == "/echo":
            content_type = self.headers.get("Content-Type", "text/plain")
            self._send_response(body=body, content_type=content_type)
        elif self.path == "/json":
            try:
                data = json.loads(body)
                response = {"received": data, "status": "ok"}
                self._send_response(
                    body=json.dumps(response).encode(),
                    content_type="application/json",
                )
            except json.JSONDecodeError:
                self._send_response(status=400, body=b"Invalid JSON")
        elif self.path == "/upload":
            # Echo back file info
            content_type = self.headers.get("Content-Type", "")
            response = {
                "content_type": content_type,
                "body_length": len(body),
                "body_preview": body[:100].decode("utf-8", errors="replace"),
            }
            self._send_response(
                body=json.dumps(response).encode(),
                content_type="application/json",
            )
        elif self.path == "/form":
            content_type = self.headers.get("Content-Type", "")
            self._send_response(
                body=json.dumps(
                    {
                        "content_type": content_type,
                        "body": body.decode("utf-8"),
                    }
                ).encode(),
                content_type="application/json",
            )
        else:
            self._send_response(status=201, body=b"Created")

    def do_PUT(self) -> None:
        """Handle PUT requests."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""
        self._store_request(body)
        self._send_response(body=b"Updated")

    def do_DELETE(self) -> None:
        """Handle DELETE requests."""
        self._store_request()
        self._send_response(status=204)

    def do_PATCH(self) -> None:
        """Handle PATCH requests."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""
        self._store_request(body)
        self._send_response(body=b"Patched")

    def do_HEAD(self) -> None:
        """Handle HEAD requests."""
        self._store_request()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", "100")
        self.send_header("X-Custom-Header", "test-value")
        self.end_headers()

    def do_OPTIONS(self) -> None:
        """Handle OPTIONS requests."""
        self._store_request()
        self.send_response(200)
        self.send_header("Allow", "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS")
        self.end_headers()

class TestBasicRequests:
    """Test basic HTTP methods."""

    def test_get_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test simple GET request."""
        stage.context = {"url": http_server}
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 200
        assert result.outputs["body"] == "OK"
        assert "elapsed_ms" in result.outputs

    def test_get_json_response(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test GET with JSON response."""
        stage.context = {
            "url": f"{http_server}/json",
            "parse_json": True,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 200
        assert result.outputs["body_json"] == {"message": "hello", "count": 42}
        assert "application/json" in result.outputs["content_type"]

    def test_post_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test POST request."""
        stage.context = {
            "url": f"{http_server}/echo",
            "method": "POST",
            "body": "test data",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 200
        assert result.outputs["body"] == "test data"

    def test_post_json(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test POST with JSON body."""
        stage.context = {
            "url": f"{http_server}/json",
            "method": "POST",
            "json": {"name": "test", "value": 123},
            "parse_json": True,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 200
        assert result.outputs["body_json"]["received"] == {"name": "test", "value": 123}
        assert MockHTTPHandler.last_request["headers"].get("Content-Type") == "application/json"

    def test_post_form(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test POST with form-encoded body."""
        stage.context = {
            "url": f"{http_server}/form",
            "method": "POST",
            "form": {"field1": "value1", "field2": "value2"},
            "parse_json": True,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        body_json = result.outputs["body_json"]
        assert "application/x-www-form-urlencoded" in body_json["content_type"]
        assert "field1=value1" in body_json["body"]
        assert "field2=value2" in body_json["body"]

    def test_put_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test PUT request."""
        stage.context = {
            "url": http_server,
            "method": "PUT",
            "body": "update data",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Updated"
        assert MockHTTPHandler.last_request["method"] == "PUT"

    def test_delete_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test DELETE request."""
        stage.context = {
            "url": http_server,
            "method": "DELETE",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 204

    def test_patch_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test PATCH request."""
        stage.context = {
            "url": http_server,
            "method": "PATCH",
            "body": "patch data",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Patched"

    def test_head_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test HEAD request."""
        stage.context = {
            "url": http_server,
            "method": "HEAD",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["status_code"] == 200
        assert result.outputs["headers"]["X-Custom-Header"] == "test-value"
        assert result.outputs["body"] == ""  # HEAD has no body
