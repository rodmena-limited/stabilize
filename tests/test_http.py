"""Tests for HTTPTask."""

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


class MockHTTPHandler(BaseHTTPRequestHandler):
    """Mock HTTP server handler for tests."""

    # Class-level storage for request inspection
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


@pytest.fixture(scope="module")
def http_server():
    """Start a test HTTP server."""
    server = HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture
def task() -> HTTPTask:
    """Create HTTPTask instance."""
    return HTTPTask()


@pytest.fixture
def stage(http_server: str) -> MagicMock:
    """Create mock stage with context."""
    mock = MagicMock()
    mock.context = {"url": http_server}
    return mock


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

    def test_options_request(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test OPTIONS request."""
        stage.context = {
            "url": http_server,
            "method": "OPTIONS",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert "Allow" in result.outputs["headers"]


class TestAuthentication:
    """Test authentication methods."""

    def test_basic_auth(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test Basic authentication."""
        stage.context = {
            "url": f"{http_server}/auth/basic",
            "auth": ["user", "password"],
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Authorized"

        # Verify Authorization header
        auth_header = MockHTTPHandler.last_request["headers"].get("Authorization", "")
        assert auth_header.startswith("Basic ")
        decoded = base64.b64decode(auth_header.split(" ")[1]).decode()
        assert decoded == "user:password"

    def test_bearer_token(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test Bearer token authentication."""
        stage.context = {
            "url": f"{http_server}/auth/bearer",
            "bearer_token": "my-secret-token",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Bearer my-secret-token"


class TestCustomHeaders:
    """Test custom headers."""

    def test_custom_headers(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test sending custom headers."""
        stage.context = {
            "url": f"{http_server}/headers",
            "headers": {
                "X-Custom-Header": "custom-value",
                "X-Another-Header": "another-value",
            },
            "parse_json": True,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        headers = result.outputs["body_json"]
        assert headers.get("X-Custom-Header") == "custom-value"
        assert headers.get("X-Another-Header") == "another-value"


class TestFileOperations:
    """Test file upload and download."""

    def test_file_upload(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test file upload with multipart/form-data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("test file content")
            temp_path = f.name

        try:
            stage.context = {
                "url": f"{http_server}/upload",
                "method": "POST",
                "upload_file": temp_path,
                "upload_field": "document",
                "parse_json": True,
            }
            result = task.execute(stage)

            assert result.status == WorkflowStatus.SUCCEEDED
            body = result.outputs["body_json"]
            assert "multipart/form-data" in body["content_type"]
            # The body includes multipart structure - verify content length is reasonable
            assert body["body_length"] > len("test file content")
        finally:
            os.unlink(temp_path)

    def test_file_upload_with_form_fields(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test file upload with additional form fields."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("upload content")
            temp_path = f.name

        try:
            stage.context = {
                "url": f"{http_server}/upload",
                "method": "POST",
                "upload_file": temp_path,
                "upload_form": {"description": "Test upload"},
                "parse_json": True,
            }
            result = task.execute(stage)

            assert result.status == WorkflowStatus.SUCCEEDED
            # Both file and form field should be in multipart body
            body = result.outputs["body_json"]
            assert body["body_length"] > len("upload content")
        finally:
            os.unlink(temp_path)

    def test_file_download(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test downloading to file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            stage.context = {
                "url": http_server,
                "download_to": temp_path,
            }
            result = task.execute(stage)

            assert result.status == WorkflowStatus.SUCCEEDED
            assert result.outputs["body"] == temp_path

            with open(temp_path) as f:
                assert f.read() == "OK"
        finally:
            os.unlink(temp_path)


class TestErrorHandling:
    """Test error handling."""

    def test_404_error(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test handling 404 error."""
        stage.context = {"url": f"{http_server}/error/404"}
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert result.context["status_code"] == 404

    def test_500_error(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test handling 500 error."""
        stage.context = {"url": f"{http_server}/error/500"}
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert result.context["status_code"] == 500

    def test_continue_on_failure(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test continue_on_failure option."""
        stage.context = {
            "url": f"{http_server}/error/404",
            "continue_on_failure": True,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.FAILED_CONTINUE
        assert result.outputs["status_code"] == 404

    def test_missing_url(self, task: HTTPTask, stage: MagicMock) -> None:
        """Test error when URL is missing."""
        stage.context = {}
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "url" in result.context.get("error", "").lower()

    def test_invalid_method(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test error for invalid HTTP method."""
        stage.context = {
            "url": http_server,
            "method": "INVALID",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "unsupported method" in result.context.get("error", "").lower()

    def test_connection_error(self, task: HTTPTask, stage: MagicMock) -> None:
        """Test handling connection errors."""
        stage.context = {
            "url": "http://127.0.0.1:59999",  # Port likely not listening
            "timeout": 1,
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL

    def test_file_not_found_upload(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test error when upload file doesn't exist."""
        stage.context = {
            "url": f"{http_server}/upload",
            "method": "POST",
            "upload_file": "/nonexistent/file.txt",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "file not found" in result.context.get("error", "").lower()


class TestExpectedStatus:
    """Test expected status validation."""

    def test_expected_status_single(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test single expected status."""
        stage.context = {
            "url": http_server,
            "expected_status": 200,
        }
        result = task.execute(stage)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_expected_status_list(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test list of expected statuses."""
        stage.context = {
            "url": f"{http_server}/error/404",
            "expected_status": [404, 410],
        }
        result = task.execute(stage)
        assert result.status == WorkflowStatus.SUCCEEDED  # 404 is expected

    def test_unexpected_status_fails(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test failure on unexpected status."""
        stage.context = {
            "url": http_server,
            "expected_status": 201,  # We'll get 200
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "unexpected status" in result.context.get("error", "").lower()


class TestRetries:
    """Test retry logic."""

    def test_retry_on_503(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test retry on 503 status (will fail after retries)."""
        stage.context = {
            "url": f"{http_server}/error/503",
            "retries": 2,
            "retry_delay": 0.1,
        }
        result = task.execute(stage)

        # Still fails after retries
        assert result.status == WorkflowStatus.TERMINAL
        assert result.context["status_code"] == 503


class TestPlaceholderSubstitution:
    """Test placeholder substitution."""

    def test_url_placeholder(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test placeholder in URL."""
        stage.context = {
            "url": f"{http_server}/param/{{value}}",
            "value": "test123",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "param=test123"

    def test_body_placeholder(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test placeholder in body."""
        stage.context = {
            "url": f"{http_server}/echo",
            "method": "POST",
            "body": "Hello, {name}!",
            "name": "World",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Hello, World!"

    def test_bearer_token_placeholder(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test placeholder in bearer token."""
        stage.context = {
            "url": f"{http_server}/auth/bearer",
            "bearer_token": "{api_key}",
            "api_key": "secret-key-123",
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Bearer secret-key-123"


class TestResponseSizeLimits:
    """Test response size limits."""

    def test_max_response_size(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test max response size limit."""
        stage.context = {
            "url": f"{http_server}/large",
            "max_response_size": 1000,  # 1KB limit
        }
        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "max size" in result.context.get("error", "").lower()


class TestSSLConfiguration:
    """Test SSL/TLS configuration."""

    def test_verify_ssl_disabled(self, task: HTTPTask, stage: MagicMock) -> None:
        """Test that verify_ssl=False doesn't crash."""
        # This won't actually test against HTTPS without a real server,
        # but ensures the SSL context is built correctly
        stage.context = {
            "url": "http://127.0.0.1:59999",
            "verify_ssl": False,
            "timeout": 1,
        }
        result = task.execute(stage)
        # Connection will fail, but SSL context should be created
        assert result.status == WorkflowStatus.TERMINAL


class TestContentTypeGuessing:
    """Test content type guessing."""

    def test_content_type_guessing(self, task: HTTPTask) -> None:
        """Test file content type guessing."""
        assert task._guess_content_type("test.txt") == "text/plain"
        assert task._guess_content_type("test.json") == "application/json"
        assert task._guess_content_type("test.pdf") == "application/pdf"
        assert task._guess_content_type("test.png") == "image/png"
        assert task._guess_content_type("test.jpg") == "image/jpeg"
        assert task._guess_content_type("test.unknown") == "application/octet-stream"


class TestRedirects:
    """Test redirect handling."""

    def test_redirect_followed(self, task: HTTPTask, stage: MagicMock, http_server: str) -> None:
        """Test that redirects are followed."""
        stage.context = {"url": f"{http_server}/redirect"}
        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["body"] == "Redirected!"
        # Final URL should be the redirected location
        assert "/redirected" in result.outputs["url"]
