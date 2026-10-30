from __future__ import annotations

from unittest.mock import MagicMock

from stabilize import HTTPTask, WorkflowStatus
from stabilize.tasks.http.utils import guess_content_type


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

    def test_content_type_guessing(self) -> None:
        """Test file content type guessing."""
        assert guess_content_type("test.txt") == "text/plain"
        assert guess_content_type("test.json") == "application/json"
        assert guess_content_type("test.pdf") == "application/pdf"
        assert guess_content_type("test.png") == "image/png"
        assert guess_content_type("test.jpg") == "image/jpeg"
        assert guess_content_type("test.unknown") == "application/octet-stream"


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
