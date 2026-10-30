from __future__ import annotations

import base64
import os
import tempfile
from unittest.mock import MagicMock

from stabilize import HTTPTask, WorkflowStatus
from tests.http_task.conftest import MockHTTPHandler


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
