from __future__ import annotations
import base64
import json
import logging
import os
import re
import ssl
import time
import uuid
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
DEFAULT_RETRY_ON_STATUS = [502, 503, 504]
CHUNK_SIZE = 8192

class HTTPTask(Task):
    """
    Make HTTP requests using Python's stdlib urllib.

    Supports all standard HTTP methods, file upload/download, authentication,
    retries, and SSL/TLS configuration with zero external dependencies.

    Context Parameters:
        url (str): Request URL (required)
        method (str): HTTP method - GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
                      (default: GET)

        Request Body (mutually exclusive):
            body (str|bytes): Raw request body
            json (dict): JSON body (auto-serialized, sets Content-Type)
            form (dict): Form-encoded body (application/x-www-form-urlencoded)

        Headers & Auth:
            headers (dict): Custom request headers
            auth (tuple|list): Basic auth as [username, password]
            bearer_token (str): Bearer token for Authorization header

        File Upload (multipart/form-data):
            upload_file (str): Path to file to upload
            upload_field (str): Form field name (default: "file")
            upload_filename (str): Override filename in upload
            upload_form (dict): Additional form fields with upload

        File Download:
            download_to (str): Path to save response body (streams large files)

        Timeouts & Retries:
            timeout (int|float): Request timeout in seconds (default: 30)
            retries (int): Number of retries on transient failure (default: 0)
            retry_delay (float): Delay between retries in seconds (default: 1.0)
            retry_on_status (list[int]): Status codes to retry (default: [502, 503, 504])

        SSL/TLS:
            verify_ssl (bool): Verify SSL certificates (default: True)
            ca_cert (str): Path to CA certificate bundle
            client_cert (str): Path to client certificate for mTLS
            client_key (str): Path to client private key for mTLS

        Response Handling:
            expected_status (int|list[int]): Expected status code(s), fail if mismatch
            max_response_size (int): Max response body in bytes (default: 10MB)
            parse_json (bool): Auto-parse JSON response body (default: False)

        Error Handling:
            continue_on_failure (bool): Return failed_continue instead of terminal
            secrets (list[str]): Context keys to mask in logs

    Outputs:
        status_code (int): HTTP response status
        headers (dict): Response headers
        body (str): Response body (or file path if download_to)
        body_json (dict|list|None): Parsed JSON (if parse_json=True and valid)
        elapsed_ms (int): Request duration in milliseconds
        url (str): Final URL after redirects
        content_type (str): Response Content-Type header
        content_length (int): Response body size in bytes

    Examples:
        # Simple GET
        context = {"url": "https://api.example.com/users"}

        # POST with JSON
        context = {
            "url": "https://api.example.com/users",
            "method": "POST",
            "json": {"name": "John", "email": "john@example.com"},
            "parse_json": True,
        }

        # With authentication
        context = {
            "url": "https://api.example.com/private",
            "bearer_token": "my-api-token",
        }

        # File upload
        context = {
            "url": "https://api.example.com/upload",
            "method": "POST",
            "upload_file": "/path/to/file.pdf",
            "upload_field": "document",
        }

        # File download
        context = {
            "url": "https://example.com/large-file.zip",
            "download_to": "/tmp/downloaded.zip",
        }

        # With retries
        context = {
            "url": "https://api.example.com/flaky",
            "retries": 3,
            "retry_delay": 2.0,
        }
    """
    SUPPORTED_METHODS = frozenset({'GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS'})
