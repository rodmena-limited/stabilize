"""HTTP request building functions."""

from __future__ import annotations

import base64
import json
import os
import uuid
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request

from stabilize.tasks.http.utils import guess_content_type, substitute_placeholders


def build_request(
    method: str,
    url: str,
    context: dict[str, Any],
    secrets: list[str],
) -> tuple[Request, bytes | None]:
    """Build urllib Request object.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        context: Request context with headers, body, auth, etc.
        secrets: List of context keys to treat as secrets

    Returns:
        Tuple of (Request object, body bytes or None)

    Raises:
        ValueError: If request configuration is invalid
        FileNotFoundError: If upload file doesn't exist
    """
    headers: dict[str, str] = {}
    body_bytes: bytes | None = None

    # Custom headers
    custom_headers = context.get("headers", {})
    for key, value in custom_headers.items():
        headers[key] = substitute_placeholders(str(value), context, secrets)

    # Authentication
    if context.get("auth"):
        auth = context["auth"]
        if isinstance(auth, (list, tuple)) and len(auth) == 2:
            credentials = f"{auth[0]}:{auth[1]}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

    if context.get("bearer_token"):
        token = substitute_placeholders(context["bearer_token"], context, secrets)
        headers["Authorization"] = f"Bearer {token}"

    # Request body
    if context.get("upload_file"):
        # Multipart file upload
        body_bytes, content_type = build_multipart(context)
        headers["Content-Type"] = content_type

    elif context.get("json") is not None:
        # JSON body
        json_body = context["json"]
        body_bytes = json.dumps(json_body).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    elif context.get("form"):
        # Form-encoded body
        form_data = context["form"]
        body_bytes = urlencode(form_data).encode("utf-8")
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

    elif context.get("body") is not None:
        # Raw body
        body = context["body"]
        if isinstance(body, str):
            body = substitute_placeholders(body, context, secrets)
            body_bytes = body.encode("utf-8")
        else:
            body_bytes = body

    # Create request
    request = Request(url, method=method, headers=headers)

    return request, body_bytes


def build_multipart(context: dict[str, Any]) -> tuple[bytes, str]:
    """Build multipart/form-data body for file upload.

    Args:
        context: Request context containing:
            - upload_file (str): Path to file to upload (required)
            - upload_field (str): Form field name (default: "file")
            - upload_filename (str): Override filename in upload
            - upload_form (dict): Additional form fields

    Returns:
        Tuple of (body bytes, Content-Type header with boundary)

    Raises:
        FileNotFoundError: If upload file doesn't exist
    """
    boundary = uuid.uuid4().hex
    body_parts: list[bytes] = []

    # Add form fields
    form_fields = context.get("upload_form", {})
    for name, value in form_fields.items():
        body_parts.append(f"--{boundary}\r\n".encode())
        body_parts.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body_parts.append(f"{value}\r\n".encode())

    # Add file
    upload_file = context["upload_file"]
    field_name = context.get("upload_field", "file")
    filename = context.get("upload_filename") or os.path.basename(upload_file)

    if ".." in upload_file:
        raise ValueError(f"Path traversal blocked in upload_file: {upload_file}")
    resolved = os.path.realpath(upload_file)
    if not os.path.isfile(resolved):
        raise FileNotFoundError(f"Upload file not found: {upload_file}")

    with open(resolved, "rb") as f:
        file_content = f.read()

    # Detect content type
    content_type = guess_content_type(filename)

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode())
    body_parts.append(f"Content-Type: {content_type}\r\n\r\n".encode())
    body_parts.append(file_content)
    body_parts.append(b"\r\n")

    # Final boundary
    body_parts.append(f"--{boundary}--\r\n".encode())

    body = b"".join(body_parts)
    return body, f"multipart/form-data; boundary={boundary}"
