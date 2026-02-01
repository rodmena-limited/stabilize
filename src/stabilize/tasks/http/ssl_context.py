"""SSL context building for HTTP requests."""

from __future__ import annotations

import ssl
from typing import Any


def build_ssl_context(context: dict[str, Any]) -> ssl.SSLContext | None:
    """Build SSL context for HTTPS requests.

    Args:
        context: Request context containing SSL configuration:
            - verify_ssl (bool): Verify SSL certificates (default: True)
            - ca_cert (str): Path to CA certificate bundle
            - client_cert (str): Path to client certificate for mTLS
            - client_key (str): Path to client private key for mTLS

    Returns:
        SSL context or None if no customization needed.
    """
    verify_ssl = context.get("verify_ssl", True)
    ca_cert = context.get("ca_cert")
    client_cert = context.get("client_cert")
    client_key = context.get("client_key")

    # No SSL customization needed
    if verify_ssl and not ca_cert and not client_cert:
        return None

    # Create context
    if verify_ssl:
        ssl_context = ssl.create_default_context()
        if ca_cert:
            ssl_context.load_verify_locations(ca_cert)
    else:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    # Client certificate (mTLS)
    if client_cert:
        ssl_context.load_cert_chain(
            certfile=client_cert,
            keyfile=client_key,
        )

    return ssl_context
