"""HTTP task constants."""

# Default settings
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
DEFAULT_RETRY_ON_STATUS = [502, 503, 504]
CHUNK_SIZE = 8192

# Supported HTTP methods
SUPPORTED_METHODS = frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"})

# Content type mappings for file uploads
CONTENT_TYPES = {
    ".txt": "text/plain",
    ".html": "text/html",
    ".htm": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json",
    ".xml": "application/xml",
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".gz": "application/gzip",
    ".tar": "application/x-tar",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".mp3": "audio/mpeg",
    ".mp4": "video/mp4",
    ".csv": "text/csv",
}
