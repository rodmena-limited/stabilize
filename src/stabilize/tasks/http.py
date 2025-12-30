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
