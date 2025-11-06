from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import structlog
import tldextract

logger = structlog.get_logger(__name__)


async def configure_logging_async(log_file: str) -> None:
    """
    Configure structlog with a simple JSON renderer to a file and console.
    """
    import logging
    from pythonjsonlogger import jsonlogger

    logger_ = logging.getLogger()
    logger_.setLevel(logging.INFO)

    # Console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    console_handler.setFormatter(console_formatter)

    # File
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    file_handler.setFormatter(file_formatter)

    logger_.handlers = [console_handler, file_handler]

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )


def normalize_domain(url_or_domain: str | None) -> Optional[str]:
    if not url_or_domain:
        return None
    s = url_or_domain.strip()
    if not s:
        return None
    if "://" not in s:
        s = "http://" + s
    try:
        parsed = urlparse(s)
        host = parsed.hostname or ""
        if not host:
            return None
        ext = tldextract.extract(host)
        if not ext.domain or not ext.suffix:
            return None
        # Return normalized registrable domain (e.g., sub.example.co.uk -> example.co.uk)
        return ".".join(part for part in [ext.domain, ext.suffix] if part)
    except Exception:
        return None


def same_domain(domain: str, url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
        ext = tldextract.extract(host)
        reg = ".".join([ext.domain, ext.suffix]) if ext.domain and ext.suffix else host
        return reg.lower() == domain.lower()
    except Exception:
        return False


async def fetch_text(http_client, url: str, allow_404: bool = False) -> Optional[str]:
    """
    Fetch text with minor random jitter and common headers. Returns None on 404 if allow_404 is True.
    """
    await asyncio.sleep(random.uniform(0.05, 0.18))
    r = await http_client.get(url)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "html" in ct or "json" in ct or ct == "":
        return r.text
    return r.text


async def fetch_json(http_client, url: str, params: Optional[dict] = None) -> dict:
    await asyncio.sleep(random.uniform(0.05, 0.15))
    r = await http_client.get(url, params=params)
    r.raise_for_status()
    return r.json()


class RateLimiter:
    """
    Token-bucket style: allow N requests per rolling minute.
    """
    def __init__(self, per_minute: int):
        from collections import deque
        self.per_minute = max(1, per_minute)
        self.timestamps = deque()  # type: ignore

    async def acquire(self) -> None:
        now = time.monotonic()
        # clear entries older than 60s
        while self.timestamps and now - self.timestamps[0] > 60:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.per_minute:
            wait_for = 60 - (now - self.timestamps[0]) + 0.01
            await asyncio.sleep(wait_for)
        self.timestamps.append(time.monotonic())


@dataclass
class ResultRow:
    company_name: str
    domain: str
    country: str
    linkedin_url: str
    founder_name: str
    found_emails: str
    emails_with_source: str
    hunter_verification: str
    apollo_results: str
    confidence: str
    extraction_method: str
    notes: str
    timestamp: str


def json_dumps_sane(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return "{}"
