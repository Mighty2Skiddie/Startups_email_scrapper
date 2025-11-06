from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

from .utils import RateLimiter, fetch_json


HUNTER_DOMAIN_SEARCH = os.getenv("HUNTER_DOMAIN_SEARCH_URL", "https://api.hunter.io/v2/domain-search")
HUNTER_EMAIL_VERIFY = os.getenv("HUNTER_EMAIL_VERIFY_URL", "https://api.hunter.io/v2/email-verifier")


async def hunter_domain_search(
    domain: str | None,
    rate_limits: dict,
    http_client,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Calls Hunter Domain Search for a domain; returns (emails, raw_json).
    """
    if not domain:
        return [], {}
    key = os.getenv("HUNTER_API_KEY")
    if not key:
        return [], {}
    params = {"domain": domain, "api_key": key, "limit": 100}
    await rate_limits["hunter"].acquire()

    async for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 3)):
        with attempt:
            data = await fetch_json(http_client, HUNTER_DOMAIN_SEARCH, params=params)
            emails = []
            for item in (data or {}).get("data", {}).get("emails", []):
                e = item.get("value")
                if e:
                    emails.append(e)
            return emails, data
    return [], {}


async def hunter_verify_bulk(
    emails: List[str],
    rate_limits: dict,
    http_client,
) -> Dict[str, Any]:
    """
    Verify a list of emails with Hunter Email Verifier.
    Returns dict: email -> {result, score, regexp, mx_records, ...}
    """
    key = os.getenv("HUNTER_API_KEY")
    if not key:
        return {}
    out: Dict[str, Any] = {}
    for e in emails:
        params = {"email": e, "api_key": key}
        await rate_limits["hunter"].acquire()
        async for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 3)):
            with attempt:
                data = await fetch_json(http_client, HUNTER_EMAIL_VERIFY, params=params)
                out[e] = (data or {}).get("data", {}) or {}
    return out
