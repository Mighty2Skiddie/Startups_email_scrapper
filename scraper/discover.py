from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import structlog
import tldextract

from .utils import RateLimiter, normalize_domain, fetch_json, fetch_text

logger = structlog.get_logger(__name__)


async def determine_domain_for_company(
    company_name: str,
    website: str,
    linkedin_url: str,
    settings,
    http_client,
    rate_limits: Dict[str, RateLimiter],
) -> Tuple[Optional[str], str, List[str]]:
    """
    Returns: (domain, discovery_method, notes)
    """
    notes: List[str] = []

    # 1) From website column
    if website:
        dom = normalize_domain(website)
        if dom:
            return dom, "website", notes

    # 2) From LinkedIn company page (fetch public page and try to parse website)
    if linkedin_url:
        try:
            html = await fetch_text(http_client, linkedin_url)
            # LinkedIn often includes "data-test-website" or a link near "Website"
            candidates = re.findall(r'(https?://[^\s"<>]+)', html, flags=re.I)
            websites = [normalize_domain(u) for u in candidates]
            websites = [w for w in websites if w]
            if websites:
                return websites[0], "linkedin", notes
        except Exception as e:
            notes.append(f"linkedin_parse_error:{e}")

    # 3) SerpAPI fallback (if allowed)
    if settings.USE_SERPAPI and os.getenv("SERPAPI_KEY"):
        try:
            q = f'{company_name} official website'
            params = {
                "engine": "google",
                "q": q,
                "api_key": os.getenv("SERPAPI_KEY"),
                "num": "5",
            }
            await rate_limits["serpapi"].acquire()
            url = "https://serpapi.com/search.json"
            data = await fetch_json(http_client, url, params=params)
            links = []
            for res in (data or {}).get("organic_results", []):
                link = res.get("link")
                dom = normalize_domain(link)
                if dom:
                    links.append(dom)
            if links:
                return links[0], "serp", notes
        except Exception as e:
            notes.append(f"serpapi_error:{e}")

    return None, "unknown", notes
