from __future__ import annotations

import asyncio
import random
import re
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import structlog
from bs4 import BeautifulSoup
from urllib import robotparser

from .extract import extract_emails_from_html
from .utils import fetch_text, same_domain, normalize_domain

logger = structlog.get_logger(__name__)

KEYWORDS = {"contact", "about", "team", "people", "career", "careers", "join", "recruit", "jobs"}


async def crawl_site_for_emails(
    domain: str,
    settings,
    http_client,
) -> Tuple[List[str], List[str]]:
    """
    BFS crawl limited to certain paths/keywords, obeying robots.txt, depth<=2, pages<=max_pages_per_site.
    Returns (emails, notes)
    """
    notes: List[str] = []
    base_url = f"https://{domain}/"
    robots_url = urljoin(base_url, "/robots.txt")

    # robots.txt
    robots = robotparser.RobotFileParser()
    try:
        txt = await fetch_text(http_client, robots_url, allow_404=True)
        if txt is not None:
            robots.parse(txt.splitlines())
            if not robots.can_fetch(settings.user_agent, base_url):
                notes.append("robots_disallow:root")
                return [], notes
        # else: no robots -> proceed
    except Exception:
        pass

    seen: Set[str] = set()
    emails: Set[str] = set()

    def enqueue_candidates(urls: List[str], q: deque):
        for u in urls:
            if u in seen:
                continue
            if not same_domain(domain, u):
                continue
            path = urlparse(u).path.lower()
            if any(k in path for k in KEYWORDS) or path in ("/", ""):
                q.append((u, 0))

    q: deque[Tuple[str, int]] = deque()
    enqueue_candidates(
        [base_url] + [urljoin(base_url, f"/{k}") for k in KEYWORDS],
        q,
    )
    seen.update(u for u, _ in q)

    pages_visited = 0
    while q and pages_visited < settings.max_pages_per_site:
        url, depth = q.popleft()
        if depth > settings.max_depth:
            continue

        # robots check
        try:
            if robots and txt is not None and not robots.can_fetch(settings.user_agent, url):
                notes.append(f"robots_disallow:{url}")
                continue
        except Exception:
            pass

        try:
            html = await fetch_text(http_client, url)
        except Exception as e:
            notes.append(f"fetch_error:{url}:{e}")
            continue

        pages_visited += 1
        emails.update(extract_emails_from_html(html, domain=domain))

        # enqueue internal keyword links
        try:
            soup = BeautifulSoup(html, "lxml")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("#") or href.startswith("mailto:"):
                    continue
                full = urljoin(url, href)
                links.append(full)
            enqueue_candidates(links, q)
            seen.update(full for full in links)
        except Exception as e:
            notes.append(f"parse_links_error:{url}:{e}")

        await asyncio.sleep(random.uniform(0.05, 0.2))  # gentle

    return sorted(emails), notes
