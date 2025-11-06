from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

from .utils import RateLimiter, fetch_json


APOLLO_PEOPLE_SEARCH = os.getenv("APOLLO_PEOPLE_SEARCH_URL", "https://api.apollo.io/v1/people/search")


async def apollo_people_for_domain(
    domain: Optional[str],
    person_hint: str,
    rate_limits: Dict[str, RateLimiter],
    http_client,
) -> List[Dict[str, Any]]:
    """
    Searches Apollo for people at a domain; includes founders/recruiters if possible.
    Returns a simplified list with name, title, email (if provided), and source meta.
    """
    key = os.getenv("APOLLO_API_KEY")
    if not key or not domain:
        return []

    payload = {
        "api_key": key,
        "page": 1,
        "q_organization_domains": domain,
        "person_titles": ["Founder", "Co-founder", "Recruiter", "Head of Talent", "Talent"],
        "per_page": 25,
    }
    if person_hint:
        payload["q_person_name"] = person_hint

    await rate_limits["apollo"].acquire()
    async for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 3)):
        with attempt:
            # Apollo's API often accepts JSON POST; if this URL or schema changes,
            # update APOLLO_PEOPLE_SEARCH via env var to the new endpoint.
            resp = await http_client.post(APOLLO_PEOPLE_SEARCH, json=payload, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
            out: List[Dict[str, Any]] = []
            for p in (data or {}).get("people", []):
                out.append(
                    {
                        "name": p.get("name"),
                        "title": p.get("title"),
                        "seniority": p.get("seniority"),
                        "email": p.get("email") or p.get("email_status") and None,  # avoid leaking unverified tokens
                        "linkedin_url": p.get("linkedin_url"),
                        "source": "apollo",
                    }
                )
            return out
    return []
