from __future__ import annotations

import json
import re
from typing import Dict, Iterable, List, Tuple

DISPOSABLE_DOMAINS = {
    "mailinator.com",
    "guerrillamail.com",
    "10minutemail.com",
    "tempmail.com",
    "yopmail.com",
}

GENERIC_PREFIXES = {"info", "hello", "contact", "hi", "support", "team", "careers", "jobs"}


def filter_emails_basic(emails: List[str], domain: str | None) -> List[str]:
    out = []
    for e in emails:
        if "@" not in e:
            continue
        local, dom = e.split("@", 1)
        if dom.lower() in DISPOSABLE_DOMAINS:
            continue
        out.append(e.strip())
    return out


def dedupe_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def assess_confidence(
    emails: List[str],
    hunter_verification: Dict[str, dict],
    emails_by_source: Dict[str, str],
) -> Tuple[str, str]:
    """
    Returns (confidence, extraction_method)
    """
    # High if any Hunter says valid
    for e in emails:
        hv = hunter_verification.get(e) if hunter_verification else None
        if hv and hv.get("result") == "valid":
            return "high", emails_by_source.get(e, "page")

    # High if Apollo provided an email (assuming Apollo tends to be precise)
    for e in emails:
        if emails_by_source.get(e) == "apollo":
            return "high", "apollo"

    # Medium if on-page and not generic, or looks like firstname.lastname@
    for e in emails:
        local = e.split("@", 1)[0].lower()
        if emails_by_source.get(e) == "page":
            if "." in local and all(part for part in local.split(".")):
                return "medium", "page"
            if local not in GENERIC_PREFIXES:
                return "medium", "page"

    # Else low
    if emails:
        method = emails_by_source.get(emails[0], "page")
    else:
        method = "serp"
    return "low", method
