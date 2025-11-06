from __future__ import annotations

import re
from typing import Iterable, List, Optional, Set

# RFC 5322-ish email regex, pragmatic
EMAIL_RE = re.compile(
    r"""
    (?:
      [a-z0-9!#$%&'*+/=?^_`{|}~-]+
      (?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*
    )
    @
    (?:
      [a-z0-9](?:[a-z0-9-]*[a-z0-9])?
      (?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

IMAGE_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp")


def extract_emails_from_html(html: str, domain: Optional[str] = None) -> List[str]:
    """
    Extract emails from HTML/JSON/inline scripts. Filters obvious false positives.
    If domain is provided, prefer same-domain emails.
    """
    candidates = set(m.group(0).strip(".,;:") for m in EMAIL_RE.finditer(html or ""))
    filtered = set()
    for e in candidates:
        # Exclude images like name@2x.png or foo@bar.jpg etc.
        if e.lower().endswith(IMAGE_EXT):
            continue
        # Some CMS leak emails in URLs like "mailto:info@x.com"
        e = e.replace("mailto:", "").strip()
        filtered.add(e)

    if not domain:
        return sorted(filtered)

    # Prefer on-domain results first
    on = sorted([e for e in filtered if e.lower().endswith("@" + domain.lower())])
    off = sorted([e for e in filtered if e.lower() not in {x.lower() for x in on}])
    return on + off
