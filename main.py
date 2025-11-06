from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import pandas as pd
import structlog

from config import SETTINGS, Settings
from scraper.io import read_input_csv, write_outputs, load_checkpoint, merge_checkpoint
from scraper.discover import determine_domain_for_company
from scraper.scrape import crawl_site_for_emails
from scraper.hunter_integration import hunter_domain_search, hunter_verify_bulk
from scraper.apollo_integration import apollo_people_for_domain
from scraper.validate import (
    assess_confidence,
    dedupe_keep_order,
    filter_emails_basic,
)
from scraper.utils import configure_logging_async, ResultRow, json_dumps_sane


logger = structlog.get_logger(__name__)


async def process_company(
    row: Dict[str, Any],
    settings: Settings,
    http_client,
    rate_limits,
) -> ResultRow:
    company_name = str(row.get("company_name") or row.get("name") or "").strip()
    linkedin_url = str(row.get("linkedin") or row.get("linkedin_url") or "").strip()
    founder_name = str(row.get("founder_name") or "").strip()
    country = str(row.get("country") or "").strip()
    website = str(row.get("website") or "").strip()

    notes: List[str] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    # 1) Domain discovery
    domain, discovery_method, dd_notes = await determine_domain_for_company(
        company_name=company_name,
        website=website,
        linkedin_url=linkedin_url,
        settings=settings,
        http_client=http_client,
        rate_limits=rate_limits,
    )
    notes.extend(dd_notes)
    emails_by_source: Dict[str, str] = {}  # email -> source
    hunter_verification: Dict[str, Any] = {}
    apollo_results: List[Dict[str, Any]] = []

    # 2) Crawl & extract
    if domain:
        crawl_emails, crawl_notes = await crawl_site_for_emails(
            domain=domain,
            settings=settings,
            http_client=http_client,
        )
        for e in crawl_emails:
            emails_by_source[e] = emails_by_source.get(e, "page")
        notes.extend(crawl_notes)
    else:
        notes.append("No domain discovered; skipping crawl")

    # 3) Hunter (optional)
    if settings.USE_HUNTER and os.getenv("HUNTER_API_KEY"):
        try:
            hunter_emails, hunter_raw = await hunter_domain_search(
                domain=domain,
                rate_limits=rate_limits,
                http_client=http_client,
            )
            for e in hunter_emails:
                emails_by_source[e] = emails_by_source.get(e, "hunter")
            # Verify found + crawled emails
            to_verify = list(emails_by_source.keys())
            hv = await hunter_verify_bulk(to_verify, rate_limits=rate_limits, http_client=http_client)
            hunter_verification = hv
        except Exception as e:
            notes.append(f"hunter_error:{type(e).__name__}:{e}")

    # 4) Apollo (optional)
    if settings.USE_APOLLO and os.getenv("APOLLO_API_KEY"):
        try:
            apollo_results = await apollo_people_for_domain(
                domain=domain,
                person_hint=founder_name,
                rate_limits=rate_limits,
                http_client=http_client,
            )
            for p in apollo_results:
                em = p.get("email")
                if em:
                    emails_by_source[em] = emails_by_source.get(em, "apollo")
        except Exception as e:
            notes.append(f"apollo_error:{type(e).__name__}:{e}")

    # 5) Clean/validate & confidence
    emails = dedupe_keep_order(filter_emails_basic(list(emails_by_source.keys()), domain))
    confidence, method = assess_confidence(
        emails=emails,
        hunter_verification=hunter_verification,
        emails_by_source=emails_by_source,
    )

    return ResultRow(
        company_name=company_name or "",
        domain=domain or "",
        country=country or "",
        linkedin_url=linkedin_url or "",
        founder_name=founder_name or "",
        found_emails=";".join(emails),
        emails_with_source=json_dumps_sane(emails_by_source),
        hunter_verification=json_dumps_sane(hunter_verification),
        apollo_results=json_dumps_sane(apollo_results),
        confidence=confidence,
        extraction_method=method if domain else discovery_method or "serp",
        notes=" | ".join(notes)[:2000],
        timestamp=timestamp,
    )


async def main_async(settings: Settings) -> None:
    await configure_logging_async(settings.log_file)
    logger.info("start", settings=settings.__dict__)

    # Read input (with checkpoint merge)
    df = read_input_csv(settings.input_csv)
    checkpoint_df = load_checkpoint(settings.checkpoint_csv)
    if not checkpoint_df.empty:
        df = merge_checkpoint(df, checkpoint_df)

    # Prepare HTTP client & rate limiters
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
    timeout = httpx.Timeout(
        connect=settings.connect_timeout,
        read=settings.request_timeout,
        write=settings.request_timeout,
        pool=settings.request_timeout,
    )
    headers = {"User-Agent": settings.user_agent, "Accept": "text/html,application/json;q=0.9,*/*;q=0.8"}

    import httpx  # local import to ensure requirements installed
    async with httpx.AsyncClient(
        timeout=timeout,
        headers=headers,
        limits=limits,
        follow_redirects=settings.follow_redirects,
        http2=True,
        verify=True,
    ) as http_client:
        # Simple token-bucket per-minute rate limiters
        from scraper.utils import RateLimiter
        rate_limits = {
            "hunter": RateLimiter(settings.budgets.hunter_per_minute),
            "apollo": RateLimiter(settings.budgets.apollo_per_minute),
            "serpapi": RateLimiter(settings.budgets.serpapi_per_minute),
        }

        tasks = []
        results: List[ResultRow] = []
        sem = asyncio.Semaphore(settings.concurrency)

        async def worker(idx: int, rec: Dict[str, Any]):
            async with sem:
                try:
                    res = await process_company(rec, settings, http_client, rate_limits)
                    results.append(res)
                    if len(results) % settings.save_every == 0:
                        write_outputs(results, settings.output_csv, settings.output_json, settings.checkpoint_csv, checkpoint=True)
                except Exception as e:
                    logger.exception("company_failed", idx=idx, error=str(e))

        records = df.to_dict(orient="records")
        for i, rec in enumerate(records):
            tasks.append(asyncio.create_task(worker(i, rec)))
        await asyncio.gather(*tasks)

    # Final write
    write_outputs(results, settings.output_csv, settings.output_json, settings.checkpoint_csv, checkpoint=False)

    # Summary
    emails_total = sum(len((r.found_emails or "").split(";")) for r in results if r.found_emails)
    hunter_verified = 0
    for r in results:
        try:
            hv = json.loads(r.hunter_verification or "{}")
            hunter_verified += sum(1 for v in hv.values() if (v or {}).get("result") == "valid")
        except Exception:
            pass
    apollo_count = sum(1 for r in results if r.apollo_results and r.apollo_results not in ("[]", ""))
    skipped_robots = sum(1 for r in results if "robots_disallow" in (r.notes or ""))

    logger.info(
        "summary",
        companies=len(results),
        emails_total=emails_total,
        hunter_verified=hunter_verified,
        apollo_found=apollo_count,
        skipped_due_to_robots=skipped_robots,
    )


def parse_args_to_settings() -> Settings:
    import argparse
    parser = argparse.ArgumentParser(description="AI Startups Email Enricher")
    parser.add_argument("--input", dest="input_csv", default=SETTINGS.input_csv)
    parser.add_argument("--output", dest="output_csv", default=SETTINGS.output_csv)
    parser.add_argument("--json", dest="output_json", default=SETTINGS.output_json)
    parser.add_argument("--concurrency", type=int, default=SETTINGS.concurrency)
    parser.add_argument("--user-agent", dest="user_agent", default=SETTINGS.user_agent)
    parser.add_argument("--use-hunter", action="store_true", default=SETTINGS.USE_HUNTER)
    parser.add_argument("--use-apollo", action="store_true", default=SETTINGS.USE_APOLLO)
    parser.add_argument("--use-serpapi", action="store_true", default=SETTINGS.USE_SERPAPI)
    parser.add_argument("--config", help="Optional python module path to load for overrides (e.g., mycfg.py)", default=None)

    args = parser.parse_args()

    # optionally load a user-provided config module
    if args.config:
        import importlib.util
        cfg_path = Path(args.config)
        spec = importlib.util.spec_from_file_location("user_config", cfg_path)
        if not spec or not spec.loader:
            raise RuntimeError(f"Could not load config module from {cfg_path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if hasattr(mod, "SETTINGS"):
            base = mod.SETTINGS
        else:
            base = SETTINGS
    else:
        base = SETTINGS

    # create a shallow copy with CLI overrides
    s = Settings(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        output_json=args.output_json,
        concurrency=args.concurrency,
        user_agent=args.user_agent,
        USE_HUNTER=args.use_hunter,
        USE_APOLLO=args.use_apollo,
        USE_SERPAPI=args.use_serpapi,
        budgets=base.budgets,
        max_pages_per_site=base.max_pages_per_site,
        max_depth=base.max_depth,
        request_timeout=base.request_timeout,
        connect_timeout=base.connect_timeout,
        follow_redirects=base.follow_redirects,
        log_file=base.log_file,
        save_every=base.save_every,
    )
    return s


if __name__ == "__main__":
    import asyncio
    settings = parse_args_to_settings()
    asyncio.run(main_async(settings))
