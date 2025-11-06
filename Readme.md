# AI Startups Email Enricher

**Goal:** Given an input CSV of AI startups, discover company domains, scrape and extract public emails, and optionally enrich with Hunter.io and Apollo. Produces a single enriched CSV and a deduplicated JSON.

- **Input (default):** `/mnt/data/ai_data_founder_leads_sample_20.csv`
- **Output:** `ai_startups_emails_enriched.csv` and `ai_startups_emails_enriched.json`

## Features
- Robust CSV ingestion with column inference (`company_name`, `website`, `linkedin`, `founder_name`, `country`)
- Domain discovery from website column, LinkedIn company pages, or SerpAPI Google search fallback
- Polite, breadth-first website crawl (max depth=2, max 15 pages/site) on contact/about/team/careers/etc.
- RFC-compliant email regex with filtering (no images, basic disposable domain filter)
- Optional **Hunter.io** domain search + email verification
- Optional **Apollo** person/domain matching
- Rate limiting, concurrency (async httpx), retries (tenacity), robots.txt compliance
- JSON-structured logging to `scraper.log`
- Checkpointing partial progress to avoid data loss on long runs
- Summary report printed at the end

## Quick Start

### 1) Python Environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
