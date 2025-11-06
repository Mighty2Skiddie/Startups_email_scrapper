

### `config.py`

from __future__ import annotations

from dataclasses import dataclass, field
import os


@dataclass
class APIBudgets:
    hunter_per_minute: int = 25
    apollo_per_minute: int = 50
    serpapi_per_minute: int = 30


@dataclass
class Settings:
    # Paths
    input_csv: str = os.getenv("INPUT_CSV", "/mnt/data/ai_data_founder_leads_sample_20.csv")
    output_csv: str = os.getenv("OUTPUT_CSV", "ai_startups_emails_enriched.csv")
    output_json: str = os.getenv("OUTPUT_JSON", "ai_startups_emails_enriched.json")
    checkpoint_csv: str = os.getenv("CHECKPOINT_CSV", "checkpoint.csv")

    # Behavior
    concurrency: int = int(os.getenv("CONCURRENCY", "8"))
    user_agent: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (compatible; AI-EmailEnricher/1.0; +https://example.com/bot)"
    )
    max_pages_per_site: int = int(os.getenv("MAX_PAGES_PER_SITE", "15"))
    max_depth: int = int(os.getenv("MAX_DEPTH", "2"))
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "15"))
    connect_timeout: float = float(os.getenv("CONNECT_TIMEOUT", "10"))
    follow_redirects: bool = True

    # Toggles (default off to avoid accidental paid API calls)
    USE_HUNTER: bool = os.getenv("USE_HUNTER", "false").lower() in {"1", "true", "yes"}
    USE_APOLLO: bool = os.getenv("USE_APOLLO", "false").lower() in {"1", "true", "yes"}
    USE_SERPAPI: bool = os.getenv("USE_SERPAPI", "false").lower() in {"1", "true", "yes"}

    # API Budgets
    budgets: APIBudgets = field(default_factory=APIBudgets)

    # Logging
    log_file: str = os.getenv("LOG_FILE", "scraper.log")

    # Misc
    save_every: int = int(os.getenv("SAVE_EVERY", "10"))  # checkpoint interval


SETTINGS = Settings()
