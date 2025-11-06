from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List
from pathlib import Path

import pandas as pd
import structlog

from .utils import ResultRow, json_dumps_sane

logger = structlog.get_logger(__name__)


EXPECTED_COLS = ["company_name", "website", "linkedin", "linkedin_url", "founder_name", "country"]


def read_input_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input CSV not found: {p}")
    df = pd.read_csv(p)
    # Try to infer expected columns
    cols = {c.lower().strip(): c for c in df.columns}
    # map to normalized names
    mapped = {}
    if "company_name" not in cols:
        if "company" in cols:
            mapped["company_name"] = cols["company"]
        elif "name" in cols:
            mapped["company_name"] = cols["name"]
    else:
        mapped["company_name"] = cols["company_name"]

    mapped["website"] = cols.get("website") or cols.get("url") or cols.get("domain")
    mapped["linkedin"] = cols.get("linkedin") or cols.get("linkedin_url")
    mapped["founder_name"] = cols.get("founder_name") or cols.get("founder")
    mapped["country"] = cols.get("country") or cols.get("location")

    # build normalized DF
    norm = pd.DataFrame()
    for k in ["company_name", "website", "linkedin", "founder_name", "country"]:
        if mapped.get(k):
            norm[k] = df[mapped[k]]
        else:
            norm[k] = ""

    missing = [k for k in ["company_name"] if norm[k].eq("").all()]
    if missing:
        logger.warning("missing_required_columns", missing=missing)

    return norm


def write_outputs(
    rows: List[ResultRow],
    out_csv: str,
    out_json: str,
    checkpoint_csv: str,
    checkpoint: bool = False,
) -> None:
    # Convert to pandas DataFrame for CSV
    data = [r.__dict__ for r in rows]
    df = pd.DataFrame(data)
    if checkpoint:
        df.to_csv(checkpoint_csv, index=False)
    else:
        df.to_csv(out_csv, index=False)
        # write deduped JSON
        # Dedupe by (company_name, domain) keeping last
        dedup_key = {}
        for r in rows:
            key = (r.company_name, r.domain)
            dedup_key[key] = r
        json_data = [x.__dict__ for x in dedup_key.values()]
        Path(out_json).write_text(json_dumps_sane(json_data), encoding="utf-8")


def load_checkpoint(checkpoint_csv: str) -> pd.DataFrame:
    p = Path(checkpoint_csv)
    if p.exists():
        try:
            return pd.read_csv(p)
        except Exception:
            logger.warning("checkpoint_read_failed", path=str(p))
    return pd.DataFrame()


def merge_checkpoint(df: pd.DataFrame, chk: pd.DataFrame) -> pd.DataFrame:
    if chk.empty:
        return df
    # Prefer rows not yet in checkpoint. We assume checkpoint already processed a subset.
    processed = set(
        (str(r.company_name).strip(), str(r.domain).strip())
        for r in chk.itertuples(index=False)
        if hasattr(r, "company_name") and hasattr(r, "domain")
    )
    mask = []
    for r in df.itertuples(index=False):
        key = (str(getattr(r, "company_name", "")).strip(), "")
        mask.append(key not in processed)
    return df[pd.Series(mask).values]
