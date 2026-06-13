"""Download & cache historical international football results.

Source: martj42/international_results (GitHub) — every men's senior international
from 1872 to present.  Free, no auth, no IP restrictions.  This is the training
corpus for the soccer strength ratings (Elo + Poisson attack/defense), which is
how the model gets a signal before any World Cup game has been played.

The CSV is cached to data/international_results.csv so retraining is offline and
FanGraphs-style rate limits never apply.

Usage:
    python -m ingest.soccer_results_history            # download (cache if fresh)
    python -m ingest.soccer_results_history --force     # force re-download
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import requests

from config import DATA_DIR

logger = logging.getLogger(__name__)

RESULTS_URL = "https://raw.githubusercontent.com/martj42/international_results/master/results.csv"
CACHE_PATH = DATA_DIR / "international_results.csv"
# Re-download if the cache is older than this many days.
CACHE_TTL_DAYS = 7

EXPECTED_COLUMNS = {"date", "home_team", "away_team", "home_score", "away_score", "tournament", "neutral"}


def download_results(force: bool = False) -> Path:
    """Download results.csv to the cache unless a fresh copy already exists."""
    if CACHE_PATH.exists() and not force:
        age_days = (time.time() - CACHE_PATH.stat().st_mtime) / 86400
        if age_days < CACHE_TTL_DAYS:
            logger.info("Using cached results (%.1f days old): %s", age_days, CACHE_PATH)
            return CACHE_PATH

    logger.info("Downloading international results from %s ...", RESULTS_URL)
    resp = requests.get(RESULTS_URL, timeout=60)
    resp.raise_for_status()
    CACHE_PATH.write_bytes(resp.content)
    logger.info("Cached %d bytes to %s", len(resp.content), CACHE_PATH)
    return CACHE_PATH


def load_history(force_download: bool = False, since_year: int | None = None) -> pd.DataFrame:
    """Load the cached results as a cleaned DataFrame.

    Returns columns: date (datetime), home_team, away_team, home_score (int),
    away_score (int), tournament, neutral (bool).  Rows with missing scores are
    dropped.  Optionally filter to matches on/after ``since_year``.
    """
    path = download_results(force=force_download)
    df = pd.read_csv(path)

    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"results.csv missing expected columns: {missing}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce")
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce")
    df = df.dropna(subset=["date", "home_team", "away_team", "home_score", "away_score"])
    df["home_score"] = df["home_score"].astype(int)
    df["away_score"] = df["away_score"].astype(int)
    if "neutral" in df.columns:
        df["neutral"] = df["neutral"].astype(str).str.lower().isin({"true", "1", "yes"})
    else:
        df["neutral"] = False

    if since_year is not None:
        df = df[df["date"].dt.year >= since_year]

    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Loaded %d international matches%s",
                len(df), f" since {since_year}" if since_year else "")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Download historical international results")
    parser.add_argument("--force", action="store_true", help="Force re-download even if cache is fresh")
    args = parser.parse_args()

    df = load_history(force_download=args.force)
    print(f"{len(df)} matches cached at {CACHE_PATH}")
    print(df.tail(5).to_string(index=False))
