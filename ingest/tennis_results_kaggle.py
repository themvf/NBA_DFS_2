"""Pull the daily-updated tennis results dataset from Kaggle (settlement source).

This is the AUTOMATED settlement/backtest backbone for the tennis Vegas model.
The Odds API `/scores` endpoint gives same-day winners (moneyline), but totals
and set-handicap need set/game detail — which a free results feed must supply.
The Kaggle dataset:

    dissfya/atp-tennis-2000-2023daily-pull   (advertised DAILY refresh)

carries final results WITH set scores and Pinnacle/Bet365 closing odds, so it
settles all three markets AND feeds closing-line-value backtests — with zero
manual processing.

── Authentication (for CI / GitHub Actions) ──────────────────────────────────
The official `kaggle` package authenticates from EITHER:
  * env vars  KAGGLE_USERNAME + KAGGLE_KEY   ← use these as GitHub Secrets
  * or ~/.kaggle/kaggle.json                  ← local dev

Get a token: kaggle.com → Account → "Create New API Token" → downloads
kaggle.json containing {"username": ..., "key": ...}. In CI, set the two values
as repo secrets and export them before this script runs.

── Usage ─────────────────────────────────────────────────────────────────────
    KAGGLE_USERNAME=... KAGGLE_KEY=... python -m ingest.tennis_results_kaggle
    python -m ingest.tennis_results_kaggle --dataset owner/slug   # override source
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from config import DATA_DIR

logger = logging.getLogger(__name__)

DEFAULT_DATASET = "dissfya/atp-tennis-2000-2023daily-pull"
DOWNLOAD_DIR = DATA_DIR / "tennis_kaggle"


def download_dataset(dataset: str = DEFAULT_DATASET, dest: Path = DOWNLOAD_DIR) -> Path:
    """Download + unzip a Kaggle dataset into `dest`. Returns the dest dir.

    Authenticates from KAGGLE_USERNAME/KAGGLE_KEY (CI) or ~/.kaggle/kaggle.json
    (local). Raises a clear error if neither is present, so an expired/missing
    token surfaces loudly in CI instead of silently producing no settlements.
    """
    if not (os.getenv("KAGGLE_KEY") or (Path.home() / ".kaggle" / "kaggle.json").exists()):
        raise RuntimeError(
            "No Kaggle credentials. Set KAGGLE_USERNAME + KAGGLE_KEY env vars "
            "(GitHub Secrets) or place ~/.kaggle/kaggle.json. "
            "Token: kaggle.com → Account → Create New API Token."
        )

    # Import is deferred: the `kaggle` package authenticates AT IMPORT TIME, so a
    # missing credential would raise here rather than at our explicit check above.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    dest.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s → %s", dataset, dest)
    api.dataset_download_files(dataset, path=str(dest), unzip=True, quiet=False)
    return dest


def load_results(dest: Path = DOWNLOAD_DIR):
    """Load the downloaded CSV(s) into a pandas DataFrame for settlement.

    The dataset ships one or more CSVs; we read the largest one (the match
    results table). Column names are dataset-specific — inspect on first run
    before wiring settlement (the P0 verification step).
    """
    import pandas as pd

    csvs = sorted(dest.glob("*.csv"), key=lambda p: p.stat().st_size, reverse=True)
    if not csvs:
        raise FileNotFoundError(f"No CSV found in {dest} — did the download succeed?")
    df = pd.read_csv(csvs[0])
    logger.info("Loaded %s rows from %s", len(df), csvs[0].name)
    return df


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Pull daily tennis results from Kaggle")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Kaggle owner/slug")
    parser.add_argument("--inspect", action="store_true", help="Print schema + recent Wimbledon rows and exit")
    args = parser.parse_args()

    dest = download_dataset(args.dataset)
    df = load_results(dest)

    if args.inspect:
        print("Columns:", list(df.columns))
        print("Rows:", len(df))
        # Best-effort peek at Wimbledon rows — column name unknown until first run.
        tourney_col = next((c for c in df.columns if "tourn" in c.lower() or "series" in c.lower()), None)
        if tourney_col:
            wimb = df[df[tourney_col].astype(str).str.contains("Wimbledon", case=False, na=False)]
            print(f"Wimbledon rows ({tourney_col}):", len(wimb))
            print(wimb.tail(5).to_string())
        return 0

    # TODO (next phase — settlement): match Kaggle rows to tennis_matches by
    # player names + date (accent-normalized), parse the set-score column into
    # home_sets/away_sets/home_games/away_games + winner, UPDATE tennis_matches,
    # then settle any rated bets. Kept out of the odds-only MVP.
    print(f"Downloaded + loaded {len(df)} result rows. Run with --inspect to see schema.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
