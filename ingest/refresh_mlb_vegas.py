"""Daily MLB Vegas refresh for schedule, scores, and lines.

This is the MLB equivalent of the NBA daily refresh workflow, but targeted at
the data that powers MLB Vegas Analysis and slate line quality:

1. Fetch today's MLB schedule.
2. Fetch today's live MLB odds.
3. Refresh today's scores if any games have already gone final.
4. Backfill recent MLB schedule/scores for missed or stale dates.
5. Backfill recent historical MLB odds for missed or stale dates.

Usage:
    python -m ingest.refresh_mlb_vegas
    python -m ingest.refresh_mlb_vegas --date 2026-04-29 --days-back 10
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import date, timedelta
from typing import TypeVar

from config import load_config
from db.database import DatabaseManager
from ingest.backfill_mlb_odds import backfill as backfill_mlb_odds
from ingest.backfill_mlb_schedule import backfill as backfill_mlb_schedule
from ingest.mlb_schedule import fetch_odds, fetch_schedule, fetch_scores

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_DAYS_BACK = 7


def _run_refresh_stage(label: str, fn: Callable[[], T]) -> tuple[bool, T | None]:
    try:
        result = fn()
        print(f"{label}: completed ({result})")
        return True, result
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s failed: %s", label, exc)
        print(f"{label}: FAILED ({type(exc).__name__}: {exc})")
        return False, None


def _write_total_predictions(db: DatabaseManager, refresh_date_iso: str, days_back: int = 7) -> int:
    """Write our_total_pred for the slate, self-healing recent gaps.

    First walk-forward-backfills the trailing window (deterministic + look-ahead
    safe, so it just fills any days a prior run missed without disturbing the
    frozen pre-game numbers), then writes today's slate.
    """
    from model.mlb_game_total_model import backfill_predictions, predict_and_write
    from model.mlb_moneyline_model import (
        backfill_predictions as ml_backfill,
        predict_and_write as ml_predict,
    )

    if days_back > 0:
        refresh_date = date.fromisoformat(refresh_date_iso)
        start = (refresh_date - timedelta(days=days_back)).isoformat()
        yesterday = (refresh_date - timedelta(days=1)).isoformat()
        if yesterday >= start:
            backfill_predictions(db, start, yesterday)
            ml_backfill(db, start, yesterday)
    total_written = predict_and_write(db, refresh_date_iso)
    ml_predict(db, refresh_date_iso)
    return total_written


def _rate_and_settle_bets(db: DatabaseManager, refresh_date_iso: str) -> int:
    """Rate today's slate into the bet ledger and settle any finals (parity with
    the soccer accountability framework). Runs after predictions are written."""
    from model.mlb_game_bets import rate_slate, settle
    rated = rate_slate(db, refresh_date_iso)
    settle(db)
    return rated


def _rolling_backfill_window(target_date: date, days_back: int) -> tuple[str, str] | None:
    if days_back <= 0:
        return None
    end_date = target_date - timedelta(days=1)
    if end_date >= target_date:
        return None
    start_date = end_date - timedelta(days=days_back - 1)
    return start_date.isoformat(), end_date.isoformat()


def run_refresh(
    db: DatabaseManager,
    odds_api_key: str,
    target_date: str | None,
    days_back: int = DEFAULT_DAYS_BACK,
) -> int:
    refresh_date = date.fromisoformat(target_date) if target_date else date.today()
    refresh_date_iso = refresh_date.isoformat()

    stages: list[tuple[str, bool, object | None]] = []

    ok, result = _run_refresh_stage("mlb_schedule_today", lambda: fetch_schedule(db, refresh_date_iso))
    stages.append(("mlb_schedule_today", ok, result))

    ok, result = _run_refresh_stage("mlb_odds_today", lambda: fetch_odds(db, odds_api_key, refresh_date_iso))
    stages.append(("mlb_odds_today", ok, result))

    ok, result = _run_refresh_stage("mlb_scores_today", lambda: fetch_scores(db, refresh_date_iso))
    stages.append(("mlb_scores_today", ok, result))

    # Our independent O/U number — train on completed games, write our_total_pred
    # for today's slate. Runs after odds so today's vegas_total is present, and
    # after scores so the newest completed games join the training set.
    ok, result = _run_refresh_stage(
        "mlb_total_predictions",
        lambda: _write_total_predictions(db, refresh_date_iso),
    )
    stages.append(("mlb_total_predictions", ok, result))

    # Rate today's slate into the accountability ledger + settle finals.
    ok, result = _run_refresh_stage(
        "mlb_bet_ledger",
        lambda: _rate_and_settle_bets(db, refresh_date_iso),
    )
    stages.append(("mlb_bet_ledger", ok, result))

    window = _rolling_backfill_window(refresh_date, days_back)
    if window is not None:
        start_date, end_date = window
        ok, result = _run_refresh_stage(
            f"mlb_schedule_backfill:{start_date}->{end_date}",
            lambda: backfill_mlb_schedule(db, start_date, end_date, dry_run=False),
        )
        stages.append((f"mlb_schedule_backfill:{start_date}->{end_date}", ok, result))

        if odds_api_key:
            ok, result = _run_refresh_stage(
                f"mlb_odds_backfill:{start_date}->{end_date}",
                lambda: backfill_mlb_odds(
                    db,
                    odds_api_key,
                    start=start_date,
                    end=end_date,
                    missing_only=True,
                    dry_run=False,
                ),
            )
            stages.append((f"mlb_odds_backfill:{start_date}->{end_date}", ok, result))
        else:
            print("mlb_odds_backfill: skipped (ODDS_API_KEY not set)")
            stages.append(("mlb_odds_backfill", True, "skipped"))

    failures = [label for label, ok, _ in stages if not ok]
    if failures:
        print(f"MLB Vegas refresh finished with failures: {', '.join(failures)}")
        return 1

    print("MLB Vegas refresh finished successfully")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Refresh MLB schedule, scores, and Vegas lines")
    parser.add_argument("--date", default=None, help="Target game date YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--days-back",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help="Rolling historical backfill window ending yesterday (default: 7)",
    )
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    raise SystemExit(run_refresh(db, config.odds_api.api_key, args.date, days_back=max(0, args.days_back)))
