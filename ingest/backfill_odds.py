"""Backfill historical Vegas game-level odds into nba_matchups.

Uses The Odds API /v4/historical/sports/basketball_nba/odds/ endpoint.
One call per game date — returns all games for that day at the requested
snapshot time (default: 23:00 UTC ≈ 6 pm ET, pre-game opening lines).

Credit cost: 3 credits per date (h2h + spreads + totals, regions=us).
Typical NBA regular season: ~130 game days × 3 = ~390 credits total.

Usage:
    # Dry-run — show dates to process and estimated credit cost, no API calls
    python -m ingest.backfill_odds --start 2025-10-21 --end 2026-04-11 --dry-run

    # Backfill all dates in range where vegas_total is already NULL
    python -m ingest.backfill_odds --start 2025-10-21 --end 2026-04-11 --missing-only

    # Force-overwrite all dates in range (re-fetches even if odds already exist)
    python -m ingest.backfill_odds --start 2025-10-21 --end 2026-04-11
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_team_abbrev_cache, insert_game_odds_history_rows
from ingest.nba_schedule import _compute_implied_totals, _ml_to_prob

logger = logging.getLogger(__name__)

# Snapshot time: 20:00 UTC = 4 pm ET (before any tip-off, including afternoon games)
# 23:00 UTC (7 pm ET) was too late — afternoon games already in-progress,
# and early 7 pm ET games were at the cutoff boundary.
SNAPSHOT_HOUR_UTC = 20
SLEEP_BETWEEN_CALLS = 1.0  # seconds — stay well within rate limits
HISTORICAL_URL = (
    "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/odds/"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dates_with_games(
    db: DatabaseManager, start: str, end: str, missing_only: bool
) -> list[str]:
    """Return sorted list of game dates in [start, end] that have matchups.

    If missing_only=True, only return dates where at least one matchup has
    vegas_total IS NULL (i.e. odds were never fetched).
    """
    if missing_only:
        rows = db.execute(
            """
            SELECT DISTINCT game_date::text AS game_date
            FROM nba_matchups
            WHERE game_date BETWEEN %s AND %s
              AND vegas_total IS NULL
            ORDER BY game_date
            """,
            (start, end),
        )
    else:
        rows = db.execute(
            """
            SELECT DISTINCT game_date::text AS game_date
            FROM nba_matchups
            WHERE game_date BETWEEN %s AND %s
            ORDER BY game_date
            """,
            (start, end),
        )
    return [r["game_date"] for r in rows]


def _snapshot_timestamp(game_date: str) -> str:
    """Return ISO-8601 UTC timestamp for the snapshot time on game_date.

    e.g. '2026-01-15' → '2026-01-15T23:00:00Z'
    """
    return f"{game_date}T{SNAPSHOT_HOUR_UTC:02d}:00:00Z"


def _fetch_historical_odds(api_key: str, snapshot: str) -> tuple[list[dict], int | None]:
    """Fetch historical odds snapshot. Returns (games_list, requests_remaining).

    The historical endpoint wraps the games array inside a 'data' key, unlike
    the live endpoint which returns the array directly.
    """
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "date": snapshot,
    }
    resp = requests.get(HISTORICAL_URL, params=params, timeout=30)
    resp.raise_for_status()

    remaining = None
    if "x-requests-remaining" in resp.headers:
        try:
            remaining = int(resp.headers["x-requests-remaining"])
        except ValueError:
            pass

    payload = resp.json()
    games = payload.get("data", [])
    return games, remaining


# ---------------------------------------------------------------------------
# Core backfill logic for one date
# ---------------------------------------------------------------------------


def _backfill_date(
    db: DatabaseManager,
    api_key: str,
    game_date: str,
    matchup_by_home: dict[str, dict],
    dry_run: bool,
) -> int:
    """Fetch historical odds for game_date and update nba_matchups.

    Returns number of matchups updated (or would-be updated in dry_run).
    """
    snapshot = _snapshot_timestamp(game_date)

    if dry_run:
        print(f"  [dry-run] Would fetch: {snapshot}")
        return 0

    try:
        games, remaining = _fetch_historical_odds(api_key, snapshot)
    except requests.RequestException as e:
        logger.warning("Historical odds request failed for %s: %s", game_date, e)
        return 0

    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = f"{game_date}T{SNAPSHOT_HOUR_UTC:02d}:00Z_backfill"

    updated = 0
    history_rows: list[dict] = []

    for g in games:
        home_name = g.get("home_team", "")
        matchup = matchup_by_home.get(home_name)
        if not matchup:
            logger.debug("No matchup for Odds API home team: %s on %s", home_name, game_date)
            continue

        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        home_spreads: list[float] = []
        total_points: list[float] = []

        bookmakers = g.get("bookmakers") or []
        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name:
                            home_prices.append(o["price"])
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                elif market["key"] == "spreads":
                    home_outcome = next(
                        (o for o in market.get("outcomes", []) if o["name"] == home_name),
                        None,
                    )
                    if home_outcome and home_outcome.get("point") is not None:
                        home_spreads.append(float(home_outcome["point"]))
                elif market["key"] == "totals":
                    over = next(
                        (o for o in market.get("outcomes", []) if o["name"] == "Over"),
                        None,
                    )
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))

        home_ml = round(sum(home_prices) / len(home_prices)) if home_prices else None
        away_ml = round(sum(away_prices) / len(away_prices)) if away_prices else None
        home_spread = (
            round(sum(home_spreads) / len(home_spreads) * 2) / 2 if home_spreads else None
        )
        vegas_total = (
            round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        )
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None
        home_implied, away_implied = _compute_implied_totals(vegas_total, home_ml, away_ml)

        db.execute(
            """
            UPDATE nba_matchups
            SET vegas_total     = COALESCE(%s, vegas_total),
                home_ml         = COALESCE(%s, home_ml),
                away_ml         = COALESCE(%s, away_ml),
                home_spread     = COALESCE(%s, home_spread),
                vegas_prob_home = COALESCE(%s, vegas_prob_home),
                home_implied    = COALESCE(%s, home_implied),
                away_implied    = COALESCE(%s, away_implied)
            WHERE id = %s
            """,
            (
                vegas_total, home_ml, away_ml, home_spread,
                vegas_prob_home, home_implied, away_implied,
                matchup["id"],
            ),
        )

        history_rows.append(
            {
                "sport": "nba",
                "matchup_id": matchup["id"],
                "event_id": g.get("id"),
                "game_date": game_date,
                "home_team_id": matchup.get("home_team_id"),
                "away_team_id": matchup.get("away_team_id"),
                "home_team_name": home_name,
                "away_team_name": away_name,
                "bookmaker_count": len(bookmakers),
                "home_ml": home_ml,
                "away_ml": away_ml,
                "home_spread": home_spread,
                "vegas_total": vegas_total,
                "vegas_prob_home": vegas_prob_home,
                "home_implied": home_implied,
                "away_implied": away_implied,
                "capture_key": capture_key,
                "captured_at": captured_at,
            }
        )
        updated += 1

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)

    remaining_str = f"  ({remaining} credits remaining)" if remaining is not None else ""
    print(f"  {game_date}: {updated}/{len(matchup_by_home)} games updated{remaining_str}")
    return updated


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def backfill(
    db: DatabaseManager,
    api_key: str,
    start: str,
    end: str,
    missing_only: bool = True,
    dry_run: bool = False,
) -> None:
    """Run the full backfill for dates in [start, end]."""
    dates = _dates_with_games(db, start, end, missing_only)

    if not dates:
        print("No dates to process.")
        return

    # Credit estimate: 3 per date (h2h + spreads + totals, 1 region)
    credits_est = len(dates) * 3
    mode = "dry-run" if dry_run else ("missing-only" if missing_only else "force")
    print(
        f"Backfill [{mode}] | {len(dates)} dates | ~{credits_est} credits estimated\n"
        f"Range: {dates[0]} to {dates[-1]}"
    )
    if dry_run:
        print()

    # Build matchup lookup once per date (re-query each date since rows vary)
    total_updated = 0
    for i, game_date in enumerate(dates, 1):
        rows = db.execute(
            """
            SELECT nm.id, nm.game_date::text, nm.home_team_id, nm.away_team_id,
                   t_home.name AS home_name
            FROM nba_matchups nm
            JOIN teams t_home ON t_home.team_id = nm.home_team_id
            WHERE nm.game_date = %s
            """,
            (game_date,),
        )
        matchup_by_home = {r["home_name"]: r for r in rows}

        n = _backfill_date(db, api_key, game_date, matchup_by_home, dry_run)
        total_updated += n

        if not dry_run and i < len(dates):
            time.sleep(SLEEP_BETWEEN_CALLS)

    if dry_run:
        print(f"\nDry-run complete: {len(dates)} dates, ~{credits_est} credits needed")
    else:
        print(f"\nDone: {total_updated} game-odds rows updated across {len(dates)} dates")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Backfill historical Vegas odds")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date   YYYY-MM-DD")
    parser.add_argument(
        "--missing-only",
        action="store_true",
        default=True,
        help="Only fetch dates where vegas_total IS NULL (default: on)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all dates even if odds already exist (overrides --missing-only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched without calling the API",
    )
    args = parser.parse_args()

    config = load_config()
    if not config.odds_api.api_key:
        raise SystemExit("ODDS_API_KEY not set in environment")

    db = DatabaseManager(config.database_url)
    backfill(
        db,
        config.odds_api.api_key,
        start=args.start,
        end=args.end,
        missing_only=not args.force,
        dry_run=args.dry_run,
    )
