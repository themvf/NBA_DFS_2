"""Backfill historical Vegas odds into mlb_matchups.

Uses The Odds API /v4/historical/sports/baseball_mlb/odds/ endpoint.
One call per game date at 20:00 UTC (4 pm ET, pre-game).

Credit cost: 3 credits per date (h2h + spreads + totals, regions=us).

Usage:
    python -m ingest.backfill_mlb_odds --start 2026-03-27 --end 2026-04-12 --dry-run
    python -m ingest.backfill_mlb_odds --start 2026-03-27 --end 2026-04-12
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import insert_game_odds_history_rows
from model.dfs_projections import compute_team_implied_total

logger = logging.getLogger(__name__)

SNAPSHOT_HOUR_UTC = 20
SLEEP_BETWEEN_CALLS = 1.0
HISTORICAL_URL = "https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds/"

# The Athletics have gone through name changes — handle all variants
_OAK_ALIASES = {"Oakland Athletics", "Athletics", "Sacramento Athletics"}


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    def _raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)
    home_raw = _raw(home_ml)
    away_raw = _raw(away_ml)
    total = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


def _dates_with_games(db: DatabaseManager, start: str, end: str, missing_only: bool) -> list[str]:
    if missing_only:
        rows = db.execute(
            """
            SELECT DISTINCT game_date::text AS game_date
            FROM mlb_matchups
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
            FROM mlb_matchups
            WHERE game_date BETWEEN %s AND %s
            ORDER BY game_date
            """,
            (start, end),
        )
    return [r["game_date"] for r in rows]


def _fetch_historical_odds(api_key: str, snapshot: str) -> tuple[list[dict], int | None]:
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,totals,spreads",
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


def _backfill_date(
    db: DatabaseManager,
    api_key: str,
    game_date: str,
    matchup_by_home: dict[str, dict],
    dry_run: bool,
) -> int:
    snapshot = f"{game_date}T{SNAPSHOT_HOUR_UTC:02d}:00:00Z"

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

        # Handle Athletics name variants
        if not matchup and home_name in _OAK_ALIASES:
            for alias in _OAK_ALIASES:
                matchup = matchup_by_home.get(alias)
                if matchup:
                    break

        if not matchup:
            logger.debug("No matchup for %s on %s", home_name, game_date)
            continue

        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        home_spreads: list[float] = []

        bookmakers = g.get("bookmakers") or []
        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name or (o["name"] in _OAK_ALIASES and home_name in _OAK_ALIASES):
                            home_prices.append(o["price"])
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                elif market["key"] == "totals":
                    over = next((o for o in market.get("outcomes", []) if o["name"] == "Over"), None)
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                elif market["key"] == "spreads":
                    home_outcome = next((o for o in market.get("outcomes", []) if o["name"] == home_name), None)
                    if home_outcome and home_outcome.get("point") is not None:
                        home_spreads.append(float(home_outcome["point"]))

        home_ml = round(sum(home_prices) / len(home_prices)) if home_prices else None
        away_ml = round(sum(away_prices) / len(away_prices)) if away_prices else None
        vegas_total = round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        home_spread = round(sum(home_spreads) / len(home_spreads) * 2) / 2 if home_spreads else None
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None

        home_implied = away_implied = None
        if vegas_total and home_ml and away_ml:
            home_implied = round(compute_team_implied_total(vegas_total, home_ml, away_ml, is_home=True), 3)
            away_implied = round(vegas_total - home_implied, 3)

        db.execute(
            """
            UPDATE mlb_matchups
            SET vegas_total     = COALESCE(%s, vegas_total),
                home_ml         = COALESCE(%s, home_ml),
                away_ml         = COALESCE(%s, away_ml),
                home_spread     = COALESCE(%s, home_spread),
                vegas_prob_home = COALESCE(%s, vegas_prob_home),
                home_implied    = COALESCE(%s, home_implied),
                away_implied    = COALESCE(%s, away_implied)
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, home_spread,
             vegas_prob_home, home_implied, away_implied, matchup["id"]),
        )
        history_rows.append({
            "sport": "mlb",
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
        })
        updated += 1

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)

    remaining_str = f"  ({remaining} credits remaining)" if remaining is not None else ""
    print(f"  {game_date}: {updated}/{len(matchup_by_home)} games updated{remaining_str}")
    return updated


def backfill(
    db: DatabaseManager,
    api_key: str,
    start: str,
    end: str,
    missing_only: bool = True,
    dry_run: bool = False,
) -> None:
    dates = _dates_with_games(db, start, end, missing_only)

    if not dates:
        print("No dates to process.")
        return

    credits_est = len(dates) * 3
    mode = "dry-run" if dry_run else ("missing-only" if missing_only else "force")
    print(
        f"MLB Odds backfill [{mode}] | {len(dates)} dates | ~{credits_est} credits estimated\n"
        f"Range: {dates[0]} to {dates[-1]}"
    )

    total_updated = 0
    for i, game_date in enumerate(dates, 1):
        rows = db.execute(
            """
            SELECT nm.id, nm.game_date::text, nm.home_team_id, nm.away_team_id,
                   t.name AS home_name
            FROM mlb_matchups nm
            JOIN mlb_teams t ON t.team_id = nm.home_team_id
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

    parser = argparse.ArgumentParser(description="Backfill historical MLB Vegas odds")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end",   required=True)
    parser.add_argument("--missing-only", action="store_true", default=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_config()
    if not config.odds_api.api_key:
        raise SystemExit("ODDS_API_KEY not set")

    db = DatabaseManager(config.database_url)
    backfill(
        db,
        config.odds_api.api_key,
        start=args.start,
        end=args.end,
        missing_only=not args.force,
        dry_run=args.dry_run,
    )
