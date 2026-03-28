"""Fetch today's NBA schedule and Vegas odds into nba_matchups.

Two data sources combined:
  1. ScoreboardV2 (stats.nba.com) — game IDs, home/away teams, tip-off times
  2. The Odds API (optional) — Vegas totals + moneylines, matched by team name

Usage:
    python -m ingest.nba_schedule                    # today's games
    python -m ingest.nba_schedule --date 2026-03-25  # specific date
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_team_abbrev_cache, upsert_nba_matchup
from ingest.nba_teams import NBA_ID_TO_ABBREV

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 0.6

# DK sometimes uses non-standard abbreviations — map to our canonical ones.
# NBA abbreviations are standardized enough that only a handful need overrides.
DK_ABBREV_OVERRIDES: dict[str, str] = {
    "GS":  "GSW",
    "SA":  "SAS",
    "NO":  "NOP",
    "NY":  "NYK",
    "PHO": "PHX",
    "OKL": "OKC",
    "UTH": "UTA",
}


def fetch_schedule(db: DatabaseManager, game_date: str | None = None) -> list[int]:
    """Fetch games for game_date (YYYY-MM-DD), upsert into nba_matchups.

    Returns list of nba_matchup IDs upserted.
    """
    from nba_api.stats.endpoints import ScoreboardV2

    from ingest.nba_stats import _call_with_retry

    target_date = game_date or date.today().isoformat()
    logger.info("Fetching NBA schedule for %s ...", target_date)
    time.sleep(SLEEP_SECONDS)

    def _fetch():
        return ScoreboardV2(game_date=target_date)

    scoreboard = _call_with_retry(_fetch, "ScoreboardV2")
    game_header = scoreboard.game_header.get_data_frame()

    if game_header.empty:
        print(f"No games found for {target_date}")
        return []

    abbrev_cache = build_team_abbrev_cache(db)

    matchup_ids = []
    for _, row in game_header.iterrows():
        nba_game_id  = str(row["GAME_ID"])
        home_nba_id  = int(row["HOME_TEAM_ID"])
        away_nba_id  = int(row["VISITOR_TEAM_ID"])

        home_abbrev = NBA_ID_TO_ABBREV.get(home_nba_id)
        away_abbrev = NBA_ID_TO_ABBREV.get(away_nba_id)

        home_team_id = abbrev_cache.get(home_abbrev) if home_abbrev else None
        away_team_id = abbrev_cache.get(away_abbrev) if away_abbrev else None

        mid = upsert_nba_matchup(
            db,
            game_date=target_date,
            game_id=nba_game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
        )
        if mid:
            matchup_ids.append(mid)

    print(f"Schedule: {len(matchup_ids)} games upserted for {target_date}")
    return matchup_ids


def fetch_odds(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch Vegas totals + moneylines from The Odds API and update nba_matchups.

    Matches games by home team name → our teams.name column.
    Returns number of matchups updated with odds.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping Vegas odds fetch")
        return 0

    target_date = game_date or date.today().isoformat()
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        games = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API request failed: %s", e)
        return 0

    # Build a lookup: canonical team name → nba_matchup.game_id for today's games
    rows = db.execute(
        """
        SELECT nm.id, nm.game_id, t_home.name AS home_name, t_away.name AS away_name
        FROM nba_matchups nm
        JOIN teams t_home ON t_home.team_id = nm.home_team_id
        JOIN teams t_away ON t_away.team_id = nm.away_team_id
        WHERE nm.game_date = %s
        """,
        (target_date,),
    )
    matchup_by_home: dict[str, dict] = {r["home_name"]: r for r in rows}

    updated = 0
    for g in games:
        # Odds API uses full team names e.g. "Los Angeles Lakers"
        home_name = g.get("home_team", "")
        matchup = matchup_by_home.get(home_name)
        if not matchup:
            logger.debug("No matchup found for Odds API home team: %s", home_name)
            continue

        h2h = next(
            (m for m in g.get("bookmakers", [{}])[0].get("markets", []) if m["key"] == "h2h"),
            None,
        )
        totals = next(
            (m for m in g.get("bookmakers", [{}])[0].get("markets", []) if m["key"] == "totals"),
            None,
        )

        home_ml = away_ml = None
        vegas_total = None
        vegas_prob_home = None

        if h2h:
            outcomes = {o["name"]: o["price"] for o in h2h.get("outcomes", [])}
            home_ml = outcomes.get(home_name)
            away_name = g.get("away_team", "")
            away_ml = outcomes.get(away_name)
            if home_ml and away_ml:
                vegas_prob_home = _ml_to_prob(home_ml, away_ml)

        if totals:
            over = next((o for o in totals.get("outcomes", []) if o["name"] == "Over"), None)
            if over:
                vegas_total = over.get("point")

        db.execute(
            """
            UPDATE nba_matchups
            SET vegas_total = %s, home_ml = %s, away_ml = %s, vegas_prob_home = %s
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, vegas_prob_home, matchup["id"]),
        )
        updated += 1

    print(f"Odds: {updated} matchups updated with Vegas lines for {target_date}")
    return updated


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    """Convert American moneylines to vig-removed home win probability."""
    def ml_to_raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = ml_to_raw(home_ml)
    away_raw = ml_to_raw(away_ml)
    total = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch NBA schedule + odds")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_schedule(db, args.date)
    fetch_odds(db, config.odds_api.api_key, args.date)
