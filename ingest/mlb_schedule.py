"""Fetch today's MLB schedule and Vegas odds into mlb_matchups.

Two data sources combined:
  1. MLB Stats API (statsapi.mlb.com) — game IDs, home/away teams,
     ballpark, probable starting pitchers
  2. The Odds API (optional) — Vegas totals + moneylines + team-specific
     implied run totals, matched by full team name

No authentication or rate limiting on the MLB Stats API.

Usage:
    python -m ingest.mlb_schedule                    # today's games
    python -m ingest.mlb_schedule --date 2025-04-01  # specific date
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_mlb_team_abbrev_cache, upsert_mlb_matchup
from ingest.mlb_teams import MLB_ID_TO_ABBREV
from model.dfs_projections import compute_team_implied_total

logger = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"


def fetch_schedule(db: DatabaseManager, game_date: str | None = None) -> list[int]:
    """Fetch games for game_date (YYYY-MM-DD), upsert into mlb_matchups.

    Includes probable starting pitchers when posted by MLB.  home_sp_id and
    away_sp_id store the MLB player_id (matches mlb_pitcher_stats.player_id
    once Phase 3 stats are loaded).

    Returns list of mlb_matchup IDs upserted.
    """
    target_date = game_date or date.today().isoformat()
    logger.info("Fetching MLB schedule for %s ...", target_date)

    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={
                "sportId": 1,
                "date": target_date,
                "hydrate": "probablePitcher",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("MLB Stats API request failed: %s", e)
        return []

    dates = data.get("dates", [])
    if not dates:
        print(f"No games found for {target_date}")
        return []

    abbrev_cache = build_mlb_team_abbrev_cache(db)

    matchup_ids: list[int] = []
    skipped = 0
    for game in dates[0].get("games", []):
        game_id    = str(game.get("gamePk", ""))
        home_info  = game.get("teams", {}).get("home", {})
        away_info  = game.get("teams", {}).get("away", {})

        home_mlb_id = home_info.get("team", {}).get("id")
        away_mlb_id = away_info.get("team", {}).get("id")

        home_abbrev = MLB_ID_TO_ABBREV.get(home_mlb_id)
        away_abbrev = MLB_ID_TO_ABBREV.get(away_mlb_id)

        home_team_id = abbrev_cache.get(home_abbrev) if home_abbrev else None
        away_team_id = abbrev_cache.get(away_abbrev) if away_abbrev else None

        if not home_team_id or not away_team_id:
            logger.warning(
                "Unknown team IDs for game %s: home_mlb_id=%s (%s) away_mlb_id=%s (%s)",
                game_id, home_mlb_id, home_abbrev, away_mlb_id, away_abbrev,
            )
            skipped += 1
            continue

        # Probable starters — store MLB player_id; NULL if not yet announced
        home_sp_id = home_info.get("probablePitcher", {}).get("id")
        away_sp_id = away_info.get("probablePitcher", {}).get("id")
        ballpark   = game.get("venue", {}).get("name")

        mid = upsert_mlb_matchup(
            db,
            game_date=target_date,
            game_id=game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_sp_id=home_sp_id,
            away_sp_id=away_sp_id,
            ballpark=ballpark,
        )
        if mid:
            matchup_ids.append(mid)

    msg = f"Schedule: {len(matchup_ids)} games upserted for {target_date}"
    if skipped:
        msg += f" ({skipped} skipped — unknown team IDs)"
    print(msg)
    return matchup_ids


def fetch_odds(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch Vegas totals + moneylines from The Odds API and update mlb_matchups.

    Computes consensus averages across ALL bookmakers (not just [0]) for
    stability.  Also derives team-specific implied run totals from moneylines
    using the same compute_team_implied_total() formula as NBA.

    Matches games by home team name against mlb_teams.name.
    Returns number of matchups updated.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping MLB odds fetch")
        return 0

    target_date = game_date or date.today().isoformat()

    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={
                "apiKey": api_key,
                "regions": "us",
                "markets": "h2h,totals",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=20,
        )
        resp.raise_for_status()
        games = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API request failed: %s", e)
        return 0

    # Build lookup: home team name → matchup row for today's MLB games
    rows = db.execute(
        """
        SELECT nm.id, t_home.name AS home_name, t_away.name AS away_name
        FROM mlb_matchups nm
        JOIN mlb_teams t_home ON t_home.team_id = nm.home_team_id
        JOIN mlb_teams t_away ON t_away.team_id = nm.away_team_id
        WHERE nm.game_date = %s
        """,
        (target_date,),
    )
    matchup_by_home: dict[str, dict] = {r["home_name"]: r for r in rows}

    # The Athletics changed name — try both variants
    _OAK_ALIASES = {"Oakland Athletics", "Athletics", "Sacramento Athletics"}

    updated = 0
    for g in games:
        home_name = g.get("home_team", "")
        matchup = matchup_by_home.get(home_name)

        # Handle Athletics name variants from the Odds API
        if not matchup and home_name in _OAK_ALIASES:
            for alias in _OAK_ALIASES:
                matchup = matchup_by_home.get(alias)
                if matchup:
                    break

        if not matchup:
            logger.debug("No matchup found for Odds API home team: %s", home_name)
            continue

        # Consensus across ALL bookmakers for h2h and totals
        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        for bm in g.get("bookmakers") or []:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name or o["name"] in _OAK_ALIASES and home_name in _OAK_ALIASES:
                            home_prices.append(o["price"])
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                elif market["key"] == "totals":
                    over = next(
                        (o for o in market.get("outcomes", []) if o["name"] == "Over"),
                        None,
                    )
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))

        home_ml    = round(sum(home_prices) / len(home_prices)) if home_prices else None
        away_ml    = round(sum(away_prices) / len(away_prices)) if away_prices else None
        vegas_total = round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None

        # Team-specific implied run totals from moneylines
        # MLB avg is ~9 runs/game total, ~4.5 per team.
        # A -200 home favorite in a 9.5 O/U gets ~5.3 implied, not 4.75.
        home_implied = away_implied = None
        if vegas_total and home_ml and away_ml:
            home_implied = round(
                compute_team_implied_total(vegas_total, home_ml, away_ml, is_home=True), 3
            )
            away_implied = round(vegas_total - home_implied, 3)

        db.execute(
            """
            UPDATE mlb_matchups
            SET vegas_total     = %s,
                home_ml         = %s,
                away_ml         = %s,
                vegas_prob_home = %s,
                home_implied    = %s,
                away_implied    = %s
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, vegas_prob_home,
             home_implied, away_implied, matchup["id"]),
        )
        updated += 1

    print(f"Odds: {updated} matchups updated with Vegas lines for {target_date}")
    return updated


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    """Convert American moneylines to vig-removed home win probability."""
    def _raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = _raw(home_ml)
    away_raw = _raw(away_ml)
    total    = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch MLB schedule + odds")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_schedule(db, args.date)
    fetch_odds(db, config.odds_api.api_key, args.date)
