"""Fetch NBA team pace/ratings and player rolling stats from stats.nba.com.

Uses the nba_api Python package which wraps stats.nba.com endpoints.
No API key required — nba_api sets a browser-like User-Agent automatically.

Rate limiting: stats.nba.com returns HTTP 429 if called too quickly.
A 0.6s sleep between requests is sufficient for single-process use.

Data fetched:
  - Team stats: pace, OffRtg, DefRtg via LeagueDashTeamStats (Advanced)
  - Player stats: 10-game rolling averages via LeagueGameLog (per CLAUDE.md)

Usage:
    python -m ingest.nba_stats
    python -m ingest.nba_stats --season 2025-26
"""

from __future__ import annotations

import argparse
import logging
import time

from datetime import date, timedelta

import pandas as pd

from config import load_config
from db.database import DatabaseManager
from db.queries import build_team_abbrev_cache, upsert_nba_team_stats, upsert_nba_player_stats
from ingest.nba_teams import NBA_ID_TO_ABBREV

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 0.6  # stay under stats.nba.com rate limit


def fetch_team_stats(db: DatabaseManager, season: str) -> int:
    """Fetch pace + OffRtg + DefRtg for all 30 teams and upsert into nba_team_stats.

    Returns number of teams updated.
    """
    from nba_api.stats.endpoints import LeagueDashTeamStats

    logger.info("Fetching team stats for season %s ...", season)
    time.sleep(SLEEP_SECONDS)

    endpoint = LeagueDashTeamStats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Advanced",
    )
    df: pd.DataFrame = endpoint.get_data_frames()[0]

    if df.empty:
        logger.warning("LeagueDashTeamStats returned empty DataFrame for season %s", season)
        return 0

    # Map NBA numeric team IDs to our team_ids via abbreviation
    abbrev_cache = build_team_abbrev_cache(db)

    # LeagueDashTeamStats Advanced has TEAM_ID (NBA numeric) but no TEAM_ABBREVIATION.
    # Map via NBA_ID_TO_ABBREV from nba_teams.py.
    updated = 0
    for _, row in df.iterrows():
        nba_id = int(row["TEAM_ID"])
        abbrev = NBA_ID_TO_ABBREV.get(nba_id, "").upper()
        team_id = abbrev_cache.get(abbrev)
        if not team_id:
            logger.warning("No DB team_id for NBA team ID %s (%s)", nba_id, row.get("TEAM_NAME"))
            continue

        upsert_nba_team_stats(
            db,
            team_id=team_id,
            season=season,
            pace=_safe_float(row.get("PACE")),
            off_rtg=_safe_float(row.get("OFF_RATING")),
            def_rtg=_safe_float(row.get("DEF_RATING")),
        )
        updated += 1

    print(f"Team stats: {updated}/30 teams updated for {season}")
    return updated


def fetch_player_rolling_stats(db: DatabaseManager, season: str, n_games: int = 10) -> int:
    """Fetch rolling n-game averages per player via LeagueGameLog.

    LeagueGameLog returns one row per player per game. We group by player,
    take the last n_games, and compute averages. Double-double rate is computed
    from individual game logs (games where 2+ stat categories >= 10).

    Returns number of players updated.
    """
    from nba_api.stats.endpoints import LeagueGameLog

    # LeagueGameLog has no last_n_games parameter — use date_from_nullable to
    # window the last ~30 days (~10–15 games per team in that span).
    date_from = (date.today() - timedelta(days=30)).strftime("%m/%d/%Y")
    logger.info("Fetching player game logs from %s for season %s ...", date_from, season)
    time.sleep(SLEEP_SECONDS)

    endpoint = LeagueGameLog(
        season=season,
        player_or_team_abbreviation="P",
        direction="DESC",
        date_from_nullable=date_from,
    )
    df: pd.DataFrame = endpoint.get_data_frames()[0]

    if df.empty:
        logger.warning("LeagueGameLog returned empty DataFrame")
        return 0

    # Normalize column names (nba_api returns uppercase)
    df.columns = [c.upper() for c in df.columns]

    # Map team abbreviations to our team_ids
    abbrev_cache = build_team_abbrev_cache(db)

    # Group by player — each player appears up to n_games times
    updated = 0
    for player_id, group in df.groupby("PLAYER_ID"):
        group = group.head(n_games)  # ensure at most n_games rows
        games = len(group)
        if games == 0:
            continue

        name = str(group["PLAYER_NAME"].iloc[0])
        team_abbrev = str(group["TEAM_ABBREVIATION"].iloc[0]).upper()
        team_id = abbrev_cache.get(team_abbrev)

        # Per-game averages
        avg_minutes = _safe_float(group["MIN"].apply(_parse_minutes).mean())
        ppg         = _safe_float(group["PTS"].mean())
        rpg         = _safe_float(group["REB"].mean())
        apg         = _safe_float(group["AST"].mean())
        spg         = _safe_float(group["STL"].mean())
        bpg         = _safe_float(group["BLK"].mean())
        tovpg       = _safe_float(group["TOV"].mean())
        threefgm_pg = _safe_float(group["FG3M"].mean())

        # Usage rate proxy: (FGA + 0.44*FTA + TOV) / (avg_min/48 * pace*2 * games)
        # Simplified: per-possession usage relative to a 100-pace game
        fga_pg  = _safe_float(group["FGA"].mean()) or 0.0
        fta_pg  = _safe_float(group["FTA"].mean()) or 0.0
        tov_pg  = tovpg or 0.0
        min_pg  = avg_minutes or 1.0
        usage_rate = ((fga_pg + 0.44 * fta_pg + tov_pg) / max(min_pg / 48.0 * 200, 1)) * 100

        # Double-double rate: fraction of games with 2+ categories >= 10
        def _has_dd(r) -> bool:
            cats = [r.get("PTS", 0) or 0, r.get("REB", 0) or 0, r.get("AST", 0) or 0,
                    r.get("STL", 0) or 0, r.get("BLK", 0) or 0]
            return sum(c >= 10 for c in cats) >= 2
        dd_rate = group.apply(_has_dd, axis=1).mean()

        # Position: nba_api LeagueGameLog doesn't include position — set None,
        # it gets populated by DK CSV eligible_positions at slate time
        upsert_nba_player_stats(
            db,
            player_id=int(player_id),
            season=season,
            team_id=team_id,
            name=name,
            position=None,
            games=games,
            avg_minutes=avg_minutes or 0.0,
            ppg=ppg or 0.0,
            rpg=rpg or 0.0,
            apg=apg or 0.0,
            spg=spg or 0.0,
            bpg=bpg or 0.0,
            tovpg=tovpg or 0.0,
            threefgm_pg=threefgm_pg or 0.0,
            usage_rate=round(usage_rate, 1),
            dd_rate=round(float(dd_rate), 3),
        )
        updated += 1

    print(f"Player stats: {updated} players updated for {season} (last {n_games} games)")
    return updated


def _parse_minutes(val) -> float:
    """Parse NBA minutes string 'MM:SS' or plain float to decimal minutes."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except (ValueError, IndexError):
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch NBA stats from stats.nba.com")
    parser.add_argument("--season", default=None, help="Season string e.g. 2025-26")
    parser.add_argument("--games", type=int, default=10, help="Rolling game window (default 10)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    season = args.season or config.nba_api.season

    fetch_team_stats(db, season)
    fetch_player_rolling_stats(db, season, n_games=args.games)
