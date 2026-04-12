"""One-time backfill of NBA final scores into nba_matchups.

Uses LeagueGameLog (team mode) for the current season to get all completed
game scores, then matches to nba_matchups by game_id.

Usage:
    python -m ingest.backfill_scores
    python -m ingest.backfill_scores --season 2024-25
"""

from __future__ import annotations

import argparse
import logging
import time

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 1.0


def backfill_scores(db: DatabaseManager, season: str = "2025-26") -> int:
    """Fetch all team game logs for the season and write scores to nba_matchups.

    Matches by game_id (NBA game ID like '0022501038').
    Returns number of matchups updated.
    """
    from nba_api.stats.endpoints import LeagueGameLog
    from ingest.nba_stats import _call_with_retry

    logger.info("Fetching team game logs for season %s ...", season)
    time.sleep(SLEEP_SECONDS)

    def _fetch():
        return LeagueGameLog(
            season=season,
            player_or_team_abbreviation="T",  # team-level logs
            timeout=90,
        )

    logs = _call_with_retry(_fetch, "LeagueGameLog-teams")
    df = logs.get_data_frames()[0]

    if df.empty:
        logger.warning("No team game logs returned for season %s", season)
        return 0

    # Each row = one team in one game.  MATCHUP contains "@ " for away games.
    # Group by GAME_ID to find both teams' scores.
    game_scores: dict[str, dict] = {}
    for _, row in df.iterrows():
        game_id   = str(row["GAME_ID"])
        team_id   = int(row["TEAM_ID"])
        pts       = row.get("PTS")
        matchup   = str(row.get("MATCHUP", ""))
        is_home   = "@" not in matchup  # "LAL vs. PHX" = home; "LAL @ PHX" = away

        if pts is None:
            continue

        if game_id not in game_scores:
            game_scores[game_id] = {}

        if is_home:
            game_scores[game_id]["home_pts"] = int(pts)
            game_scores[game_id]["home_team_id"] = team_id
        else:
            game_scores[game_id]["away_pts"] = int(pts)
            game_scores[game_id]["away_team_id"] = team_id

    logger.info("Parsed scores for %d games", len(game_scores))

    updated = 0
    for game_id, scores in game_scores.items():
        home_pts = scores.get("home_pts")
        away_pts = scores.get("away_pts")
        if home_pts is None or away_pts is None:
            continue

        result = db.execute_one(
            """
            UPDATE nba_matchups
            SET home_score = %s, away_score = %s
            WHERE game_id = %s
              AND (home_score IS NULL OR away_score IS NULL)
            RETURNING id
            """,
            (home_pts, away_pts, game_id),
        )
        if result:
            updated += 1

    logger.info("Backfill complete: %d matchups updated", updated)
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill NBA game scores into nba_matchups")
    parser.add_argument("--season", default="2025-26", help="NBA season (default: 2025-26)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    db._ensure_schema()
    n = backfill_scores(db, season=args.season)
    print(f"Done — {n} matchups updated with final scores")
