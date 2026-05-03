"""One-time backfill of NBA final scores into nba_matchups.

Uses LeagueGameLog (team mode) for the current season to get completed
game scores, then matches to nba_matchups by game_id. By default this
checks regular season, play-in, and playoffs so postseason Vegas results
are included in backtests.

Usage:
    python -m ingest.backfill_scores
    python -m ingest.backfill_scores --season 2024-25
    python -m ingest.backfill_scores --season 2025-26 --season-type Playoffs
    python -m ingest.backfill_scores --season-type Playoffs --start 2026-04-18 --end 2026-05-03
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime
from typing import Iterable

from config import load_config
from db.database import DatabaseManager
from db.queries import build_team_abbrev_cache
from ingest.nba_teams import NBA_ID_TO_ABBREV

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 1.0
DEFAULT_SEASON_TYPES = ("Regular Season", "PlayIn", "Playoffs")
NBA_ABBREV_TO_ID = {abbrev: nba_id for nba_id, abbrev in NBA_ID_TO_ABBREV.items()}


def _normalize_season_types(season_type_args: Iterable[str] | None) -> list[str]:
    """Normalize CLI season-type values into NBA API season type names."""
    if not season_type_args:
        return list(DEFAULT_SEASON_TYPES)

    normalized: list[str] = []
    aliases = {
        "all": list(DEFAULT_SEASON_TYPES),
        "regular": ["Regular Season"],
        "regular season": ["Regular Season"],
        "play-in": ["PlayIn"],
        "playin": ["PlayIn"],
        "play in": ["PlayIn"],
        "playoff": ["Playoffs"],
        "playoffs": ["Playoffs"],
        "postseason": ["Playoffs"],
    }
    for raw in season_type_args:
        value = raw.strip()
        if not value:
            continue
        resolved = aliases.get(value.lower(), [value])
        for season_type in resolved:
            if season_type not in normalized:
                normalized.append(season_type)
    return normalized or list(DEFAULT_SEASON_TYPES)


def _safe_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iso_date(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    parse_text = text[:19] if "T" in text else text
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(parse_text, fmt).date().isoformat()
        except ValueError:
            continue
    return text[:10] if len(text) >= 10 else None


def _map_nba_team_to_db_team(nba_team_id: int | None, abbrev_cache: dict[str, int]) -> int | None:
    if nba_team_id is None:
        return None
    abbrev = NBA_ID_TO_ABBREV.get(nba_team_id)
    if not abbrev:
        return None
    return abbrev_cache.get(abbrev)


def _matchup_row_to_game_id_lookup(row: dict) -> dict[str, int | None | str]:
    home_nba_id = _safe_int(row.get("home_nba_id"))
    away_nba_id = _safe_int(row.get("away_nba_id"))
    return {
        "game_id": str(row["game_id"]),
        "home_nba_id": home_nba_id or NBA_ABBREV_TO_ID.get(str(row.get("home_abbrev") or "")),
        "away_nba_id": away_nba_id or NBA_ABBREV_TO_ID.get(str(row.get("away_abbrev") or "")),
    }


def backfill_scores(
    db: DatabaseManager,
    season: str = "2025-26",
    season_types: Iterable[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """Fetch team game logs for the season and write scores to nba_matchups.

    Matches by game_id (NBA game ID like '0022501038'). If the row came from
    odds-only ingestion and lacks a game_id, falls back to date + home/away team
    matching and writes the NBA game ID at the same time as the final score.
    Returns number of matchups updated.
    """
    from nba_api.stats.endpoints import LeagueGameLog
    from ingest.nba_stats import _call_with_retry

    selected_season_types = _normalize_season_types(season_types)
    abbrev_cache = build_team_abbrev_cache(db)
    matchup_by_game_id = {
        str(row["game_id"]): _matchup_row_to_game_id_lookup(row)
        for row in db.execute(
            """
            SELECT
                nm.game_id,
                home.nba_id AS home_nba_id,
                home.abbreviation AS home_abbrev,
                away.nba_id AS away_nba_id,
                away.abbreviation AS away_abbrev
            FROM nba_matchups nm
            JOIN teams home ON home.team_id = nm.home_team_id
            JOIN teams away ON away.team_id = nm.away_team_id
            WHERE nm.game_id IS NOT NULL
              AND nm.game_id <> ''
            """
        )
    }
    updated = 0

    for season_type in selected_season_types:
        logger.info("Fetching team game logs for season %s (%s) ...", season, season_type)
        time.sleep(SLEEP_SECONDS)

        def _fetch():
            return LeagueGameLog(
                season=season,
                season_type_all_star=season_type,
                player_or_team_abbreviation="T",  # team-level logs
                date_from_nullable=date_from or "",
                date_to_nullable=date_to or "",
                timeout=90,
            )

        logs = _call_with_retry(_fetch, f"LeagueGameLog-teams-{season_type}")
        df = logs.get_data_frames()[0]

        if df.empty:
            logger.warning("No team game logs returned for season %s (%s)", season, season_type)
            continue

        # Each row = one team in one game. MATCHUP contains "@ " for away games.
        # Group by GAME_ID to find both teams' scores.
        game_scores: dict[str, dict] = {}
        for _, row in df.iterrows():
            game_id = str(row["GAME_ID"])
            team_id = _safe_int(row.get("TEAM_ID"))
            pts = _safe_int(row.get("PTS"))
            matchup = str(row.get("MATCHUP", ""))
            game_date = _iso_date(row.get("GAME_DATE"))
            is_home = True if " vs. " in matchup else False if " @ " in matchup else None

            if team_id is None or pts is None:
                continue

            if game_id not in game_scores:
                game_scores[game_id] = {"game_date": game_date, "team_points": {}}
            game_scores[game_id]["team_points"][team_id] = pts

            if is_home is True:
                game_scores[game_id]["home_pts"] = pts
                game_scores[game_id]["home_nba_team_id"] = team_id
            elif is_home is False:
                game_scores[game_id]["away_pts"] = pts
                game_scores[game_id]["away_nba_team_id"] = team_id

        logger.info("Parsed scores for %d games (%s)", len(game_scores), season_type)

        for game_id, scores in game_scores.items():
            matchup = matchup_by_game_id.get(game_id)
            team_points = scores.get("team_points", {})
            if matchup:
                home_pts = team_points.get(matchup.get("home_nba_id"))
                away_pts = team_points.get(matchup.get("away_nba_id"))
            else:
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
                continue

            home_team_id = _map_nba_team_to_db_team(scores.get("home_nba_team_id"), abbrev_cache)
            away_team_id = _map_nba_team_to_db_team(scores.get("away_nba_team_id"), abbrev_cache)
            game_date = scores.get("game_date")
            if game_date is None or home_team_id is None or away_team_id is None:
                continue

            result = db.execute_one(
                """
                UPDATE nba_matchups
                SET game_id = %s, home_score = %s, away_score = %s
                WHERE game_date = %s
                  AND home_team_id = %s
                  AND away_team_id = %s
                  AND (game_id IS NULL OR game_id = '')
                  AND (home_score IS NULL OR away_score IS NULL)
                RETURNING id
                """,
                (game_id, home_pts, away_pts, game_date, home_team_id, away_team_id),
            )
            if result:
                updated += 1

    logger.info(
        "Backfill complete: %d matchups updated for %s (%s)",
        updated,
        season,
        ", ".join(selected_season_types),
    )
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill NBA game scores into nba_matchups")
    parser.add_argument("--season", default="2025-26", help="NBA season (default: 2025-26)")
    parser.add_argument(
        "--season-type",
        action="append",
        dest="season_types",
        help=(
            "NBA season type to fetch. Repeatable. Defaults to all supported score "
            "types: Regular Season, PlayIn, and Playoffs."
        ),
    )
    parser.add_argument("--start", dest="date_from", help="Optional start date YYYY-MM-DD")
    parser.add_argument("--end", dest="date_to", help="Optional end date YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    db._ensure_schema()
    n = backfill_scores(
        db,
        season=args.season,
        season_types=args.season_types,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    print(f"Done - {n} matchups updated with final scores")
