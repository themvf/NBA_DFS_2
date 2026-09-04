"""Full-season NFL schedule grid from nflverse, for the survivor pool tool.

WHY THIS EXISTS ALONGSIDE ingest/nfl_schedule.py
------------------------------------------------
`nfl_matchups` is the live-odds layer: it holds whatever The Odds API is
currently listing, and its `week` column is NULL for every row because the
provider does not send one. A survivor grid needs the opposite thing -- all 272
regular-season games, numbered by week, from the moment the season is
published, including the games no book has priced yet.

nflverse's `games.csv` is that: free, no auth, already fetched elsewhere in this
repo for bye weeks, and it carries `spread_line` / moneylines for the games the
market HAS priced. So this module owns the grid and links each row back to the
matching `nfl_matchups` row when one exists; it never duplicates that identity.

Team-code mapping is fail-closed on purpose. nflverse writes `LA` and `WAS`
where `nfl_teams` writes `LAR` and `WSH`, and the last time this repo let an
unmapped code through (`AZ`), the entire Arizona roster silently carried a NULL
team and a NULL bye week for weeks before anyone noticed.

Usage:
    python -m ingest.nfl_season_schedule
    python -m ingest.nfl_season_schedule --season 2026
"""

from __future__ import annotations

import argparse
import io
import logging
import time
from datetime import date, datetime, time as wall_time
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

NFLVERSE_SCHEDULE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
)

# nflverse code -> nfl_teams.abbreviation. Kept identical to
# ingest/ff_independent.py::TEAM_ABBREV_OVERRIDES; if that map grows, this one
# must grow with it.
TEAM_ABBREV_OVERRIDES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}
EASTERN = ZoneInfo("America/New_York")


def fetch_schedule(timeout: int = 60, attempts: int = 3) -> pd.DataFrame:
    """Download games.csv with exponential backoff."""
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(NFLVERSE_SCHEDULE_URL, timeout=timeout)
            response.raise_for_status()
            return pd.read_csv(io.BytesIO(response.content), low_memory=False)
        except Exception as exc:  # noqa: BLE001 - retried below, re-raised after
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse schedule fetch failed: {last}")


def team_id_map(db: DatabaseManager) -> dict[str, int]:
    rows = db.execute("SELECT team_id, abbreviation FROM nfl_teams")
    by_abbrev = {row["abbreviation"]: row["team_id"] for row in rows}
    if len(by_abbrev) < 32:
        from ingest.nfl_teams import seed_teams

        seed_teams(db)
        rows = db.execute("SELECT team_id, abbreviation FROM nfl_teams")
        by_abbrev = {row["abbreviation"]: row["team_id"] for row in rows}
    return by_abbrev


def _resolve(code: object, by_abbrev: dict[str, int]) -> int:
    raw = str(code or "").strip().upper()
    mapped = TEAM_ABBREV_OVERRIDES.get(raw, raw)
    if mapped not in by_abbrev:
        raise ValueError(
            f"unmapped nflverse team code {raw!r} -- add it to "
            f"TEAM_ABBREV_OVERRIDES rather than dropping the row"
        )
    return by_abbrev[mapped]


def _num(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return float(value)


def _int(value: object) -> int | None:
    numeric = _num(value)
    return None if numeric is None else int(numeric)


def _kickoff(gameday: object, gametime: object) -> datetime | None:
    """Build an aware kickoff from nflverse's Eastern-local date and time.

    A fixed UTC offset is incorrect for most of the regular season: September
    games are EDT (UTC-4), while December games are EST (UTC-5).  ZoneInfo also
    preserves the correct offset if daylight-saving rules change later.
    """
    if gameday is None or gametime is None:
        return None
    try:
        if pd.isna(gameday) or pd.isna(gametime):
            return None
    except (TypeError, ValueError):
        pass

    day_text = str(gameday).strip()
    time_text = str(gametime).strip()
    if not day_text or not time_text:
        return None
    day = date.fromisoformat(day_text)
    clock = wall_time.fromisoformat(time_text)
    return datetime.combine(day, clock, tzinfo=EASTERN)


def _link_matchup_ids(db: DatabaseManager, season: int) -> dict[tuple[int, int, str], int]:
    """Map (home_team_id, away_team_id, game_date) -> nfl_matchups.id.

    Keyed on the date as well as the pairing so a rescheduled game links to the
    right odds row, and so this cannot silently collapse two meetings of the
    same teams.
    """
    rows = db.execute(
        """
        SELECT id, home_team_id, away_team_id, game_date::text AS game_date
        FROM nfl_matchups
        WHERE season = %s AND season_type = 'regular'
        """,
        (season,),
    )
    return {
        (row["home_team_id"], row["away_team_id"], row["game_date"]): row["id"]
        for row in rows
    }


def load_season(db: DatabaseManager, season: int, schedule: pd.DataFrame | None = None) -> int:
    frame = schedule if schedule is not None else fetch_schedule()
    games = frame[(frame["season"] == season) & (frame["game_type"] == "REG")]
    if games.empty:
        raise RuntimeError(f"nflverse has no REG games for season {season}")

    by_abbrev = team_id_map(db)
    matchups = _link_matchup_ids(db, season)
    values: list[tuple[object, ...]] = []

    for _, row in games.iterrows():
        home_id = _resolve(row["home_team"], by_abbrev)
        away_id = _resolve(row["away_team"], by_abbrev)
        gameday = None if pd.isna(row.get("gameday")) else str(row["gameday"])
        kickoff = _kickoff(row.get("gameday"), row.get("gametime"))

        home_score = _int(row.get("home_score"))
        away_score = _int(row.get("away_score"))
        spread = _num(row.get("spread_line"))

        values.append(
            (
                season,
                _int(row["week"]),
                str(row["game_type"]),
                str(row["game_id"]),
                matchups.get((home_id, away_id, gameday or "")),
                gameday,
                kickoff,
                home_id,
                away_id,
                bool(_int(row.get("div_game")) or 0),
                None if pd.isna(row.get("roof")) else str(row["roof"]),
                None if pd.isna(row.get("surface")) else str(row["surface"]),
                _int(row.get("home_rest")),
                _int(row.get("away_rest")),
                spread,
                _num(row.get("total_line")),
                _int(row.get("home_moneyline")),
                _int(row.get("away_moneyline")),
                "nflverse" if spread is not None else None,
                home_score,
                away_score,
                home_score is not None and away_score is not None,
            )
        )

    db.execute_many(
        """
        INSERT INTO nfl_season_games (
            season, week, game_type, nflverse_game_id, matchup_id,
            gameday, kickoff, home_team_id, away_team_id,
            div_game, roof, surface, home_rest, away_rest,
            quoted_spread_line, quoted_total_line,
            quoted_home_ml, quoted_away_ml, quote_source,
            home_score, away_score, completed, source_captured_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, NOW()
        )
        ON CONFLICT (nflverse_game_id) DO UPDATE SET
            week = EXCLUDED.week,
            matchup_id = COALESCE(EXCLUDED.matchup_id, nfl_season_games.matchup_id),
            gameday = EXCLUDED.gameday,
            kickoff = EXCLUDED.kickoff,
            home_rest = EXCLUDED.home_rest,
            away_rest = EXCLUDED.away_rest,
            quoted_spread_line = EXCLUDED.quoted_spread_line,
            quoted_total_line = EXCLUDED.quoted_total_line,
            quoted_home_ml = EXCLUDED.quoted_home_ml,
            quoted_away_ml = EXCLUDED.quoted_away_ml,
            quote_source = EXCLUDED.quote_source,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            completed = EXCLUDED.completed,
            source_captured_at = NOW()
        """,
        values,
    )
    return len(values)


def verify_season(db: DatabaseManager, season: int) -> list[str]:
    """Health gate: 32 teams, one bye each, no week gaps.

    Returns a list of problems. An empty list is a pass. This blocks the grid
    and nothing else -- see the survivor spec's rule that a gate may only
    block what it actually covers.
    """
    problems: list[str] = []

    weeks = db.execute(
        "SELECT DISTINCT week FROM nfl_season_games WHERE season = %s ORDER BY week",
        (season,),
    )
    week_numbers = [row["week"] for row in weeks]
    if not week_numbers:
        return [f"no games loaded for season {season}"]
    expected = list(range(1, max(week_numbers) + 1))
    if week_numbers != expected:
        problems.append(f"week gaps: have {week_numbers}, expected {expected}")

    appearances = db.execute(
        """
        SELECT t.abbreviation, COUNT(g.id) AS played
        FROM nfl_teams t
        LEFT JOIN nfl_season_games g
          ON g.season = %s AND (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
        GROUP BY t.abbreviation
        ORDER BY t.abbreviation
        """,
        (season,),
    )
    if len(appearances) != 32:
        problems.append(f"expected 32 teams, found {len(appearances)}")
    total_weeks = len(expected)
    for row in appearances:
        byes = total_weeks - row["played"]
        if byes != 1:
            problems.append(
                f"{row['abbreviation']} plays {row['played']} of {total_weeks} weeks "
                f"({byes} byes, expected 1)"
            )
    return problems


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    written = load_season(db, args.season)
    print(f"Loaded {written} {args.season} regular-season games")

    problems = verify_season(db, args.season)
    if problems:
        for problem in problems:
            print(f"  FAIL {problem}")
        raise SystemExit(1)
    print("  PASS 32 teams x every week x exactly one bye")


if __name__ == "__main__":
    main()
