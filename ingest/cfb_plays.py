"""Backfill CFBD drives and plays for completed seasons.

Why this is a separate, leak-proof backfill
-------------------------------------------
``ratings/*``, ``talent`` and ``player/returning`` return END-OF-SEASON state
when queried for a past season, so a backtest that joins them to a week-3 game
is invalid without a point-in-time capture.  Drives and plays are different in
kind: a play that happened is a fact with its own clock, down, distance and
result.  Re-fetching 2022 today returns the same events it returned then, so
this backfill carries no as-of ambiguity and needs no snapshot discipline.

What this does NOT claim
------------------------
Nothing here is a signal, a feature set, or evidence of an edge.  It is the
raw material score-range and endgame work depends on, ingested once.  The
separate question of whether CFBD's LIVE play feed arrives fast enough to beat
an in-play book is a latency measurement, not this script.

Usage
-----
    python -m ingest.cfb_plays --start-season 2022 --end-season 2025
    python -m ingest.cfb_plays --season 2024 --audit-only
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from ingest.cfb_history import (
    CFBD_BASE,
    _TransactionDb,
    _upsert_game,
    payload_hash,
)
from ingest.game_odds_market import parse_iso

logger = logging.getLogger(__name__)

# CFBD's play feed is scoped by classification; ``fbs`` keeps the team
# universe and the row volume bounded to the cohort every other CFB table in
# this repo is built on.  Widening it is a scope decision, not a default.
CLASSIFICATION = "fbs"
SEASON_TYPES = ("regular", "postseason")
PERIOD_SECONDS = 900


def _cache_path(cache_dir: Path, endpoint: str, season: int, season_type: str, week: int) -> Path:
    return cache_dir / f"{endpoint}-{season}-{season_type}-w{week:02d}.json.gz"


def fetch_cfbd_week(
    endpoint: str,
    *,
    api_key: str,
    season: int,
    season_type: str,
    week: int,
    cache_dir: Path,
    use_cache: bool = True,
    attempts: int = 4,
) -> list[dict]:
    """Fetch one week of ``drives`` or ``plays``, cached as gzipped JSON.

    Both endpoints are week-scoped.  Weeks come from the season's own schedule
    payload rather than a blind 1..20 probe, so an empty response means the
    week genuinely had no plays rather than that we guessed the range wrong.
    """
    path = _cache_path(cache_dir, endpoint, season, season_type, week)
    if use_cache and path.exists():
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    if not api_key:
        raise ValueError("CFBD_API_KEY is required (cached files may be replayed without it)")
    response = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                f"{CFBD_BASE}/{endpoint}",
                params={
                    "year": season,
                    "week": week,
                    "seasonType": season_type,
                    "classification": CLASSIFICATION,
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=120,
            )
            response.raise_for_status()
            payload = response.json() or []
            if not isinstance(payload, list):
                raise ValueError(f"CFBD /{endpoint} returned a non-list payload")
            cache_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                json.dump(payload, handle)
            logger.info(
                "CFBD %s %s %s w%s: %s rows; quota remaining=%s",
                endpoint, season, season_type, week, len(payload),
                response.headers.get("x-requests-remaining", "?"),
            )
            return payload
        except (requests.RequestException, ValueError) as exc:
            retryable = not isinstance(exc, requests.HTTPError) or (
                exc.response is not None and exc.response.status_code in (429, 500, 502, 503, 504)
            )
            if attempt == attempts - 1 or not retryable:
                raise
            delay = 2 ** attempt
            logger.warning(
                "CFBD /%s %s w%s attempt %s failed; retrying in %ss: %s",
                endpoint, season, week, attempt + 1, delay, exc,
            )
            time.sleep(delay)
    raise RuntimeError(f"CFBD /{endpoint} {season} {season_type} w{week} failed: {response}")


def schedule_weeks(games: list[dict], season: int) -> dict[str, list[int]]:
    """Week list per season type, taken from the season's own schedule."""
    weeks: dict[str, set[int]] = {season_type: set() for season_type in SEASON_TYPES}
    for game in games:
        if int(game.get("season") or 0) != season:
            continue
        season_type = str(game.get("seasonType") or "regular").lower()
        if season_type not in weeks:
            continue
        if game.get("week") is not None:
            weeks[season_type].add(int(game["week"]))
    return {season_type: sorted(values) for season_type, values in weeks.items()}


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def wallclock(value: Any):
    """Parse CFBD's play wallclock, tolerating the field being absent.

    ``parse_iso`` raises on a missing value because its callers require a
    commence time.  A play legitimately may not carry a wallclock, so a
    missing or unparseable one becomes NULL rather than failing the week.
    """
    if not value:
        return None
    try:
        return parse_iso(value)
    except (ValueError, TypeError):
        return None


def clock_seconds(clock: Any) -> int | None:
    """Seconds left in the period from CFBD's ``{minutes, seconds}`` clock."""
    if not isinstance(clock, dict):
        return None
    minutes = _int(clock.get("minutes"))
    seconds = _int(clock.get("seconds"))
    if minutes is None and seconds is None:
        return None
    return (minutes or 0) * 60 + (seconds or 0)


def game_seconds_remaining(period: int | None, seconds_left: int | None) -> int | None:
    """Seconds left in REGULATION, derived — never read from the feed.

    Overtime is 0, not a negative number: a period past the fourth has no
    regulation time left, and letting it go negative would silently corrupt
    any endgame slice that filters on a threshold.
    """
    if period is None or seconds_left is None:
        return None
    if period >= 4:
        return 0 if period > 4 else seconds_left
    return (4 - period) * PERIOD_SECONDS + seconds_left


def drive_rows(
    drives: list[dict],
    *,
    matchup_ids: dict[int, int],
    team_ids: dict[int, dict[str, int]],
    season: int,
    season_type: str,
) -> tuple[list[tuple], Counter]:
    rows: list[tuple] = []
    skipped: Counter[str] = Counter()
    for drive in drives:
        source_game = _int(drive.get("gameId"))
        drive_id = _int(drive.get("id"))
        if drive_id is None or source_game is None:
            skipped["missing_identity"] += 1
            continue
        matchup_id = matchup_ids.get(source_game)
        if matchup_id is None:
            skipped["game_not_in_schedule"] += 1
            continue
        names = team_ids.get(source_game, {})
        offense = str(drive.get("offense") or "")
        defense = str(drive.get("defense") or "")
        if offense not in names or defense not in names:
            skipped["team_name_unmapped"] += 1
        rows.append((
            drive_id, matchup_id, source_game, season, season_type,
            _int(drive.get("driveNumber")), offense, defense,
            names.get(offense), names.get(defense),
            drive.get("isHomeOffense"), drive.get("scoring"),
            drive.get("driveResult"),
            _int(drive.get("startPeriod")), _int(drive.get("startYardsToGoal")),
            clock_seconds(drive.get("startTime")),
            _int(drive.get("endPeriod")), _int(drive.get("endYardsToGoal")),
            clock_seconds(drive.get("endTime")),
            _int(drive.get("plays")), _int(drive.get("yards")),
            _int(drive.get("startOffenseScore")), _int(drive.get("startDefenseScore")),
            _int(drive.get("endOffenseScore")), _int(drive.get("endDefenseScore")),
            payload_hash(drive),
        ))
    return rows, skipped


def play_rows(
    plays: list[dict],
    *,
    matchup_ids: dict[int, int],
    team_ids: dict[int, dict[str, int]],
    season: int,
    season_type: str,
    week: int,
) -> tuple[list[tuple], Counter]:
    rows: list[tuple] = []
    skipped: Counter[str] = Counter()
    for play in plays:
        source_game = _int(play.get("gameId"))
        play_id = _int(play.get("id"))
        if play_id is None or source_game is None:
            skipped["missing_identity"] += 1
            continue
        matchup_id = matchup_ids.get(source_game)
        if matchup_id is None:
            skipped["game_not_in_schedule"] += 1
            continue
        names = team_ids.get(source_game, {})
        offense = str(play.get("offense") or "")
        defense = str(play.get("defense") or "")
        if offense not in names or defense not in names:
            skipped["team_name_unmapped"] += 1
        period = _int(play.get("period"))
        seconds_left = clock_seconds(play.get("clock"))
        rows.append((
            play_id, matchup_id, source_game, _int(play.get("driveId")),
            season, season_type, week,
            _int(play.get("driveNumber")), _int(play.get("playNumber")),
            offense, defense, names.get(offense), names.get(defense),
            _int(play.get("offenseScore")), _int(play.get("defenseScore")),
            period, seconds_left, game_seconds_remaining(period, seconds_left),
            _int(play.get("offenseTimeouts")), _int(play.get("defenseTimeouts")),
            _int(play.get("yardline")), _int(play.get("yardsToGoal")),
            _int(play.get("down")), _int(play.get("distance")),
            _int(play.get("yardsGained")), play.get("scoring"),
            play.get("playType"), play.get("playText"), _float(play.get("ppa")),
            wallclock(play.get("wallclock")), payload_hash(play),
        ))
    return rows, skipped


_DRIVE_INSERT = """
    INSERT INTO cfb_drives (
        cfbd_drive_id, game_id, cfbd_game_id, season, season_type,
        drive_number, offense_name, defense_name, offense_team_id, defense_team_id,
        is_home_offense, scoring, drive_result,
        start_period, start_yards_to_goal, start_seconds_remaining,
        end_period, end_yards_to_goal, end_seconds_remaining,
        play_count, yards,
        start_offense_score, start_defense_score, end_offense_score, end_defense_score,
        source_payload_hash
    ) VALUES %s
    ON CONFLICT (cfbd_drive_id) DO UPDATE SET
        drive_result = EXCLUDED.drive_result,
        play_count = EXCLUDED.play_count,
        yards = EXCLUDED.yards,
        offense_team_id = COALESCE(EXCLUDED.offense_team_id, cfb_drives.offense_team_id),
        defense_team_id = COALESCE(EXCLUDED.defense_team_id, cfb_drives.defense_team_id),
        source_payload_hash = EXCLUDED.source_payload_hash
"""

_PLAY_INSERT = """
    INSERT INTO cfb_plays (
        cfbd_play_id, game_id, cfbd_game_id, cfbd_drive_id,
        season, season_type, week, drive_number, play_number,
        offense_name, defense_name, offense_team_id, defense_team_id,
        offense_score, defense_score,
        period, clock_seconds_remaining, game_seconds_remaining,
        offense_timeouts, defense_timeouts,
        yardline, yards_to_goal, down, distance, yards_gained,
        scoring, play_type, play_text, ppa, wallclock, source_payload_hash
    ) VALUES %s
    ON CONFLICT (cfbd_play_id) DO UPDATE SET
        play_type = EXCLUDED.play_type,
        play_text = EXCLUDED.play_text,
        ppa = EXCLUDED.ppa,
        yards_gained = EXCLUDED.yards_gained,
        offense_team_id = COALESCE(EXCLUDED.offense_team_id, cfb_plays.offense_team_id),
        defense_team_id = COALESCE(EXCLUDED.defense_team_id, cfb_plays.defense_team_id),
        source_payload_hash = EXCLUDED.source_payload_hash
"""


def ingest_season(
    db: DatabaseManager,
    *,
    season: int,
    games: list[dict],
    fetch_week,
) -> dict:
    """Upsert every drive and play for one season.

    ``fetch_week(endpoint, season_type, week) -> list[dict]`` is injected so
    the same code path runs against cached files, the live API, or fixtures.
    """
    from psycopg2.extras import execute_values

    weeks = schedule_weeks(games, season)
    games_by_id = {
        int(game["id"]): game for game in games
        if game.get("id") is not None and int(game.get("season") or 0) == season
    }
    matchup_ids: dict[int, int] = {}
    team_ids: dict[int, dict[str, int]] = {}
    team_cache: dict[int, int] = {}
    venue_cache: dict[tuple[int | None, str], int] = {}
    skipped: Counter[str] = Counter()
    per_week: dict[str, dict[str, int]] = {}
    drive_total = play_total = 0

    with db.connect() as connection:
        tx = _TransactionDb(connection)
        cursor = connection.cursor()
        for season_type in SEASON_TYPES:
            for week in weeks.get(season_type, []):
                drives = fetch_week("drives", season_type, week)
                plays = fetch_week("plays", season_type, week)
                touched = {
                    _int(row.get("gameId"))
                    for row in (*drives, *plays)
                } - {None}
                for source_game in sorted(touched):
                    if source_game in matchup_ids:
                        continue
                    game = games_by_id.get(source_game)
                    if game is None:
                        skipped["game_not_in_schedule"] += 1
                        continue
                    matchup_ids[source_game] = _upsert_game(
                        tx, game, team_cache=team_cache, venue_cache=venue_cache,
                    )
                    team_ids[source_game] = {
                        str(game["homeTeam"]): team_cache[int(game["homeId"])],
                        str(game["awayTeam"]): team_cache[int(game["awayId"])],
                    }

                drive_values, drive_skips = drive_rows(
                    drives, matchup_ids=matchup_ids, team_ids=team_ids,
                    season=season, season_type=season_type,
                )
                play_values, play_skips = play_rows(
                    plays, matchup_ids=matchup_ids, team_ids=team_ids,
                    season=season, season_type=season_type, week=week,
                )
                skipped.update({f"drive:{k}": v for k, v in drive_skips.items()})
                skipped.update({f"play:{k}": v for k, v in play_skips.items()})
                if drive_values:
                    execute_values(cursor, _DRIVE_INSERT, drive_values, page_size=1000)
                if play_values:
                    execute_values(cursor, _PLAY_INSERT, play_values, page_size=1000)
                drive_total += len(drive_values)
                play_total += len(play_values)
                per_week[f"{season_type}-w{week:02d}"] = {
                    "drives": len(drive_values), "plays": len(play_values),
                }

    return {
        "season": season,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "classification": CLASSIFICATION,
        "weeks": weeks,
        "games_upserted": len(matchup_ids),
        "drive_rows": drive_total,
        "play_rows": play_total,
        "plays_per_game": round(play_total / len(matchup_ids), 1) if matchup_ids else 0,
        "skipped": dict(sorted(skipped.items())),
        "per_week": per_week,
    }


def audit_rows(drives: list[dict], plays: list[dict]) -> dict:
    """Field-completeness audit run on the raw payload, before any write."""
    drive_ids = {_int(row.get("id")) for row in drives} - {None}
    linked = sum(1 for play in plays if _int(play.get("driveId")) in drive_ids)
    def coverage(rows: list[dict], field: str) -> float:
        return sum(1 for row in rows if row.get(field) is not None) / len(rows) if rows else 0.0
    return {
        "drives": len(drives),
        "plays": len(plays),
        "duplicate_play_ids": len(plays) - len({_int(p.get("id")) for p in plays} - {None}),
        "plays_linked_to_a_drive": linked,
        "play_drive_link_rate": linked / len(plays) if plays else 0.0,
        "field_coverage": {
            field: round(coverage(plays, field), 4)
            for field in ("down", "distance", "yardsGained", "clock", "ppa", "playType", "wallclock")
        },
        "drive_field_coverage": {
            field: round(coverage(drives, field), 4)
            for field in ("driveResult", "startYardsToGoal", "plays", "yards")
        },
    }


def _write_audit(artifact_dir: Path, season: int, report: dict) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / f"cfb-plays-audit-{season}.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--start-season", type=int, default=2022)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=DATA_DIR / "cfb" / "play-cache")
    parser.add_argument(
        "--schedule-cache-dir", type=Path, default=DATA_DIR / "cfb" / "history-cache",
        help="Where ingest.cfb_history caches its /games payloads; reused here for free.",
    )
    parser.add_argument("--artifact-dir", type=Path, default=Path("artifacts/cfb/play-audits"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from ingest.cfb_history import fetch_cfbd

    api_key = os.getenv("CFBD_API_KEY", "")
    seasons = [args.season] if args.season else list(range(args.start_season, args.end_season + 1))
    db = None if args.audit_only else DatabaseManager(load_config().database_url or "")

    for season in seasons:
        games = fetch_cfbd(
            "games", api_key=api_key, season=season,
            cache_dir=args.schedule_cache_dir, use_cache=not args.no_cache,
        )

        def fetch_week(endpoint: str, season_type: str, week: int, _season=season) -> list[dict]:
            return fetch_cfbd_week(
                endpoint, api_key=api_key, season=_season, season_type=season_type,
                week=week, cache_dir=args.cache_dir, use_cache=not args.no_cache,
            )

        if args.audit_only:
            weeks = schedule_weeks(games, season)
            drives: list[dict] = []
            plays: list[dict] = []
            for season_type in SEASON_TYPES:
                for week in weeks.get(season_type, []):
                    drives.extend(fetch_week("drives", season_type, week))
                    plays.extend(fetch_week("plays", season_type, week))
            report = {"season": season, "weeks": weeks, **audit_rows(drives, plays)}
        else:
            report = ingest_season(db, season=season, games=games, fetch_week=fetch_week)

        _write_audit(args.artifact_dir, season, report)
        print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
