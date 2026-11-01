"""Capture NFL game-day inactive lists from ESPN's league injuries feed.

Teams post inactives about 90 minutes before kickoff. ESPN's free league-wide
injuries endpoint (one request, all 32 teams) marks each listed player with
``details.fantasyStatus = INACTIVE`` and the time ESPN recorded it. Breece Hall,
2026 week 8, is the case this exists for: Sleeper and FantasyPros both still
read Questionable while ESPN had him INACTIVE at 16:32 UTC, 88 minutes before
an 18:00 kickoff.

Rows are written as ``nfl_official`` inactive-list observations through the
same path as a reviewed import (ingest/nfl_official_inactives.py), so every
existing consumer reads them: the Game Leaders page, DFS availability and the
pre-lock monitor. Only INACTIVE rows are stored: absence from the list never
means active. A row counts only when ESPN stamped it within three hours before
that team's kickoff and the run itself is pregame. A player already recorded
inactive for the same game is not written again.

Usage:
    python -m ingest.nfl_espn_inactives [--dry-run] [--payload FILE]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase
from ingest.nfl_official_inactives import persist_import, prepare_import

VERSION = "espn-inactives-v1"
URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries"
WINDOW = timedelta(hours=3)
# ESPN team codes that differ from nfl_teams.abbreviation.
TEAM_ALIASES = {"WAS": "WSH", "LA": "LAR", "JAC": "JAX"}
SOURCE_LABEL = "ESPN league injuries feed: NFL game-day inactive list"
# ff_players holds fantasy positions only; an inactive lineman or defender has
# no row to match, which is expected. An unmatched player at one of these is a
# real gap and fails the run.
TRACKED_POSITIONS = {"QB", "RB", "FB", "WR", "TE", "K"}


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _team(value: Any) -> str:
    code = str(value or "").upper()
    return TEAM_ALIASES.get(code, code)


def _athlete_id(athlete: dict[str, Any]) -> str | None:
    """The league feed carries no athlete id field; it is in the player-card links."""
    if athlete.get("id"):
        return str(athlete["id"])
    for link in athlete.get("links") or []:
        match = re.search(r"/id/(\d+)", str(link.get("href") or ""))
        if match:
            return match.group(1)
    return None


def parse_inactives(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Every entry ESPN marks INACTIVE, with its team, id and timestamp."""
    rows = []
    for team in payload.get("injuries") or []:
        for entry in team.get("injuries") or []:
            fantasy = ((entry.get("details") or {}).get("fantasyStatus") or {})
            if str(fantasy.get("abbreviation") or "").upper() != "INACTIVE":
                continue
            athlete = entry.get("athlete") or {}
            recorded = _time(entry.get("date"))
            espn_id = _athlete_id(athlete)
            if recorded is None or espn_id is None:
                continue
            rows.append({
                "espn_id": espn_id,
                "name": athlete.get("displayName") or "",
                "team": _team((athlete.get("team") or {}).get("abbreviation")),
                "position": str((athlete.get("position") or {}).get("abbreviation") or "").upper(),
                "recorded_at": recorded,
                "reason": (entry.get("details") or {}).get("type"),
            })
    return rows


def select_for_games(rows: list[dict[str, Any]], games: list[dict[str, Any]],
                     now: datetime) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Group INACTIVE rows by the upcoming game they belong to.

    A row qualifies when its team plays a game that has not kicked off and ESPN
    stamped it inside the three hours before that kickoff. Anything else
    (an earlier week's list, a stale entry, a team with no game soon) is
    returned as skipped with the reason.
    """
    by_team = {}
    for game in games:
        if game["kickoff"] > now:
            by_team[game["home_team"]] = game
            by_team[game["away_team"]] = game
    grouped: dict[str, list[dict[str, Any]]] = {}
    skipped = []
    for row in rows:
        game = by_team.get(row["team"])
        if game is None:
            continue
        if not game["kickoff"] - WINDOW <= row["recorded_at"] < game["kickoff"]:
            skipped.append({**row, "skip": "recorded outside the 3 hours before kickoff"})
            continue
        grouped.setdefault(game["game_id"], []).append(row)
    return grouped, skipped


def upcoming_games(db: Any, now: datetime) -> list[dict[str, Any]]:
    return db.execute(
        """SELECT g.season,g.week,g.nflverse_game_id game_id,g.kickoff,
                  home.abbreviation home_team,away.abbreviation away_team
           FROM nfl_season_games g
           JOIN nfl_teams home ON home.team_id=g.home_team_id
           JOIN nfl_teams away ON away.team_id=g.away_team_id
           WHERE g.kickoff > %s AND g.kickoff <= %s""",
        (now, now + WINDOW),
    )


def resolve_player(db: Any, season: int, row: dict[str, Any]) -> dict[str, Any] | None:
    """ESPN id first; else one exact name on that team. Ambiguity is unresolved."""
    found = db.execute(
        "SELECT id,gsis_id FROM ff_players WHERE season=%s AND espn_id=%s AND team_abbrev=%s",
        (season, row["espn_id"], row["team"]),
    )
    if len(found) != 1:
        found = db.execute(
            """SELECT id,gsis_id FROM ff_players
               WHERE season=%s AND team_abbrev=%s AND lower(canonical_name)=lower(%s)""",
            (season, row["team"], row["name"]),
        )
    return found[0] if len(found) == 1 else None


def already_recorded(db: Any, season: int, week: int, game_id: str) -> set[int]:
    return {int(r["player_id"]) for r in db.execute(
        """SELECT DISTINCT o.player_id FROM ff_player_injury_observations o
           JOIN ff_source_snapshots s ON s.id=o.source_snapshot_id
           WHERE o.source='nfl_official' AND o.season=%s AND s.week=%s
             AND o.raw_payload->>'gameId'=%s AND o.raw_payload->>'status'='INACTIVE'""",
        (season, week, game_id),
    )}


def capture(db: Any, payload: dict[str, Any], now: datetime, *, apply: bool) -> dict[str, Any]:
    games = upcoming_games(db, now)
    rows = parse_inactives(payload)
    grouped, skipped = select_for_games(rows, games, now)
    game_by_id = {g["game_id"]: g for g in games}
    report: dict[str, Any] = {
        "version": VERSION, "checked_at": now.isoformat(), "upcoming_games": len(games),
        "espn_inactive_rows": len(rows), "games": [], "unmatched": [], "untracked": 0,
        "skipped": [{"name": s["name"], "team": s["team"], "reason": s["skip"]} for s in skipped],
    }
    for game_id, entries in sorted(grouped.items()):
        game = game_by_id[game_id]
        season, week = int(game["season"]), int(game["week"])
        recorded = already_recorded(db, season, week, game_id)
        records, new_entries = [], []
        for entry in entries:
            player = resolve_player(db, season, entry)
            if player is None:
                if entry["position"] in TRACKED_POSITIONS:
                    report["unmatched"].append({"name": entry["name"], "team": entry["team"],
                                                "position": entry["position"], "espn_id": entry["espn_id"]})
                else:
                    report["untracked"] += 1
                continue
            if int(player["id"]) in recorded:
                continue
            records.append({"game_id": game_id, "player_id": int(player["id"]), "status": "INACTIVE"})
            new_entries.append(entry)
        summary = {"game_id": game_id, "inactive": len(entries), "new": len(records),
                   "players": [e["name"] for e in new_entries]}
        if records:
            prepared = prepare_import(db, {
                "season": season, "week": week, "source_label": SOURCE_LABEL,
                "source_published_at": max(e["recorded_at"] for e in new_entries).isoformat(),
                "records": records,
            }, reviewed_by=f"automated:{VERSION}", reviewed_at=now)
            if apply:
                result = persist_import(
                    db, prepared, dataset_prefix="espn-inactives",
                    eligibility_reason="ESPN relay of the exact-game NFL inactive list; captured automatically, not human-reviewed",
                )
                summary["source_snapshot_id"] = result["sourceSnapshotId"]
        report["games"].append(summary)
    return report


def fetch() -> dict[str, Any]:
    response = requests.get(URL, timeout=60, headers={"User-Agent": "NBADFS_v2 inactive capture"})
    response.raise_for_status()
    return response.json()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="validate and report; write nothing")
    parser.add_argument("--payload", type=Path, help="read a saved ESPN response instead of fetching")
    parser.add_argument("--now", help="ISO time to evaluate as (testing only; implies --dry-run)")
    args = parser.parse_args(argv)
    now = _time(args.now) if args.now else datetime.now(timezone.utc)
    if now is None:
        parser.error("--now must be a timezone-aware ISO time")
    apply = not args.dry_run and not args.now
    payload = json.loads(args.payload.read_text(encoding="utf-8")) if args.payload else fetch()
    db = RefreshDatabase(load_config().database_url)
    failed = False
    try:
        report = capture(db, payload, now, apply=apply)
        report["applied"] = apply
        print(json.dumps(report, indent=2, default=str))
        # A skill player ESPN lists that we cannot place is a gap to look at.
        return 1 if report["unmatched"] else 0
    except Exception:
        failed = True
        raise
    finally:
        db.close(error=failed or not apply)


if __name__ == "__main__":
    sys.exit(main())
