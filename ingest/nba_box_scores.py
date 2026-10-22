"""NBA box scores from ESPN's public site API, for grading the points lines.

Why ESPN and not stats.nba.com or cdn.nba.com: stats.nba.com blocks GitHub
runners (the reason ``daily_stats.yml`` was removed), and cdn.nba.com's
schedule and box-score JSON answered 403 from a home connection on
2026-10-22. ESPN's scoreboard and summary endpoints need no key and are
reachable from cloud runners. They are a results feed, not a line source:
nothing here costs Odds API credits.

Endpoints (both GET, JSON):

- ``site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=YYYYMMDD``
  lists the games of a calendar date with status and scores;
- ``.../summary?event={id}`` carries the box score: per team, ``statistics[0]``
  with a ``keys`` list (``minutes``, ``points``, ``fieldGoalsMade-fieldGoalsAttempted``
  ...) and one ``athletes`` entry per player with ``stats`` in that order,
  ``starter`` and ``didNotPlay`` plus a ``reason``.

Only games ESPN marks final (``STATUS_FINAL``) are written. A player who did
not play is stored with ``played = false`` and null stats, never a zero: a
zero would grade a scratched player's line as a miss. ``fetched_at`` is
restamped on every write so /health can watch the table.

Usage:
    python -m ingest.nba_box_scores                      # dry run, yesterday ET
    python -m ingest.nba_box_scores --date 2026-10-21 --apply
    python -m ingest.nba_box_scores --days-back 3 --apply   # catch up
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import time
from typing import Any
from zoneinfo import ZoneInfo

import requests

from config import load_config
from db.database import DatabaseManager, fetch_rows
from model.nba_points_ladder import name_key

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
EASTERN = ZoneInfo("America/New_York")
USER_AGENT = "NBADFS-v2 box-score grader (github.com/themvf/NBA_DFS_2)"
FINAL_STATUS = "STATUS_FINAL"
REQUEST_PAUSE_SECONDS = 0.5


def eastern_date(at: datetime) -> date:
    return at.astimezone(EASTERN).date()


def season_label(game_date: date) -> str:
    """"2026-27" for any date from October 2026 to September 2027."""
    start = game_date.year if game_date.month >= 10 else game_date.year - 1
    return f"{start}-{str(start + 1)[-2:]}"


@dataclass
class GameInfo:
    espn_event_id: str
    start_time: datetime
    game_date: date
    status: str
    completed: bool
    home_team: str
    home_abbrev: str | None
    home_espn_id: str | None
    home_score: int | None
    away_team: str
    away_abbrev: str | None
    away_espn_id: str | None
    away_score: int | None


@dataclass
class PlayerLine:
    espn_event_id: str
    espn_athlete_id: str
    player: str
    team: str
    team_abbrev: str | None
    is_home: bool
    starter: bool | None
    played: bool
    dnp_reason: str | None
    minutes: float | None
    points: float | None
    fgm: float | None
    fga: float | None
    fg3m: float | None
    fg3a: float | None
    ftm: float | None
    fta: float | None
    rebounds: float | None
    assists: float | None
    steals: float | None
    blocks: float | None
    turnovers: float | None

    @property
    def player_key(self) -> str:
        return name_key(self.player)


def _int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    try:
        out = float(str(value))
    except (TypeError, ValueError):
        return None
    return out


def _parse_start(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def parse_scoreboard(payload: dict[str, Any]) -> list[GameInfo]:
    games: list[GameInfo] = []
    for event in payload.get("events") or []:
        comps = event.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        start = _parse_start(event.get("date") or comp.get("date"))
        status = ((comp.get("status") or {}).get("type") or {})
        sides: dict[str, dict[str, Any]] = {}
        for team in comp.get("competitors") or []:
            sides[str(team.get("homeAway"))] = team
        home = sides.get("home") or {}
        away = sides.get("away") or {}
        if not home or not away:
            continue
        games.append(GameInfo(
            espn_event_id=str(event["id"]),
            start_time=start,
            game_date=eastern_date(start),
            status=str(status.get("name") or "UNKNOWN"),
            completed=bool(status.get("completed")),
            home_team=str((home.get("team") or {}).get("displayName") or ""),
            home_abbrev=(home.get("team") or {}).get("abbreviation"),
            home_espn_id=str((home.get("team") or {}).get("id") or "") or None,
            home_score=_int(home.get("score")),
            away_team=str((away.get("team") or {}).get("displayName") or ""),
            away_abbrev=(away.get("team") or {}).get("abbreviation"),
            away_espn_id=str((away.get("team") or {}).get("id") or "") or None,
            away_score=_int(away.get("score")),
        ))
    return games


def _split_pair(value: Any) -> tuple[float | None, float | None]:
    """"7-19" -> (7, 19)."""
    text = str(value or "")
    if "-" not in text:
        return None, None
    made, att = text.split("-", 1)
    return _float(made), _float(att)


def parse_summary(payload: dict[str, Any], game: GameInfo) -> list[PlayerLine]:
    """Every listed player of both teams; a non-player keeps null stats."""
    out: list[PlayerLine] = []
    home_id = game.home_espn_id
    for block in (payload.get("boxscore") or {}).get("players") or []:
        team = block.get("team") or {}
        team_id = str(team.get("id") or "")
        team_name = str(team.get("displayName") or "")
        is_home = bool(home_id) and team_id == home_id
        if not is_home and team_name and name_key(team_name) == name_key(game.home_team):
            is_home = True
        groups = block.get("statistics") or []
        if not groups:
            continue
        group = groups[0]
        keys = [str(k) for k in group.get("keys") or []]
        for entry in group.get("athletes") or []:
            athlete = entry.get("athlete") or {}
            dnp = bool(entry.get("didNotPlay"))
            stats = {k: v for k, v in zip(keys, entry.get("stats") or [])}
            fgm, fga = _split_pair(stats.get("fieldGoalsMade-fieldGoalsAttempted"))
            fg3m, fg3a = _split_pair(stats.get("threePointFieldGoalsMade-threePointFieldGoalsAttempted"))
            ftm, fta = _split_pair(stats.get("freeThrowsMade-freeThrowsAttempted"))
            minutes = _float(stats.get("minutes"))
            points = _float(stats.get("points"))
            played = not dnp and points is not None
            out.append(PlayerLine(
                espn_event_id=game.espn_event_id,
                espn_athlete_id=str(athlete.get("id") or ""),
                player=str(athlete.get("displayName") or ""),
                team=team_name,
                team_abbrev=team.get("abbreviation"),
                is_home=is_home,
                starter=entry.get("starter") if isinstance(entry.get("starter"), bool) else None,
                played=played,
                dnp_reason=(str(entry.get("reason")) if dnp and entry.get("reason") else None),
                minutes=minutes if played else None,
                points=points if played else None,
                fgm=fgm if played else None,
                fga=fga if played else None,
                fg3m=fg3m if played else None,
                fg3a=fg3a if played else None,
                ftm=ftm if played else None,
                fta=fta if played else None,
                rebounds=_float(stats.get("rebounds")) if played else None,
                assists=_float(stats.get("assists")) if played else None,
                steals=_float(stats.get("steals")) if played else None,
                blocks=_float(stats.get("blocks")) if played else None,
                turnovers=_float(stats.get("turnovers")) if played else None,
            ))
    return [p for p in out if p.espn_athlete_id and p.player]


def fetch_scoreboard(target: date, session: requests.Session | None = None) -> dict[str, Any]:
    http = session or requests
    response = http.get(f"{ESPN_BASE}/scoreboard", params={"dates": target.strftime("%Y%m%d"), "limit": 50},
                        headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_summary(espn_event_id: str, session: requests.Session | None = None) -> dict[str, Any]:
    http = session or requests
    response = http.get(f"{ESPN_BASE}/summary", params={"event": espn_event_id},
                        headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    return response.json()


def stored_final_games(db: DatabaseManager, target: date) -> set[str]:
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT espn_event_id FROM nba_box_score_games WHERE game_date = %s AND completed AND players > 0", (target,))
        return {str(r["espn_event_id"]) for r in fetch_rows(cur)}


def persist_game(db: DatabaseManager, game: GameInfo, players: list[PlayerLine]) -> None:
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO nba_box_score_games
                   (espn_event_id, season, game_date, start_time, status, completed, home_team, home_abbrev, home_espn_id,
                    home_score, away_team, away_abbrev, away_espn_id, away_score, players, source, fetched_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'espn', NOW())
               ON CONFLICT (espn_event_id) DO UPDATE SET
                   season = EXCLUDED.season, game_date = EXCLUDED.game_date, start_time = EXCLUDED.start_time,
                   status = EXCLUDED.status, completed = EXCLUDED.completed,
                   home_team = EXCLUDED.home_team, home_abbrev = EXCLUDED.home_abbrev, home_espn_id = EXCLUDED.home_espn_id,
                   home_score = EXCLUDED.home_score, away_team = EXCLUDED.away_team, away_abbrev = EXCLUDED.away_abbrev,
                   away_espn_id = EXCLUDED.away_espn_id, away_score = EXCLUDED.away_score, players = EXCLUDED.players,
                   fetched_at = NOW()""",
            (game.espn_event_id, season_label(game.game_date), game.game_date, game.start_time, game.status, game.completed,
             game.home_team, game.home_abbrev, game.home_espn_id, game.home_score, game.away_team, game.away_abbrev,
             game.away_espn_id, game.away_score, len(players)),
        )
        for p in players:
            cur.execute(
                """INSERT INTO nba_box_score_players
                       (espn_event_id, game_date, espn_athlete_id, player, player_key, team, team_abbrev, is_home, starter,
                        played, dnp_reason, minutes, points, fgm, fga, fg3m, fg3a, ftm, fta, rebounds, assists, steals,
                        blocks, turnovers, fetched_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (espn_event_id, espn_athlete_id) DO UPDATE SET
                       game_date = EXCLUDED.game_date, player = EXCLUDED.player, player_key = EXCLUDED.player_key,
                       team = EXCLUDED.team, team_abbrev = EXCLUDED.team_abbrev, is_home = EXCLUDED.is_home,
                       starter = EXCLUDED.starter, played = EXCLUDED.played, dnp_reason = EXCLUDED.dnp_reason,
                       minutes = EXCLUDED.minutes, points = EXCLUDED.points, fgm = EXCLUDED.fgm, fga = EXCLUDED.fga,
                       fg3m = EXCLUDED.fg3m, fg3a = EXCLUDED.fg3a, ftm = EXCLUDED.ftm, fta = EXCLUDED.fta,
                       rebounds = EXCLUDED.rebounds, assists = EXCLUDED.assists, steals = EXCLUDED.steals,
                       blocks = EXCLUDED.blocks, turnovers = EXCLUDED.turnovers, fetched_at = NOW()""",
                (p.espn_event_id, game.game_date, p.espn_athlete_id, p.player, p.player_key, p.team, p.team_abbrev,
                 p.is_home, p.starter, p.played, p.dnp_reason, p.minutes, p.points, p.fgm, p.fga, p.fg3m, p.fg3a,
                 p.ftm, p.fta, p.rebounds, p.assists, p.steals, p.blocks, p.turnovers),
            )


def ingest_date(db: DatabaseManager | None, target: date, *, apply: bool, refetch: bool = False,
                session: requests.Session | None = None) -> dict[str, Any]:
    games = [g for g in parse_scoreboard(fetch_scoreboard(target, session)) if g.game_date == target]
    already = stored_final_games(db, target) if (db is not None and not refetch) else set()
    summary: dict[str, Any] = {
        "date": target.isoformat(), "games": len(games),
        "final": sum(1 for g in games if g.completed and g.status == FINAL_STATUS),
        "notFinal": [f"{g.away_team} @ {g.home_team} ({g.status})" for g in games if not (g.completed and g.status == FINAL_STATUS)],
        "alreadyStored": 0, "written": 0, "players": 0, "didNotPlay": 0,
    }
    for game in games:
        if not (game.completed and game.status == FINAL_STATUS):
            continue
        if game.espn_event_id in already:
            summary["alreadyStored"] += 1
            continue
        if not apply:
            continue
        time.sleep(REQUEST_PAUSE_SECONDS)
        players = parse_summary(fetch_summary(game.espn_event_id, session), game)
        if not players:
            # A final game with no box score is reported, never written as an empty game.
            summary.setdefault("emptyBoxScores", []).append(f"{game.away_team} @ {game.home_team}")
            continue
        if db is None:
            raise ValueError("database connection is required with --apply")
        persist_game(db, game, players)
        summary["written"] += 1
        summary["players"] += len(players)
        summary["didNotPlay"] += sum(1 for p in players if not p.played)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", help="Eastern date of the games (YYYY-MM-DD); default yesterday ET")
    parser.add_argument("--days-back", type=int, default=1, help="also fetch this many earlier dates (catch-up); default 1 = the date only")
    parser.add_argument("--apply", action="store_true", help="write box scores; without it, only list the games")
    parser.add_argument("--refetch", action="store_true", help="re-fetch games already stored as final")
    args = parser.parse_args()
    last = date.fromisoformat(args.date) if args.date else eastern_date(datetime.now(timezone.utc)) - timedelta(days=1)
    config = load_config()
    db = None
    if args.apply:
        db = DatabaseManager(config.database_url)
        db.require_tables(["nba_box_score_games", "nba_box_score_players"])
    results = []
    with requests.Session() as session:
        for back in range(max(1, args.days_back) - 1, -1, -1):
            results.append(ingest_date(db, last - timedelta(days=back), apply=args.apply, refetch=args.refetch, session=session))
    print(json.dumps({"applied": args.apply, "dates": results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
