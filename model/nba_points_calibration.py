"""Nightly grade of the books' NBA points lines against the box score.

For each game on an Eastern date, take the LAST capture of the player-points
market made before tip-off (``prop_odds_history``, sport='nba'), read it
exactly as the Top Scorers page does (``model/nba_points_ladder.py``: consensus
line, de-vigged P(over), and from the alternate ladder the floor, ceiling and
P(20+) ... P(40+)), and put each player's actual points beside it from the ESPN
box score (``ingest/nba_box_scores.py``). One row per (game, player, grade
version) in ``nba_points_calibration``.

What the rows let the page say: over how many player-games, how often "25+"
happened when the ladder said 25+ was x%, and the Brier score of each reading.
The market's number is unvalidated until this record exists; this is the
record.

Rules:

- The graded snapshot is the latest capture whose ``captured_at`` is before
  the game's ``commence_time``. A post-tip capture is never graded (in-play
  prices are a different proposition). A game with no pre-tip capture has no
  rows.
- A player is matched to the box score by accent-free name within his own
  game (both teams). No match -> ``unmatched``; two players sharing a key in
  one game -> ``ambiguous``; a listed player who did not play -> ``dnp``
  (books void the line, so the row carries no outcome). Only ``graded`` rows
  enter the calibration cells; the others are counted and shown.
- Pushes cannot happen on a .5 line; an integer line (rare) that lands
  exactly is ``push`` and is excluded like a void.
- Rows are upserted per grade version. A changed reader bumps
  ``GRADE_VERSION`` and writes new rows; it never rewrites old ones.

Usage:
    python -m model.nba_points_calibration                     # yesterday ET, dry run
    python -m model.nba_points_calibration --date 2026-10-21 --apply
    python -m model.nba_points_calibration --days-back 3 --apply
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any

from config import load_config
from db.database import DatabaseManager, fetch_rows
from ingest.nba_box_scores import eastern_date
from model.nba_points_ladder import LADDER_VERSION, THRESHOLDS, name_key, read_player, team_key

GRADE_VERSION = "nba-points-grade-v1"
# Same threshold as SHORT_MINUTES in web/src/lib/nba-points-calibration-charts.ts. A stated cut, not fitted.
SHORT_MINUTES = 15
MAIN_MARKET = "player_points"
LADDER_MARKET = "player_points_alternate"


@dataclass
class BoxPlayer:
    player: str
    player_key: str
    team: str
    is_home: bool
    played: bool
    points: float | None
    minutes: float | None


@dataclass
class BoxGame:
    espn_event_id: str
    home_team: str
    away_team: str
    players: list[BoxPlayer]


@dataclass
class Capture:
    """One player's row in the last pre-tip capture of one game."""
    event_id: str
    commence_time: datetime
    captured_at: datetime
    home_team: str
    away_team: str
    player: str
    books: dict[str, Any]


@dataclass
class GradeRow:
    game_date: date
    event_id: str
    espn_event_id: str | None
    player: str
    player_key: str
    team: str | None
    commence_time: datetime
    captured_at: datetime
    minutes_before_tip: float
    line: float
    p_over: float | None
    books: int
    floor: float | None
    ceiling: float | None
    at_least: dict[int, float | None]
    ladder_books: int
    status: str
    actual_points: float | None
    minutes: float | None


def load_pretip_captures(db: DatabaseManager, target: date) -> tuple[list[Capture], dict[tuple[str, str], dict[str, Any]]]:
    """Main-line rows and {(event, player_key): ladder books} from each game's last pre-tip capture."""
    sql = """
        WITH latest AS (
            SELECT event_id, market, max(captured_at) AS captured_at
            FROM prop_odds_history
            WHERE sport = 'nba' AND game_date = %s AND market IN (%s, %s)
              AND commence_time IS NOT NULL AND captured_at < commence_time
            GROUP BY event_id, market
        )
        SELECT p.event_id, p.market, p.commence_time, p.captured_at, p.home_team_name, p.away_team_name, p.player, p.books
        FROM prop_odds_history p
        JOIN latest l ON l.event_id = p.event_id AND l.market = p.market AND l.captured_at = p.captured_at
        WHERE p.sport = 'nba' AND p.market IN (%s, %s)
    """
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, (target, MAIN_MARKET, LADDER_MARKET, MAIN_MARKET, LADDER_MARKET))
        rows = fetch_rows(cur)
    mains: list[Capture] = []
    ladders: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        books = r["books"] if isinstance(r["books"], dict) else json.loads(r["books"])
        if r["market"] == LADDER_MARKET:
            ladders[(str(r["event_id"]), name_key(r["player"]))] = books
            continue
        mains.append(Capture(
            event_id=str(r["event_id"]), commence_time=r["commence_time"], captured_at=r["captured_at"],
            home_team=str(r["home_team_name"] or ""), away_team=str(r["away_team_name"] or ""),
            player=str(r["player"]), books=books,
        ))
    return mains, ladders


def load_box_scores(db: DatabaseManager, target: date) -> list[BoxGame]:
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT g.espn_event_id, g.home_team, g.away_team, p.player, p.player_key, p.team, p.is_home, p.played, p.points, p.minutes
               FROM nba_box_score_games g
               JOIN nba_box_score_players p ON p.espn_event_id = g.espn_event_id
               WHERE g.game_date = %s AND g.completed""",
            (target,),
        )
        rows = fetch_rows(cur)
    games: dict[str, BoxGame] = {}
    for r in rows:
        game = games.setdefault(str(r["espn_event_id"]), BoxGame(str(r["espn_event_id"]), str(r["home_team"]), str(r["away_team"]), []))
        game.players.append(BoxPlayer(
            player=str(r["player"]), player_key=str(r["player_key"]), team=str(r["team"]), is_home=bool(r["is_home"]),
            played=bool(r["played"]), points=None if r["points"] is None else float(r["points"]),
            minutes=None if r["minutes"] is None else float(r["minutes"]),
        ))
    return list(games.values())


def match_game(home_team: str, away_team: str, games: list[BoxGame]) -> BoxGame | None:
    """The box score whose two teams are this event's two teams; None otherwise (never a guess)."""
    h, a = team_key(home_team), team_key(away_team)
    hits = [g for g in games if team_key(g.home_team) == h and team_key(g.away_team) == a]
    return hits[0] if len(hits) == 1 else None


def grade_captures(target: date, captures: list[Capture], ladders: dict[tuple[str, str], dict[str, Any]],
                   games: list[BoxGame]) -> list[GradeRow]:
    """Pure: one GradeRow per main-line capture row that reads to a consensus line."""
    rows: list[GradeRow] = []
    box_by_event: dict[str, BoxGame | None] = {}
    for cap in captures:
        reading = read_player(cap.books, ladders.get((cap.event_id, name_key(cap.player))))
        if reading is None:
            continue
        if cap.event_id not in box_by_event:
            box_by_event[cap.event_id] = match_game(cap.home_team, cap.away_team, games)
        game = box_by_event[cap.event_id]
        key = name_key(cap.player)
        status, actual, minutes, team = "no_box_score", None, None, None
        if game is not None:
            hits = [p for p in game.players if p.player_key == key]
            if len(hits) > 1:
                status = "ambiguous"
            elif not hits:
                status = "unmatched"
            else:
                hit = hits[0]
                team = hit.team
                minutes = hit.minutes
                if not hit.played or hit.points is None:
                    status = "dnp"
                elif hit.points == reading.line:
                    status, actual = "push", hit.points
                else:
                    status, actual = "graded", hit.points
        dist = reading.distribution
        rows.append(GradeRow(
            game_date=target, event_id=cap.event_id, espn_event_id=game.espn_event_id if game else None,
            player=cap.player, player_key=key, team=team, commence_time=cap.commence_time, captured_at=cap.captured_at,
            minutes_before_tip=(cap.commence_time - cap.captured_at).total_seconds() / 60.0,
            line=reading.line, p_over=reading.p_over, books=len(reading.lines),
            floor=dist.floor if dist else None, ceiling=dist.ceiling if dist else None,
            at_least=(dist.at_least if dist else {n: None for n in THRESHOLDS}),
            ladder_books=dist.ladder_books if dist else 0,
            status=status, actual_points=actual, minutes=minutes,
        ))
    return rows


def persist_rows(db: DatabaseManager, rows: list[GradeRow]) -> int:
    sql = """
        INSERT INTO nba_points_calibration
            (grade_version, ladder_version, game_date, event_id, espn_event_id, player, player_key, team, commence_time,
             captured_at, minutes_before_tip, line, p_over, books, floor, ceiling, p20, p25, p30, p35, p40, ladder_books,
             status, actual_points, minutes, graded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (grade_version, event_id, player_key) DO UPDATE SET
            ladder_version = EXCLUDED.ladder_version, game_date = EXCLUDED.game_date, espn_event_id = EXCLUDED.espn_event_id,
            player = EXCLUDED.player, team = EXCLUDED.team, commence_time = EXCLUDED.commence_time,
            captured_at = EXCLUDED.captured_at, minutes_before_tip = EXCLUDED.minutes_before_tip, line = EXCLUDED.line,
            p_over = EXCLUDED.p_over, books = EXCLUDED.books, floor = EXCLUDED.floor, ceiling = EXCLUDED.ceiling,
            p20 = EXCLUDED.p20, p25 = EXCLUDED.p25, p30 = EXCLUDED.p30, p35 = EXCLUDED.p35, p40 = EXCLUDED.p40,
            ladder_books = EXCLUDED.ladder_books, status = EXCLUDED.status, actual_points = EXCLUDED.actual_points,
            minutes = EXCLUDED.minutes, graded_at = NOW()
    """
    with db.connect() as conn:
        cur = conn.cursor()
        for r in rows:
            cur.execute(sql, (
                GRADE_VERSION, LADDER_VERSION, r.game_date, r.event_id, r.espn_event_id, r.player, r.player_key, r.team,
                r.commence_time, r.captured_at, r.minutes_before_tip, r.line, r.p_over, r.books, r.floor, r.ceiling,
                r.at_least.get(20), r.at_least.get(25), r.at_least.get(30), r.at_least.get(35), r.at_least.get(40),
                r.ladder_books, r.status, r.actual_points, r.minutes,
            ))
    return len(rows)


def summarize(rows: list[GradeRow]) -> dict[str, Any]:
    """Counts by status plus, over graded rows, predicted vs observed and Brier per reading."""
    by_status: dict[str, int] = defaultdict(int)
    for r in rows:
        by_status[r.status] += 1
    graded = [r for r in rows if r.status == "graded" and r.actual_points is not None]
    cells: dict[str, Any] = {}

    def cell(name: str, pairs: list[tuple[float, bool]]) -> None:
        if not pairs:
            cells[name] = {"n": 0}
            return
        n = len(pairs)
        observed = sum(1 for _, hit in pairs if hit) / n
        cells[name] = {
            "n": n,
            "predicted": sum(p for p, _ in pairs) / n,
            "observed": observed,
            "brier": sum((p - (1.0 if hit else 0.0)) ** 2 for p, hit in pairs) / n,
            # Brier of giving everyone this night's hit share: the best any constant does in-sample.
            # The page adds a line-only reference across nights; one night cannot.
            "baseBrier": observed * (1 - observed),
        }

    cell("over", [(r.p_over, r.actual_points > r.line) for r in graded if r.p_over is not None])  # type: ignore[operator]
    for n in THRESHOLDS:
        cell(f"{n}+", [(r.at_least[n], r.actual_points >= n) for r in graded if r.at_least.get(n) is not None])  # type: ignore[operator]
    floors = [(r.floor, r.actual_points) for r in graded if r.floor is not None]
    ceilings = [(r.ceiling, r.actual_points) for r in graded if r.ceiling is not None]
    return {
        "rows": len(rows),
        "byStatus": dict(by_status),
        "games": len({r.event_id for r in rows}),
        "gamesWithBoxScore": len({r.event_id for r in rows if r.espn_event_id}),
        "cells": cells,
        # Graded players who played under SHORT_MINUTES: books settle them as played, so they stay
        # graded; counted so an early exit is not mistaken for a pricing miss.
        "shortMinutes": sum(1 for r in graded if r.minutes is not None and r.minutes < SHORT_MINUTES),
        # The floor is the score beaten 90% of the time; the ceiling the one beaten 10% of the time.
        "floorBeatRate": (sum(1 for f, a in floors if a > f) / len(floors)) if floors else None,  # type: ignore[operator]
        "ceilingBeatRate": (sum(1 for c, a in ceilings if a > c) / len(ceilings)) if ceilings else None,  # type: ignore[operator]
    }


def grade_date(db: DatabaseManager, target: date, *, apply: bool) -> dict[str, Any]:
    captures, ladders = load_pretip_captures(db, target)
    games = load_box_scores(db, target)
    rows = grade_captures(target, captures, ladders, games)
    summary = {"date": target.isoformat(), "preTipCaptureRows": len(captures), "boxScoreGames": len(games), **summarize(rows)}
    if apply and rows:
        summary["written"] = persist_rows(db, rows)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", help="Eastern date of the games (YYYY-MM-DD); default yesterday ET")
    parser.add_argument("--days-back", type=int, default=1, help="also grade this many earlier dates; default 1 = the date only")
    parser.add_argument("--apply", action="store_true", help="write rows; without it, only print the summary")
    args = parser.parse_args()
    last = date.fromisoformat(args.date) if args.date else eastern_date(datetime.now(timezone.utc)) - timedelta(days=1)
    config = load_config()
    db = DatabaseManager(config.database_url)
    db.require_tables(["prop_odds_history", "nba_box_score_games", "nba_box_score_players", "nba_points_calibration"])
    results = []
    with db.reuse_connection():
        for back in range(max(1, args.days_back) - 1, -1, -1):
            results.append(grade_date(db, last - timedelta(days=back), apply=args.apply))
    print(json.dumps({"applied": args.apply, "gradeVersion": GRADE_VERSION, "ladderVersion": LADDER_VERSION, "dates": results},
                     indent=2, default=str))
    for r in results:
        if r["preTipCaptureRows"] and not r["boxScoreGames"]:
            print(f"::warning::{r['date']}: {r['preTipCaptureRows']} pre-tip line rows but no final box scores stored; "
                  "run ingest.nba_box_scores first")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
