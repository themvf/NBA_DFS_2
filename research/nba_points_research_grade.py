"""Research copy of the NBA points grade, with Codex's revised pre-tip rules.

Used ONLY by the NBA ghost research code (research/nba_points_ghost*.py,
nba_points_forward.py, nba_ghost_runner.py, nba_points_daily.py). The live
nightly grade is model/nba_points_calibration.py (`nba-points-grade-v1`), which
the registered ladder-v2 tail test reads; its v2 name is reserved for that
test's outcome (docs/nba-points-ladder-v2-registration.md), so this module has
its own version and no writer: persist/CLI were removed and it never touches
nba_points_calibration. Differences from the live grade: the Herb/Herbert alias,
and the pre-tip boundary is the earlier of the market and ESPN scheduled starts
(a capture after either is `after_start`).

Original description follows.

Nightly grade of the books' NBA points lines against the box score.

For each game on an Eastern date, take the LAST capture of the player-points
market made before the earlier known scheduled start (``prop_odds_history``, sport='nba'), read it
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
  the earlier of the market and matched ESPN scheduled starts when available.
  Scheduled times do not prove actual tip time. A game with no eligible capture
  has no rows. Version 2 retains version 1 grades and adds the Herb/Herbert alias.
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
from model.nba_points_ladder import LADDER_VERSION, THRESHOLDS, name_key, player_key, read_player, team_key

GRADE_VERSION = "nba-points-research-grade-v1"
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
    start_time: datetime | None = None


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


def select_pretip_rows(rows: list[dict[str, Any]], games: list[BoxGame]) -> list[dict[str, Any]]:
    """Last event/market snapshot before the earlier known scheduled start.

    Re-select an earlier entire snapshot when providers disagree. Do not keep
    a removed player's older quote or call scheduled time actual tip-off.
    """
    valid = []
    latest: dict[tuple[str, str], datetime] = {}
    for row in rows:
        if row["commence_time"] is None:
            continue
        game = match_game(row["home_team_name"], row["away_team_name"], games)
        boundary = min(row["commence_time"], game.start_time) if game and game.start_time else row["commence_time"]
        if row["captured_at"] >= boundary:
            continue
        key = (str(row["event_id"]), row["market"])
        latest[key] = max(latest.get(key, row["captured_at"]), row["captured_at"])
        valid.append(row)
    return [r for r in valid if r["captured_at"] == latest[(str(r["event_id"]), r["market"])]]


def load_pretip_captures(db: DatabaseManager, target: date, games: list[BoxGame] | None = None) -> tuple[list[Capture], dict[tuple[str, str], dict[str, Any]]]:
    """Main-line rows and {(event, player_key): ladder books} from each game's last pre-tip capture."""
    sql = """
        SELECT p.event_id, p.market, p.commence_time, p.captured_at, p.home_team_name, p.away_team_name, p.player, p.books
        FROM prop_odds_history p
        WHERE p.sport = 'nba' AND p.game_date = %s AND p.market IN (%s, %s)
          AND p.commence_time IS NOT NULL AND p.captured_at < p.commence_time
    """
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, (target, MAIN_MARKET, LADDER_MARKET))
        rows = fetch_rows(cur)
    rows = select_pretip_rows(rows, games if games is not None else load_box_scores(db, target))
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
            """SELECT g.espn_event_id, g.home_team, g.away_team, g.start_time, p.player, p.player_key, p.team, p.is_home, p.played, p.points, p.minutes
               FROM nba_box_score_games g
               JOIN nba_box_score_players p ON p.espn_event_id = g.espn_event_id
               WHERE g.game_date = %s AND g.completed""",
            (target,),
        )
        rows = fetch_rows(cur)
    games: dict[str, BoxGame] = {}
    for r in rows:
        game = games.setdefault(str(r["espn_event_id"]), BoxGame(str(r["espn_event_id"]), str(r["home_team"]), str(r["away_team"]), [], r["start_time"]))
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
        boundary = min(cap.commence_time, game.start_time) if game and game.start_time else cap.commence_time
        if cap.captured_at >= boundary:
            status = "after_start"
        elif game is not None:
            hits = [p for p in game.players if player_key(p.player) == player_key(cap.player)]
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
        cells[name] = {
            "n": n,
            "predicted": sum(p for p, _ in pairs) / n,
            "observed": sum(1 for _, hit in pairs if hit) / n,
            "brier": sum((p - (1.0 if hit else 0.0)) ** 2 for p, hit in pairs) / n,
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
        # The floor is the score beaten 90% of the time; the ceiling the one beaten 10% of the time.
        "floorBeatRate": (sum(1 for f, a in floors if a > f) / len(floors)) if floors else None,  # type: ignore[operator]
        "ceilingBeatRate": (sum(1 for c, a in ceilings if a > c) / len(ceilings)) if ceilings else None,  # type: ignore[operator]
    }
