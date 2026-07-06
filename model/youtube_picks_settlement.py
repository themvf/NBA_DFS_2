"""Settlement for the YouTube Picks Channel Tracking pipeline (CLAUDE.md,
"YouTube Picks Settlement", 2026-07-06).

Scope (fixed, pre-registered before writing any grading logic): **moneyline
only, for MLB/soccer/tennis.** Spread/total grading needs a structured
numeric line value this project's extraction schema doesn't capture yet
(only free-text `selection`, e.g. "Braves -1.5 runs") -- parsing that
reliably enough to grade real money outcomes is a separate, deferred piece
of work, not silently faked here. Everything else -- other sports
(WNBA/NFL/F1/other), and prop/futures/other bet types even within
MLB/soccer/tennis -- is classified 'unsettleable' up front rather than
left ambiguously 'pending' forever, so the UI can tell "waiting on a game"
apart from "will never be graded."

Three phases, run every time:
  1. classify   -- mark out-of-scope picks 'unsettleable' (one-time per pick)
  2. resolve    -- fuzzy-match subject/opponent to a real game/match, store
                   matchup_ref (frozen: "{sport}:{row_id}:{side}") so grading
                   never needs to re-run the fuzzy match
  3. grade      -- for resolved picks whose game is now final, compare the
                   picked side to the actual winner; status -> won/lost

Usage:
    python -m model.youtube_picks_settlement
"""

from __future__ import annotations

import argparse
import logging
from datetime import timedelta

from rapidfuzz import fuzz, process

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    get_resolvable_youtube_picks,
    get_resolved_pending_youtube_picks,
    mark_youtube_picks_unsettleable,
    set_youtube_pick_matchup_ref,
    settle_youtube_pick,
)

logger = logging.getLogger(__name__)

_SUPPORTED_SPORTS = ("mlb", "soccer", "tennis")
_SUPPORTED_BET_TYPES = ("moneyline",)
_MATCH_SCORE_CUTOFF = 65


def _best_name_match(name: str | None, choices: dict[int, str]) -> int | None:
    if not name or not choices:
        return None
    result = process.extractOne(name, choices, scorer=fuzz.token_set_ratio, score_cutoff=_MATCH_SCORE_CUTOFF)
    return result[2] if result else None


def _date_window(published_at) -> tuple[str, str]:
    """Picks are almost always about the game happening the day they're
    posted (or the next day, for late-night/early-morning posts) -- a
    2-day window keeps this simple without over-fitting to exact hours."""
    d = published_at.date()
    return d.isoformat(), (d + timedelta(days=1)).isoformat()


def _find_team_games(db: DatabaseManager, table: str, day: str, subject_id: int, opponent_id: int | None) -> list[dict]:
    if opponent_id is not None:
        return db.execute(
            f"""SELECT id, home_team_id FROM {table}
                WHERE game_date = %s
                  AND ((home_team_id=%s AND away_team_id=%s) OR (home_team_id=%s AND away_team_id=%s))""",
            (day, subject_id, opponent_id, opponent_id, subject_id),
        )
    return db.execute(
        f"""SELECT id, home_team_id FROM {table}
            WHERE game_date = %s AND (home_team_id=%s OR away_team_id=%s)""",
        (day, subject_id, subject_id),
    )


def _resolve_team_game(db: DatabaseManager, pick: dict, table: str, teams_table: str) -> str | None:
    """Shared MLB/soccer resolution: fuzzy-match subject/opponent to
    teams_table, find a matching row in `table`. Tries the exact publish
    date first, only widening to the next day if nothing matches there --
    a fixed BETWEEN(date, date+1) window was found to produce false
    ambiguity for back-to-back series (same two teams playing on
    consecutive days), which this avoids without guessing between them.
    Returns 'sport:row_id:side' or None if not confidently resolved."""
    teams = {r["team_id"]: r["name"] for r in db.execute(f"SELECT team_id, name FROM {teams_table}")}
    subject_id = _best_name_match(pick["subject"], teams)
    if subject_id is None:
        return None
    opponent_id = _best_name_match(pick["opponent"], teams)

    day, next_day = _date_window(pick["published_at"])
    games = _find_team_games(db, table, day, subject_id, opponent_id)
    if not games:
        games = _find_team_games(db, table, next_day, subject_id, opponent_id)
    if len(games) != 1:
        return None
    side = "home" if games[0]["home_team_id"] == subject_id else "away"
    return f"{pick['sport']}:{games[0]['id']}:{side}"


def _find_tennis_candidates(db: DatabaseManager, day: str) -> list[dict]:
    return db.execute(
        "SELECT id, home_player, away_player FROM tennis_matches WHERE match_date = %s", (day,)
    )


def _resolve_tennis_match(db: DatabaseManager, pick: dict) -> str | None:
    """Same exact-date-first strategy as _resolve_team_game -- see its
    docstring for why a fixed 2-day window was avoided."""
    day, next_day = _date_window(pick["published_at"])
    candidates = _find_tennis_candidates(db, day)
    if not candidates:
        candidates = _find_tennis_candidates(db, next_day)
    if not candidates:
        return None

    players: dict[int, str] = {}
    for c in candidates:
        players[c["id"] * 2] = c["home_player"]
        players[c["id"] * 2 + 1] = c["away_player"]
    subject_key = _best_name_match(pick["subject"], players)
    if subject_key is None:
        return None
    match_id = subject_key // 2
    side = "home" if subject_key % 2 == 0 else "away"
    return f"tennis:{match_id}:{side}"


def resolve_pending_picks(db: DatabaseManager) -> int:
    """Phase 2: fuzzy-match resolvable picks to a real game/match."""
    picks = get_resolvable_youtube_picks(db, _SUPPORTED_SPORTS, _SUPPORTED_BET_TYPES)
    resolved = 0
    for p in picks:
        if p["published_at"] is None:
            continue
        if p["sport"] == "mlb":
            ref = _resolve_team_game(db, p, "mlb_matchups", "mlb_teams")
        elif p["sport"] == "soccer":
            ref = _resolve_team_game(db, p, "soccer_matchups", "soccer_teams")
        elif p["sport"] == "tennis":
            ref = _resolve_tennis_match(db, p)
        else:
            continue
        if ref:
            set_youtube_pick_matchup_ref(db, p["id"], ref)
            resolved += 1
    return resolved


def grade_resolved_picks(db: DatabaseManager) -> int:
    """Phase 3: grade resolved picks whose game/match is now final."""
    picks = get_resolved_pending_youtube_picks(db)
    graded = 0
    for p in picks:
        sport, row_id, side = p["matchup_ref"].split(":")
        row_id = int(row_id)

        if sport == "mlb":
            g = db.execute(
                "SELECT home_score, away_score FROM mlb_matchups WHERE id = %s", (row_id,)
            )
            if not g or g[0]["home_score"] is None or g[0]["away_score"] is None:
                continue
            hs, as_ = g[0]["home_score"], g[0]["away_score"]
            winner = "home" if hs > as_ else "away"
            detail = f"Final {as_}-{hs}"
            status = "won" if side == winner else "lost"

        elif sport == "soccer":
            g = db.execute(
                "SELECT reg_home_score, reg_away_score FROM soccer_matchups WHERE id = %s", (row_id,)
            )
            if not g or g[0]["reg_home_score"] is None or g[0]["reg_away_score"] is None:
                continue
            hs, as_ = g[0]["reg_home_score"], g[0]["reg_away_score"]
            winner = "draw" if hs == as_ else ("home" if hs > as_ else "away")
            detail = f"Final (90') {as_}-{hs}"
            status = "won" if side == winner else "lost"

        elif sport == "tennis":
            g = db.execute("SELECT winner FROM tennis_matches WHERE id = %s", (row_id,))
            if not g or g[0]["winner"] not in ("home", "away"):
                continue
            winner = g[0]["winner"]
            detail = f"Winner: {winner}"
            status = "won" if side == winner else "lost"

        else:
            continue

        settle_youtube_pick(db, p["id"], status, detail)
        graded += 1
    return graded


def run(db: DatabaseManager) -> dict:
    unsettleable = mark_youtube_picks_unsettleable(db, _SUPPORTED_SPORTS, _SUPPORTED_BET_TYPES)
    resolved = resolve_pending_picks(db)
    graded = grade_resolved_picks(db)
    print(f"YouTube picks settlement: {unsettleable} marked unsettleable, "
          f"{resolved} newly resolved to a game, {graded} graded this run")
    return {"unsettleable": unsettleable, "resolved": resolved, "graded": graded}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="Settle YouTube picks (MLB/soccer/tennis moneyline)").parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    run(db)
