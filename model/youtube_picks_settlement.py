"""Settlement for the YouTube Picks Channel Tracking pipeline (CLAUDE.md,
"YouTube Picks Settlement").

Scope: **moneyline, total, and spread**, for the sports where this project
has real final-score data and existing fuzzy team-name matching --
MLB/soccer (both scores available) and tennis (moneyline only: we have a
winner, not game counts, so tennis totals/spreads stay out of scope).

Grades from free-text `selection` via a conservative parser
(`_parse_total`/`_parse_spread`). Anything that can't be graded honestly is
left out, not faked:
  - partial-game markets (first 5 innings / F5, first half, ...) -- we only
    store full-game finals, so these are marked 'unsettleable' (and a pick
    mis-graded as a full-game moneyline under the old logic is corrected
    back to 'unsettleable');
  - props, futures ("to advance", "to win World Cup"), and every sport
    without a result pipeline (WNBA/NFL/F1/other) -- 'unsettleable' up
    front, so the UI can tell "waiting on a game" from "never graded".

Phases, run every time:
  1. classify   -- (a) mark out-of-scope (sport, bet_type) 'unsettleable';
                   (b) reclassify partial-game markets 'unsettleable'
  2. resolve    -- fuzzy-match to a real game/match, store matchup_ref
                   (frozen: "{sport}:{row_id}:{side}") so grading never
                   re-runs the fuzzy match
  3. grade      -- for resolved picks whose game is final, grade the
                   moneyline / total / spread; status -> won/lost/push

Usage:
    python -m model.youtube_picks_settlement
"""

from __future__ import annotations

import argparse
import logging
import re
from datetime import timedelta

from rapidfuzz import fuzz, process

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    get_resolvable_youtube_picks,
    get_resolved_pending_youtube_picks,
    get_unsettleable_in_scope_youtube_picks,
    get_youtube_picks_for_subgame_check,
    mark_youtube_pick_unsettleable,
    mark_youtube_picks_unsettleable,
    reopen_youtube_pick,
    set_youtube_pick_matchup_ref,
    settle_youtube_pick,
)

logger = logging.getLogger(__name__)

# Supported (sport, bet_type) combinations. Moneyline for all three sports
# with result data; total/spread only where we have both teams' numeric
# scores (MLB runs, soccer goals). Tennis has a winner but no game counts,
# so its totals/spreads are intentionally absent.
_ALLOWED_PAIRS = (
    ("mlb", "moneyline"),
    ("mlb", "total"),
    ("mlb", "spread"),
    ("soccer", "moneyline"),
    ("soccer", "total"),
    ("soccer", "spread"),
    ("tennis", "moneyline"),
)

_MATCH_SCORE_CUTOFF = 65

# Partial-game markets we cannot grade from a full-game final.
_SUBGAME_RE = re.compile(
    r"\b(first 5|first five|1st 5|f5|first 3|first three|first half|1st half|"
    r"first inning|1st inning|first period|1st period|first set|1st set)\b",
    re.IGNORECASE,
)

# Split a combined "Team A vs Team B" subject into its two sides.
_VERSUS_RE = re.compile(r"\s+(?:vs\.?|v\.?|versus|and|@|/)\s+", re.IGNORECASE)


# --------------------------------------------------------------------------
# Parsers (free-text selection -> structured market)
# --------------------------------------------------------------------------

def _parse_total(selection: str) -> tuple[float, str, bool] | None:
    """-> (line, side 'over'|'under', is_team_total) or None."""
    s = selection.lower()
    m = re.search(r"\b(over|under)\b\s+(?:the\s+)?(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(2)), m.group(1), ("team total" in s)


def _parse_spread(selection: str) -> float | None:
    """-> signed spread line relative to the subject (e.g. -1.5), or None."""
    m = re.search(r"([+-]\d+(?:\.\d+)?)", selection)
    return float(m.group(1)) if m else None


def _is_subgame(*texts: str | None) -> bool:
    return any(t and _SUBGAME_RE.search(t) for t in texts)


# --------------------------------------------------------------------------
# Resolution (subject/opponent -> a specific game/match)
# --------------------------------------------------------------------------

def _best_name_match(name: str | None, choices: dict[int, str]) -> int | None:
    if not name or not choices:
        return None
    result = process.extractOne(name, choices, scorer=fuzz.token_set_ratio, score_cutoff=_MATCH_SCORE_CUTOFF)
    return result[2] if result else None


def _date_window(published_at) -> tuple[str, str]:
    d = published_at.date()
    return d.isoformat(), (d + timedelta(days=1)).isoformat()


def _two_team_names(pick: dict) -> tuple[str | None, str | None]:
    """The two sides of the game. Normally subject + opponent; for a game
    total the opponent is null and both teams live in the subject ("Pirates
    vs Nationals"), so split it."""
    name_a, name_b = pick["subject"], pick["opponent"]
    if name_b is None and name_a:
        parts = _VERSUS_RE.split(name_a)
        if len(parts) == 2:
            name_a, name_b = parts[0].strip(), parts[1].strip()
    return name_a, name_b


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


def _resolve_team_game(db: DatabaseManager, pick: dict, table: str, teams: dict[int, str]) -> str | None:
    """Shared MLB/soccer resolution. Tries the exact publish date first, only
    widening to the next day if nothing matches -- a fixed BETWEEN(date,
    date+1) window produced false ambiguity for back-to-back series. `side`
    is the subject team's side; for a game total both teams are summed so the
    side is irrelevant, but for a team total / spread it selects the subject's
    score. `teams` is the pre-loaded {team_id: name} map for the sport (loaded
    once per run, not per pick). Returns 'sport:row_id:side' or None."""
    name_a, name_b = _two_team_names(pick)
    subject_id = _best_name_match(name_a, teams)
    if subject_id is None:
        return None
    opponent_id = _best_name_match(name_b, teams)

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
    return f"tennis:{subject_key // 2}:{'home' if subject_key % 2 == 0 else 'away'}"


# --------------------------------------------------------------------------
# Grading
# --------------------------------------------------------------------------

def _grade_moneyline(side: str, winner: str) -> str:
    return "won" if side == winner else "lost"


def _grade_total(selection: str, home: int, away: int, side: str) -> tuple[str, str] | None:
    parsed = _parse_total(selection)
    if parsed is None:
        return None
    line, over_under, is_team = parsed
    value = (home if side == "home" else away) if is_team else (home + away)
    label = "team total" if is_team else "total"
    if value == line:
        return "push", f"{label} {value} = {line}"
    is_over = value > line
    won = is_over if over_under == "over" else not is_over
    return ("won" if won else "lost"), f"{label} {value} {over_under} {line}"


def _grade_spread(selection: str, home: int, away: int, side: str) -> tuple[str, str] | None:
    line = _parse_spread(selection)
    if line is None:
        return None
    subj, opp = (home, away) if side == "home" else (away, home)
    margin = subj - opp
    adjusted = margin + line
    detail = f"margin {margin:+d}, line {line:+g}"
    if adjusted == 0:
        return "push", detail
    return ("won" if adjusted > 0 else "lost"), detail


def _load_teams(db: DatabaseManager, teams_table: str) -> dict[int, str]:
    return {r["team_id"]: r["name"] for r in db.execute(f"SELECT team_id, name FROM {teams_table}")}


def resolve_pending_picks(db: DatabaseManager) -> int:
    picks = get_resolvable_youtube_picks(db, _ALLOWED_PAIRS)
    if not picks:
        return 0
    # Load each sport's team map once, not per pick.
    mlb_teams = _load_teams(db, "mlb_teams") if any(p["sport"] == "mlb" for p in picks) else {}
    soccer_teams = _load_teams(db, "soccer_teams") if any(p["sport"] == "soccer" for p in picks) else {}

    resolved = 0
    for p in picks:
        if p["published_at"] is None:
            continue
        if p["sport"] == "mlb":
            ref = _resolve_team_game(db, p, "mlb_matchups", mlb_teams)
        elif p["sport"] == "soccer":
            ref = _resolve_team_game(db, p, "soccer_matchups", soccer_teams)
        elif p["sport"] == "tennis":
            ref = _resolve_tennis_match(db, p)
        else:
            continue
        if ref:
            set_youtube_pick_matchup_ref(db, p["id"], ref)
            resolved += 1
    return resolved


def grade_resolved_picks(db: DatabaseManager) -> int:
    picks = get_resolved_pending_youtube_picks(db)
    graded = 0
    for p in picks:
        sport, row_id_s, side = p["matchup_ref"].split(":")
        row_id = int(row_id_s)
        bet_type = p["bet_type"]
        selection = p["selection"]

        if sport in ("mlb", "soccer"):
            if sport == "mlb":
                g = db.execute("SELECT home_score, away_score FROM mlb_matchups WHERE id = %s", (row_id,))
                hs = g[0]["home_score"] if g else None
                as_ = g[0]["away_score"] if g else None
                score_prefix = "Final"
            else:
                g = db.execute(
                    "SELECT reg_home_score, reg_away_score FROM soccer_matchups WHERE id = %s", (row_id,)
                )
                hs = g[0]["reg_home_score"] if g else None
                as_ = g[0]["reg_away_score"] if g else None
                score_prefix = "Final (90')"
            if hs is None or as_ is None:
                continue
            score_str = f"{score_prefix} {as_}-{hs}"

            if bet_type == "moneyline":
                winner = "draw" if hs == as_ else ("home" if hs > as_ else "away")
                status, detail = _grade_moneyline(side, winner), score_str
            elif bet_type == "total":
                res = _grade_total(selection, hs, as_, side)
                if res is None:
                    logger.warning("Unparseable total, leaving pending: %r (pick %s)", selection, p["id"])
                    continue
                status, note = res
                detail = f"{score_str} · {note}"
            elif bet_type == "spread":
                res = _grade_spread(selection, hs, as_, side)
                if res is None:
                    logger.warning("Unparseable spread, leaving pending: %r (pick %s)", selection, p["id"])
                    continue
                status, note = res
                detail = f"{score_str} · {note}"
            else:
                continue

        elif sport == "tennis":
            g = db.execute("SELECT winner FROM tennis_matches WHERE id = %s", (row_id,))
            if not g or g[0]["winner"] not in ("home", "away"):
                continue
            status, detail = _grade_moneyline(side, g[0]["winner"]), f"Winner: {g[0]['winner']}"

        else:
            continue

        settle_youtube_pick(db, p["id"], status, detail)
        graded += 1
    return graded


def reopen_in_scope_unsettleable(db: DatabaseManager) -> int:
    """Re-open picks a narrower prior pass wrote off as 'unsettleable' but
    which are now in scope (e.g. totals/spreads after moneyline-only
    settlement) -- except genuine partial-game markets, which stay
    unsettleable. Makes settlement self-healing when scope widens."""
    reopened = 0
    for p in get_unsettleable_in_scope_youtube_picks(db, _ALLOWED_PAIRS):
        if _is_subgame(p["selection"], p["game_context"]):
            continue
        reopen_youtube_pick(db, p["id"])
        reopened += 1
    return reopened


def reclassify_subgame_picks(db: DatabaseManager) -> int:
    """Mark partial-game markets 'unsettleable' -- including any already
    graded under the old moneyline-only logic (e.g. an F5 moneyline scored
    on the full-game final), which is corrected back here."""
    fixed = 0
    for p in get_youtube_picks_for_subgame_check(db):
        if _is_subgame(p["selection"], p["game_context"]):
            mark_youtube_pick_unsettleable(db, p["id"])
            fixed += 1
    return fixed


def run(db: DatabaseManager) -> dict:
    reopened = reopen_in_scope_unsettleable(db)
    unsettleable = mark_youtube_picks_unsettleable(db, _ALLOWED_PAIRS)
    subgame = reclassify_subgame_picks(db)
    resolved = resolve_pending_picks(db)
    graded = grade_resolved_picks(db)
    print(
        f"YouTube picks settlement: {reopened} reopened (now in scope), "
        f"{unsettleable} marked unsettleable (scope), "
        f"{subgame} reclassified as partial-game, {resolved} newly resolved, "
        f"{graded} graded this run"
    )
    return {
        "reopened": reopened,
        "unsettleable": unsettleable,
        "subgame": subgame,
        "resolved": resolved,
        "graded": graded,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="Settle YouTube picks (MLB/soccer/tennis moneyline/total/spread)").parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    run(db)
