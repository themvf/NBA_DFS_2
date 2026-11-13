"""How often a quoted NBA player does not play, by book count and by how long before
his own tip the lines were read (``nba-points-sit-lead-v1``, exploratory, 2026-11-12).

The contest builder's first sitting rates (``DNP_BY_BOOKS`` in research/nba_wta_trios.py)
were measured from each game's LAST capture before its own tip. The owner's contest
locks at the night's first tip, and he builds entries before that, so the lines he reads
are hours ahead of the later games. This counts every pre-tip capture of every game.

Rules:
- One row per player per capture: the consensus line read the usual way
  (model/nba_points_ladder.read_player), the number of books at that line, and the hours
  from the capture to the player's own tip.
- Sat = his finished game's box score has no row for him, or lists him as not playing.
- A quoted name that matches no box score on ANY night of the window is a spelling
  difference between the books and ESPN, not an absence: set aside and listed.
- A game with no single matching final box score is left out.

Read-only. Usage:  python -m research.nba_sit_lead --start 2026-10-20 --end 2026-11-11
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
import json

from config import load_config
from db.database import DatabaseManager, fetch_rows
from model.nba_points_calibration import load_box_scores, match_game
from model.nba_points_ladder import player_key, read_player
from research.nba_wta_trios import SIT_LEAD_HOURS

BOOK_BANDS = ((1, 1, "1 book"), (2, 3, "2-3 books"), (4, 10**6, "4+ books"))


def lead_band(hours: float) -> int:
    for i, upper in enumerate(SIT_LEAD_HOURS):
        if hours < upper:
            return i
    return len(SIT_LEAD_HOURS)


def measure(db: DatabaseManager, start: date, end: date) -> dict:
    rows: list[dict] = []
    pending: list[dict] = []          # no box row: absent, or a name the box scores spell differently
    seen_keys: set[str] = set()
    d = start
    while d <= end:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT event_id, commence_time, captured_at, home_team_name, away_team_name, player, books
                   FROM prop_odds_history
                   WHERE sport = 'nba' AND game_date = %s AND market = 'player_points'
                     AND commence_time IS NOT NULL AND captured_at < commence_time""",
                (d,),
            )
            captures = fetch_rows(cur)
        games = load_box_scores(db, d) if captures else []
        for g in games:
            seen_keys.update(player_key(p.player) for p in g.players)
        matched: dict[str, object] = {}
        for r in captures:
            event = str(r["event_id"])
            if event not in matched:
                matched[event] = match_game(str(r["home_team_name"] or ""), str(r["away_team_name"] or ""), games)
            game = matched[event]
            if game is None:
                continue
            books = r["books"] if isinstance(r["books"], dict) else json.loads(r["books"])
            reading = read_player(books, None)
            if reading is None:
                continue
            key = player_key(r["player"])
            hits = [p for p in game.players if player_key(p.player) == key]
            if len(hits) > 1:
                continue
            row = {"date": d, "player": str(r["player"]), "key": key, "books": len(reading.lines), "line": reading.line,
                   "lead": (r["commence_time"] - r["captured_at"]).total_seconds() / 3600.0}
            if not hits:
                pending.append(row)
            else:
                rows.append({**row, "sat": (not hits[0].played) or hits[0].points is None})
        d += timedelta(days=1)
    spelled: Counter[str] = Counter()
    for row in pending:
        if row["key"] in seen_keys:
            rows.append({**row, "sat": True})
        else:
            spelled[row["player"]] += 1
    cells = {}
    for lo, hi, name in BOOK_BANDS:
        for band in range(len(SIT_LEAD_HOURS) + 1):
            sub = [r for r in rows if lo <= r["books"] <= hi and lead_band(r["lead"]) == band]
            cells[(name, band)] = (sum(r["sat"] for r in sub), len(sub))
    return {"rows": len(rows), "nights": len({r["date"] for r in rows}), "cells": cells, "spelledDifferently": dict(spelled)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out = measure(db, date.fromisoformat(args.start), date.fromisoformat(args.end))
    edges = ("under %.1f h" % SIT_LEAD_HOURS[0],
             *("%.1f-%.1f h" % (SIT_LEAD_HOURS[i], SIT_LEAD_HOURS[i + 1]) for i in range(len(SIT_LEAD_HOURS) - 1)),
             "%.1f h or more" % SIT_LEAD_HOURS[-1])
    print(f"{out['rows']} player-captures on {out['nights']} nights")
    for _, _, name in BOOK_BANDS:
        sat_all = sum(out["cells"][(name, b)][0] for b in range(len(edges)))
        n_all = sum(out["cells"][(name, b)][1] for b in range(len(edges)))
        print(f"{name}: {sat_all}/{n_all} = {sat_all / n_all:.4f}" if n_all else f"{name}: no rows")
        for b, label in enumerate(edges):
            sat, n = out["cells"][(name, b)]
            print(f"    lines read {label} before his tip: {sat}/{n}" + (f" = {sat / n:.4f}" if n else ""))
    print("names the box scores spell differently (set aside):", out["spelledDifferently"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
