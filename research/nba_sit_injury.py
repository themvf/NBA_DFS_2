"""How often a quoted NBA player does not play, by his status on the official injury
report read before the contest lock (``nba-points-sit-injury-v1``, exploratory, 2026-11-13).

The lock is the night's first tip. For each night the script takes each game's last
capture before the lock and the last official report observed before the lock
(``nba_injury_observations``), and counts who then sat.

Rules:
- A player's status is the report row for his own game (date, player, both teams). No
  row: "not listed", split by whether both teams in his game had submitted their report.
- Sat = his finished game's box score has no row for him, or lists him as not playing.
  A quoted name that matches no box score on any night of the window is a spelling
  difference, not an absence, and is set aside.
- ``--morning`` uses each night's FIRST report and first capture instead, to show how
  much a morning status is worth.

Read-only. Usage:  python -m research.nba_sit_injury --start 2026-11-02 --end 2026-11-12
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, timedelta
import json

from config import load_config
from db.database import DatabaseManager, fetch_rows
from model.nba_points_calibration import Capture, grade_captures, load_box_scores
from model.nba_points_ladder import player_key, team_key

ORDER = ("Out", "Doubtful", "Questionable", "Probable", "Available", "not listed, both teams submitted",
         "not listed, a team not submitted", "no report")


def _team(name: str) -> str:
    k = team_key(name)
    return "losangelesclippers" if k == "laclippers" else k


def measure(db: DatabaseManager, start: date, end: date, *, morning: bool = False, min_books: int = 2) -> dict:
    rows: list[dict] = []
    pending: list[dict] = []
    seen: set[str] = set()
    d = start
    while d <= end:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT min(commence_time) AS first_tip, min(captured_at) AS first_capture FROM prop_odds_history
                   WHERE sport = 'nba' AND game_date = %s AND market = 'player_points' AND commence_time IS NOT NULL""",
                (d,),
            )
            head = fetch_rows(cur)[0]
            if head["first_tip"] is None:
                d += timedelta(days=1)
                continue
            lock = head["first_tip"]
            cutoff = head["first_capture"] + timedelta(minutes=45) if morning else lock
            cur.execute(
                """WITH latest AS (SELECT event_id, max(captured_at) AS captured_at FROM prop_odds_history
                                   WHERE sport = 'nba' AND game_date = %s AND market = 'player_points' AND captured_at < %s
                                   GROUP BY event_id)
                   SELECT p.event_id, p.commence_time, p.captured_at, p.home_team_name, p.away_team_name, p.player, p.books
                   FROM prop_odds_history p JOIN latest l ON l.event_id = p.event_id AND l.captured_at = p.captured_at
                   WHERE p.sport = 'nba' AND p.market = 'player_points'""",
                (d, cutoff),
            )
            captures = fetch_rows(cur)
            cur.execute(
                """SELECT published_at, observed_at, states, report->'team_submissions' AS submissions
                   FROM nba_injury_observations
                   WHERE observed_at < %s AND published_at <= observed_at
                     AND EXISTS (SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date' = %s)
                   ORDER BY published_at DESC, observed_at DESC LIMIT 1""",
                (cutoff, d.isoformat()),
            )
            report = fetch_rows(cur)
        status: dict[tuple[str, str, str], str] = {}
        submitted: dict[tuple[str, str], bool] = {}
        lead = None
        if report:
            rep = report[0]
            lead = (lock - rep["observed_at"]).total_seconds() / 3600.0
            states = rep["states"] if isinstance(rep["states"], list) else json.loads(rep["states"])
            subs = rep["submissions"] if isinstance(rep["submissions"], list) else json.loads(rep["submissions"] or "[]")
            for s in states:
                if s.get("game_date") == d.isoformat() and s.get("player_key") and s.get("status"):
                    status[(s["player_key"], s.get("home_key", ""), s.get("away_key", ""))] = s["status"]
            for s in subs:
                if s.get("game_date") == d.isoformat():
                    submitted[(_team(str(s.get("team", ""))), str(s.get("matchup", "")))] = s.get("submission") == "submitted"
        games = load_box_scores(db, d)
        for g in games:
            seen.update(player_key(p.player) for p in g.players)
        caps = [Capture(str(r["event_id"]), r["commence_time"], r["captured_at"], str(r["home_team_name"] or ""),
                        str(r["away_team_name"] or ""), str(r["player"]),
                        r["books"] if isinstance(r["books"], dict) else json.loads(r["books"])) for r in captures]
        teams_of = {c.event_id: (_team(c.home_team), _team(c.away_team)) for c in caps}
        for g in grade_captures(d, caps, {}, games):
            if g.status not in ("graded", "dnp", "push", "unmatched") or g.books < min_books:
                continue
            home, away = teams_of[g.event_id]
            key = player_key(g.player)
            if not report:
                label = "no report"
            elif (key, home, away) in status:
                label = status[(key, home, away)]
            else:
                both = [v for (team, _), v in submitted.items() if team in (home, away)]
                label = "not listed, both teams submitted" if len(both) == 2 and all(both) else "not listed, a team not submitted"
            row = {"date": d, "player": g.player, "key": key, "line": g.line, "label": label, "reportLead": lead,
                   "tipAfterLock": (g.commence_time - lock).total_seconds() / 3600.0}
            if g.status == "unmatched":
                pending.append(row)
            else:
                rows.append({**row, "sat": g.status == "dnp"})
        d += timedelta(days=1)
    spelled = sorted({r["player"] for r in pending if r["key"] not in seen})
    rows += [{**r, "sat": True} for r in pending if r["key"] in seen]
    cells: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for r in rows:
        cells[r["label"]][0] += r["sat"]
        cells[r["label"]][1] += 1
    return {"rows": rows, "cells": dict(cells), "spelledDifferently": spelled,
            "reportLeads": sorted({round(r["reportLead"], 1) for r in rows if r["reportLead"] is not None})}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--morning", action="store_true", help="each night's first report and first capture")
    parser.add_argument("--list", action="store_true", help="also list every player who sat")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out = measure(db, date.fromisoformat(args.start), date.fromisoformat(args.end), morning=args.morning)
    rows = out["rows"]
    print(f"{len(rows)} players with a line at two or more books on {len({r['date'] for r in rows})} nights; "
          f"hours from the report to the lock: {out['reportLeads']}")
    for label in ORDER:
        if label in out["cells"]:
            sat, n = out["cells"][label]
            print(f"  {label:36s} sat {sat:3d} of {n:4d} = {sat / n:.3f}")
    print("names the box scores spell differently (set aside):", out["spelledDifferently"])
    if args.list:
        for r in sorted((r for r in rows if r["sat"]), key=lambda r: (r["date"], -r["line"])):
            print(f"    {r['date']} {r['player']} line {r['line']} | {r['label']} | tips {r['tipAfterLock']:.1f} h after the lock")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
