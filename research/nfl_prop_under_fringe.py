"""Fringe-receiver tag on the flagged unders of nfl-prop-under-v2 (docs/nfl-prop-under-fringe-study.md).

Reads the same priced rows and bets as research/nfl_prop_under_study.py (imported, not
changed), then tags each flagged receiving-yards or receptions under by the player's role
on DraftKings' own board: his line's rank among his team's lines for that market in the
same pricing snapshot. Rank 3 or worse is "fringe". Reports fringe against top-two
flagged unders. With --write-db it appends one run row a week and, at the single look
after week 18, the one verdict (both tables append-only).

    python -m research.nfl_prop_under_fringe sizes --week 7     # tag counts only, no results
    python -m research.nfl_prop_under_fringe grade [--write-db] [--out path]
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from research import nfl_prop_under_study as under
from research.nfl_prop_under_role_change import paired_week_interval

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-prop-under-fringe-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())


def parent_registration() -> dict:
    return json.loads((ROOT / REGISTRATION["parent_registration"]).read_text())


def board_ranks(rows: Iterable[Mapping[str, Any]], markets: Iterable[str]) -> dict[tuple, int]:
    """(game_id, metric, identity) -> competition rank of the line within (game, team, metric).

    Rank 1 + the number of teammates whose line is strictly higher, so tied lines share a rank
    and the order rows arrive in cannot change a tag. Rows without a team are left unranked."""
    markets = set(markets)
    groups: dict[tuple, list[Mapping[str, Any]]] = defaultdict(list)
    for r in rows:
        if r["metric"] in markets and r.get("team"):
            groups[(r["game_id"], r["team"], r["metric"])].append(r)
    ranks = {}
    for members in groups.values():
        for r in members:
            ranks[(r["game_id"], r["metric"], r["identity"])] = 1 + sum(o["line"] > r["line"] for o in members)
    return ranks


def tag_for(rank: int | None, registration: Mapping[str, Any] = REGISTRATION) -> str:
    if rank is None:
        return "unranked"
    return "fringe" if rank >= registration["tag"]["fringe_min_rank"] else "top"


def summarize(bets: list[dict], registration: Mapping[str, Any] = REGISTRATION, final: bool = False) -> dict:
    """`final` is True only once every regular-season game is graded: the single look."""
    parent = parent_registration()
    graded = [b for b in bets if b["result"] != "void"]
    in_scope = [b for b in graded if b["metric"] in registration["tag"]["markets"]]
    fringe = [b for b in in_scope if b["fringe"]["tag"] == "fringe"]
    top = [b for b in in_scope if b["fringe"]["tag"] == "top"]
    boot = registration["bootstrap"]
    floors = registration["floors"]
    floors_met = (len(fringe) >= floors["fringe_bets"] and len(top) >= floors["top_bets"]
                  and len({b["week"] for b in fringe}) >= floors["game_weeks"])
    interval = paired_week_interval(fringe, top, boot["resamples"], boot["seed"], boot["interval"])
    fringe_own = under.clustered_interval(fringe, boot["resamples"], boot["seed"], boot["interval"]) if fringe else None
    verdict = None
    if final:
        if not floors_met or not interval:
            verdict = "INCONCLUSIVE"
        else:
            verdict = "FRINGE_WORSE" if interval[1] < 0 else "FRINGE_BETTER" if interval[0] > 0 else "NO_DIFFERENCE"

    def arm(rows):
        return under.summarize(rows, registration=parent, with_interval=False)

    roi_f, roi_t = arm(fringe)["roi"], arm(top)["roi"]
    return {
        "study": registration["study"],
        "fringe": {**arm(fringe), "roi_interval": [round(x, 4) for x in fringe_own] if fringe_own else None},
        "top": arm(top),
        "difference_roi": round(roi_f - roi_t, 4) if roi_f is not None and roi_t is not None else None,
        "difference_interval": [round(x, 4) for x in interval] if interval else None,
        "floors": floors, "floors_met": floors_met, "verdict_if_first": verdict,
        "status": "final" if final else "descriptive until the final look",
        "descriptive": {
            "fringe_by_market": {m: arm([b for b in fringe if b["metric"] == m]) for m in registration["tag"]["markets"]},
            "fringe_by_rank": {str(k): arm([b for b in fringe if (b["fringe"]["rank"] >= 4) == (k == "4+")])
                               for k in ("3", "4+")},
            "rushing_flagged_out_of_scope": arm([b for b in graded if b["metric"] not in registration["tag"]["markets"]]),
            "unranked": sum(b["fringe"]["tag"] == "unranked" for b in in_scope),
        },
    }


def attach_teams(cur, rows: list[dict], parent: Mapping[str, Any]) -> None:
    """Give each priced row its team, from the same table row."""
    cur.execute("""SELECT game_id, metric, identity, computed_at, team FROM nfl_game_leader_market
                   WHERE game_id LIKE %s AND version = %s AND book = %s""",
                (f"{parent['season']}_%", parent["market_version"], parent["book"]))
    team = {(g, m, i, c): t for g, m, i, c, t in cur.fetchall()}
    for r in rows:
        r["team"] = team.get((r["game_id"], r["metric"], r["identity"], r["computed_at"]))


def tag_rows(chosen: list[dict], registration: Mapping[str, Any] = REGISTRATION) -> dict[tuple, dict]:
    ranks = board_ranks(chosen, registration["tag"]["markets"])
    out = {}
    for r in chosen:
        key = (r["game_id"], r["metric"], r["identity"])
        rank = ranks.get(key)
        out[key] = {"rank": rank, "tag": tag_for(rank, registration) if r["metric"] in registration["tag"]["markets"]
                    else "out_of_scope"}
    return out


def write_run(out: Mapping[str, Any], report: Mapping[str, Any]) -> tuple | None:
    """Append this run; write the verdict only at the final look, once."""
    import os
    from db.database import DatabaseManager
    DatabaseManager(os.environ["DATABASE_URL"])  # applies db/schema.py (no-op when unchanged)
    conn = under._connect()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO nfl_prop_under_fringe_runs (study, evaluated_at, fringe_bets, top_bets,
                           fringe_game_weeks, difference_roi, report)
                       VALUES (%s, now(), %s, %s, %s, %s, %s) RETURNING id""",
                    (out["study"], out["fringe"]["bets"], out["top"]["bets"], out["fringe"]["game_weeks"],
                     out["difference_roi"], json.dumps(report, default=str)))
        run_id = cur.fetchone()[0]
        if out["verdict_if_first"]:
            cur.execute("""INSERT INTO nfl_prop_under_fringe_verdicts (study, verdict, run_id, decided_at)
                           VALUES (%s, %s, %s, now()) ON CONFLICT (study) DO NOTHING""",
                        (out["study"], out["verdict_if_first"], run_id))
        conn.commit()
        cur.execute("SELECT verdict, decided_at FROM nfl_prop_under_fringe_verdicts WHERE study = %s", (out["study"],))
        return cur.fetchone()
    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sizes", help="count tags on one week's flagged rows; no results are read")
    s.add_argument("--week", type=int, required=True)
    g = sub.add_parser("grade")
    g.add_argument("--out", type=Path)
    g.add_argument("--write-db", action="store_true")
    a = p.parse_args()
    parent = parent_registration()
    conn = under._connect()
    cur = conn.cursor()
    rows, kickoffs, weeks, completed = under.load_inputs(cur, parent["season"])
    attach_teams(cur, rows, parent)
    conn.rollback()
    conn.close()
    if a.command == "sizes":
        chosen = under.select_rows(rows, kickoffs, None, None, registration=parent)
        week_rows = [r for r in chosen if weeks.get(r["game_id"]) == a.week]
        tags = tag_rows(week_rows)
        counts: dict[str, int] = defaultdict(int)
        for r in week_rows:
            if under.flagged(r, parent):
                counts[tags[(r["game_id"], r["metric"], r["identity"])]["tag"]] += 1
        print(json.dumps({"week": a.week, "games": len({r["game_id"] for r in week_rows}), "flagged_by_tag": counts},
                         indent=1))
        return
    start = datetime.fromisoformat(REGISTRATION["registered_at"])
    stats, covered, nfl_source = under.load_stats(parent["season"], weeks, completed)
    report = under.grade(rows, kickoffs, weeks, stats, covered, registration=parent)
    c_start, c_end = under.cohort_window(parent)
    chosen = under.select_rows(rows, kickoffs, c_start, c_end, registration=parent)
    tags = tag_rows(chosen)
    bets = [{**b, "fringe": tags[(b["game_id"], b["metric"], b["identity"])]}
            for b in report["bets"] if kickoffs[b["game_id"]] > start]
    last = REGISTRATION["verdict"]["last_week"]
    final = all(g in covered for g, w in weeks.items() if w <= last)
    out = summarize(bets, final=final)
    out.update(evaluated_at=datetime.now(timezone.utc).isoformat(), nflverse_results=nfl_source,
               parent_study=parent["study"], registration_sha256=under.file_digest(REGISTRATION_PATH))
    full = {**out, "bets": bets}
    if a.write_db:
        out["frozen_verdict"] = write_run(out, full)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(full, indent=1, default=str))
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
