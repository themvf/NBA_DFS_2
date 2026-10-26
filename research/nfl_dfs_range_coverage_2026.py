"""Range coverage of the 2026 saved DFS builds: how often players finish outside P10-P90.

Part of ``nfl-dfs-range-calibration-v1`` (docs/nfl-dfs-range-calibration-study.md).
For every QB/RB/WR/TE the row is what the DFS page showed in the latest saved,
non-research optimizer build made before his game's kickoff (``load()`` in
``model/nfl_week_projection_review.py``), projected 5 or more and not ruled out.
A projected player with no stat line whose team's game has results did not play
and is scored 0 (DraftKings' rule); he is reported in the ``did not play`` split,
which the participation-ledger study owns.

    python -m research.nfl_dfs_range_coverage_2026 --weeks 1-7

Reads the database only. Writes ``artifacts/nfl-dfs-range-calibration/coverage_2026.json``.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from pathlib import Path

from model.nfl_week_projection_review import _num, load

SEASON = 2026
POSITIONS = ("RB", "TE", "WR", "QB")
MIN_PROJECTION = 5.0
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "nfl-dfs-range-calibration" / "coverage_2026.json"


def collect(db, weeks) -> list[dict]:
    rows = []
    for week in weeks:
        for pos in POSITIONS:
            proj, results, finished = load(db, SEASON, week, pos)
            touches = {int(r["player_id"]): r for r in db.execute(
                """SELECT DISTINCT ON (r.player_id) r.player_id, w.source_row->>'carries' c,
                          w.source_row->>'receptions' rec
                   FROM nfl_dfs_player_week_results r JOIN ff_player_week_stats w ON w.id = r.player_week_stat_id
                   WHERE r.season = %s AND r.week = %s AND r.position = %s AND r.scoring_status = 'exact'
                   ORDER BY r.player_id, r.computed_at DESC""", (SEASON, week, pos))}
            for pid, p in proj.items():
                ourp = float(p.get("ourProj") or 0)
                if p.get("isOut") or ourp <= 0:
                    continue
                r = results.get(pid)
                dnp = False
                if r is None:
                    if p.get("team") not in finished:
                        continue  # game not scored yet
                    r = {"points": 0.0}
                    dnp = True
                sm = p.get("statMeans") or {}
                st = touches.get(pid) or {}
                av = p.get("availability") or {}
                rows.append({
                    "week": week, "pos": pos, "pid": pid, "name": p.get("name"), "team": p.get("team"),
                    "game": p.get("gameKey") or p.get("gameInfo"),
                    "proj": ourp, "p10": _num(p.get("floor")), "p50": _num(p.get("median")), "p90": _num(p.get("ceiling")),
                    "actual": float(r["points"]), "dnp": dnp, "anchored": bool(p.get("marketAnchor")),
                    "q": p.get("status") in ("Q", "D") or av.get("status") in ("QUESTIONABLE", "DOUBTFUL"),
                    "pstatus": p.get("projectionStatus"),
                    "proj_touches": float(sm.get("carries") or 0) + float(sm.get("receptions") or 0),
                    "act_touches": (float(st.get("c") or 0) + float(st.get("rec") or 0)) if st else (0.0 if dnp else None),
                })
    return rows


def boot(rows, key, draws=4000, seed=20261025):
    games = defaultdict(list)
    for r in rows:
        games[(r["week"], r["game"])].append(r)
    keys = list(games)
    rnd = random.Random(seed)
    vals = []
    for _ in range(draws):
        sample = [r for k in (rnd.choice(keys) for _ in keys) for r in games[k]]
        vals.append(sum(key(r) for r in sample) / len(sample))
    vals.sort()
    return vals[int(0.025 * draws)], vals[int(0.975 * draws)], len(keys)


def pct(rows, key):
    return f"{100 * sum(key(r) for r in rows) / len(rows):3.0f}%" if rows else "  - "


def report(rows: list[dict], weeks) -> str:
    rows = [r for r in rows if r["proj"] >= MIN_PROJECTION and r["p10"] is not None and r["p90"] is not None]
    below = lambda r: r["actual"] < r["p10"]  # noqa: E731
    above = lambda r: r["actual"] > r["p90"]  # noqa: E731
    inside = lambda r: r["p10"] <= r["actual"] <= r["p90"]  # noqa: E731
    out = ["Below P10 / above P90 / inside, players projected >= 5 (targets 10 / 10 / 80)"]
    out.append("pos week | all: n below above | played: n below above inside | played not-Q: n below | dnp n")
    for pos in POSITIONS:
        for wk in list(weeks) + ["all"]:
            rs = [r for r in rows if r["pos"] == pos and (wk == "all" or r["week"] == wk)]
            if not rs:
                continue
            pl = [r for r in rs if not r["dnp"]]
            pq = [r for r in pl if not r["q"]]
            out.append(f"{pos:3} {str(wk):>4} | {len(rs):4d} {pct(rs, below)} {pct(rs, above)} | "
                       f"{len(pl):4d} {pct(pl, below)} {pct(pl, above)} {pct(pl, inside)} | "
                       f"{len(pq):4d} {pct(pq, below)} | {len(rs) - len(pl):3d}")
        out.append("")
    out.append("Played rows, game-clustered 95% intervals, all weeks:")
    for pos in POSITIONS:
        pl = [r for r in rows if r["pos"] == pos and not r["dnp"]]
        if not pl:
            continue
        b = boot(pl, below); a = boot(pl, above)
        out.append(f"  {pos}: n={len(pl)} games={b[2]}  below P10 {pct(pl, below)} [{100*b[0]:.0f}, {100*b[1]:.0f}]"
                   f"  above P90 {pct(pl, above)} [{100*a[0]:.0f}, {100*a[1]:.0f}]")
    out.append("\nPlayed RBs below P10: touch shortfall (actual touches < half of projected)")
    rb = [r for r in rows if r["pos"] == "RB" and not r["dnp"] and r["act_touches"] is not None and r["proj_touches"] > 0]
    lo = [r for r in rb if below(r)]; hi = [r for r in rb if not below(r)]
    short = lambda r: r["act_touches"] < 0.5 * r["proj_touches"]  # noqa: E731
    out.append(f"  below P10: {sum(map(short, lo))} of {len(lo)}; at or above P10: {sum(map(short, hi))} of {len(hi)}")
    return "\n".join(out)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--weeks", default="1-7")
    parser.add_argument("--from-artifact", action="store_true", help="re-print the stored rows without the database")
    args = parser.parse_args(argv)
    lo, hi = (int(x) for x in args.weeks.split("-"))
    weeks = range(lo, hi + 1)
    if args.from_artifact:
        rows = json.loads(OUT.read_text(encoding="utf-8"))["rows"]
    else:
        from config import load_config
        from db.database import DatabaseManager
        db = DatabaseManager(load_config().database_url, initialize_schema=False)
        rows = collect(db, weeks)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({"season": SEASON, "weeks": list(weeks), "rows": rows}, indent=1, default=str), encoding="utf-8")
    print(report(rows, weeks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
