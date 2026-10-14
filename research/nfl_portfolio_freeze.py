"""Freeze gate for nfl-portfolio-objective-v1: realized thresholds from stored contests, implied ones from the challenger.

Reads the stored flagship score curves (nfl_dfs_field_contests), the frozen
discovery inputs that carry an ownership prior (the 2026-10-13 attribution
artifacts, weeks 4-5), and runs web/scripts/research-nfl-portfolio-freeze.ts.
Writes artifacts/nfl-portfolio-objective-v1/freeze.json, which the study runner
requires before saving a portfolio arm. Read-only against the database.

    python -m research.nfl_portfolio_freeze [--out artifacts/nfl-portfolio-objective-v1/freeze.json]
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
from pathlib import Path

from config import load_config
from db.database import DatabaseManager

ROOT = Path(__file__).resolve().parents[1]
MODULE_FILES = ["web/src/lib/nfl-dfs/portfolio-objective.ts", "web/src/lib/nfl-dfs/captain-simulation.ts",
                "web/scripts/nfl-pool-rule-study-core.ts", "web/scripts/research-nfl-portfolio-freeze.ts"]
# Discovery slates with an ownership prior in the frozen snapshot, and the flagship contest each was played in.
CHECKS = [
    ("artifacts/nfl-classic-attribution-2026-10-13/inputs/2026-w4-classic.json.gz", "classic", "2026 w4 Classic", "196151357"),
    ("artifacts/nfl-classic-attribution-2026-10-13/inputs/2026-w5-classic.json.gz", "classic", "2026 w5 Classic", "196403752"),
    ("artifacts/nfl-showdown-attribution-2026-10-13/inputs/2026-w4-ATL_at_NO.json.gz", "showdown", "2026 w4 ATL@NO", "196285155"),
    ("artifacts/nfl-showdown-attribution-2026-10-13/inputs/2026-w4-PIT_at_CLE.json.gz", "showdown", "2026 w4 PIT@CLE", "196187074"),
    # IND@WAS (22k entries, DraftKings export curve). TB@DAL's frozen input has no ownership prior (no saved build),
    # and BUF@LAR's 500-entry MEGA is not a comparable field; neither is checked.
    ("artifacts/nfl-showdown-attribution-2026-10-13/inputs/2026-w4-IND_at_WAS.json.gz", "showdown", "2026 w4 IND@WAS", "196204783"),
]


def curve_at(curve: list, fraction: float, entries: int) -> float:
    """Score at the given top fraction of the field, from the stored (rank, score) curve."""
    rank = max(1, math.ceil(fraction * entries))
    best = None
    for r, score in curve:
        if r <= rank:
            best = score
    return float(best if best is not None else curve[0][1])


def realized(db, contest_id: str) -> dict:
    row = db.execute("SELECT entry_count, winning_score, median_score, score_curve, format FROM nfl_dfs_field_contests WHERE contest_id=%s", (contest_id,))[0]
    curve = row["score_curve"] if isinstance(row["score_curve"], list) else json.loads(row["score_curve"])
    n = row["entry_count"]
    return {"entries": n, "format": row["format"], "winning": float(row["winning_score"]), "top01Pct": curve_at(curve, 0.001, n),
            "top1Pct": curve_at(curve, 0.01, n), "median": float(row["median_score"])}


def spreads(db) -> dict:
    out = {}
    for fmt in ("classic", "showdown"):
        rows = db.execute("""SELECT entry_count, score_curve FROM nfl_dfs_field_contests c
                             WHERE format=%s AND score_curve IS NOT NULL AND season IS NOT NULL AND entry_count >= 10000
                               AND EXISTS (SELECT 1 FROM nfl_dfs_field_top_entries t WHERE t.contest_id=c.contest_id)""", (fmt,))
        values = [curve_at(r["score_curve"] if isinstance(r["score_curve"], list) else json.loads(r["score_curve"]), 0.01, r["entry_count"]) for r in rows]
        out[fmt] = {"top1PctSd": round(statistics.stdev(values), 2) if len(values) > 1 else None, "contests": len(values), "values": values}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=ROOT / "artifacts/nfl-portfolio-objective-v1/freeze.json")
    args = ap.parse_args()
    if args.out.exists():
        ap.error(f"{args.out} exists; a freeze is written once (delete it deliberately to re-run)")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    slates = []
    for file, fmt, label, contest in CHECKS:
        r = realized(db, contest)
        assert r["format"] == fmt, (label, r["format"])
        slates.append({"file": str(ROOT / file), "format": fmt, "label": label, "contestId": contest, "entries": r["entries"],
                       "realized": {k: r[k] for k in ("winning", "top01Pct", "top1Pct", "median")}})
    spec = {"slates": slates, "spreads": spreads(db), "moduleFiles": [str(ROOT / f) for f in MODULE_FILES]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    spec_path = args.out.with_name("freeze-spec.json")
    spec_path.write_text(json.dumps(spec, indent=1), encoding="utf-8")
    subprocess.run(["node", "-r", "./scripts/server-only-stub.cjs", "--import", "tsx", "scripts/research-nfl-portfolio-freeze.ts",
                    str(spec_path), str(args.out)], cwd=ROOT / "web", check=True)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
