"""Freeze gate for nfl-portfolio-objective-v3 (docs/nfl-portfolio-objective-study.md, "v3").

Per format, re-reads each registered gate slate's pregame owner build (the
same capture the study runner makes, without the before-kickoff clock check
and without enrollment), takes the realized top 1% from the stored contest
score curve, and runs web/scripts/research-nfl-portfolio-freeze.ts, which
applies the registered rule: where the realized top 1% falls in the model's
20,000 evaluation draws (coverage and bias), and whether arm B's selection
holds under a second selection seed (Jaccard >= 0.80).

Writes artifacts/nfl-portfolio-objective-v3/freeze-<format>.json once; the
runner requires a passing one before saving a portfolio arm in that format.
Read-only against the database. The gate inputs are archived beside it with
their SHA-256.

    python -m research.nfl_portfolio_freeze --format showdown
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import subprocess
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from research.nfl_pool_rule_study import capture, encoded

ROOT = Path(__file__).resolve().parents[1]
VERSION = "nfl-portfolio-objective-v3"
OUT_DIR = ROOT / "artifacts/nfl-portfolio-objective-v3"
MODULE_FILES = ["web/src/lib/nfl-dfs/portfolio-objective.ts", "web/src/lib/nfl-dfs/captain-simulation.ts",
                "web/scripts/nfl-pool-rule-study-core.ts", "web/scripts/research-nfl-portfolio-freeze.ts",
                "research/nfl_portfolio_freeze.py"]
MIN_ENTRIES = 10_000
MIN_SLATES = 3
# Registered 2026-10-30 (docs/nfl-portfolio-objective-study.md, "v3", commit a6362585) before any realized
# top 1% of these contests was read: (label, DraftKings contest id, the owner's latest pregame build).
GATE_SLATES = {
    "showdown": [
        ("2026 w6 SEA@DEN", "196689718", "36c70002-0a44-4c2e-93c0-45924c71be93"),
        ("2026 w6 WAS@SF", "196786695", "21bf2279-bb8d-4222-84bc-5456489d22b0"),
        ("2026 w7 NE@CHI", "196947078", "3091aa86-5c03-4935-a098-9a997eb1a76f"),
    ],
    # No Classic contest since week 5 has a full-field curve; Classic stays unchecked until three do.
    "classic": [],
}


def curve_at(curve: list, fraction: float, entries: int) -> float:
    """Score at the given top fraction of the field, from the stored (rank, score) curve (v1/v2's reading)."""
    rank = max(1, math.ceil(fraction * entries))
    best = None
    for r, score in curve:
        if r <= rank:
            best = score
    return float(best if best is not None else curve[0][1])


def gate_slate(db, fmt: str, label: str, contest_id: str, source_run: str) -> dict:
    """Check the registered preconditions, then capture the build and read the realized thresholds."""
    contest = db.execute("""SELECT contest_id, format, entry_count, winning_score, median_score, score_curve, slate_upload_id
                            FROM nfl_dfs_field_contests WHERE contest_id=%s""", (contest_id,))
    if len(contest) != 1:
        raise ValueError(f"{label}: contest {contest_id} not stored")
    c = contest[0]
    if c["format"] != fmt or c["score_curve"] is None or c["entry_count"] < MIN_ENTRIES:
        raise ValueError(f"{label}: needs a {fmt} contest with a full curve and at least {MIN_ENTRIES:,} entries")
    run = db.execute("SELECT upload_id, created_at FROM nfl_dfs_optimizer_runs WHERE run_id=%s", (source_run,))[0]
    if str(run["upload_id"]) != str(c["slate_upload_id"]):
        raise ValueError(f"{label}: source build is not on the contest's salary slate")
    captured = capture(db, source_run, "portfolio", gate_replay=True)
    if captured["format"] != fmt:
        raise ValueError(f"{label}: source build is {captured['format']}, not {fmt}")
    missing = [p["dkPlayerId"] for p in captured["players"] if not p.get("ownership")]
    if missing:
        raise ValueError(f"{label}: {len(missing)} players without the ownership prior")
    later = db.execute("""SELECT run_id FROM nfl_dfs_optimizer_runs
                          WHERE upload_id=%s AND NOT (settings ? 'researchStudy') AND settings->>'mode'='gpp'
                            AND created_at > %s AND created_at < %s""",
                       (c["slate_upload_id"], run["created_at"], captured["firstKickoff"]))
    if later:
        raise ValueError(f"{label}: a later pregame build exists ({later[0]['run_id']}); the registration names the latest")
    curve = c["score_curve"] if isinstance(c["score_curve"], list) else json.loads(c["score_curve"])
    n = c["entry_count"]
    realized = {"winning": float(c["winning_score"]), "top01Pct": curve_at(curve, 0.001, n),
                "top1Pct": curve_at(curve, 0.01, n), "median": float(c["median_score"])}
    return {"label": label, "contestId": contest_id, "entries": n, "sourceRunId": source_run,
            "captured": captured, "realized": realized}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--format", required=True, choices=sorted(GATE_SLATES))
    args = ap.parse_args()
    out = OUT_DIR / f"freeze-{args.format}.json"
    if out.exists():
        ap.error(f"{out} exists; a freeze is written once (a failed format is not re-gated on these slates)")
    slates = GATE_SLATES[args.format]
    if len(slates) < MIN_SLATES:
        ap.error(f"{args.format}: {len(slates)} registered gate slates, the rule needs at least {MIN_SLATES}; the format stays unchecked")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    inputs_dir = OUT_DIR / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    spec_slates = []
    for label, contest_id, source_run in slates:
        g = gate_slate(db, args.format, label, contest_id, source_run)
        raw = encoded(g["captured"])
        path = inputs_dir / f"{args.format}-{contest_id}.json.gz"
        path.write_bytes(gzip.compress(raw, mtime=0))
        spec_slates.append({"label": label, "contestId": contest_id, "entries": g["entries"], "sourceRunId": source_run,
                            "input": str(path), "inputSha256": hashlib.sha256(raw).hexdigest(), "realized": g["realized"]})
    spec = {"version": VERSION, "format": args.format, "slates": spec_slates,
            "moduleFiles": {f: str(ROOT / f) for f in MODULE_FILES}}
    spec_path = OUT_DIR / f"freeze-{args.format}-spec.json"
    spec_path.write_text(json.dumps(spec, indent=1), encoding="utf-8")
    subprocess.run(["node", "--max-old-space-size=6144", "-r", "./scripts/server-only-stub.cjs", "--import", "tsx",
                    "scripts/research-nfl-portfolio-freeze.ts", str(spec_path), str(out)], cwd=ROOT / "web", check=True)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
