"""Every DraftKings rushing-yards under, 2023-2025 (`nfl-rushing-under-history-v1`).

docs/nfl-rushing-under-history-study.md. Same lines, bet and settlement as the forward
study (research/nfl_rushing_under.py, imported unchanged) over three past seasons pulled
from the Odds API archive (ingest/nfl_prop_backfill.py). Clusters are (season, week).
Graded once: with --write-db it appends a run row, and with --final (passed only after
every registered season's pull finished with no failures) the one verdict.

    python -m research.nfl_rushing_under_history grade [--final] [--write-db] [--out path]
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from research import nfl_prop_under_study as under
from research import nfl_rushing_under as ru

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-rushing-under-history-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())


def season_bets(season: int, games: list[dict], captures: list[dict], names: Mapping, stats: Mapping,
                covered: set[str], registration: Mapping[str, Any] = REGISTRATION) -> tuple[list[dict], int]:
    """One season's bets. `week` is the (season, week) cluster key; `nfl_week` keeps the week."""
    first, last = registration["weeks"]
    reg = {**registration, "weeks": [first, last]}
    lines = [r for r in ru.closing_lines(captures, games, reg, cohort_only=False) if first <= r["week"] <= last]
    bets, pending = ru.settle_lines(lines, names, stats, covered)
    for b in bets:
        b["season"], b["nfl_week"], b["week"] = season, b["week"], season * 100 + b["week"]
    return bets, pending


def difference_interval(a: list[dict], b: list[dict], resamples: int, seed: int,
                        level: float = 0.95) -> tuple[float, float] | None:
    """ROI(a) - ROI(b), resampling game weeks with both groups drawn together."""
    weeks = sorted({x["week"] for x in a} | {x["week"] for x in b})
    if len(weeks) < 2:
        return None

    def sums(rows):
        s, n = defaultdict(float), defaultdict(int)
        for r in rows:
            s[r["week"]] += r["pnl"]
            n[r["week"]] += 1
        return np.array([s[w] for w in weeks]), np.array([n[w] for w in weeks])
    sa, na = sums(a)
    sb, nb = sums(b)
    draws = np.random.default_rng(seed).integers(0, len(weeks), size=(resamples, len(weeks)))
    da, db_ = na[draws].sum(axis=1), nb[draws].sum(axis=1)
    ok = (da > 0) & (db_ > 0)
    diff = sa[draws].sum(axis=1)[ok] / da[ok] - sb[draws].sum(axis=1)[ok] / db_[ok]
    if len(diff) < resamples * 0.9:
        return None
    tail = (1 - level) / 2
    return float(np.quantile(diff, tail)), float(np.quantile(diff, 1 - tail))


def verdict(primary: Mapping[str, Any], floors_met: bool, positive_seasons: int,
            registration: Mapping[str, Any] = REGISTRATION) -> str:
    interval = primary.get("roi_interval")
    if not floors_met or not interval:
        return "INCONCLUSIVE"
    if interval[1] < 0:
        return "LOSES"
    if interval[0] > 0 and positive_seasons >= registration["stability"]["min_positive_seasons"]:
        return "PASS"
    return "NO_EDGE"


def summarize(bets: list[dict], registration: Mapping[str, Any] = REGISTRATION, final: bool = False) -> dict:
    primary = under.summarize(bets, registration=registration)
    floors = registration["floors"]
    floors_met = primary["bets"] >= floors["graded_bets"] and primary["game_weeks"] >= floors["game_weeks"]
    by_season = {s: under.summarize([b for b in bets if b["season"] == s], registration, with_interval=False)
                 for s in registration["seasons"]}
    positive = sum(1 for v in by_season.values() if v["roi"] is not None and v["roi"] > 0)
    boot = registration["bootstrap"]
    split = registration["secondary"]["split_line"]
    graded = [b for b in bets if b["result"] != "void"]
    small = [b for b in graded if b["line"] < split]
    large = [b for b in graded if b["line"] >= split]
    sec = difference_interval(small, large, boot["resamples"], boot["seed"], boot["interval"])
    gap_ci = ru.calibration_interval(bets, boot["resamples"], boot["seed"], boot["interval"])
    voids = defaultdict(int)
    for b in bets:
        if b["result"] == "void":
            voids[b["void_reason"]] += 1
    s_roi = under.summarize(small, registration, with_interval=False)["roi"]
    l_roi = under.summarize(large, registration, with_interval=False)["roi"]
    return {
        "study": registration["study"],
        "primary": primary, "floors": floors, "floors_met": floors_met,
        "positive_seasons": positive,
        "verdict_if_first": verdict(primary, floors_met, positive, registration) if final else None,
        "status": "final" if final else "pull not finished",
        "by_season": by_season,
        "calibration": {"pooled": ru.calibration(bets),
                        "pooled_gap_interval": [round(x, 4) for x in gap_ci] if gap_ci else None,
                        "by_season": {s: ru.calibration([b for b in bets if b["season"] == s])
                                      for s in registration["seasons"]}},
        "secondary_small_lines": {
            "small": under.summarize(small, registration, with_interval=False),
            "large": under.summarize(large, registration, with_interval=False),
            "difference_roi": round(s_roi - l_roi, 4) if s_roi is not None and l_roi is not None else None,
            "difference_interval": [round(x, 4) for x in sec] if sec else None,
        },
        "by_week": {w: under.summarize([b for b in bets if b["week"] == w], registration, with_interval=False)
                    for w in sorted({b["week"] for b in bets})},
        "voids": dict(voids),
    }


def write_run(out: Mapping[str, Any], full: Mapping[str, Any]) -> tuple | None:
    return ru.write_run(out, full)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("grade")
    g.add_argument("--out", type=Path)
    g.add_argument("--write-db", action="store_true")
    g.add_argument("--final", action="store_true",
                   help="the single look: pass only after every season's pull finished with no failures")
    a = p.parse_args()
    if REGISTRATION.get("status") != "registered":
        raise SystemExit("DRAFT: the study is not registered")
    bets, pending, sources, pulled = [], 0, {}, {}
    for season in REGISTRATION["seasons"]:
        reg = {**ru.REGISTRATION, **REGISTRATION, "season": season}
        conn = under._connect()
        cur = conn.cursor()
        games, captures = ru.load_inputs(cur, reg)
        conn.rollback()
        conn.close()
        weeks = {gm["game_id"]: gm["week"] for gm in games}
        completed = {gm["game_id"] for gm in games if gm["completed"]}
        stats, covered, src = under.load_stats(season, weeks, completed)
        names, _ = ru.load_names(season)
        rows, n_pending = season_bets(season, games, captures, names, stats, covered, reg)
        bets += rows
        pending += n_pending
        sources[season] = src
        pulled[season] = {"games": len(games), "games_with_lines": len({b["game_id"] for b in rows})}
    final = a.final
    out = summarize(bets, final=final)
    out.update(evaluated_at=datetime.now(timezone.utc).isoformat(), pending_lines=pending,
               coverage=pulled, nflverse_results=sources,
               registration_sha256=under.file_digest(REGISTRATION_PATH))
    full = {**out, "bets": bets}
    if a.write_db:
        out["frozen_verdict"] = write_run(out, full)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(full, indent=1, default=str))
    print(json.dumps({k: v for k, v in out.items() if k != "by_week"}, indent=1, default=str))


if __name__ == "__main__":
    main()
