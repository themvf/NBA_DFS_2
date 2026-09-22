"""Grader for the v4 zero-history prior study (docs/nfl-dfs-v4-zero-history-prior-study.md).

For every completed DK slate upload, recompute BOTH v3 and v4 from the same
point-in-time history, the same frozen team environment and the same seed,
grade both on the identical slate population under DraftKings' convention
(`model/nfl_dfs_slate_reportcard.py`), and report the paired v4 - v3 MAE per
position x cohort with a weeks-clustered bootstrap.

Verdicts are computed ONLY once the confirmation window (weeks 4-10) is
scorable through week 10. Earlier weeks are printed as INSPECTED and never
enter the verdict. Nothing here writes to production tables.

Usage:
    python -m model.nfl_dfs_zero_history_prior_study --season 2026
    python -m model.nfl_dfs_zero_history_prior_study --season 2026 --week 2
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_projections import _history
from ingest.nfl_dfs_slate_reportcard import completed_uploads, inputs
from model.nfl_dfs_historical import MODEL_CONFIG, ProjectionContext, project_player
from model.nfl_dfs_slate_reportcard import build_slate_report, latest_per_slate
from model.nfl_dfs_zero_history_prior import project_player_v4

STUDY_VERSION = "nfl-dfs-v4-prior-study-v1"
WINDOW_WEEKS = tuple(range(4, 11))       # confirmation window, frozen
INSPECTED_WEEKS = (1, 2, 3)              # discovery / mechanical checks, excluded
MIN_WEEKS = 5
MIN_HIST0_ROWS = 40
BOOTSTRAP_ITERS = 2000
SEED = 20260922
POSITIONS = ("QB", "RB", "WR", "TE")
COHORTS = ("hist_0", "hist_1_5", "hist_6_plus")


def run_context(db, run_id: str) -> tuple[dict, dict[int, float | None]]:
    run = db.execute("SELECT run_id, season, week, seed, model_config FROM nfl_dfs_projection_runs WHERE run_id=%s", (run_id,))[0]
    rows = db.execute("SELECT player_id, feature_snapshot FROM nfl_dfs_player_projections WHERE run_id=%s AND player_id IS NOT NULL", (run_id,))
    env = {int(r["player_id"]): (r["feature_snapshot"] or {}).get("team_implied_total") for r in rows}
    return dict(run), env


def recompute_streams(history, players, run: dict, env: dict[int, float | None]) -> tuple[dict, dict]:
    """v3 and v4 forecasts keyed by ff_player_id for every projectable slate row."""
    config = {**MODEL_CONFIG, **(run.get("model_config") or {})}
    v3, v4 = {}, {}
    for p in players:
        pid = p.get("ff_player_id")
        if pid is None or pid not in env or p.get("position") not in POSITIONS:
            continue
        kwargs = dict(player_id=int(pid), player_gsis_id=None, player_name=p["name"], position=p["position"],
                      historical_rows=history, cutoff_season=run["season"], cutoff_week=run["week"],
                      context=ProjectionContext(team_implied_total=env[pid]), seed=int(run["seed"]), config=config)
        a = project_player(**kwargs)
        b = project_player_v4(**kwargs)
        for store, proj in ((v3, a), (v4, b)):
            if proj.model_proj_fpts is not None:
                store[pid] = {"mean": proj.model_proj_fpts, "p10": proj.floor_fpts, "p90": proj.ceiling_fpts,
                              "status": proj.projection_status, "history_games": proj.history_games}
    return v3, v4


def paired_rows(report_v3: dict, report_v4: dict) -> list[dict]:
    b = {r["ff_player_id"]: r for r in report_v4["rows"] if r["error"] is not None}
    out = []
    for r in report_v3["rows"]:
        other = b.get(r["ff_player_id"])
        if r["error"] is None or other is None:
            continue
        out.append({"week": report_v3["week"], "position": r["position"], "cohort": r["cohort"],
                    "ff_player_id": r["ff_player_id"], "name": r["name"],
                    "v3": r["projected"], "v4": other["projected"], "actual": r["actual"],
                    "d_abs": other["absolute_error"] - r["absolute_error"],
                    "d_err": other["error"] - r["error"]})
    return out


def paired_summary(rows: list[dict], *, iters: int = BOOTSTRAP_ITERS, seed: int = SEED) -> dict:
    by_week: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        by_week[r["week"]].append(r)
    weeks = sorted(by_week)
    rng = random.Random(seed)
    cells = {}
    for pos in POSITIONS + ("all",):
        for cohort in COHORTS:
            def sel(wk):
                return [r for r in by_week[wk] if (pos == "all" or r["position"] == pos) and r["cohort"] == cohort]
            base = [r for wk in weeks for r in sel(wk)]
            if not base:
                cells[f"{pos}:{cohort}"] = {"n": 0, "weeks": 0}
                continue
            def stat(rs):
                return (sum(r["d_abs"] for r in rs) / len(rs), sum(r["d_err"] for r in rs) / len(rs))
            d_mae, d_bias = stat(base)
            maes, biases = [], []
            if len(weeks) >= 2:
                for _ in range(iters):
                    sample = [r for wk in (rng.choice(weeks) for _ in weeks) for r in sel(wk)]
                    if sample:
                        m, b = stat(sample); maes.append(m); biases.append(b)
            def ci(v):
                if len(v) < 100:
                    return None
                v = sorted(v)
                return [v[int(0.025 * len(v))], v[int(0.975 * len(v)) - 1]]
            cells[f"{pos}:{cohort}"] = {
                "n": len(base), "weeks": len({wk for wk in weeks if sel(wk)}),
                "mae_v3": sum(abs(r["actual"] - r["v3"]) for r in base) / len(base),
                "mae_v4": sum(abs(r["actual"] - r["v4"]) for r in base) / len(base),
                "d_mae": d_mae, "d_mae_ci": ci(maes),
                "bias_v3": sum(r["actual"] - r["v3"] for r in base) / len(base),
                "bias_v4": sum(r["actual"] - r["v4"] for r in base) / len(base),
                "d_bias": d_bias, "d_bias_ci": ci(biases),
            }
    return {"weeks": weeks, "cells": cells}


def verdicts(summary: dict, weeks_scorable: set[int]) -> dict:
    """Per-position verdict, or the reason none can be given yet."""
    window_done = all(w in weeks_scorable for w in WINDOW_WEEKS)
    if not window_done:
        missing = [w for w in WINDOW_WEEKS if w not in weeks_scorable]
        return {"status": "no_verdict", "reason": f"confirmation window not complete; weeks not yet scorable: {missing}"}
    out = {"status": "graded"}
    for pos in POSITIONS:
        h0 = summary["cells"].get(f"{pos}:hist_0", {})
        h1 = summary["cells"].get(f"{pos}:hist_1_5", {})
        h6 = summary["cells"].get(f"{pos}:hist_6_plus", {})
        if h0.get("weeks", 0) < MIN_WEEKS or h0.get("n", 0) < MIN_HIST0_ROWS or not h0.get("d_mae_ci"):
            out[pos] = {"verdict": "insufficient", "n": h0.get("n", 0), "weeks": h0.get("weeks", 0)}
            continue
        gain = h0["d_mae_ci"][1] < 0
        # Both other cohorts delegate to v3 by construction; a lower bound
        # above zero here means the construction drifted, and fails the study.
        no_regression = all((not c.get("d_mae_ci")) or c["d_mae_ci"][0] <= 0 for c in (h1, h6))
        out[pos] = {"verdict": "PASS" if gain and no_regression else "FAIL",
                    "hist_0_d_mae_ci": h0["d_mae_ci"], "hist_1_5_d_mae_ci": h1.get("d_mae_ci"),
                    "hist_6_plus_d_mae_ci": h6.get("d_mae_ci")}
    out["promote"] = all(out.get(p, {}).get("verdict") == "PASS" for p in POSITIONS)
    return out


def grade(db, season: int, week: int | None = None, now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    reports_v3, reports_v4, skipped = [], [], []
    history_cache: dict[tuple[int, int], list] = {}
    for upload in completed_uploads(db, season, week):
        run, env = run_context(db, upload["projection_run_id"])
        key = (run["season"], run["week"])
        if key not in history_cache:
            history_cache[key] = _history(db, *key)
        data = inputs(db, upload)
        v3, v4 = recompute_streams(history_cache[key], data["players"], run, env)
        rep3 = build_slate_report(upload=upload, now=now, forecasts=v3, **data)
        rep4 = build_slate_report(upload=upload, now=now, forecasts=v4, **data)
        if rep3["scorable_games"] == 0:
            skipped.append((upload["upload_id"], "not scorable yet"))
            continue
        for rep in (rep3, rep4):
            rep["upload_created_at"] = upload["created_at"].isoformat()
        reports_v3.append(rep3); reports_v4.append(rep4)
    kept3 = {r["upload_id"]: r for r in latest_per_slate(reports_v3)}
    kept4 = {r["upload_id"]: r for r in reports_v4 if r["upload_id"] in kept3}
    rows = [row for uid in kept3 for row in paired_rows(kept3[uid], kept4[uid])]
    inspected = [r for r in rows if r["week"] in INSPECTED_WEEKS]
    window = [r for r in rows if r["week"] in WINDOW_WEEKS]
    weeks_scorable = {r["week"] for r in window}
    return {
        "study_version": STUDY_VERSION, "season": season, "evaluated_at": now.isoformat(),
        "uploads_graded": sorted(kept3), "skipped": skipped,
        "inspected": {"weeks": sorted({r["week"] for r in inspected}), "summary": paired_summary(inspected) if inspected else None},
        "window": {"weeks": sorted(weeks_scorable), "summary": paired_summary(window) if window else None},
        "verdicts": verdicts(paired_summary(window) if window else {"cells": {}}, weeks_scorable),
    }


def _print(result: dict) -> None:
    def show(label, summary):
        print(f"\n== {label} ==")
        if not summary:
            print("  (no scored rows)"); return
        print(f"  weeks {summary['weeks']}")
        for pos in POSITIONS + ("all",):
            for cohort in COHORTS:
                c = summary["cells"][f"{pos}:{cohort}"]
                if not c.get("n"):
                    continue
                ci = c["d_mae_ci"]
                ci_s = f"[{ci[0]:+.2f}, {ci[1]:+.2f}]" if ci else "(single week: no CI)"
                print(f"  {pos:<3} {cohort:<12} n={c['n']:>4} wk={c['weeks']}  MAE v3 {c['mae_v3']:.2f} -> v4 {c['mae_v4']:.2f}"
                      f"  dMAE {c['d_mae']:+.2f} {ci_s}   bias v3 {c['bias_v3']:+.2f} -> v4 {c['bias_v4']:+.2f}")
    print(f"{result['study_version']} · season {result['season']} · uploads {len(result['uploads_graded'])} · skipped {len(result['skipped'])}")
    show(f"INSPECTED weeks {result['inspected']['weeks']} — excluded from the verdict", result["inspected"]["summary"])
    show(f"CONFIRMATION WINDOW weeks {result['window']['weeks']} of {list(WINDOW_WEEKS)}", result["window"]["summary"])
    print("\n== verdict ==")
    print(json.dumps(result["verdicts"], indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--week", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    result = grade(db, args.season, args.week)
    if args.json:
        print(json.dumps(result, default=str))
    else:
        _print(result)


if __name__ == "__main__":
    main()
