"""Fit and grade `nfl-ownership-fit-v1` on the imported full-field Classic contests.

Registered in docs/nfl-ownership-model.md ("Phase 2 fit") before this first ran.
For every 2026 week with a linked Classic contest at >= 890/900 recorded
ownership mass, the largest field is the REFERENCE slate; its forecast
population is the newest owner run created before kickoff (what the Monday
three-way grade reads). Folds: leave-one-slate-out (the gate) and walk-forward
(weeks 4+, trained on earlier weeks; reported, not gating). The v5 prior is
graded on the identical held-out rows. Other Classic contests on a reference
slate (the owner's exports) are extra evaluation and never pooled into the gate.

    python -m research.nfl_ownership_fit            # prints, writes artifacts/nfl-ownership-fit-v1/
    python -m research.nfl_ownership_fit --no-write
    python -m research.nfl_ownership_fit --freeze   # also writes the shadow model + fixture

`--freeze` writes the all-slates fit to web/src/data/nfl-ownership-fit-v1.json,
which the NFL DFS page runs as a SHADOW beside v5 (builds keep v5), and the
cross-language fixture (the newest reference slate's inputs and this module's
predictions). Only slates whose first kickoff is after the frozen file's
`fitted_at` count toward the switch rule in docs/nfl-ownership-model.md.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from model.nfl_ownership_fit import VERSION, feature_frame, fit, gate, predict, predict_raw
from model.nfl_ownership_scorecard import error_metrics, prior_classic, spearman_avg
from research.nfl_ownership_inventory import connect
from research.nfl_ownership_three_way import TEAM, norm

SEASON = 2026
MIN_MASS = 890.0
ARTIFACT_DIR = Path("artifacts") / "nfl-ownership-fit-v1"
MODEL_PATH = Path("web/src/data/nfl-ownership-fit-v1.json")
FIXTURE_PATH = Path("tests/fixtures/nfl_ownership_fit_fixture.json")
FIXTURE_COLS = ("name", "position", "team", "salary", "proj", "dk_avg", "dk_status", "game_key")
TIERS = ((20, 101), (10, 20), (5, 10), (1, 5), (0, 1))


def load_contests(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""SELECT c.contest_id, c.contest_name, c.week, c.entry_count, c.slate_upload_id,
            (SELECT sum(drafted_pct) FROM nfl_dfs_field_ownership o WHERE o.contest_id = c.contest_id) AS mass,
            (SELECT MIN(to_timestamp((regexp_match(game_info, '(\\d\\d/\\d\\d/\\d\\d\\d\\d \\d\\d:\\d\\d[AP]M)'))[1],
                                     'MM/DD/YYYY HH12:MIAM')::timestamp AT TIME ZONE 'America/New_York')
               FROM nfl_dfs_slate_players sp WHERE sp.upload_id = c.slate_upload_id) AS kickoff
            FROM nfl_dfs_field_contests c
            WHERE c.format = 'classic' AND c.season = %s AND c.slate_upload_id IS NOT NULL
            ORDER BY c.week, c.entry_count DESC""", (SEASON,))
        return [dict(r) for r in cur.fetchall()]


def load_snapshot(conn, upload_id: str, kickoff) -> tuple[dict, list]:
    with conn.cursor() as cur:
        cur.execute("""SELECT r.run_id, r.created_at, r.input_snapshot FROM nfl_dfs_optimizer_runs r
            JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id
            WHERE r.upload_id = %s AND u.origin = 'upload' AND NOT (r.settings ? 'researchStudy')
              AND r.status IN ('complete', 'partial') AND r.created_at < %s
            ORDER BY r.created_at DESC LIMIT 1""", (upload_id, kickoff))
        run = cur.fetchone()
    if not run:
        raise SystemExit(f"no pre-kickoff owner run on slate {upload_id}")
    snap = run["input_snapshot"] if isinstance(run["input_snapshot"], list) else json.loads(run["input_snapshot"])
    return dict(run), snap


def load_field(conn, contest_id: str) -> dict[str, float]:
    with conn.cursor() as cur:
        cur.execute("SELECT player_name, drafted_pct FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest_id,))
        return {norm(r["player_name"]): float(r["drafted_pct"]) for r in cur.fetchall()}


def slate_frame(snapshot: list, field: dict[str, float]) -> pd.DataFrame:
    """Active players of one snapshot with the field's label and the v5 prior."""
    rows = []
    for p in snapshot:
        info = str(p.get("gameInfo") or "")
        m = re.match(r"\s*([A-Z]{2,4}@[A-Z]{2,4})", info)
        rows.append({"name": p.get("name"), "position": str(p.get("position") or "").upper(),
                     "team": TEAM.get(p.get("team"), p.get("team")), "salary": int(p.get("salary") or 0),
                     "proj": p.get("ourProj"), "dk_avg": p.get("dkAvg"), "dk_status": p.get("status") or "",
                     "is_out": bool(p.get("isOut")), "pstatus": p.get("projectionStatus"),
                     "game_key": m.group(1) if m else info[:12]})
    df = pd.DataFrame(rows)
    df["prior"] = prior_classic(df, "v5")
    df["actual"] = [field.get(norm(n), field.get(norm(str(n).split()[-1]), 0.0)) for n in df["name"]]
    active = df[~df.is_out & (df.pstatus != "out") & df.position.isin(("QB", "RB", "WR", "TE", "DST"))].reset_index(drop=True)
    return feature_frame(active)


def grade_rows(pred: np.ndarray, actual: np.ndarray) -> dict:
    em = error_metrics(pred, actual)
    return {"n": int(len(pred)), "spearman": round(spearman_avg(pred, actual), 3),
            "mae": round(em["mae"], 3), "bias": round(em["bias"], 3)}


def tier_table(frame: pd.DataFrame, cols: list[str]) -> list[dict]:
    out = []
    for lo, hi in TIERS:
        s = frame[(frame.actual >= lo) & (frame.actual < hi)]
        if len(s):
            row = {"tier": f"{lo}-{hi if hi < 101 else ''}", "n": int(len(s)), "actual": round(float(s.actual.mean()), 1)}
            for c in cols:
                row[c] = round(float(s[c].mean()), 1)
                row[f"{c}_mae"] = round(float((s[c] - s.actual).abs().mean()), 2)
            out.append(row)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--freeze", action="store_true", help="write the shadow model file and the fixture")
    args = ap.parse_args(argv)
    conn = connect()
    try:
        contests = [c for c in load_contests(conn) if c["mass"] is not None and float(c["mass"]) >= MIN_MASS]
        reference, extras = {}, []
        for c in contests:
            if c["week"] not in reference:
                reference[c["week"]] = c
            else:
                extras.append(c)
        slates = {}
        for week, c in sorted(reference.items()):
            run, snap = load_snapshot(conn, c["slate_upload_id"], c["kickoff"])
            df = slate_frame(snap, load_field(conn, c["contest_id"]))
            slates[week] = {"contest": c, "run": run, "frame": df}
            print(f"wk{week} {c['contest_name'][:40]:40s} {c['entry_count']:>8,} entries, mass {float(c['mass']):.0f}, "
                  f"run {str(run['run_id'])[:8]} at {run['created_at']:%m-%d %H:%M}Z (kickoff {c['kickoff']:%m-%d %H:%M}Z), "
                  f"{len(df)} active players")
        extra_frames = []
        for c in extras:
            run, snap = load_snapshot(conn, c["slate_upload_id"], c["kickoff"])
            extra_frames.append((c, slate_frame(snap, load_field(conn, c["contest_id"]))))
    finally:
        conn.close()

    weeks = sorted(slates)
    # (a) leave-one-slate-out
    loso = []
    for w in weeks:
        model = fit([(f"wk{t}", slates[t]["frame"]) for t in weeks if t != w])
        df = slates[w]["frame"].copy()
        df["fit"] = predict(model, df)
        df["fit_raw"] = predict_raw(model, df)
        df["week"] = w
        loso.append(df)
    pooled = pd.concat(loso, ignore_index=True)
    # (b) walk-forward
    wf = []
    for w in weeks:
        earlier = [t for t in weeks if t < w]
        if len(earlier) < 3:
            continue
        model = fit([(f"wk{t}", slates[t]["frame"]) for t in earlier])
        df = slates[w]["frame"].copy()
        df["fit"] = predict(model, df)
        df["fit_raw"] = predict_raw(model, df)
        df["week"] = w
        wf.append(df)
    wf_pooled = pd.concat(wf, ignore_index=True) if wf else None

    def summary(frame: pd.DataFrame) -> dict:
        a = frame.actual.to_numpy(float)
        return {"fit": grade_rows(frame.fit.to_numpy(float), a), "fit_raw": grade_rows(frame.fit_raw.to_numpy(float), a),
                "prior_v5": grade_rows(frame.prior.to_numpy(float), a)}

    result = {"version": VERSION, "computed_at": datetime.now(timezone.utc).isoformat(),
              "reference_slates": {f"wk{w}": {"contest_id": s["contest"]["contest_id"], "entries": s["contest"]["entry_count"],
                                              "run_id": str(s["run"]["run_id"]), "run_at": s["run"]["created_at"].isoformat(),
                                              "active_players": int(len(s["frame"]))} for w, s in slates.items()},
              "loso": {"pooled": summary(pooled), "by_week": {f"wk{w}": summary(pooled[pooled.week == w]) for w in weeks},
                       "by_position": {p: summary(pooled[pooled.position == p]) for p in ("QB", "RB", "WR", "TE", "DST")},
                       "tiers": tier_table(pooled, ["fit", "prior"])},
              "walk_forward": None if wf_pooled is None else {"pooled": summary(wf_pooled),
                                                              "by_week": {f"wk{w}": summary(wf_pooled[wf_pooled.week == w]) for w in sorted(wf_pooled.week.unique())}},
              "extra_contests": []}
    for c, df in extra_frames:
        w = c["week"]
        model = fit([(f"wk{t}", slates[t]["frame"]) for t in weeks if t != w])
        df = df.copy()
        df["fit"] = predict(model, df)
        df["fit_raw"] = predict_raw(model, df)
        result["extra_contests"].append({"contest_id": c["contest_id"], "week": w, "entries": c["entry_count"], **summary(df)})
    g = result["loso"]["pooled"]
    result["gate"] = gate(g["fit"]["spearman"], g["fit"]["mae"], g["fit"]["bias"], len(weeks), g["prior_v5"]["mae"])
    full = fit([(f"wk{t}", slates[t]["frame"]) for t in weeks])
    result["model_all_slates"] = full

    print(f"\n{VERSION}: {len(weeks)} reference slates, {len(pooled)} held-out player rows (leave-one-slate-out)")
    for k, label in (("fit", "fitted (primary)"), ("fit_raw", "fitted raw-900 (secondary)"), ("prior_v5", "v5 prior")):
        m = g[k]
        print(f"  {label:28s} spearman {m['spearman']:.3f}  MAE {m['mae']:.3f}  bias {m['bias']:+.3f}")
    print("\n  by week (fit spearman/MAE vs prior spearman/MAE):")
    for w in weeks:
        s = result["loso"]["by_week"][f"wk{w}"]
        print(f"    wk{w}: fit {s['fit']['spearman']:.3f}/{s['fit']['mae']:.2f}   prior {s['prior_v5']['spearman']:.3f}/{s['prior_v5']['mae']:.2f}   n={s['fit']['n']}")
    print("\n  by position:")
    for p, s in result["loso"]["by_position"].items():
        print(f"    {p:3s}: fit {s['fit']['spearman']:.3f}/{s['fit']['mae']:.2f}   prior {s['prior_v5']['spearman']:.3f}/{s['prior_v5']['mae']:.2f}   n={s['fit']['n']}")
    print("\n  field tiers (pooled): pred vs actual")
    for t in result["loso"]["tiers"]:
        print(f"    field {t['tier']:>6}% n={t['n']:4d}  actual {t['actual']:5.1f}  fit {t['fit']:5.1f} (MAE {t['fit_mae']:5.2f})  prior {t['prior']:5.1f} (MAE {t['prior_mae']:5.2f})")
    if wf_pooled is not None:
        s = result["walk_forward"]["pooled"]
        print(f"\n  walk-forward (train on earlier weeks only, weeks {sorted(wf_pooled.week.unique())}): "
              f"fit {s['fit']['spearman']:.3f}/{s['fit']['mae']:.3f}/{s['fit']['bias']:+.3f}   prior {s['prior_v5']['spearman']:.3f}/{s['prior_v5']['mae']:.3f}")
        for w, s in result["walk_forward"]["by_week"].items():
            print(f"    {w}: fit {s['fit']['spearman']:.3f}/{s['fit']['mae']:.2f}   prior {s['prior_v5']['spearman']:.3f}/{s['prior_v5']['mae']:.2f}")
    for e in result["extra_contests"]:
        print(f"\n  extra contest {e['contest_id']} (wk{e['week']}, {e['entries']:,} entries): fit {e['fit']['spearman']:.3f}/{e['fit']['mae']:.2f}   prior {e['prior_v5']['spearman']:.3f}/{e['prior_v5']['mae']:.2f}")
    print(f"\n  GATE: {result['gate']['verdict']}  " + "  ".join(f"{k}={'ok' if v else 'FAIL'}" for k, v in result["gate"]["checks"].items()))
    print("  budgets (mean recorded position totals, all slates): " + ", ".join(f"{p} {b:.0f}" for p, b in full["budgets"].items()))

    if not args.no_write:
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = ARTIFACT_DIR / f"result-{stamp}.json"
        path.write_text(json.dumps(result, indent=1, default=str), encoding="utf-8")
        print(f"\n  written {path}")
    if args.freeze:
        frozen = {**full, "fitted_at": datetime.now(timezone.utc).isoformat(),
                  "training_contests": {f"wk{w}": s["contest"]["contest_id"] for w, s in slates.items()}}
        MODEL_PATH.write_text(json.dumps(frozen, indent=1) + "\n", encoding="utf-8")
        last = slates[weeks[-1]]["frame"]
        # The frame holds a missing projection as 0 plus a flag; the fixture keeps it missing.
        raw = [{**{c: _plain(r[c]) for c in FIXTURE_COLS},
                "proj": None if r["proj_missing"] else float(r["proj"]),
                "dk_avg": None if r["dk_missing"] else float(r["dk_avg"])} for _, r in last.iterrows()]
        pred = predict(frozen, feature_frame(pd.DataFrame(raw)))
        FIXTURE_PATH.write_text(json.dumps({"week": int(weeks[-1]), "players": [{**r, "own": float(v)} for r, v in zip(raw, pred)]},
                                           indent=1) + "\n", encoding="utf-8")
        print(f"  frozen {MODEL_PATH} and {FIXTURE_PATH}")
    return 0


def _plain(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


if __name__ == "__main__":
    raise SystemExit(main())
