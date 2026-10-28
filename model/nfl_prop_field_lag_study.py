"""Field lag: does the Wednesday-to-close move in a player's prop lines predict
where the field's ownership is stale? Pre-registered 2026-10-28 as
``nfl-prop-field-lag-v1`` (docs/nfl-prop-field-lag-study.md).

Mechanism under test: DraftKings salaries are set Monday, most of the field
builds Wednesday to Saturday from the lines of that time, and the points are
scored against the closing lines. If the field does not re-optimize, a player
whose lines rose is owned less than his closing number deserves (leverage) and
a player whose lines fell is owned more (chalk to avoid). The closing line, not
movement, stays the projection; this is a question about the ownership model.

Population: QB/RB/WR/TE on each week's largest Classic flagship contest with
imported ownership, who have a calibrated DK-points read from BOTH the
Wednesday capture (role ``wed`` in nfl_prop_implied_lines) and the close.
A player with a Wednesday read and no closing read is a ``pulled`` line and is
reported separately, never as a zero move.

Primary (H1, field lag): two ownership models of the same form, log(1 +
ownership) on implied points, value (points per $1K) and position, one fed the
Wednesday read and one the close. Each is scored leave-one-week-out in
ownership points. H1 passes if MAE(Wednesday model) - MAE(close model) has a
bootstrap 95% CI below zero (the wider of week-clustered and row-level).
Descriptive: the blend weight w on Wednesday-versus-close that best explains
ownership, and the ownership residual by |move| tercile.
Control (H0, market efficiency): correlation of the move with actual minus
closing implied points; prediction is a CI that includes zero.

Floors, conjunctive: 8 slates and 700 player-slates. The verdict is computed
once at the floor (``--verdict`` writes it to artifacts/ and refuses to
overwrite). Nothing here changes a projection or an ownership prior.

Usage:
    python -m model.nfl_prop_field_lag_study --season 2026 [--weeks 8-18] [--verdict]
"""
from __future__ import annotations

import argparse
import json
import statistics as st
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

STUDY_VERSION = "nfl-prop-field-lag-v1"
EARLY_ROLE = "wed"
LATE_ROLE = "close"
MIN_SLATES = 8
MIN_ROWS = 700
BOOTSTRAP_DRAWS = 2000
POSITIONS = ("QB", "RB", "WR", "TE")
VERDICT_PATH = Path("artifacts") / "nfl_prop_field_lag_verdict.json"


# ── data ─────────────────────────────────────────────────────────────────────

def flagship_contests(db, season: int, weeks: list[int]) -> list[dict]:
    """One Classic contest per week: the largest field with imported ownership and a linked slate."""
    rows = db.execute(
        """SELECT DISTINCT ON (c.week) c.week, c.contest_id, c.slate_upload_id::text AS upload_id, c.entry_count
           FROM nfl_dfs_field_contests c
           WHERE c.season = %s AND c.week = ANY(%s) AND c.format = 'classic' AND c.slate_upload_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM nfl_dfs_field_ownership o WHERE o.contest_id = c.contest_id)
           ORDER BY c.week, c.entry_count DESC NULLS LAST, c.imported_at DESC""", (season, weeks))
    return [dict(r) for r in rows]


def slate_players(db, upload_id: str) -> dict[str, dict]:
    out = {}
    for r in db.execute(
            """SELECT normalized_name, name, position, team, salary, is_out
               FROM nfl_dfs_slate_players WHERE upload_id = %s""", (upload_id,)):
        if r["position"] in POSITIONS:
            out[r["normalized_name"]] = dict(r)
    return out


def contest_ownership(db, contest_id: str) -> dict[str, dict]:
    return {r["normalized_name"]: {"own": float(r["drafted_pct"] or 0.0), "fpts": r["fpts"]}
            for r in db.execute("SELECT normalized_name, drafted_pct, fpts FROM nfl_dfs_field_ownership WHERE contest_id = %s",
                                (contest_id,))}


def implied_points(lines_by_player: dict, week: int, key: str, position: str):
    """Calibrated DK points from one capture's lines, or None when a required market is missing."""
    from model import nfl_prop_implied as pim
    d = lines_by_player.get((week, key))
    if not d:
        return None
    s = pim.stat_line(position, d["lines"], calibration=pim.CALIBRATION_V1)
    return None if s is None else pim.dk_points(position, s)


def assemble(db, season: int, weeks: list[int]) -> tuple[list[dict], list[dict], list[dict]]:
    """(rows with both reads, pulled lines, slates used)."""
    from research.nfl_prop_implied import load
    contests = flagship_contests(db, season, weeks)
    used_weeks = [c["week"] for c in contests]
    early = load(db, season, used_weeks, role=EARLY_ROLE) if used_weeks else {}
    late = load(db, season, used_weeks, role=LATE_ROLE) if used_weeks else {}
    rows, pulled, slates = [], [], []
    for c in contests:
        players = slate_players(db, c["upload_id"])
        own = contest_ownership(db, c["contest_id"])
        n_before = len(rows)
        for key, p in players.items():
            if p["is_out"] or not p["salary"]:
                continue
            e = implied_points(early, c["week"], key, p["position"])
            if e is None:
                continue
            l = implied_points(late, c["week"], key, p["position"])
            o = own.get(key, {"own": 0.0, "fpts": None})
            base = {"week": c["week"], "contest_id": c["contest_id"], "player": p["name"], "position": p["position"],
                    "team": p["team"], "salary": float(p["salary"]), "early_pts": e, "own": o["own"], "actual": o["fpts"]}
            if l is None:
                pulled.append(base)
            else:
                rows.append({**base, "late_pts": l, "delta": l - e})
        slates.append({**c, "rows": len(rows) - n_before})
    return rows, pulled, slates


# ── analysis (pure) ───────────────────────────────────────────────────────────

def design(rows: list[dict], basis: str) -> np.ndarray:
    """Ownership features from one read of the lines: ``early`` (Wednesday), ``late`` (close) or ``mix``."""
    cols = []
    for r in rows:
        pts = r[f"{basis}_pts"]
        x = [1.0, pts, pts / (r["salary"] / 1000.0)]
        x += [1.0 if r["position"] == p else 0.0 for p in POSITIONS[1:]]
        cols.append(x)
    return np.array(cols, dtype=float)


def target(rows: list[dict]) -> np.ndarray:
    return np.log1p(np.array([r["own"] for r in rows], dtype=float))


def fit(X: np.ndarray, y: np.ndarray, ridge: float = 1e-6) -> np.ndarray:
    xtx = X.T @ X + ridge * np.eye(X.shape[1])
    return np.linalg.solve(xtx, X.T @ y)


def blended(rows: list[dict], w: float) -> list[dict]:
    """Rows whose ``mix_pts`` is w x Wednesday + (1 - w) x close: the field's effective read."""
    return [{**r, "mix_pts": w * r["early_pts"] + (1 - w) * r["late_pts"]} for r in rows]


def loo_week_errors(rows: list[dict], basis: str) -> list[float]:
    """Leave-one-week-out absolute errors in ownership points (back-transformed), row order preserved."""
    weeks = sorted({r["week"] for r in rows})
    errs = [float("nan")] * len(rows)
    for w in weeks:
        train_idx = [i for i, r in enumerate(rows) if r["week"] != w]
        test_idx = [i for i, r in enumerate(rows) if r["week"] == w]
        if len(train_idx) < 10 or not test_idx:
            continue
        beta = fit(design([rows[i] for i in train_idx], basis), target([rows[i] for i in train_idx]))
        pred = np.expm1(design([rows[i] for i in test_idx], basis) @ beta)
        for i, p in zip(test_idx, pred):
            errs[i] = abs(float(p) - rows[i]["own"])
    return errs


def mae_gap(rows: list[dict]) -> float:
    """Primary statistic: MAE(Wednesday-based model) - MAE(close-based model), leave-one-week-out.

    Negative means Wednesday's lines explain the field's ownership better than
    the closing lines do: the field is lagged. Positive means the field updated.
    """
    e, l = loo_week_errors(rows, "early"), loo_week_errors(rows, "late")
    pairs = [(a, b) for a, b in zip(e, l) if a == a and b == b]
    if not pairs:
        return float("nan")
    return float(st.mean(a - b for a, b in pairs))


def field_lag_weight(rows: list[dict], grid=tuple(i / 10 for i in range(11))) -> tuple[float, dict[float, float]]:
    """Descriptive: the w in w x Wednesday + (1 - w) x close that best explains ownership out of sample."""
    maes = {}
    for w in grid:
        errs = loo_week_errors(blended(rows, w), "mix")
        vals = [x for x in errs if x == x]
        maes[w] = float(st.mean(vals)) if vals else float("nan")
    best = min(maes, key=lambda w: (maes[w], w))
    return best, maes


def control_correlation(rows: list[dict]) -> tuple[float, int]:
    pts = [(r["delta"], r["actual"] - r["late_pts"]) for r in rows if r["actual"] is not None]
    if len(pts) < 3:
        return float("nan"), len(pts)
    a, b = np.array(pts).T
    return float(np.corrcoef(a, b)[0, 1]), len(pts)


def _resample_ci(samples, stat, draws: int, seed: int) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    vals = []
    for _ in range(draws):
        idx = rng.integers(0, len(samples), size=len(samples))
        sample = []
        for i in idx:
            sample.extend(samples[i])
        try:
            v = stat(sample)
        except (np.linalg.LinAlgError, ValueError):
            continue
        if v == v:
            vals.append(v)
    if not vals:
        return float("nan"), float("nan")
    return float(np.percentile(vals, 2.5)), float(np.percentile(vals, 97.5))


def bootstrap_ci(rows: list[dict], stat, draws: int = BOOTSTRAP_DRAWS, seed: int = 20261028) -> tuple[float, float]:
    """95% CI: the WIDER of a week-clustered and a row-level bootstrap.

    Weeks are the dependence unit (one slate's ownership moves together), so the
    cluster bootstrap is the honest one; with eight clusters it is also
    anti-conservative, so the row-level interval is taken where it is wider.
    """
    by_week = defaultdict(list)
    for r in rows:
        by_week[r["week"]].append(r)
    clustered = _resample_ci([by_week[w] for w in sorted(by_week)], stat, draws, seed)
    rowwise = _resample_ci([[r] for r in rows], stat, draws, seed + 1)
    return min(clustered[0], rowwise[0]), max(clustered[1], rowwise[1])


def tercile_residuals(rows: list[dict]) -> list[dict]:
    """Mean ownership residual from the close-based model by |delta| tercile (reported, never gated)."""
    beta = fit(design(rows, "late"), target(rows))
    resid = np.expm1(target(rows)) - np.expm1(design(rows, "late") @ beta)
    order = sorted(range(len(rows)), key=lambda i: abs(rows[i]["delta"]))
    out = []
    n = len(rows)
    for t in range(3):
        idx = order[t * n // 3:(t + 1) * n // 3]
        if not idx:
            continue
        out.append({"tercile": t + 1, "n": len(idx),
                    "abs_delta_range": (round(abs(rows[idx[0]]["delta"]), 2), round(abs(rows[idx[-1]]["delta"]), 2)),
                    "mean_delta": round(float(st.mean(rows[i]["delta"] for i in idx)), 2),
                    "mean_own_residual": round(float(st.mean(resid[i] for i in idx)), 2),
                    "corr_delta_residual": round(float(np.corrcoef([rows[i]["delta"] for i in idx], [resid[i] for i in idx])[0, 1]), 3)
                    if len(idx) > 2 else None})
    return out


def floors_met(rows: list[dict]) -> dict:
    n_slates = len({r["week"] for r in rows})
    return {"slates": n_slates, "rows": len(rows), "min_slates": MIN_SLATES, "min_rows": MIN_ROWS,
            "met": n_slates >= MIN_SLATES and len(rows) >= MIN_ROWS}


def analyze(rows: list[dict], pulled: list[dict], slates: list[dict], draws: int = BOOTSTRAP_DRAWS) -> dict:
    floors = floors_met(rows)
    out = {"study": STUDY_VERSION, "computed_at": datetime.now(timezone.utc).isoformat(), "floors": floors,
           "slates": slates, "pulled": len(pulled)}
    if len(rows) < 20 or len({r["week"] for r in rows}) < 2:
        out["note"] = "fewer than 20 rows or 2 weeks: nothing to describe yet"
        return out
    gap = mae_gap(rows)
    lo, hi = bootstrap_ci(rows, mae_gap, draws)
    w, maes = field_lag_weight(rows)
    corr, n_ctrl = control_correlation(rows)
    clo, chi = bootstrap_ci(rows, lambda s: control_correlation(s)[0], draws)
    e, l = loo_week_errors(rows, "early"), loo_week_errors(rows, "late")
    out["descriptive"] = {
        "delta_mean": round(float(st.mean(r["delta"] for r in rows)), 3),
        "delta_sd": round(float(st.pstdev(r["delta"] for r in rows)), 3),
        "loo_mae_early_model": round(float(st.mean(x for x in e if x == x)), 3),
        "loo_mae_close_model": round(float(st.mean(x for x in l if x == x)), 3),
        "mae_gap": round(gap, 4), "mae_gap_ci95": (round(lo, 4), round(hi, 4)),
        "field_lag_weight": w, "field_lag_weight_maes": {str(k): round(v, 3) for k, v in maes.items()},
        "control_corr": round(corr, 4) if corr == corr else None, "control_n": n_ctrl,
        "control_ci95": (round(clo, 4), round(chi, 4)),
        "terciles": tercile_residuals(rows),
    }
    if floors["met"]:
        h1 = hi < 0
        h0 = clo <= 0 <= chi
        out["verdict"] = {"h1_field_lag": "PASS" if h1 else "FAIL", "h0_market_efficiency": "HOLDS" if h0 else "VIOLATED",
                          "reason": ("the Wednesday-based ownership model beats the close-based one out of sample, CI below zero" if h1
                                     else "the MAE gap's CI includes zero or favours the close-based model")}
    return out


def print_report(res: dict) -> None:
    f = res["floors"]
    print(f"{res['study']}  slates {f['slates']}/{f['min_slates']}  rows {f['rows']}/{f['min_rows']}  "
          f"floors {'MET' if f['met'] else 'NOT MET'}  pulled lines {res['pulled']}")
    for s in res["slates"]:
        print(f"  week {s['week']:2}  contest {s['contest_id']}  entries {s['entry_count']}  rows {s['rows']}")
    d = res.get("descriptive")
    if not d:
        print("  " + res.get("note", ""))
        return
    print(f"  delta (close - Wed implied pts): mean {d['delta_mean']:+.2f}  sd {d['delta_sd']:.2f}")
    print(f"  LOO-week ownership MAE: Wednesday-based {d['loo_mae_early_model']:.3f}  close-based {d['loo_mae_close_model']:.3f}")
    print(f"  H1 MAE gap (Wednesday - close): {d['mae_gap']:+.4f}  95% CI {d['mae_gap_ci95']}  (negative = field lagged)")
    print(f"  field's effective read: w {d['field_lag_weight']:.1f} x Wednesday + {1 - d['field_lag_weight']:.1f} x close (descriptive)")
    print(f"  H0 corr(delta, actual - close): {d['control_corr']}  n {d['control_n']}  95% CI {d['control_ci95']}")
    for t in d["terciles"]:
        print(f"  |delta| tercile {t['tercile']}: n {t['n']}  range {t['abs_delta_range']}  mean delta {t['mean_delta']:+.2f}  "
              f"mean own residual {t['mean_own_residual']:+.2f}  corr {t['corr_delta_residual']}")
    v = res.get("verdict")
    if v:
        print(f"  VERDICT: H1 {v['h1_field_lag']}  H0 {v['h0_market_efficiency']}  ({v['reason']})")
    else:
        print("  no verdict: below the pre-registered floors")


def parse_weeks(text: str) -> list[int]:
    if "-" in text:
        a, b = text.split("-")
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in text.split(",")]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="8-18", help="weeks with a Wednesday capture (the first is 2026 week 8)")
    parser.add_argument("--verdict", action="store_true", help="write the verdict file once the floors are met")
    args = parser.parse_args(argv)
    from config import load_config
    from db.database import DatabaseManager
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows, pulled, slates = assemble(db, args.season, parse_weeks(args.weeks))
    res = analyze(rows, pulled, slates)
    print_report(res)
    if args.verdict:
        if "verdict" not in res:
            print("refusing to write a verdict below the floors")
            return 1
        if VERDICT_PATH.exists():
            print(f"{VERDICT_PATH} already exists; the verdict is computed once")
            return 1
        VERDICT_PATH.parent.mkdir(exist_ok=True)
        VERDICT_PATH.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
        print(f"wrote {VERDICT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
