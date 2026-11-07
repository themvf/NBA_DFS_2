"""Freeze and back-test `nfl-ownership-rank-shape-v1` (model/nfl_ownership_rank_shape.py).

    python -m research.nfl_ownership_rank_shape --from-pickle PATH --freeze   # write the frozen curves once
    python -m research.nfl_ownership_rank_shape --from-pickle PATH            # leave-one-slate-out report

The pickle is a scorecard result (`python -m research.nfl_ownership_scorecard
--pickle PATH`). Curves come from the PRIMARY canonical frames of the
complete-label reference contests (observed + reconciled_zero labels only,
label mass >= 98.5% of the slot's total): per Classic position, and per
Showdown slot over every player, the mean actual ownership at each rank and the
mean group total. The freeze refuses to overwrite an existing file.
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from model import nfl_ownership_rank_shape as rs

MASS_FLOOR = 0.985
SLOT_TOTAL = {"overall": 900.0, "FLEX": 500.0, "CPT": 100.0}


def frames_for(result: dict, fmt: str, slot: str, version: str = rs.BASE_VERSION) -> list[dict]:
    out = []
    for f in result["frames"]:
        if not (f["primary"] and f["horizon"] == "canonical" and f["fmt"] == fmt and f["slot"] == slot
                and f["version"] == version):
            continue
        g = f["frame"]
        if not g["label_class"].isin(["observed", "reconciled_zero"]).all():
            continue
        if g["actual"].sum() < SLOT_TOTAL[slot] * MASS_FLOOR:
            continue
        out.append(f)
    return out


def build_curves(frames: list[dict], fmt: str) -> dict:
    acc: dict[str, list[np.ndarray]] = {}
    for f in frames:
        g = f["frame"]
        groups = g["position"].astype(str) if fmt == "classic" else pd.Series(["all"] * len(g), index=g.index)
        for grp, gg in g.groupby(groups):
            acc.setdefault(str(grp), []).append(np.sort(gg["actual"].to_numpy(float))[::-1])
    out = {}
    for grp, arrs in acc.items():
        L = max(len(a) for a in arrs)
        m = np.full((len(arrs), L), np.nan)
        for i, a in enumerate(arrs):
            m[i, : len(a)] = a
        curve = np.nanmean(m, axis=0)
        curve = np.maximum.accumulate(curve[::-1])[::-1]  # non-increasing by construction of sorted actuals, kept explicit
        out[grp] = {"curve": [round(float(x), 4) for x in curve], "total": round(float(np.mean([a.sum() for a in arrs])), 3),
                    "slates": len(arrs)}
    return out


def freeze(result: dict, path: Path) -> dict:
    if path.exists():
        raise SystemExit(f"{path} exists; a new curve set is a new version")
    curves: dict = {"classic": {}, "showdown": {}}
    sources: dict = {}
    sel = frames_for(result, "classic", "overall")
    curves["classic"]["position"] = build_curves(sel, "classic")
    sources["classic"] = [{"week": f["week"], "contest": f["contest"], "label": f["label"]} for f in sel]
    for slot in ("CPT", "FLEX"):
        sel = frames_for(result, "showdown", slot)
        curves["showdown"][slot] = build_curves(sel, "showdown")
        sources[f"showdown:{slot}"] = [{"week": f["week"], "contest": f["contest"], "label": f["label"]} for f in sel]
    doc = {"version": rs.VERSION, "base_version": rs.BASE_VERSION, "frozen_at": datetime.now(timezone.utc).isoformat(),
           "sigma": rs.SIGMA, "draws": rs.DRAWS, "seed": rs.SEED, "log_floor": rs.LOG_FLOOR,
           "design_weeks": [rs.DESIGN_WEEKS.start, rs.DESIGN_WEEKS.stop - 1], "mass_floor": MASS_FLOOR,
           "curves": curves, "sources": sources}
    doc["curves_digest"] = rs.curves_digest(doc)
    path.write_text(json.dumps(doc, indent=1), encoding="utf-8")
    return doc


def loo_report(result: dict, fmt: str, slot: str) -> pd.DataFrame:
    sel = frames_for(result, fmt, slot)
    rows = []
    for i, f in enumerate(sel):
        others = sel[:i] + sel[i + 1:]
        if not others:
            continue
        curve_set = build_curves(others, fmt)
        g = f["frame"]
        a = g["actual"].to_numpy(float)
        p = g["pred"].to_numpy(float)
        groups = g["position"].astype(str).to_numpy() if fmt == "classic" else np.array(["all"] * len(g))
        new = rs.soft_rank_shape(p, groups, curve_set, rs.SIGMA[fmt])
        top = np.argsort(-p)[:5]
        rows.append({"week": f["week"], "label": f["label"][:24], "n": len(g), "mae_base": np.abs(p - a).mean(),
                     "mae_rs": np.abs(new - a).mean(), "rho_base": pd.Series(p).rank().corr(pd.Series(a).rank()),
                     "rho_rs": pd.Series(new).rank().corr(pd.Series(a).rank()),
                     "top5_bias_base": (p[top] - a[top]).mean(), "top5_bias_rs": (new[top] - a[top]).mean()})
    return pd.DataFrame(rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-pickle", required=True)
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--path", default=str(rs.CURVES_PATH))
    a = ap.parse_args(argv)
    result = pickle.loads(Path(a.from_pickle).read_bytes())
    if a.freeze:
        doc = freeze(result, Path(a.path))
        print(f"froze {rs.VERSION} to {a.path}: digest {doc['curves_digest'][:12]}, "
              + ", ".join(f"{k} {len(v)} slates" for k, v in doc["sources"].items()))
        return 0
    pd.set_option("display.width", 200)
    for fmt, slot in (("classic", "overall"), ("showdown", "CPT"), ("showdown", "FLEX")):
        d = loo_report(result, fmt, slot)
        print(f"\n=== {fmt} {slot}: leave-one-slate-out, sigma {rs.SIGMA[fmt]} on the {rs.BASE_VERSION} ordering ===")
        if d.empty:
            print("no frames")
            continue
        print(d.round(3).to_string(index=False))
        print("pooled MAE base %.3f -> rs %.3f; rs better on %d of %d; Spearman %.3f -> %.3f; top-5 bias %+.2f -> %+.2f"
              % (d.mae_base.mean(), d.mae_rs.mean(), (d.mae_rs < d.mae_base).sum(), len(d), d.rho_base.mean(),
                 d.rho_rs.mean(), d.top5_bias_base.mean(), d.top5_bias_rs.mean()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
