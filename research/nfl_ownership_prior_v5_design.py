"""Design grade of the drafted `nfl-ownership-prior-v5` (docs/nfl-ownership-prior-v5.md).

Reads a saved scorecard result (`python -m research.nfl_ownership_scorecard --pickle PATH`,
scorecard v3: the canonical forecast is the LATEST capture at or before first
kickoff minus 20 minutes) and reports, on the reference contests of the 2026
weeks 2-6 slates, which are all v5 DESIGN slates:

1. How each v5 constant was chosen: the captain position shares (observed
   full-field captain labels only), the flattening grids per format, the
   punt-tier and Classic-budget comparisons.
2. The design-slate comparison v3 / v4 / v5 per format and slot: MAE, bias,
   mean per-slate Spearman (slate-clustered 95% intervals), top-5 concentration
   error, captain-by-position error and kicker error.

Pure over the pickle: no database, no writes. Nothing here is validation; v5 is
graded for real only on week 7 onward.

    python -m research.nfl_ownership_prior_v5_design --from-pickle PATH [--out PATH]
"""

from __future__ import annotations

import argparse
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from model import nfl_ownership_scorecard as sc
from model.nfl_showdown_ownership_eval import CAPTAIN_POSITION_SHARES_V5, prior_showdown_v5

V3, V4, V5 = "nfl-ownership-prior-v3", "nfl-ownership-prior-v4", "nfl-ownership-prior-v5"
VERSIONS = (V3, V4, V5)
CAPTAIN_POSITIONS = ("QB", "RB", "WR", "TE", "DST", "K")
SHOWDOWN_GRID = [round(a, 2) for a in np.arange(0.30, 0.751, 0.05)]
CLASSIC_GRID = [round(a, 2) for a in np.arange(0.40, 1.001, 0.05)]


# --------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------

def design_frames(result: dict) -> dict:
    """{(fmt, slot): {slate_key: {version: frame, "df": input, "label", "week", "complete"}}}
    for the reference contest, canonical horizon, latest capture."""
    slates = {s["key"]: s for s in result["slates"]}
    out: dict = defaultdict(lambda: defaultdict(dict))
    for f in result["frames"]:
        if f["horizon"] != "canonical" or not f["latest"] or f["version"] not in VERSIONS:
            continue
        d = out[(f["fmt"], f["slot"])][f["slate"]]
        d[f["version"]] = f["frame"]
        d.update({"df": f["df"], "label": f["label"], "week": f["week"],
                  "complete": bool(slates[f["slate"]]["reference_complete"])})
    return out


def _join(frame: pd.DataFrame, df: pd.DataFrame, pred) -> pd.DataFrame:
    m = pd.DataFrame({"normalized_name": df["normalized_name"].to_numpy(), "pred": np.asarray(pred, float)})
    return frame.drop(columns=["pred"]).merge(m, on="normalized_name", how="inner")


def _metrics(frames: list[pd.DataFrame]) -> dict:
    samples = [sc.SlateSample(str(i), f["pred"].to_numpy(float), f["actual"].to_numpy(float)) for i, f in enumerate(frames)]
    p = sc.pooled(samples)
    ci = sc.cluster_bootstrap(samples)
    top = [_top5(f) for f in frames]
    return {"slates": p["slates"], "n": p["n"], "mae": p["mae"], "bias": p["bias"], "rho": p["spearman_mean"],
            "mae_ci": ci.get("mae"), "bias_ci": ci.get("bias"), "rho_ci": ci.get("spearman_mean"),
            "top5": float(np.mean(top)) if top else None}


def _top5(frame: pd.DataFrame) -> float:
    """Summed predicted minus actual over the five highest-predicted players."""
    o = frame.sort_values("pred", ascending=False, kind="stable").iloc[:5]
    return float((o["pred"] - o["actual"]).sum())


# --------------------------------------------------------------------------
# Constants: how they were chosen
# --------------------------------------------------------------------------

def captain_shares(frames: dict) -> pd.DataFrame:
    """Per Showdown slate, OBSERVED captain labels summed by position (never
    synthesized zeros), raw and normalised to 100."""
    rows = []
    for key, d in frames[("showdown", "CPT")].items():
        g = d[V3]
        g = g[g["label_class"] == "observed"]
        s = g.groupby(g["position"].str.upper())["actual"].sum()
        rows.append({"slate": d["label"], "week": d["week"], "observed_total": float(s.sum()),
                     **{p: float(s.get(p, 0.0)) for p in CAPTAIN_POSITIONS}})
    df = pd.DataFrame(rows).sort_values(["week", "slate"]).reset_index(drop=True)
    norm = df[list(CAPTAIN_POSITIONS)].div(df[list(CAPTAIN_POSITIONS)].sum(axis=1), axis=0) * 100
    for p in CAPTAIN_POSITIONS:
        df[f"{p}_norm"] = norm[p]
    return df


def showdown_variant(frames: dict, alpha: float, punt: bool) -> dict:
    out = {}
    for slot in ("FLEX", "CPT"):
        fs = []
        for key, d in frames[("showdown", slot)].items():
            flex, cap, _ = prior_showdown_v5(sc.showdown_frame(d["df"]), alpha=alpha, punt_tier=punt)
            fs.append(_join(d[V3], d["df"], np.round(flex if slot == "FLEX" else cap, 2)))
        out[slot] = _metrics(fs)
    return out


def classic_variant(frames: dict, alpha: float, budgets: str) -> dict:
    complete, incomplete = [], []
    for key, d in frames[("classic", "overall")].items():
        pred = sc.prior_classic(d["df"], "v5", alpha=alpha, budgets=sc.CLASSIC_BUDGETS[budgets])
        (complete if d["complete"] else incomplete).append(_join(d[V3], d["df"], pred))
    return {"complete": _metrics(complete), "incomplete": _metrics(incomplete)}


# --------------------------------------------------------------------------
# Comparison v3 / v4 / v5
# --------------------------------------------------------------------------

def comparison(frames: dict) -> list[dict]:
    rows = []
    for (fmt, slot), slates in sorted(frames.items()):
        for subset in ("complete", "incomplete"):
            for v in VERSIONS:
                fs = [d[v] for d in slates.values() if v in d and d["complete"] == (subset == "complete")]
                if not fs:
                    continue
                m = _metrics(fs)
                m.update({"format": fmt, "slot": slot, "subset": subset, "version": v})
                if slot == "CPT":
                    m.update(_captain_position_error(fs))
                if slot == "FLEX":
                    m.update(_kicker_error(fs))
                rows.append(m)
    return rows


def _captain_position_error(frames: list[pd.DataFrame]) -> dict:
    """Per slate and position: predicted captain sum minus OBSERVED captain sum."""
    diffs = defaultdict(list)
    for f in frames:
        obs = f[f["label_class"] == "observed"]
        pos = f["position"].str.upper()
        for p in CAPTAIN_POSITIONS:
            diffs[p].append(float(f.loc[pos == p, "pred"].sum() - obs.loc[obs["position"].str.upper() == p, "actual"].sum()))
    allabs = [abs(x) for v in diffs.values() for x in v]
    return {"cpt_pos_mae": float(np.mean(allabs)), "cpt_pos_bias": {p: float(np.mean(v)) for p, v in diffs.items()}}


def _kicker_error(frames: list[pd.DataFrame]) -> dict:
    k = pd.concat([f[f["position"].str.upper() == "K"] for f in frames])
    if k.empty:
        return {}
    return {"k_n": int(len(k)), "k_actual": float(k["actual"].mean()), "k_pred": float(k["pred"].mean()),
            "k_mae": float((k["pred"] - k["actual"]).abs().mean())}


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def _f(v, d=2, sign=False):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "-"
    return f"{v:+.{d}f}" if sign else f"{v:.{d}f}"


def _ci(ci, d=2):
    return "" if not ci else f" [{ci[0]:.{d}f}, {ci[1]:.{d}f}]"


def render(result: dict) -> str:
    frames = design_frames(result)
    L = [f"# NFL DFS ownership — `nfl-ownership-prior-v5` design grade", "",
         f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `python -m research.nfl_ownership_prior_v5_design` "
         f"from a `{sc.SCORECARD_VERSION}` regrade (canonical horizon, LATEST capture at or before first kickoff minus 20 "
         "minutes from any source, reference contest per slate). Registration: docs/nfl-ownership-prior-v5.md.", "",
         "**Every slate here is a v5 DESIGN slate**: v5's constants were chosen on them. These numbers say how well the "
         "constants fit the data they were read off; they are not validation. v5 is graded for real only on week 7 "
         "onward. Pooled MAE/bias are player-weighted; Spearman is the mean of per-slate tied-rank values; intervals "
         "resample slates (2,000 reps, seed 20261020). Classic `complete` = the three slates with complete labels (the "
         "gate population); `incomplete` = weeks 4 and 6, observed labels only, descriptive.", ""]

    # 1. Captain shares
    cs = captain_shares(frames)
    L += ["## 1. Showdown captain position shares (observed full-field captain labels)", "",
          "| wk | slate | observed total | " + " | ".join(CAPTAIN_POSITIONS) + " |",
          "|---:|---|---:|" + "---:|" * len(CAPTAIN_POSITIONS)]
    for _, r in cs.iterrows():
        L.append(f"| {r['week']} | {r['slate']} | {r['observed_total']:.2f} | " + " | ".join(f"{r[p]:.2f}" for p in CAPTAIN_POSITIONS) + " |")
    L.append("| | **mean raw** | | " + " | ".join(f"**{cs[p].mean():.2f}**" for p in CAPTAIN_POSITIONS) + " |")
    L.append("| | **mean normalised (v5)** | | " + " | ".join(f"**{cs[p + '_norm'].mean():.2f}**" for p in CAPTAIN_POSITIONS) + " |")
    L.append("| | slate SD (normalised) | | " + " | ".join(f"{cs[p + '_norm'].std():.2f}" for p in CAPTAIN_POSITIONS) + " |")
    L += ["", "Frozen in v5: " + ", ".join(f"{p} {v:.2f}" for p, v in CAPTAIN_POSITION_SHARES_V5.items())
          + ". Each slate's observed sums are normalised to 100 before averaging (unmatched and OUT players' labels are "
          "a fraction of a point per slate).", ""]

    # 2. Flattening grids
    L += ["## 2. Flattening exponent (score^alpha), full-pool MAE", "",
          "Showdown: one alpha for the flex main pool and the captain weight within a position; captain shares as above.", "",
          "| alpha | punt tier | flex MAE | flex Spearman | flex top-5 err | captain MAE | captain Spearman |",
          "|---:|---|---:|---:|---:|---:|---:|"]
    for punt in (True, False):
        for a in SHOWDOWN_GRID:
            m = showdown_variant(frames, a, punt)
            L.append(f"| {a:.2f} | {'yes' if punt else 'no'} | {_f(m['FLEX']['mae'], 3)} | {_f(m['FLEX']['rho'], 3)} | "
                     f"{_f(m['FLEX']['top5'], 1, True)} | {_f(m['CPT']['mae'], 3)} | {_f(m['CPT']['rho'], 3)} |")
    L += ["", "Classic (v5 shape at each alpha, both budget sets):", "",
          "| alpha | budgets | complete MAE | complete Spearman | complete top-5 err | incomplete MAE | incomplete Spearman |",
          "|---:|---|---:|---:|---:|---:|---:|"]
    for b in ("v4", "v3"):
        for a in CLASSIC_GRID:
            m = classic_variant(frames, a, b)
            L.append(f"| {a:.2f} | {b} | {_f(m['complete']['mae'], 3)} | {_f(m['complete']['rho'], 3)} | "
                     f"{_f(m['complete']['top5'], 1, True)} | {_f(m['incomplete']['mae'], 3)} | {_f(m['incomplete']['rho'], 3)} |")
    L.append("")

    # 3. Comparison
    rows = comparison(frames)
    L += ["## 3. Design-slate comparison v3 / v4 / v5", "",
          "| format | slot | labels | version | slates | n | MAE [95%] | bias [95%] | mean Spearman [95%] | top-5 err (mean per slate) | captain-by-position MAE | kicker actual / pred / MAE |",
          "|---|---|---|---|---:|---:|---|---|---|---:|---:|---|"]
    for r in rows:
        k = f"{_f(r.get('k_actual'), 1)} / {_f(r.get('k_pred'), 1)} / {_f(r.get('k_mae'), 1)} (n {r['k_n']})" if r.get("k_n") else "-"
        L.append(f"| {r['format']} | {r['slot']} | {r['subset']} | {r['version'].replace('nfl-ownership-prior-', '')} | {r['slates']} | "
                 f"{r['n']} | {_f(r['mae'], 3)}{_ci(r['mae_ci'], 3)} | {_f(r['bias'], 3, True)}{_ci(r['bias_ci'], 3)} | "
                 f"{_f(r['rho'], 3)}{_ci(r['rho_ci'], 3)} | {_f(r['top5'], 1, True)} | {_f(r.get('cpt_pos_mae'), 2)} | {k} |")
    L += ["", "Captain-by-position mean signed error (predicted minus observed captain sum, points per slate):", "",
          "| version | " + " | ".join(CAPTAIN_POSITIONS) + " |", "|---|" + "---:|" * len(CAPTAIN_POSITIONS)]
    for r in rows:
        if r["slot"] == "CPT":
            L.append(f"| {r['version'].replace('nfl-ownership-prior-', '')} | "
                     + " | ".join(_f(r["cpt_pos_bias"][p], 2, True) for p in CAPTAIN_POSITIONS) + " |")
    L.append("")

    # 4. Per slate
    L += ["## 4. Per slate (MAE / Spearman)", "", "| wk | slate | slot | labels | v3 | v4 | v5 |", "|---:|---|---|---|---|---|---|"]
    per = []
    for (fmt, slot), slates in frames.items():
        for key, d in slates.items():
            cells = []
            for v in VERSIONS:
                f = d.get(v)
                cells.append("-" if f is None else f"{np.mean(np.abs(f['pred'] - f['actual'])):.2f} / {sc.spearman_avg(f['pred'], f['actual']):.3f}")
            per.append((d["week"], d["label"], slot, "complete" if d["complete"] else "observed only", cells))
    for wk, label, slot, lab, cells in sorted(per):
        L.append(f"| {wk} | {label} | {slot} | {lab} | " + " | ".join(cells) + " |")
    L.append("")
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--from-pickle", required=True, help="result saved by research.nfl_ownership_scorecard --pickle")
    ap.add_argument("--out", default=None, help="markdown artifact path")
    args = ap.parse_args(argv)
    result = pickle.loads(Path(args.from_pickle).read_bytes())
    text = render(result)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
