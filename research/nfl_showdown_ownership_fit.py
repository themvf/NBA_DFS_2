"""Fit `nfl-ownership-fit-showdown-v1` on every imported Showdown contest and grade it.

One contest per slate (the largest field linked to it). Leave-one-slate-out
against the v5 prior on the same rows, then a fit on every slate written to
web/src/data/nfl-showdown-ownership-fit-v1.json, plus the cross-language
fixture (one slate's inputs and this module's predictions).

    python -m research.nfl_showdown_ownership_fit           # grade, write model + fixture
    python -m research.nfl_showdown_ownership_fit --no-write
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_showdown_ownership_eval import prior_showdown_v5
from model.nfl_showdown_ownership_fit import VERSION, fit, predict
from research.nfl_showdown_ownership_eval import load_slate

MODEL_PATH = Path("web/src/data/nfl-showdown-ownership-fit-v1.json")
FIXTURE_PATH = Path("tests/fixtures/nfl_showdown_ownership_fit_fixture.json")
MIN_MASS = 590.0


def load_all(db: PipelineDatabase) -> list[tuple[str, int, pd.DataFrame]]:
    rows = db.execute("""SELECT DISTINCT ON (c.slate_upload_id) c.contest_id
        FROM nfl_dfs_field_contests c
        WHERE c.format = 'showdown' AND c.season = 2026 AND c.slate_upload_id IS NOT NULL
          AND (SELECT sum(drafted_pct) FROM nfl_dfs_field_ownership o WHERE o.contest_id = c.contest_id) >= %s
        ORDER BY c.slate_upload_id, c.entry_count DESC""", (MIN_MASS,))
    out = []
    for r in rows:
        week, slate, _, _ = load_slate(db, r["contest_id"])
        out.append((r["contest_id"], week, slate[slate["pts"] > 0].reset_index(drop=True)))
    return sorted(out, key=lambda t: (t[1], t[0]))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)
    slates = load_all(PipelineDatabase(load_config().database_url))
    held = []
    for i, (cid, week, s) in enumerate(slates):
        model = fit([t[2] for j, t in enumerate(slates) if j != i])
        f, c = predict(model, s)
        v5f, v5c, _ = prior_showdown_v5(s)
        held.append(s.assign(fit_f=f, fit_c=c, v5_f=v5f, v5_c=v5c, cid=cid, week=week))
    a = pd.concat(held, ignore_index=True)
    print(f"{VERSION}: {len(slates)} slates, {len(a)} held-out rows (leave-one-slate-out)")
    for slot, act in (("f", "flex_act"), ("c", "cpt_act")):
        for name in ("v5", "fit"):
            col = f"{name}_{slot}"
            print(f"  {act:8s} {name:3s}  MAE {np.mean(np.abs(a[col] - a[act])):.2f}  spearman {spearmanr(a[col], a[act])[0]:.3f}")
    wins_f = wins_c = 0
    for cid, g in a.groupby("cid"):
        wins_f += np.mean(np.abs(g.fit_f - g.flex_act)) < np.mean(np.abs(g.v5_f - g.flex_act))
        wins_c += np.mean(np.abs(g.fit_c - g.cpt_act)) < np.mean(np.abs(g.v5_c - g.cpt_act))
    print(f"  fit beats v5 on flex MAE in {wins_f}/{len(slates)} slates, captain MAE in {wins_c}/{len(slates)}")
    for lo, hi in ((30, 101), (15, 30), (5, 15), (0, 5)):
        t = a[(a.flex_act >= lo) & (a.flex_act < hi)]
        print(f"  flex field {lo}-{hi}%: n={len(t)} actual {t.flex_act.mean():.1f}  v5 {t.v5_f.mean():.1f}  fit {t.fit_f.mean():.1f}")
    for lo, hi in ((10, 101), (4, 10), (0, 4)):
        t = a[(a.cpt_act >= lo) & (a.cpt_act < hi)]
        print(f"  cpt field {lo}-{hi}%: n={len(t)} actual {t.cpt_act.mean():.1f}  v5 {t.v5_c.mean():.1f}  fit {t.fit_c.mean():.1f}")

    full = fit([t[2] for t in slates])
    full["fitted_at"] = datetime.now(timezone.utc).isoformat()
    full["training_contests"] = [cid for cid, _, _ in slates]
    if not args.no_write:
        MODEL_PATH.write_text(json.dumps(full, indent=1) + "\n", encoding="utf-8")
        cid, _, s = slates[-1]
        f, c = predict(full, s)
        cols = ["position", "salary", "pts", "team", "dk_status"]
        FIXTURE_PATH.write_text(json.dumps({"contest_id": cid, "players": [
            {**{k: (None if pd.isna(r[k]) else (float(r[k]) if k in ("salary", "pts") else str(r[k]))) for k in cols},
             "flex": round(float(fi), 4), "captain": round(float(ci), 4)}
            for (_, r), fi, ci in zip(s.iterrows(), f, c)]}, indent=1) + "\n", encoding="utf-8")
        print(f"\n  wrote {MODEL_PATH} and {FIXTURE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
