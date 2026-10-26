"""Weekly three-way ownership grade (`nfl-ownership-three-way-v1`, 2026-10-26).

For one week's Classic Millionaire (the stat-api import in `nfl_dfs_field_*`,
linked to the owner's slate), grades three ownership forecasts against the
field's actual ownership:

  prior     the stated v5 prior, recomputed from the frozen run snapshot
  bounded   that prior held inside the FantasyPros range imported for the
            slate's date (`nfl_dfs_ownership_bands`, newest paste)
  midpoint  the middle of the FantasyPros range, the prior where no range

The forecast population is the newest owner optimizer run on the linked
slate before its first kickoff (what the build actually read). Descriptive,
one slate per run: it prints, it writes nothing. Run it Monday after the
flagship review has imported the contest:

    python -m research.nfl_ownership_three_way --week 7
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata

import numpy as np
import pandas as pd

from model.nfl_ownership_scorecard import error_metrics, prior_classic, spearman_avg, top_k_overlap
from research.nfl_ownership_inventory import connect

TEAM = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}
SEASON = 2026


def norm(name: str) -> str:
    n = unicodedata.normalize("NFKD", name or "").lower()
    n = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", n)
    return re.sub(r"[^a-z]", "", n)


def band_key(team: str, position: str, salary: int, name: str) -> tuple:
    return (team, position, salary, norm(name.split()[-1]) if position == "DST" else norm(name))


def load(conn, week: int, contest_id: str | None):
    with conn.cursor() as cur:
        if contest_id:
            cur.execute("SELECT contest_id, contest_name, entry_count, slate_upload_id FROM nfl_dfs_field_contests WHERE contest_id = %s", (contest_id,))
        else:
            cur.execute("""SELECT contest_id, contest_name, entry_count, slate_upload_id FROM nfl_dfs_field_contests
                WHERE format = 'classic' AND season = %s AND week = %s AND slate_upload_id IS NOT NULL
                ORDER BY entry_count DESC LIMIT 1""", (SEASON, week))
        contest = cur.fetchone()
        if not contest:
            raise SystemExit(f"no linked Classic contest for week {week}; run the flagship import first")
        cur.execute("SELECT player_name, drafted_pct FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest["contest_id"],))
        field = {norm(r["player_name"]): float(r["drafted_pct"]) for r in cur.fetchall()}
        cur.execute("""SELECT r.run_id, r.created_at, r.input_snapshot, u.upload_id FROM nfl_dfs_optimizer_runs r
            JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id
            WHERE r.upload_id = %s AND u.origin = 'upload' AND NOT (r.settings ? 'researchStudy')
              AND r.status IN ('complete', 'partial') ORDER BY r.created_at DESC LIMIT 1""", (contest["slate_upload_id"],))
        run = cur.fetchone()
        if not run:
            raise SystemExit(f"no owner run on slate {contest['slate_upload_id']}")
        snapshot = run["input_snapshot"] if isinstance(run["input_snapshot"], list) else json.loads(run["input_snapshot"])
        cur.execute("SELECT MIN(game_info) g FROM nfl_dfs_slate_players WHERE upload_id = %s", (contest["slate_upload_id"],))
        cur.execute("""SELECT rows, captured_at FROM nfl_dfs_ownership_bands WHERE format = 'classic'
            AND slate_date = (SELECT MIN((regexp_match(game_info, '(\\d\\d/\\d\\d/\\d\\d\\d\\d)'))[1]::date)
                              FROM nfl_dfs_slate_players WHERE upload_id = %s)
            ORDER BY captured_at DESC, id DESC LIMIT 1""", (contest["slate_upload_id"],))
        bands = cur.fetchone()
    return contest, field, run, snapshot, bands


def grade(snapshot: list, field: dict, bands: dict | None) -> tuple[pd.DataFrame, dict]:
    rows = []
    for p in snapshot:
        own = p.get("ownership") if isinstance(p.get("ownership"), dict) else {}
        rows.append({"name": p.get("name"), "position": str(p.get("position") or "").upper(), "team": TEAM.get(p.get("team"), p.get("team")),
                     "salary": int(p.get("salary") or 0), "proj": p.get("ourProj"), "dk_avg": p.get("dkAvg"),
                     "dk_status": p.get("status") or "", "is_out": bool(p.get("isOut")), "pstatus": p.get("projectionStatus"),
                     "build": own.get("total"), "source": own.get("source")})
    df = pd.DataFrame(rows)
    df["prior"] = prior_classic(df, "v5")
    fp = {}
    for b in (bands or {}).get("rows") or []:
        fp[band_key(b["team"], b["position"], int(b["salary"]), b["name"])] = (b["low"], b["high"])
    lo, hi = [], []
    for r in df.itertuples():
        band = fp.get(band_key(r.team, r.position, r.salary, r.name))
        lo.append(band[0] if band else np.nan); hi.append(band[1] if band else np.nan)
    df["fp_low"], df["fp_high"] = lo, hi
    df["bounded"] = np.where(df.fp_low.isna(), df.prior, np.clip(df.prior, df.fp_low, df.fp_high))
    df["midpoint"] = np.where(df.fp_low.isna(), df.prior, (df.fp_low + df.fp_high) / 2)
    df["actual"] = [field.get(norm(n), field.get(norm(n.split()[-1]), 0.0)) for n in df["name"]]
    g = df[~df.is_out & (df.pstatus != "out")].copy()
    out = {}
    for col in ("prior", "bounded", "midpoint", "build"):
        pred = g[col].fillna(0).to_numpy(dtype=float); act = g["actual"].to_numpy(dtype=float)
        em = error_metrics(pred, act)
        tiers = []
        for a, b in ((20, 101), (10, 20), (5, 10), (1, 5), (0, 1)):
            s = g[(g.actual >= a) & (g.actual < b)]
            if len(s):
                tiers.append({"tier": f"{a}-{b if b < 101 else ''}", "n": len(s), "pred": round(float(s[col].fillna(0).mean()), 1),
                              "actual": round(float(s.actual.mean()), 1), "mae": round(float((s[col].fillna(0) - s.actual).abs().mean()), 2)})
        out[col] = {"spearman": round(spearman_avg(pred, act), 3), "mae": round(em["mae"], 2), "bias": round(em["bias"], 2),
                    "top10": top_k_overlap(g["name"].tolist(), pred, act), "total": round(float(pred.sum())), "tiers": tiers}
    return g, out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--contest", default=None, help="DraftKings contest id (default: the week's largest linked Classic)")
    ap.add_argument("--misses", type=int, default=12)
    args = ap.parse_args()
    conn = connect()
    try:
        contest, field, run, snapshot, bands = load(conn, args.week, args.contest)
    finally:
        conn.close()
    mass = sum(field.values())
    print(f"week {args.week}: {contest['contest_name']} ({contest['entry_count']:,} entries), field rows {len(field)}, "
          f"ownership mass {mass:.0f}/900; run {str(run['run_id'])[:8]} at {run['created_at']:%Y-%m-%d %H:%M} UTC, "
          f"{len(snapshot)} players; FantasyPros ranges {'imported ' + bands['captured_at'].strftime('%Y-%m-%d %H:%M') + ' UTC' if bands else 'NOT imported (bounded and midpoint = prior)'}")
    g, out = grade(snapshot, field, bands)
    print(f"graded players {len(g)}")
    for col, label in (("prior", "our prior v5"), ("bounded", "prior held in FP range"), ("midpoint", "FP midpoint"), ("build", "what the build used")):
        m = out[col]
        print(f"\n{label:24s} spearman {m['spearman']:.3f}  MAE {m['mae']:.2f}  bias {m['bias']:+.2f}  top10 {m['top10']}/10  total {m['total']}%")
        for t in m["tiers"]:
            print(f"   field {t['tier']:>6}% n={t['n']:3d}  pred {t['pred']:5.1f}  actual {t['actual']:5.1f}  MAE {t['mae']:5.2f}")
    g["err"] = g["build"].fillna(0) - g["actual"]
    print(f"\nBiggest misses of the build's number:")
    for r in g.reindex(g["err"].abs().sort_values(ascending=False).index).head(args.misses).itertuples():
        fpr = f"FP {r.fp_low:.0f}-{r.fp_high:.0f}" if not np.isnan(r.fp_low) else "no FP row"
        print(f"  {r.name:24s} {r.position:3s} ${r.salary:5d}  field {r.actual:5.1f}  build {float(r.build or 0):5.1f}  prior {r.prior:5.1f}  {fpr}")


if __name__ == "__main__":
    main()
