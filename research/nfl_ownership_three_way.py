"""Weekly three-way ownership grade (`nfl-ownership-three-way-v1`, 2026-10-26).

For EVERY linked Classic contest of one week (the stat-api Millionaire import
and the owner's own DraftKings exports in `nfl_dfs_field_*`, each linked to its
slate), grades three ownership forecasts against that field's actual ownership:

  prior     the stated v5 prior, recomputed from the frozen run snapshot
  bounded   that prior held inside the FantasyPros range imported for the
            slate's date (`nfl_dfs_ownership_bands`, newest paste)
  midpoint  the middle of the FantasyPros range, the prior where no range
  fit       the fitted Classic model (`nfl-ownership-fit-v1`), recomputed from
            the frozen web/src/data/nfl-ownership-fit-v1.json on the build's own
            inputs (identical to what the page computed; the fixture pins it). It
            is the LIVE number since 2026-10-31 and v5 is the shadow; builds save
            both (`ownership.total`/`source` and `ownership.shadow`). Only a slate
            whose first kickoff is after the model's `fitted_at` counts toward
            the switch rule in docs/nfl-ownership-model.md; earlier weeks are
            in the model's own training data and are printed as such.

The forecast population is the newest owner optimizer run on the linked
slate before its first kickoff (what the build actually read). When two or
more contests share a slate, their fields are compared with each other
(rank agreement, and the players one room owned far more than the other).
Descriptive: it prints, it writes nothing. Run it Monday after the flagship
review has imported the Millionaire and the owner's export is imported
(`python -m ingest.nfl_dfs_field_audit --contest FILE --keep-all`):

    python -m research.nfl_ownership_three_way --week 7
    python -m research.nfl_ownership_three_way --week 7 --contest 196905290   # one contest
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import unicodedata

import numpy as np
import pandas as pd

from model.nfl_ownership_fit import feature_frame, predict as fit_predict
from model.nfl_ownership_scorecard import error_metrics, prior_classic, spearman_avg, top_k_overlap
from research.nfl_ownership_inventory import connect

TEAM = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}
SEASON = 2026
FIT_MODEL_PATH = Path(__file__).resolve().parents[1] / "web/src/data/nfl-ownership-fit-v1.json"


def fit_model() -> dict:
    return json.loads(FIT_MODEL_PATH.read_text(encoding="utf-8"))


def game_key(info: str) -> str:
    m = re.match(r"\s*([A-Z]{2,4}@[A-Z]{2,4})", info or "")
    return m.group(1) if m else (info or "")[:12]


def norm(name: str) -> str:
    n = unicodedata.normalize("NFKD", name or "").lower()
    n = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", n)
    return re.sub(r"[^a-z]", "", n)


def band_key(team: str, position: str, salary: int, name: str) -> tuple:
    return (team, position, salary, norm(name.split()[-1]) if position == "DST" else norm(name))


def list_contests(conn, week: int) -> list[dict]:
    """Every linked Classic contest of the week, largest field first."""
    with conn.cursor() as cur:
        cur.execute("""SELECT contest_id, contest_name, entry_count, slate_upload_id FROM nfl_dfs_field_contests
            WHERE format = 'classic' AND season = %s AND week = %s AND slate_upload_id IS NOT NULL
            ORDER BY entry_count DESC""", (SEASON, week))
        return list(cur.fetchall())


def compare_fields(reference: pd.DataFrame, other: pd.DataFrame, top: int = 8) -> dict:
    """How two fields on the same slate differ. Both frames are `grade` outputs
    (name, position, salary, actual). Returns rank agreement of the two fields'
    ownership, the mean absolute gap, and the players each room owned most
    more than the other (other minus reference, percentage points)."""
    a = reference[["name", "position", "salary", "actual"]].rename(columns={"actual": "ref"})
    b = other[["name", "actual"]].rename(columns={"actual": "other"})
    m = a.merge(b, on="name", how="inner")
    m["gap"] = m["other"] - m["ref"]
    both = m[(m.ref > 0) | (m.other > 0)]
    sp = spearman_avg(both.ref.to_numpy(float), both.other.to_numpy(float)) if len(both) > 2 else float("nan")

    def rows(d):
        return [{"name": r.name, "position": r.position, "salary": int(r.salary), "ref": round(float(r.ref), 1),
                 "other": round(float(r.other), 1), "gap": round(float(r.gap), 1)} for r in d.itertuples()]
    return {"n": int(len(both)), "spearman": round(float(sp), 3), "mae": round(float(both.gap.abs().mean()), 2),
            "chalk_ref": round(float(m.ref.max()), 1), "chalk_other": round(float(m.other.max()), 1),
            "more_in_other": rows(m.sort_values("gap", ascending=False).head(top)),
            "more_in_reference": rows(m.sort_values("gap").head(top))}


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
        cur.execute("""SELECT MIN(to_timestamp((regexp_match(game_info, '(\\d\\d/\\d\\d/\\d\\d\\d\\d \\d\\d:\\d\\d[AP]M)'))[1],
                                     'MM/DD/YYYY HH12:MIAM')::timestamp AT TIME ZONE 'America/New_York') AS kickoff
            FROM nfl_dfs_slate_players WHERE upload_id = %s""", (contest["slate_upload_id"],))
        contest = {**contest, "kickoff": cur.fetchone()["kickoff"]}
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
                     "build": own.get("total"), "source": own.get("source"), "shadow": own.get("shadow"),
                     "game_key": game_key(str(p.get("gameInfo") or ""))})
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
    # The fitted model on the build's own inputs. Recomputed rather than read from
    # `ownership.total`, which carries LineStar or a typed number where those won.
    active = g[g.position.isin(("QB", "RB", "WR", "TE", "DST")) & (g.salary > 0)]
    g["fit"] = 0.0
    if len(active):
        g.loc[active.index, "fit"] = fit_predict(fit_model(), feature_frame(active.reset_index(drop=True)))
    g.attrs["fit_live"] = bool((g["source"] == "nfl-ownership-fit-v1").any())
    out = {}
    for col in ("prior", "bounded", "midpoint", "fit", "build"):
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


def grade_one(conn, week: int, contest_id: str, misses: int) -> pd.DataFrame | None:
    try:
        contest, field, run, snapshot, bands = load(conn, week, contest_id)
    except SystemExit as exc:
        print(f"\n== contest {contest_id}: {exc}")
        return None
    mass = sum(field.values())
    print(f"\n== {contest['contest_name']} ({contest['entry_count']:,} entries, contest {contest['contest_id']}): "
          f"field rows {len(field)}, ownership mass {mass:.0f}/900; run {str(run['run_id'])[:8]} at "
          f"{run['created_at']:%Y-%m-%d %H:%M} UTC, {len(snapshot)} players; FantasyPros ranges "
          f"{'imported ' + bands['captured_at'].strftime('%Y-%m-%d %H:%M') + ' UTC' if bands else 'NOT imported (bounded and midpoint = prior)'}")
    g, out = grade(snapshot, field, bands)
    print(f"graded players {len(g)}")
    fitted_at = datetime.fromisoformat(fit_model()["fitted_at"])
    counts = contest.get("kickoff") is not None and contest["kickoff"] > fitted_at
    print(f"fitted model: {'LIVE in this build (v5 was the shadow)' if g.attrs['fit_live'] else 'shadow (the build used v5)'}; "
          f"{'COUNTS toward the switch rule' if counts else 'in-sample or before the freeze, does NOT count'} "
          f"(model fitted {fitted_at:%Y-%m-%d %H:%M} UTC)")
    for col, label in (("prior", "our prior v5"), ("bounded", "prior held in FP range"), ("midpoint", "FP midpoint"),
                       ("fit", "fitted model"), ("build", "what the build used")):
        m = out[col]
        print(f"\n{label:24s} spearman {m['spearman']:.3f}  MAE {m['mae']:.2f}  bias {m['bias']:+.2f}  top10 {m['top10']}/10  total {m['total']}%")
        for t in m["tiers"]:
            print(f"   field {t['tier']:>6}% n={t['n']:3d}  pred {t['pred']:5.1f}  actual {t['actual']:5.1f}  MAE {t['mae']:5.2f}")
    g["err"] = g["build"].fillna(0) - g["actual"]
    print(f"\nBiggest misses of the build's number:")
    for r in g.reindex(g["err"].abs().sort_values(ascending=False).index).head(misses).itertuples():
        fpr = f"FP {r.fp_low:.0f}-{r.fp_high:.0f}" if not np.isnan(r.fp_low) else "no FP row"
        print(f"  {r.name:24s} {r.position:3s} ${r.salary:5d}  field {r.actual:5.1f}  build {float(r.build or 0):5.1f}  prior {r.prior:5.1f}  fit {r.fit:5.1f}  {fpr}")
    return g


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--contest", default=None, help="DraftKings contest id (default: every linked Classic contest of the week)")
    ap.add_argument("--misses", type=int, default=12)
    args = ap.parse_args()
    conn = connect()
    try:
        contests = ([{"contest_id": args.contest, "slate_upload_id": None}] if args.contest else list_contests(conn, args.week))
        if not contests:
            raise SystemExit(f"no linked Classic contest for week {args.week}; run the flagship import first")
        graded = []
        for c in contests:
            g = grade_one(conn, args.week, c["contest_id"], args.misses)
            if g is not None:
                graded.append((c, g))
    finally:
        conn.close()
    if len(graded) < 2:
        return
    ref_c, ref_g = graded[0]
    for c, g in graded[1:]:
        if c["slate_upload_id"] != ref_c["slate_upload_id"]:
            print(f"\n== contest {c['contest_id']} is on a different slate than {ref_c['contest_id']}; fields not compared")
            continue
        cmp = compare_fields(ref_g, g)
        print(f"\n== Field vs field: contest {c['contest_id']} against {ref_c['contest_id']} (reference): "
              f"n={cmp['n']} rank agreement {cmp['spearman']:.3f}, mean gap {cmp['mae']:.2f} pts, "
              f"top chalk {cmp['chalk_other']:.1f}% vs {cmp['chalk_ref']:.1f}%")
        print("  owned more in this contest:")
        for r in cmp["more_in_other"]:
            print(f"    {r['name']:24s} {r['position']:3s} ${r['salary']:5d}  {r['other']:5.1f} vs {r['ref']:5.1f}  ({r['gap']:+.1f})")
        print("  owned more in the reference:")
        for r in cmp["more_in_reference"]:
            print(f"    {r['name']:24s} {r['position']:3s} ${r['salary']:5d}  {r['other']:5.1f} vs {r['ref']:5.1f}  ({r['gap']:+.1f})")


if __name__ == "__main__":
    main()
