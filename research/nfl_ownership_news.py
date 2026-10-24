"""Design and grading for `nfl-ownership-news-v1` (docs/nfl-ownership-news-chalk-study.md).

    python -m research.nfl_ownership_news design [--freeze] [--out PATH]
    python -m research.nfl_ownership_news grade [--write-db] [--out PATH]

Both modes read the same frozen forecast inputs and labels the ownership
scorecard grades (research/nfl_ownership_scorecard.py `run`, scorecard v3:
reference contest, canonical horizon, latest capture), recompute v5 and the
news-chalk candidates exactly from each frozen input, and score them.

- design: slates whose first kickoff is before DESIGN_CUTOFF. Runs the premise
  check, scores C0/C1/C2, chooses per section 4, and with --freeze appends one
  row per format to nfl_ownership_news_freezes (append-only; a second freeze of a
  format is refused by its unique key).
- grade: slates first kicking off on or after DESIGN_CUTOFF whose reference
  contest was imported after that format's freeze. Appends one row per slate to
  nfl_ownership_news_grades, and the one-look verdict to
  nfl_ownership_news_verdicts when both floors are met (or at season end).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from db.nfl_ownership_news_schema import STATEMENTS
from model import nfl_ownership_news as news
from model import nfl_ownership_scorecard as sc
from research import nfl_ownership_scorecard as card
from research.nfl_ownership_inventory import connect, rows

SPEC_PATH = Path(__file__).resolve().parents[1] / "docs" / "nfl-ownership-news-chalk-study.json"
SPEC = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
STUDY = SPEC["study"]
SEASON = 2026
DESIGN_CUTOFF = datetime(2026, 10, 25, tzinfo=timezone.utc)
GRID = SPEC["candidates"]["C2"]["weight_multiplier_grid"]
TOLERANCE = {"classic": SPEC["design"]["step3_admissible_observed_mae_tolerance"]["classic"],
             "showdown": SPEC["design"]["step3_admissible_observed_mae_tolerance"]["showdown_flex"]}
GUARD = {"classic": SPEC["grading"]["guard_observed_mae_upper_bound"]["classic"],
         "showdown": SPEC["grading"]["guard_observed_mae_upper_bound"]["showdown_flex"]}
MIN_SLATES = SPEC["grading"]["min_slates"]
MIN_ROWS = SPEC["grading"]["min_labeled_beneficiary_rows"]
GATE_SLOT = {"classic": "overall", "showdown": "FLEX"}
V5 = card.VERSION_NAMES["v5"]
REGULAR_SEASON_END = datetime(2027, 1, 11, tzinfo=timezone.utc)  # day after the 2026 week-18 games


# --------------------------------------------------------------------------
# Inputs
# --------------------------------------------------------------------------

def load_box(conn) -> pd.DataFrame:
    rs = rows(conn, """
        SELECT w.player_id, w.season, w.week, w.team, p.position,
               COALESCE((w.source_row->>'targets')::float, 0) AS targets,
               COALESCE((w.source_row->>'carries')::float, 0) AS carries,
               COALESCE((w.source_row->>'attempts')::float, 0) AS attempts
        FROM ff_player_week_stats w JOIN ff_players p ON p.id = w.player_id
        WHERE w.source = 'nflverse' AND w.season_type = 'REG' AND w.season BETWEEN %s AND %s""",
              (SEASON - news.SEASONS_BACK, SEASON))
    return pd.DataFrame(rs, columns=["player_id", "season", "week", "team", "position", "targets", "carries", "attempts"])


def slate_inputs(conn, weeks) -> list[dict]:
    """One entry per slate: its reference contest's canonical, primary (latest) frozen
    input and the v5 gate frames for each slot, from the scorecard's own machinery."""
    card.WEEKS = weeks
    result = card.run(conn)
    info = {s["key"]: s for s in result["slates"]}
    out: dict[tuple, dict] = {}
    for f in result["frames"]:
        if f["horizon"] != "canonical" or not f["primary"] or f["version"] != V5:
            continue
        s = info[f["slate"]]
        if f["contest"] != s["reference"]:
            continue
        e = out.setdefault((f["slate"], f["contest"]), {
            "slate": f["slate"], "label": f["label"], "week": f["week"], "fmt": f["fmt"], "contest": f["contest"],
            "first_kickoff": s["first_kickoff"], "reference_complete": s["reference_complete"], "df": f["df"],
            "source": f["source"], "frames": {}})
        e["frames"][f["slot"]] = f["frame"]
    mass = {(r["slate_key"], r["contest_id"]): r["label_mass"] for r in result["rows"]
            if r.get("row_kind") == "contest" and r.get("contest_role") == "reference"}
    for (slate, contest), e in out.items():
        e["label_mass"] = mass.get((slate, contest))
    imported = {r["contest_id"]: r["imported_at"] for r in rows(conn, "SELECT contest_id, imported_at FROM nfl_dfs_field_contests")}
    for e in out.values():
        e["imported_at"] = imported.get(e["contest"])
    return sorted(out.values(), key=lambda e: (e["first_kickoff"], e["fmt"]))


# --------------------------------------------------------------------------
# Scoring one slate
# --------------------------------------------------------------------------

def predictions(df: pd.DataFrame, fmt: str, ben: set, candidate: str, m: float | None) -> dict[str, pd.Series]:
    """{slot: predicted % by normalized_name} for C0 (v5), C1 or C2."""
    if candidate == "C0":
        frame, mult = df, None
    else:
        frame = news.candidate_frame(df, ben)
        mult = news.weights(df, ben, m) if candidate == "C2" else None
    if fmt == "classic":
        return {"overall": pd.Series(sc.prior_classic(frame, "v5", weight_multiplier=mult), index=df["normalized_name"])}
    flex, cap, _ = sc.prior_showdown_version(frame, "v5", weight_multiplier=mult)
    return {"FLEX": pd.Series(flex, index=df["normalized_name"]), "CPT": pd.Series(cap, index=df["normalized_name"])}


def score_slate(e: dict, box: pd.DataFrame, candidates: list[tuple[str, float | None]]) -> dict:
    df = e["df"].reset_index(drop=True)
    trig = news.find_trigger(df, box, SEASON, e["week"])
    ben_names = {str(df.loc[i, "normalized_name"]) for i in trig.beneficiaries}
    v5 = predictions(df, e["fmt"], trig.beneficiaries, "C0", None)
    slots = {}
    for slot, gate in e["frames"].items():
        g = gate[["normalized_name", "actual", "label_class", "pred"]].copy()
        g["actual"] = g["actual"].astype(float)
        g["ben"] = g["normalized_name"].isin(ben_names)
        g["v5"] = g["normalized_name"].map(v5[slot])
        repro = float((g["v5"] - g["pred"].astype(float)).abs().max()) if len(g) else 0.0
        if repro > 0.011:
            raise SystemExit(f"{e['slate']} {slot}: v5 recomputation differs from the scorecard's by {repro:.3f}")
        labeled = set(g["normalized_name"])
        unlabeled = sorted(ben_names - labeled)
        res = {"observed_n": int(len(g)), "flagged_n": int(g["ben"].sum()), "unlabeled": unlabeled, "v5_repro_max_diff": repro,
               "v5_observed_mae": float((g["v5"] - g["actual"]).abs().mean()) if len(g) else None,
               "v5_flagged_mae": float((g.loc[g["ben"], "v5"] - g.loc[g["ben"], "actual"]).abs().mean()) if g["ben"].any() else None,
               "candidates": {}, "rows": g}
        for cand, m in candidates:
            p = predictions(df, e["fmt"], trig.beneficiaries, cand, m)[slot]
            c = g["normalized_name"].map(p)
            err_c, err_v = (c - g["actual"]).abs(), (g["v5"] - g["actual"]).abs()
            fl = g["ben"]
            worst_c = list(err_c[fl]) + [abs(float(p.get(n, 0.0)) - float(v5[slot].get(n, 0.0))) for n in unlabeled]
            worst_v = list(err_v[fl]) + [0.0] * len(unlabeled)
            res["candidates"][label(cand, m)] = {
                "observed_mae": float(err_c.mean()) if len(g) else None,
                "flagged_mae": float(err_c[fl].mean()) if fl.any() else None,
                "flagged_bias": float((c - g["actual"])[fl].mean()) if fl.any() else None,
                "flagged_mae_worst": float(np.mean(worst_c)) if worst_c else None,
                "v5_flagged_mae_worst": float(np.mean(worst_v)) if worst_v else None,
                "pred": c}
        slots[slot] = res
    return {"slate": e["slate"], "label": e["label"], "week": e["week"], "fmt": e["fmt"], "contest": e["contest"],
            "first_kickoff": e["first_kickoff"], "imported_at": e["imported_at"], "label_mass": e["label_mass"],
            "source": e["source"], "trigger": trig.details(df),
            "first_game": {v.ff_player_id: v.first_game for v in trig.vacancies}, "slots": slots}


def label(cand: str, m: float | None) -> str:
    return cand if m is None else f"{cand}:m={m:g}"


# --------------------------------------------------------------------------
# Design
# --------------------------------------------------------------------------

def premise(scored: list[dict], fmt: str) -> dict:
    slot = GATE_SLOT[fmt]
    pooled = pd.concat([s["slots"][slot]["rows"].assign(slate=s["slate"]) for s in scored if slot in s["slots"]],
                       ignore_index=True)
    if pooled.empty:
        return {"passed": False, "reason": "no labeled rows"}
    pooled["under"] = pooled["actual"] - pooled["v5"]
    top = pooled.sort_values("under", ascending=False, kind="stable").head(10)
    share_top, share_all = float(top["ben"].mean()), float(pooled["ben"].mean())
    return {"passed": share_all > 0 and share_top > 2 * share_all, "top10_beneficiary_share": share_top,
            "observed_beneficiary_share": share_all, "observed_rows": int(len(pooled)),
            "top10": [{"slate": r.slate, "player": r.normalized_name, "actual": r.actual, "v5": r.v5, "beneficiary": bool(r.ben)}
                      for r in top.itertuples()]}


def mean_over(scored, fmt, key, cand=None, require_flag=False):
    slot = GATE_SLOT[fmt]
    vals = []
    for s in scored:
        r = s["slots"].get(slot)
        if r is None or (require_flag and not r["flagged_n"]):
            continue
        v = r[key] if cand is None else r["candidates"][cand][key]
        if v is not None:
            vals.append(v)
    return (float(np.mean(vals)) if vals else None), len(vals)


def choose(scored: list[dict], fmt: str) -> dict:
    v5_obs, n_obs = mean_over(scored, fmt, "v5_observed_mae")
    v5_flag, n_flag = mean_over(scored, fmt, "v5_flagged_mae", require_flag=True)
    table = []
    for cand, m in [("C1", None)] + [("C2", m) for m in GRID]:
        lab = label(cand, m)
        obs, _ = mean_over(scored, fmt, "observed_mae", lab)
        flag, _ = mean_over(scored, fmt, "flagged_mae", lab, require_flag=True)
        admissible = obs is not None and v5_obs is not None and obs <= v5_obs + TOLERANCE[fmt]
        table.append({"candidate": cand, "m": m, "observed_mae": obs, "flagged_mae": flag, "admissible": admissible})
    ok = [t for t in table if t["admissible"] and t["flagged_mae"] is not None]
    # Ties go to the simpler candidate: C1 first, then the smaller m (the table is in that order).
    best = min(ok, key=lambda t: t["flagged_mae"]) if ok else None
    decision = "NOT_BUILT"
    if best is not None and v5_flag is not None and best["flagged_mae"] < v5_flag:
        decision = "CHALLENGER"
    return {"v5_observed_mae": v5_obs, "v5_flagged_mae": v5_flag, "slates_observed": n_obs, "slates_flagged": n_flag,
            "table": table, "chosen": best if decision == "CHALLENGER" else None, "decision": decision}


def design(conn) -> dict:
    box = load_box(conn)
    entries = [e for e in slate_inputs(conn, range(1, 8)) if e["first_kickoff"] < DESIGN_CUTOFF]
    cands = [("C1", None)] + [("C2", m) for m in GRID]
    scored = [score_slate(e, box, cands) for e in entries]
    out = {"study": STUDY, "mode": "design", "ran_at": datetime.now(timezone.utc).isoformat(), "formats": {}}
    for fmt in ("classic", "showdown"):
        sf = [s for s in scored if s["fmt"] == fmt]
        prem = premise(sf, fmt)
        ch = choose(sf, fmt) if prem["passed"] else {"decision": "NOT_BUILT", "reason": "premise check failed"}
        # Descriptive only: what step 3 would have shown. It cannot change a stopped study.
        after_stop = None if prem["passed"] else choose(sf, fmt)
        out["formats"][fmt] = {"slates": [summary(s) for s in sf], "premise": prem, "choice": ch,
                               "not_used_after_stop": after_stop,
                               "captain": captain_report(sf, ch.get("chosen")) if fmt == "showdown" else None}
    out["_scored"] = scored
    return out


def captain_report(scored, chosen) -> dict | None:
    if not chosen:
        return None
    lab = label(chosen["candidate"], chosen["m"])
    vals = [(s["slots"]["CPT"]["candidates"][lab]["flagged_mae"], s["slots"]["CPT"]["v5_flagged_mae"])
            for s in scored if "CPT" in s["slots"] and s["slots"]["CPT"]["flagged_n"]]
    if not vals:
        return {"slates": 0}
    return {"slates": len(vals), "challenger_flagged_mae": float(np.mean([a for a, _ in vals])),
            "v5_flagged_mae": float(np.mean([b for _, b in vals]))}


def summary(s: dict) -> dict:
    slot = GATE_SLOT[s["fmt"]]
    r = s["slots"].get(slot, {})
    return {"slate": s["slate"], "label": s["label"], "week": s["week"], "contest": s["contest"],
            "first_kickoff": s["first_kickoff"].isoformat(), "vacancies": len(s["trigger"]["vacancies"]),
            "beneficiaries": len(s["trigger"]["beneficiaries"]), "flagged_labeled": r.get("flagged_n"),
            "unlabeled_beneficiaries": len(r.get("unlabeled", [])), "observed_rows": r.get("observed_n"),
            "label_mass": s["label_mass"], "v5_flagged_mae": r.get("v5_flagged_mae"), "v5_observed_mae": r.get("v5_observed_mae")}


def git_sha() -> str:
    if os.environ.get("GITHUB_SHA"):
        return os.environ["GITHUB_SHA"]
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def dirty() -> bool:
    try:
        return bool(subprocess.check_output(["git", "status", "--porcelain", "--", "model", "research", "docs", "db"], text=True).strip())
    except Exception:
        return True


def freeze(conn, result: dict) -> list[dict]:
    from psycopg2.extras import Json

    if dirty():
        raise SystemExit("refusing to freeze: model/, research/, docs/ or db/ has uncommitted changes")
    sha = git_sha()
    written = []
    with conn.cursor() as cur:
        for st in STATEMENTS:
            cur.execute(st)
        for fmt, f in result["formats"].items():
            ch = f["choice"]
            chosen = ch.get("chosen")
            cur.execute("""INSERT INTO nfl_ownership_news_freezes (study, format, decision, candidate, m, commit_sha, design)
                           VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (study, format) DO NOTHING
                           RETURNING format, decision, candidate, m, frozen_at""",
                        (STUDY, fmt, ch["decision"], chosen["candidate"] if chosen else None,
                         chosen["m"] if chosen else None, sha, Json(jsonable(f))))
            r = cur.fetchone()
            if r is None:
                raise SystemExit(f"{fmt} is already frozen; a freeze is written once")
            written.append(dict(r))
    conn.commit()
    return written


# --------------------------------------------------------------------------
# Grade
# --------------------------------------------------------------------------

def t_interval(diffs: list[float]) -> tuple[float, float, float]:
    d = np.asarray(diffs, float)
    mean = float(d.mean())
    if len(d) < 2:
        return mean, float("nan"), float("nan")
    half = float(stats.t.ppf(0.975, len(d) - 1) * d.std(ddof=1) / math.sqrt(len(d)))
    return mean, mean - half, mean + half


def grade(conn, write: bool) -> dict:
    with conn.cursor() as cur:
        for st in STATEMENTS:
            cur.execute(st)
    conn.commit()
    freezes = {r["format"]: r for r in rows(conn, "SELECT * FROM nfl_ownership_news_freezes WHERE study = %s", (STUDY,))}
    out = {"study": STUDY, "mode": "grade", "ran_at": datetime.now(timezone.utc).isoformat(), "formats": {}}
    if not freezes:
        out["note"] = "no freeze row yet"
        return out
    box = load_box(conn)
    weeks = range(7, 23)
    entries = slate_inputs(conn, weeks)
    for fmt, fz in freezes.items():
        if fz["decision"] != "CHALLENGER":
            out["formats"][fmt] = {"decision": fz["decision"], "note": "design stopped; nothing to grade"}
            continue
        cand = (fz["candidate"], float(fz["m"]) if fz["m"] is not None else None)
        lab = label(*cand)
        gradable = [e for e in entries if e["fmt"] == fmt and e["first_kickoff"] >= DESIGN_CUTOFF
                    and e["imported_at"] is not None and e["imported_at"] > fz["frozen_at"]]
        per = []
        for e in gradable:
            s = score_slate(e, box, [cand])
            r = s["slots"][GATE_SLOT[fmt]]
            if not r["flagged_n"]:
                continue
            c = r["candidates"][lab]
            per.append({"slate": s["slate"], "contest": s["contest"], "week": s["week"], "first_kickoff": s["first_kickoff"],
                        "imported_at": s["imported_at"], "flagged_n": r["flagged_n"], "unlabeled_n": len(r["unlabeled"]),
                        "label_mass": s["label_mass"],
                        "diff_excluded": c["flagged_mae"] - r["v5_flagged_mae"],
                        "diff_worst": c["flagged_mae_worst"] - c["v5_flagged_mae_worst"],
                        "diff_observed": c["observed_mae"] - r["v5_observed_mae"],
                        "flagged_bias": c["flagged_bias"], "trigger": s["trigger"],
                        "captain": ({"diff": s["slots"]["CPT"]["candidates"][lab]["flagged_mae"] - s["slots"]["CPT"]["v5_flagged_mae"]}
                                    if fmt == "showdown" and "CPT" in s["slots"] and s["slots"]["CPT"]["flagged_n"] else None)})
        f_out = {"candidate": lab, "frozen_at": fz["frozen_at"].isoformat(), "slates": per,
                 "n_slates": len(per), "n_rows": int(sum(p["flagged_n"] for p in per))}
        floors = f_out["n_slates"] >= MIN_SLATES and f_out["n_rows"] >= MIN_ROWS
        season_over = datetime.now(timezone.utc) >= REGULAR_SEASON_END
        if per:
            f_out["primary_excluded"] = t_interval([p["diff_excluded"] for p in per])
            f_out["primary_worst"] = t_interval([p["diff_worst"] for p in per])
            f_out["guard"] = t_interval([p["diff_observed"] for p in per])
        f_out["verdict"] = verdict(f_out, fmt) if floors else ("INCONCLUSIVE" if season_over else None)
        out["formats"][fmt] = f_out
        if write:
            persist_grade(conn, fmt, f_out, floors or season_over)
    return out


def verdict(f: dict, fmt: str) -> str:
    ex, wo, gd = f["primary_excluded"], f["primary_worst"], f["guard"]
    if ex[0] >= 0 or wo[0] >= 0 or gd[0] > GUARD[fmt]:
        return "FAIL"
    if ex[2] < 0 and wo[2] < 0 and gd[2] <= GUARD[fmt]:
        return "PASS"
    return "INCONCLUSIVE"


def persist_grade(conn, fmt: str, f: dict, decide: bool) -> None:
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        for p in f["slates"]:
            cur.execute("""INSERT INTO nfl_ownership_news_grades (study, format, slate_key, contest_id, week, first_kickoff,
                               imported_at, candidate, flagged_n, unlabeled_n, diff_excluded, diff_worst, diff_observed, details)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (study, format, slate_key) DO NOTHING""",
                        (STUDY, fmt, p["slate"], p["contest"], p["week"], p["first_kickoff"], p["imported_at"], f["candidate"],
                         p["flagged_n"], p["unlabeled_n"], p["diff_excluded"], p["diff_worst"], p["diff_observed"],
                         Json(jsonable({"label_mass": p["label_mass"], "flagged_bias": p["flagged_bias"],
                                        "captain": p["captain"], "trigger": p["trigger"]}))))
        if decide and f.get("verdict"):
            cur.execute("""INSERT INTO nfl_ownership_news_verdicts (study, format, verdict, n_slates, n_rows, details)
                           VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (study, format) DO NOTHING""",
                        (STUDY, fmt, f["verdict"], f["n_slates"], f["n_rows"],
                         Json(jsonable({k: f.get(k) for k in ("candidate", "primary_excluded", "primary_worst", "guard")}))))
    conn.commit()


# --------------------------------------------------------------------------

def jsonable(o):
    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items() if not str(k).startswith("_") and k != "rows"}
    if isinstance(o, (list, tuple)):
        return [jsonable(v) for v in o]
    if isinstance(o, pd.Series):
        return None
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        return None if not math.isfinite(float(o)) else float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return o


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("mode", choices=["design", "grade"])
    ap.add_argument("--freeze", action="store_true", help="design: append the freeze rows")
    ap.add_argument("--write-db", action="store_true", help="grade: append grades and any verdict")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    conn = connect()
    try:
        if args.mode == "design":
            result = design(conn)
            if args.freeze:
                result["frozen"] = freeze(conn, result)
        else:
            result = grade(conn, args.write_db)
    finally:
        conn.close()
    text = json.dumps(jsonable(result), indent=2, default=str)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
    for fmt, f in result.get("formats", {}).items():
        ch = f.get("choice", {})
        print(fmt, "decision:", ch.get("decision", f.get("verdict")), "chosen:", (ch.get("chosen") or {}),
              "premise:", {k: v for k, v in (f.get("premise") or {}).items() if k != "top10"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
