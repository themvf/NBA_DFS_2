"""Baseline regrade of NFL DFS ownership forecasts under `nfl-ownership-scorecard-v2`.

Task 4 of docs/nfl-ownership-handoff-2026-10-19.md; the grading contract is
docs/nfl-ownership-scorecard-v1.md (now scorecard v2). Read-only except for the
append-only `nfl_ownership_scorecard_rows` (ON CONFLICT DO NOTHING: a grade is
written once; a re-grade under a new scorecard or grader version appends).

Per slate with an imported contest (2026, weeks 2-6):

1. Frozen inputs, per horizon (canonical = last capture at or before first
   kickoff minus 20 min; early = first capture inside 24 h), per source in the
   scorecard's order of preference: pool capture (every game of the slate in one
   capture instant), owner optimizer run's `input_snapshot`, ghost freeze. The
   preferred available source is `primary`; others are graded beside it.
2. Forecast versions from that one frozen input: `nfl-ownership-prior-v3` and
   `-v4` recomputed exactly (Classic: Python port of the TS; Showdown: the pinned
   mirrors), plus the STORED ownership when its version is neither (v1/v2).
   The recomputation is checked against the stored numbers when the stored
   version is the same algorithm.
3. Labels: classes from model/nfl_ownership_provenance.py (v1 rules), classified
   in memory for every contest (persisted rows are compared, never rewritten).
   Only observed + reconciled_zero enter metrics.
4. Populations: full pool (gate), relevant (pregame-defined), and diagnostics
   (actual >= 1%, predicted chalk, per position, per salary tier) for the
   reference contest's primary forecast.

    python -m research.nfl_ownership_scorecard [--dry-run] [--out PATH] [--json PATH]
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from db.nfl_ownership_scorecard_schema import STATEMENTS
from model import nfl_ownership_provenance as prov
from model import nfl_ownership_scorecard as sc
from model.nfl_dfs_field_audit import normalize_name
from research.nfl_ownership_inventory import (
    attach_contests, connect, ghost_freezes, load_slates, mark_horizons, owner_runs, pool_captures, rows,
)
from research.nfl_ownership_provenance import load_records

SEASON = 2026
WEEKS = range(2, 7)
SOURCE_ORDER = ("pool_capture", "optimizer_run", "ghost_freeze")
VERSION_NAMES = {"v3": "nfl-ownership-prior-v3", "v4": "nfl-ownership-prior-v4", "v2": "nfl-ownership-prior-v2"}


# --------------------------------------------------------------------------
# Frozen inputs
# --------------------------------------------------------------------------

def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _player_row(p: dict, src: str) -> dict:
    """One player from a pool-capture `evidence` object or a snapshot player."""
    if src == "pool_capture":
        e = p.get("evidence") or {}
        own = {"total": _num(e.get("ownPct")), "flex": _num(e.get("flexOwnPct")),
               "captain": _num(e.get("captainOwnPct")), "source": e.get("ownSource")}
        dk_avg, status = _num(e.get("avgFptsDk")), e.get("dkStatus")
        base = e
    else:
        base = p
        o = p.get("ownership")
        if isinstance(o, dict):
            own = {"total": _num(o.get("total")), "flex": _num(o.get("flex")), "captain": _num(o.get("captain")),
                   "source": o.get("source")}
        else:
            own = {"total": _num(o if o is not None else p.get("ownPct")), "flex": None, "captain": None,
                   "source": p.get("ownSource")}
        dk_avg, status = _num(p.get("dkAvg")), p.get("status", p.get("dkStatus"))
    avail = base.get("availability") if isinstance(base.get("availability"), dict) else {}
    return {
        "name": base.get("name") or p.get("name"), "position": str(base.get("position") or p.get("position") or "").upper(),
        "team": base.get("team") or p.get("team"), "salary": _num(base.get("salary") if base.get("salary") is not None else p.get("salary")),
        "captain_salary": _num(base.get("captainSalary")), "proj": _num(base.get("ourProj")), "dk_avg": dk_avg,
        "dk_status": (status or "") if isinstance(status, str) else "", "is_out": bool(base.get("isOut")),
        "stored_total": own["total"], "stored_flex": own["flex"], "stored_captain": own["captain"],
        "stored_source": own["source"],
        "anchored": isinstance(base.get("marketAnchor"), dict),
        "absence_commit": isinstance(base.get("absenceCommit"), dict),
        "absence_commit_obj": base.get("absenceCommit") if isinstance(base.get("absenceCommit"), dict) else None,
        "availability_status": avail.get("status") or base.get("availabilityStatus"),
        "depth_role": base.get("depthRole"), "prior_opportunity": base.get("priorOpportunity"),
        "inherited": base.get("inherited") is not None and base.get("inherited") != [],
        "has_availability": bool(avail), "has_status_key": ("dkStatus" in base) or ("status" in base),
    }


def load_capture_players(conn, cap: dict, run_ids: dict, freeze_ids: dict) -> tuple[pd.DataFrame, dict]:
    src = cap["source"]
    meta: dict = {}
    if src == "pool_capture":
        key = cap["id"].split(":", 1)[1]
        rs = rows(conn, """SELECT payload->'players' AS players, payload->>'fileDigest' AS file_digest,
                                  payload->>'codeRevision' AS code_revision
                           FROM nfl_dfs_pool_captures WHERE upload_id = %s::uuid AND capture_key = %s""",
                  (cap["upload_id"], key))
        players = [p for r in rs for p in (r["players"] or [])]
        meta = {"file_digest": rs[0]["file_digest"] if rs else None,
                "code_revision": sorted({r["code_revision"] for r in rs if r["code_revision"]})}
        full_id = f"pool:{cap['upload_id']}:{key}"
    elif src == "optimizer_run":
        rid = run_ids[cap["id"]]
        r = rows(conn, """SELECT r.input_snapshot, u.file_digest FROM nfl_dfs_optimizer_runs r
                          JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id WHERE r.run_id = %s::uuid""", (rid,))[0]
        players = r["input_snapshot"] if isinstance(r["input_snapshot"], list) else []
        meta = {"file_digest": r["file_digest"]}
        full_id = f"run:{rid}"
    else:
        fid = freeze_ids[cap["id"]]
        r = rows(conn, """SELECT g.input_snapshot, u.file_digest FROM nfl_dfs_ghost_freezes g
                          JOIN nfl_dfs_slate_uploads u ON u.upload_id = g.upload_id WHERE g.freeze_id = %s::uuid""", (fid,))[0]
        players = r["input_snapshot"] or []
        meta = {"file_digest": r["file_digest"]}
        full_id = f"ghost:{fid}"
    df = pd.DataFrame([_player_row(p, src) for p in players])
    meta["capture_id"] = full_id
    if df.empty:
        return df, meta
    df["normalized_name"] = df["name"].map(normalize_name)
    dup = df["normalized_name"].duplicated(keep="first")
    meta["duplicate_names"] = sorted(df.loc[dup, "normalized_name"].unique().tolist())
    df = df[~dup].reset_index(drop=True)
    versions = sorted({v for v in df["stored_source"].dropna().unique()})
    meta["stored_versions"] = versions
    missing = []
    if not versions:
        missing.append("ownership_version")
    if not meta.get("file_digest"):
        missing.append("salary_file_digest")
    if not df["has_availability"].all():
        missing.append(f"availability ({int((~df['has_availability']).sum())} players)")
    if not df["has_status_key"].all():
        missing.append(f"dk_status ({int((~df['has_status_key']).sum())} players)")
    meta["provenance_missing"] = missing
    return df, meta


def holds_input(df: pd.DataFrame) -> bool:
    return not df.empty and df["proj"].notna().any() and df["dk_avg"].notna().any()


# --------------------------------------------------------------------------
# Forecast versions
# --------------------------------------------------------------------------

def forecasts(df: pd.DataFrame, fmt: str, favorite, underdog) -> tuple[dict, dict]:
    """{version label: DataFrame(normalized_name, overall | CPT, FLEX)} and reproduction notes."""
    out: dict[str, pd.DataFrame] = {}
    repro: dict = {}
    stored_versions = sorted(df["stored_source"].dropna().unique())
    stored = stored_versions[0] if len(stored_versions) == 1 else (",".join(stored_versions) or None)
    if fmt == "classic":
        for v in ("v3", "v4"):
            out[VERSION_NAMES[v]] = pd.DataFrame({"normalized_name": df["normalized_name"], "overall": sc.prior_classic(df, v)})
        stored_frame = pd.DataFrame({"normalized_name": df["normalized_name"], "overall": df["stored_total"]})
        same = {"nfl-ownership-prior-v2": "v3", "nfl-ownership-prior-v3": "v3", "nfl-ownership-prior-v4": "v4"}.get(stored)
        if same and df["stored_total"].notna().any():
            d = (out[VERSION_NAMES[same]]["overall"] - df["stored_total"]).abs()
            repro = {"stored": stored, "compared_to": VERSION_NAMES[same], "max_abs_diff": round(float(d.max()), 4),
                     "players_over_0.01": int((d > 0.0101).sum())}
    else:
        for v in ("v3", "v4"):
            flex, cap, notes = sc.prior_showdown_version(df, v, favorite, underdog)
            out[VERSION_NAMES[v]] = pd.DataFrame({"normalized_name": df["normalized_name"], "CPT": cap, "FLEX": flex})
            if v == "v4":
                repro["v4_notes"] = {k: (round(float(x), 3) if isinstance(x, float) else x) for k, x in notes.items()}
        stored_frame = pd.DataFrame({"normalized_name": df["normalized_name"], "CPT": df["stored_captain"],
                                     "FLEX": df["stored_flex"]})
        same = {"nfl-ownership-prior-v3": "v3", "nfl-ownership-prior-v4": "v4"}.get(stored)
        if stored == "nfl-ownership-prior-v2":
            f2, c2, _ = sc.prior_showdown_version(df, "v2")
            d = np.maximum(np.abs(f2 - df["stored_flex"].to_numpy(float)), np.abs(c2 - df["stored_captain"].to_numpy(float)))
            repro.update({"stored": stored, "compared_to": "v2 mirror (value exponent 1.5)",
                          "max_abs_diff": round(float(np.nanmax(d)), 4) if np.isfinite(d).any() else None})
        if same and df["stored_flex"].notna().any():
            r = out[VERSION_NAMES[same]]
            d = np.maximum((r["FLEX"] - df["stored_flex"]).abs(), (r["CPT"] - df["stored_captain"]).abs())
            repro.update({"stored": stored, "compared_to": VERSION_NAMES[same], "max_abs_diff": round(float(d.max()), 4),
                          "players_over_0.01": int((d > 0.0101).sum())})
    stored_has_values = (df["stored_total"].notna().any() if fmt == "classic" else df["stored_flex"].notna().any())
    if stored and stored_has_values and stored not in (VERSION_NAMES["v3"], VERSION_NAMES["v4"]):
        out[f"stored:{stored}"] = stored_frame
    repro["stored_version"] = stored
    return out, repro


# --------------------------------------------------------------------------
# Game script (v4 Showdown), as research/nfl_showdown_ownership_eval.py resolves it
# --------------------------------------------------------------------------

def game_script(conn, week, teams) -> tuple[str | None, str | None, str]:
    from research.nfl_showdown_ownership_eval import _team_key

    teams = [t for t in teams if t]
    if week is None or len(set(teams)) != 2:
        return None, None, "unknown: not a two-team slate"
    by_key = {_team_key(t): str(t) for t in set(teams)}
    for g in rows(conn, """
            SELECT h.abbreviation AS home, a.abbreviation AS away,
                   COALESCE(m.home_ml, g.market_home_ml, g.quoted_home_ml) AS home_ml,
                   COALESCE(m.away_ml, g.market_away_ml, g.quoted_away_ml) AS away_ml,
                   CASE WHEN m.home_ml IS NOT NULL THEN 'nfl_matchups'
                        WHEN g.market_home_ml IS NOT NULL THEN 'nfl_season_games.market'
                        WHEN g.quoted_home_ml IS NOT NULL THEN 'nfl_season_games.quoted' END AS src
            FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id
            JOIN nfl_teams a ON a.team_id = g.away_team_id
            LEFT JOIN nfl_matchups m ON m.id = g.matchup_id AND m.home_ml IS NOT NULL AND m.away_ml IS NOT NULL
            WHERE g.season = %s AND g.week = %s AND g.game_type = 'REG'""", (SEASON, week)):
        home, away = by_key.get(_team_key(g["home"])), by_key.get(_team_key(g["away"]))
        if not home or not away or home == away:
            continue
        if g["home_ml"] is None or g["away_ml"] is None or float(g["home_ml"]) == float(g["away_ml"]):
            return None, None, "unknown: no moneyline"
        fav = home if float(g["home_ml"]) < float(g["away_ml"]) else away
        return fav, (away if fav == home else home), f"{g['src']} home {g['home_ml']} away {g['away_ml']} (current value, not as of capture)"
    return None, None, "unknown: game not found"


# --------------------------------------------------------------------------
# Grading
# --------------------------------------------------------------------------

def relevant_names(df: pd.DataFrame, fmt: str, stored_pred: pd.Series) -> set[str]:
    """Population 2: pregame-defined from the frozen forecast only. Classic: top N by
    the frozen forecast's predicted ownership plus top N by field points per $1,000,
    per position, union. Showdown: everyone."""
    active = df[~df["is_out"]]
    if fmt == "showdown":
        return set(active["normalized_name"])
    out: set[str] = set()
    fp = [sc.field_points(p, a) for p, a in zip(active["proj"], active["dk_avg"])]
    value = pd.Series([(f or 0.0) / (max(s or 0, 1) / 1000.0) for f, s in zip(fp, active["salary"])], index=active.index)
    for pos, n in sc.N_RELEVANT_CLASSIC.items():
        g = active[active["position"] == pos]
        if g.empty:
            continue
        out |= set(g.assign(_p=stored_pred.reindex(g.index).fillna(0.0)).sort_values("_p", ascending=False, kind="stable")
                   .head(n)["normalized_name"])
        out |= set(g.assign(_v=value.reindex(g.index)).sort_values("_v", ascending=False, kind="stable").head(n)["normalized_name"])
    return out


def label_frame(provenance: prov.ContestProvenance, slot: str) -> pd.DataFrame:
    return pd.DataFrame([{"normalized_name": l.normalized_name, "label_class": l.label_class, "actual": l.label_pct}
                         for l in provenance.labels if l.slot == slot])


def grade_contest(df: pd.DataFrame, fc: dict, provenance: prov.ContestProvenance, fmt: str, *, diagnostics: bool,
                  relevant: set[str]) -> list[dict]:
    """Rows (one per version, population, slot) for one contest and one frozen input."""
    out = []
    active = df[~df["is_out"]]
    for slot in sc.iter_slots(fmt):
        labels = label_frame(provenance, slot)
        if labels.empty:
            continue
        joined_all = active[["normalized_name", "name", "position", "salary", "anchored"]].merge(
            labels, on="normalized_name", how="left")
        classes = Counter(joined_all["label_class"].fillna("no_label"))
        unmatched_labels = sorted(set(labels.loc[labels["label_class"].isin(sc.GATE_CLASSES), "normalized_name"])
                                  - set(df["normalized_name"]))
        out_players_labelled = int(df[df["is_out"]]["normalized_name"].isin(labels["normalized_name"]).sum())
        gate = joined_all[joined_all["label_class"].isin(sc.GATE_CLASSES)]
        for version, frame in fc.items():
            if slot not in frame or frame[slot].isna().all():
                continue
            g = gate.merge(frame[["normalized_name", slot]].rename(columns={slot: "pred"}), on="normalized_name", how="left")
            g = g[g["pred"].notna()].copy()
            g["actual"] = g["actual"].astype(float)
            g["tier"] = g["salary"].map(lambda s: sc.salary_tier(float(s or 0), fmt))
            pops = {"full_pool": g, "relevant": g[g["normalized_name"].isin(relevant)]}
            if diagnostics:
                pops["diag:actual_ge_1"] = g[g["actual"] >= 1.0]
                thr = sc.CHALK_THRESHOLD["classic" if fmt == "classic" else slot]
                pops[f"diag:pred_chalk_ge_{thr:g}"] = g[g["pred"] >= thr]
                for pos, gg in g.groupby("position"):
                    pops[f"diag:position:{pos}"] = gg
                for tier, gg in g.groupby("tier"):
                    pops[f"diag:salary_tier:{tier}"] = gg
            for pop, frame_ in pops.items():
                m = sc.summarize(frame_, fmt, slot)
                if pop == "full_pool":
                    m["concentration_top5"] = sc.concentration(frame_)
                out.append({"version": version, "population": pop, "slot": slot, "metrics": m,
                            "class_counts": {k: int(v) for k, v in classes.items()},
                            "details": {"gate_labels_not_in_forecast": unmatched_labels[:20],
                                        "gate_labels_not_in_forecast_n": len(unmatched_labels),
                                        "out_players_with_label": out_players_labelled,
                                        "active_players": int(len(active)),
                                        "excluded_label_rows": int(sum(classes[c] for c in sc.EXCLUDED_CLASSES))},
                            "frame": g if pop == "full_pool" else None})
    return out


def label_mass(provenance: prov.ContestProvenance, fmt: str) -> float | None:
    vals = [l.label_pct for l in provenance.labels if l.slot == "overall" and l.label_class in sc.GATE_CLASSES]
    return round(sum(vals) / (prov.ROSTER_SLOTS[fmt] * 100), 5) if vals else None


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def run(conn) -> dict:
    slates, upload_slate = load_slates(conn, SEASON)
    contests = attach_contests(conn, slates, upload_slate, SEASON)
    pool_captures(conn, slates, upload_slate)
    owner_runs(conn, slates, upload_slate)
    ghost_freezes(conn, slates, upload_slate)
    for s in slates.values():
        mark_horizons(s)
    run_ids = {r["run_id"][:8]: r["run_id"] for r in rows(conn, "SELECT run_id::text FROM nfl_dfs_optimizer_runs")}
    freeze_ids = {r["f"][:8]: r["f"] for r in rows(conn, "SELECT freeze_id::text AS f FROM nfl_dfs_ghost_freezes")}

    records, meta = load_records(conn)
    provs = {r.contest_id: prov.classify_contest(r, mass_floor=prov.DEFAULT_MASS_FLOOR) for r in records}
    persisted = defaultdict(dict)
    for r in rows(conn, """SELECT contest_id, normalized_name, slot, label_class FROM nfl_ownership_label_provenance
                           WHERE version = %s""", (prov.VERSION,)):
        persisted[r["contest_id"]][(r["normalized_name"], r["slot"])] = r["label_class"]
    prov_check = {}
    for cid, p in provs.items():
        mem = {(l.normalized_name, l.slot): l.label_class for l in p.labels}
        if cid not in persisted:
            prov_check[cid] = "classified in memory only (not in nfl_ownership_label_provenance)"
        else:
            diff = sum(1 for k, v in mem.items() if persisted[cid].get(k) != v) + len(set(persisted[cid]) - set(mem))
            prov_check[cid] = "matches persisted" if diff == 0 else f"{diff} label(s) differ from persisted"

    result = {"rows": [], "slates": [], "prov_check": prov_check, "frames": [],
              "unlinked": [{"contest_id": c["contest_id"], "week": c["week"], "name": c["contest_name"], "link": c["link"]}
                           for c in contests if c["week"] in WEEKS and not c.get("slate_key")]}
    graded = [s for s in slates.values() if s.contests and s.week in WEEKS and s.first_kickoff]
    graded.sort(key=lambda s: (s.first_kickoff, s.fmt))
    for s in graded:
        fmt = s.fmt
        teams = []
        if fmt == "showdown":
            teams = [t for g in s.games for t in g.split("@")]
        fav, dog, gs_src = game_script(conn, s.week, teams) if fmt == "showdown" else (None, None, "n/a")
        cs = sorted(s.contests, key=lambda c: -(c["entry_count"] or 0))
        complete = [c for c in cs if provs.get(c["contest_id"]) and provs[c["contest_id"]].complete]
        reference = complete[0] if complete else next(
            (c for c in cs if provs.get(c["contest_id"]) and any(l.label_class in sc.GATE_CLASSES
                                                                  for l in provs[c["contest_id"]].labels)), None)
        slate_info = {"key": s.key, "label": s.label, "fmt": fmt, "week": s.week, "first_kickoff": s.first_kickoff,
                      "games": s.games, "favorite": fav, "underdog": dog, "game_script_source": gs_src,
                      "reference": reference["contest_id"] if reference else None,
                      "reference_complete": bool(complete), "contests": [c["contest_id"] for c in cs], "horizons": {}}
        result["slates"].append(slate_info)
        for horizon in ("canonical", "early"):
            chosen = []
            for source in SOURCE_ORDER:
                cap = next((c for c in s.captures if c["source"] == source and c.get(horizon)), None)
                if cap is None:
                    continue
                if source == "pool_capture" and cap["games_covered"] != len(s.games):
                    continue
                df, cmeta = load_capture_players(conn, cap, run_ids, freeze_ids)
                if not holds_input(df):
                    continue
                chosen.append((source, cap, df, cmeta))
            slate_info["horizons"][horizon] = [(src, cmeta["capture_id"], cap["at"]) for src, cap, _, cmeta in chosen]
            if not chosen:
                for c in cs:
                    result["rows"].append(base_row(s, c, horizon, "none", "none", None, None, "none", None, "n/a",
                                                   "none", "none", reference, provs) | {
                        "row_kind": "no_frozen_forecast",
                        "details": {"reason": f"no {horizon}-horizon capture of any source holds projections and DK averages "
                                              "for every game of the slate"}})
                continue
            latest_rank = max(range(len(chosen)), key=lambda i: (chosen[i][1]["at"], -i))
            for rank, (source, cap, df, cmeta) in enumerate(chosen):
                primary = rank == 0
                latest = rank == latest_rank
                fc, repro = forecasts(df, fmt, fav, dog)
                stored_pred = df["stored_total"] if df["stored_total"].notna().any() else (
                    fc[VERSION_NAMES["v3"]]["overall"] if fmt == "classic" else pd.Series(0.0, index=df.index))
                relevant = relevant_names(df, fmt, stored_pred)
                for c in cs:
                    p = provs.get(c["contest_id"])
                    if p is None:
                        continue
                    is_ref = reference is not None and c["contest_id"] == reference["contest_id"]
                    for g in grade_contest(df, fc, p, fmt, diagnostics=is_ref and primary, relevant=relevant):
                        version = g["version"]
                        vshort = "v3" if version.endswith("v3") and not version.startswith("stored") else (
                            "v4" if version.endswith("v4") and not version.startswith("stored") else "stored")
                        row = base_row(s, c, horizon, source, cmeta["capture_id"], cap["at"], s.first_kickoff, version,
                                       repro.get("stored_version"), sc.design_tag(vshort, fmt, s.week) if vshort != "stored" else "stored_forecast",
                                       g["population"], g["slot"], reference, provs)
                        m = g["metrics"]
                        row.update({"row_kind": "contest", "n": m["n"], "mae": m["mae"], "bias": m["bias"], "rmse": m["rmse"],
                                    "spearman": m["spearman"], "pearson": m["pearson"], "class_counts": g["class_counts"],
                                    "metrics": m, "details": g["details"] | {
                                        "source_rank": "primary" if primary else "secondary_source",
                                        "latest_capture": latest,
                                        "reproduction": repro, "provenance_missing": cmeta.get("provenance_missing"),
                                        "duplicate_names": cmeta.get("duplicate_names"),
                                        "code_revision": cmeta.get("code_revision"),
                                        "game_script": {"favorite": fav, "underdog": dog, "source": gs_src},
                                        "anchored_players": int(df["anchored"].sum()), "players": int(len(df))}})
                        result["rows"].append(row)
                        if g["frame"] is not None and is_ref:
                            result["frames"].append({"slate": s.key, "week": s.week, "fmt": fmt, "label": s.label,
                                                     "horizon": horizon, "source": source, "primary": primary,
                                                     "latest": latest,
                                                     "version": version, "slot": g["slot"], "contest": c["contest_id"],
                                                     "frame": g["frame"], "df": df, "design": row["design_tag"]})
    return result


def base_row(s, c, horizon, source, capture_id, at, kickoff, version, stored, design, population, slot, reference, provs) -> dict:
    p = provs.get(c["contest_id"])
    is_ref = reference is not None and c["contest_id"] == reference["contest_id"]
    return {"scorecard_version": sc.SCORECARD_VERSION, "grader_version": sc.GRADER_VERSION, "season": SEASON,
            "week": s.week, "format": s.fmt, "slate_key": s.key, "contest_id": c["contest_id"],
            "contest_role": "reference" if is_ref else "secondary", "entry_count": c["entry_count"], "horizon": horizon,
            "source": source, "capture_id": capture_id, "captured_at": at,
            "minutes_before_kickoff": (kickoff - at).total_seconds() / 60 if (at and kickoff) else None,
            "forecast_version": version, "stored_version": stored, "design_tag": design, "population": population,
            "slot": slot, "n": None, "mae": None, "bias": None, "rmse": None, "spearman": None, "pearson": None,
            "labels_complete": p.complete if p else None, "label_mass": label_mass(p, s.fmt) if p else None,
            "class_counts": {}, "metrics": {}, "details": {}}


# --------------------------------------------------------------------------
# Pooling, questions, rendering, persistence
# --------------------------------------------------------------------------

def pooled_rows(result: dict) -> list[dict]:
    """Reference contest, primary source, gate population, per (format, horizon, version, slot);
    `all` slates and `not_design` slates; labels-complete reference contests only."""
    out = []
    for selector in ("primary", "latest"):
        out += _pooled_for(result, selector)
    return out


def _pooled_for(result: dict, selector: str) -> list[dict]:
    """selector `primary`: the scorecard's source preference (pool capture first);
    `latest`: the latest capture at the horizon across sources (shown beside it)."""
    def chosen(flag_primary, flag_latest):
        return flag_primary if selector == "primary" else flag_latest

    out = []
    groups = defaultdict(list)
    for r in result["rows"]:
        if (r["row_kind"] == "contest" and r["contest_role"] == "reference" and r["population"] == "full_pool"
                and chosen(r["details"].get("source_rank") == "primary", r["details"].get("latest_capture"))
                and r["labels_complete"]):
            groups[(r["format"], r["horizon"], r["forecast_version"], r["slot"])].append(r)
    frames = {(f["slate"], f["horizon"], f["version"], f["slot"]): f for f in result["frames"]
              if chosen(f["primary"], f["latest"])}
    for (fmt, horizon, version, slot), rs in sorted(groups.items()):
        for subset in ("all", "not_design"):
            pick = [r for r in rs if subset == "all" or r["design_tag"] != "design"]
            samples = []
            for r in pick:
                f = frames.get((r["slate_key"], horizon, version, slot))
                if f is not None:
                    samples.append(sc.SlateSample(r["slate_key"], f["frame"]["pred"].to_numpy(float),
                                                  f["frame"]["actual"].to_numpy(float)))
            pooled = sc.pooled(samples)
            ci = sc.cluster_bootstrap(samples)
            out.append({"scorecard_version": sc.SCORECARD_VERSION, "grader_version": sc.GRADER_VERSION, "row_kind": "pooled",
                        "season": SEASON, "week": None, "format": fmt, "slate_key": f"pooled:{subset}",
                        "contest_id": f"POOLED:{fmt}:{subset}", "contest_role": "pooled", "entry_count": None,
                        "horizon": horizon, "source": selector, "capture_id": f"pooled:{selector}", "captured_at": None,
                        "minutes_before_kickoff": None, "forecast_version": version, "stored_version": None,
                        "design_tag": subset, "population": "full_pool", "slot": slot, "n": pooled["n"], "mae": pooled["mae"],
                        "bias": pooled["bias"], "rmse": pooled["rmse"], "spearman": pooled["spearman_mean"], "pearson": None,
                        "labels_complete": True, "label_mass": None, "class_counts": {},
                        "metrics": {"pooled": pooled, "bootstrap_95": ci,
                                    "per_slate": [{"slate": r["slate_key"], "week": r["week"], "contest": r["contest_id"],
                                                   "design": r["design_tag"], "n": r["n"], "mae": r["mae"], "bias": r["bias"],
                                                   "spearman": r["spearman"]} for r in pick]},
                        "details": {"note": "Spearman is the mean of per-slate tied-rank Spearman; MAE/bias/RMSE are "
                                            "player-weighted over the concatenated slates; bootstrap resamples slates."}})
    return out


def persist(conn, rs: list[dict]) -> int:
    from psycopg2.extras import Json, execute_values

    with conn.cursor() as cur:
        for st in STATEMENTS:
            cur.execute(st)
        cols = ["scorecard_version", "grader_version", "row_kind", "season", "week", "format", "slate_key", "contest_id",
                "contest_role", "entry_count", "horizon", "source", "capture_id", "captured_at", "minutes_before_kickoff",
                "forecast_version", "stored_version", "design_tag", "population", "slot", "n", "mae", "bias", "rmse",
                "spearman", "pearson", "labels_complete", "label_mass", "class_counts", "metrics", "details"]
        values = [tuple(Json(_jsonable(r[c])) if c in ("class_counts", "metrics", "details") else r[c] for c in cols)
                  for r in rs]
        res = execute_values(cur, f"""INSERT INTO nfl_ownership_scorecard_rows ({', '.join(cols)}) VALUES %s
                                      ON CONFLICT DO NOTHING RETURNING 1""", values, page_size=500, fetch=True)
    conn.commit()
    return len(res)


def _jsonable(o):
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        f = float(o)
        return None if not math.isfinite(f) else f
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return o


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="grade and report, write nothing")
    ap.add_argument("--out", default=None, help="markdown artifact path")
    ap.add_argument("--pickle", default=None, help="save the in-memory result")
    ap.add_argument("--from-pickle", default=None, help="render a saved result (no database, no write)")
    args = ap.parse_args(argv)
    import pickle

    from research.nfl_ownership_scorecard_report import render

    if args.from_pickle:
        result = pickle.loads(Path(args.from_pickle).read_bytes())
        result["pooled"] = pooled_rows(result)
    else:
        conn = connect()
        try:
            result = run(conn)
            result["pooled"] = pooled_rows(result)
            result["written"] = None if args.dry_run else persist(conn, result["rows"] + result["pooled"])
            if not args.dry_run:
                result["stored_total"] = rows(conn, """SELECT count(*) AS n FROM nfl_ownership_scorecard_rows
                                                       WHERE scorecard_version = %s AND grader_version = %s""",
                                              (sc.SCORECARD_VERSION, sc.GRADER_VERSION))[0]["n"]
        finally:
            conn.close()
        if args.pickle:
            Path(args.pickle).write_bytes(pickle.dumps(result))
    text = render(result, result.get("written"), result.get("stored_total"))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
    print(f"rows {len(result['rows'])} pooled {len(result['pooled'])} written {result.get('written')}"
          + (f"; wrote {args.out}" if args.out else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
