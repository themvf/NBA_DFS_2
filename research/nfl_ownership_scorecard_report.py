"""Readable artifact for the ownership scorecard baseline regrade (research/nfl_ownership_scorecard.py).

Renders the in-memory result of `run()` (plus pooled rows) as markdown: per-slate
grades, pooled tables with slate-clustered intervals, source disagreements,
diagnostics, and the four descriptive questions the owner raised (value chalk,
Showdown captain share by position, Showdown kickers, concentration). Pure
formatting; no database.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from model import nfl_ownership_scorecard as sc

V3, V4 = "nfl-ownership-prior-v3", "nfl-ownership-prior-v4"


def _f(v, d=2, sign=False):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "-"
    return f"{v:+.{d}f}" if sign else f"{v:.{d}f}"


def _ci(ci, d=2):
    return "-" if not ci else f"[{ci[0]:.{d}f}, {ci[1]:.{d}f}]"


def _short(v: str) -> str:
    return v.replace("nfl-ownership-prior-", "").replace("stored:", "stored ")


def _slate_label(result, key):
    s = next((x for x in result["slates"] if x["key"] == key), None)
    return s["label"] if s else key


def ref_rows(result, horizon="canonical", population="full_pool", selector="primary"):
    def keep(r):
        return (r["details"].get("source_rank") == "primary") if selector == "primary" else bool(r["details"].get("latest_capture"))
    return [r for r in result["rows"] if r["row_kind"] == "contest" and r["contest_role"] == "reference"
            and r["horizon"] == horizon and r["population"] == population and keep(r)]


def frames(result, horizon="canonical", selector="primary"):
    return [f for f in result["frames"] if f["horizon"] == horizon and (f["primary"] if selector == "primary" else f["latest"])]


def render(result: dict, written: int | None, stored_total: int | None = None) -> str:
    L: list[str] = []
    now = datetime.now(timezone.utc)
    L += [f"# NFL DFS ownership — scorecard regrade ({sc.SCORECARD_VERSION})", "",
          f"Generated {now:%Y-%m-%d %H:%M} UTC by `python -m research.nfl_ownership_scorecard` "
          f"(grader `{sc.GRADER_VERSION}`). Read-only except the append-only `nfl_ownership_scorecard_rows` "
          f"({'dry run, nothing written' if written is None else f'{written} new rows written this run; {stored_total} rows under this scorecard and grader version in total, one grade per forecast (ON CONFLICT DO NOTHING)'}).", "",
          "Status: BUILT, TESTED (unit tests on the metric functions and label gating), BACKTESTED on frozen 2026 "
          "inputs. Descriptive only: no verdict, no constant changed. Showdown has no registered gate; the Classic "
          "Phase 2 gate needs >= 4 held-out slates of a FITTED model and is not evaluated here. The drafted v5 is "
          "graded beside v3 and v4; weeks 2-6 are v5 `design` slates (their v5 design grade is "
          "`python -m research.nfl_ownership_prior_v5_design`) and week 7 on is v5's prospective grade "
          "(docs/nfl-ownership-prior-v5.md section 5).", ""]
    L += ["## How to read this", "",
          "- **Frozen input.** Per slate and horizon (canonical = last capture at or before first kickoff minus "
          "20 min; early = first capture inside 24 h), each source's capture is loaded: pool capture (one capture "
          "instant covering every game; ownership and DK average read from each player's `evidence`), the owner "
          "optimizer run's `input_snapshot`, the ghost freeze. Scorecard v3 (owner, 2026-10-20): `primary` is the "
          "LATEST of them, from any source; `preferred` is the v1/v2 preference order (pool, run, ghost), pooled "
          "beside it in section 3 for comparison.",
          "- **Versions.** v3, v4 and the drafted v5 are recomputed from the same frozen input (Classic: Python "
          "port of ownership-prior.ts; Showdown: the pinned mirrors in model/nfl_showdown_ownership_eval.py). Where the "
          "input stored a v3 (or the identical Classic v2) forecast, the recomputation reproduced it to 0.00 on "
          "every player (section 7). A stored v1/v2 Showdown forecast is graded as `stored`.",
          "- **Four slates' inputs hold projections and DK averages but no stored ownership** (week 2 Classic and "
          "NYG@LAR, week 3 Classic and ATL@GB; `stored none` in section 2), so their v3/v4 are recomputations from "
          "a frozen pregame input, not stored forecasts. The task-1 inventory counted only captures with a stored "
          "`ownPct` and listed them as `no_frozen_forecast`; graded here, the history is 16 slates with a frozen "
          "input rather than the inventory's 12 with a stored forecast. They are `design` slates for v4 (and the "
          "two Showdowns for v3).",
          "- **Labels.** Classes from model/nfl_ownership_provenance.py (v1 rules, mass floor 0.995), classified in "
          "memory; contests already in `nfl_ownership_label_provenance` matched the persisted classes exactly, the "
          "three week-6 imports are in memory only. Only `observed` and `reconciled_zero` are scored.",
          "- **Population.** Players OUT in the frozen input are not graded (their prediction is 0 by construction); "
          "this is how the WAS@SF pre-grade counted (45 active). Full pool = every active player in the input with a "
          "gate-class label. Reference contest = the largest field whose labels are complete; when no contest on a "
          "slate is complete (weeks 4 and 6 Classic: stat-api top-100 users, mass < 0.995 or partial standings) the "
          "largest contest is graded on its observed labels only and is NOT gate-eligible (excluded from pooled "
          "tables).",
          "- **Relevant (population 2).** Classic: per position, top N_RELEVANT by the frozen STORED forecast "
          "(v3 recomputation where nothing was stored) plus top N by field points per $1,000, union; the same set is "
          "used for every version so versions are compared on one population. Showdown: every active player.",
          "- **Design tags.** v4: weeks 2-5 `design` (both formats). v3: Showdown weeks 2-3 `design`. v5: weeks "
          "2-6 `design` (both formats; its constants were chosen on them). Week 6 is v4's first non-design data and week 7 v5's. "
          "`not_design` pooled rows exclude design slates.",
          "- **Game script (v4 Showdown).** Favorite/underdog from the game's moneyline as the eval harness reads it "
          "(nfl_matchups, then nfl_season_games). These are CURRENT stored values, not as of the capture.",
          "- Spearman uses averaged tied ranks. Pooled MAE/bias/RMSE are player-weighted over concatenated slates; "
          "pooled Spearman is the mean of per-slate values. 95% intervals resample SLATES (2,000 reps, seed "
          "20261020).", ""]

    # 1. Coverage
    L += ["## 1. Slates and frozen inputs", "",
          "| wk | slate | first kickoff UTC | reference contest | entries | labels complete | canonical sources (capture, UTC) | early sources | favorite |",
          "|---:|---|---|---|---:|---|---|---|---|"]
    entries = {r["contest_id"]: r["entry_count"] for r in result["rows"]}
    for s in result["slates"]:
        def srcs(h):
            return "; ".join(f"{a.split('_')[0]} {str(c)[5:16]}" for a, _, c in s["horizons"].get(h, [])) or "none"
        L.append(f"| {s['week']} | {s['label']} | {s['first_kickoff']:%m-%d %H:%M} | {s['reference'] or '-'} | "
                 f"{entries.get(s['reference'], 0) or 0:,} | {'yes' if s['reference_complete'] else 'NO'} | {srcs('canonical')} | "
                 f"{srcs('early')} | {s['favorite'] or '-'} |")
    if result.get("unlinked"):
        L.append("")
    for u in result.get("unlinked", []):
        L.append(f"- Not graded: contest {u['contest_id']} (week {u['week']}, {u['name'][:60]}) has no salary upload "
                 f"({u['link']}), so there is no slate, no frozen input and no `no_frozen_forecast` row.")
    nff = [r for r in result["rows"] if r["row_kind"] == "no_frozen_forecast"]
    L += ["", f"`no_frozen_forecast` rows: {len(nff)}" + (" (" + ", ".join(sorted({f'{r['contest_id']} {r['horizon']}' for r in nff})) + ")" if nff else "") + ".", ""]

    # 2. Per-slate grades
    L += ["## 2. Per-slate grades — reference contest, canonical horizon, primary source, full pool (gate population)", "",
          "| wk | slate | contest | source / capture (min before) | stored | anchored/players | version | tag | slot | n | MAE | bias | RMSE | Spearman | Pearson | top-10 |",
          "|---:|---|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in sorted(ref_rows(result), key=lambda r: (r["week"], r["slate_key"], r["slot"], r["forecast_version"])):
        d = r["details"]
        L.append(f"| {r['week']} | {_slate_label(result, r['slate_key'])} | {r['contest_id']} | {r['source'].split('_')[0]} "
                 f"{str(r['captured_at'])[5:16]} ({_f(r['minutes_before_kickoff'], 0)}) | {_short(r['stored_version'] or 'none')} | "
                 f"{d['anchored_players']}/{d['players']} | {_short(r['forecast_version'])} | {r['design_tag']} | {r['slot']} | "
                 f"{r['n']} | {_f(r['mae'])} | {_f(r['bias'], 2, True)} | {_f(r['rmse'])} | {_f(r['spearman'], 3)} | "
                 f"{_f(r['pearson'], 3)} | {r['metrics'].get('top10_overlap')} |" + ("" if r["labels_complete"] else " **labels incomplete: observed only, not gate-eligible**"))
    L.append("")

    # Relevant + early, compact
    L += ["### Relevant population (pregame-defined) and early horizon, v3 vs v4", "",
          "| wk | slate | slot | relevant n | v3 MAE / rho | v4 MAE / rho | early: source | early v3 MAE / rho | early v4 MAE / rho |",
          "|---:|---|---|---:|---|---|---|---|---|"]
    rel = {(r["slate_key"], r["slot"], r["forecast_version"]): r for r in ref_rows(result, population="relevant")}
    early = {(r["slate_key"], r["slot"], r["forecast_version"]): r for r in ref_rows(result, horizon="early")}
    for (key, slot, v), r3 in sorted(rel.items(), key=lambda kv: (kv[1]["week"], kv[0])):
        if v != V3:
            continue
        r4 = rel.get((key, slot, V4))
        e3, e4 = early.get((key, slot, V3)), early.get((key, slot, V4))
        L.append(f"| {r3['week']} | {_slate_label(result, key)} | {slot} | {r3['n']} | {_f(r3['mae'])} / {_f(r3['spearman'], 3)} | "
                 f"{_f(r4 and r4['mae'])} / {_f(r4 and r4['spearman'], 3)} | {e3['source'].split('_')[0] + ' ' + str(e3['captured_at'])[5:16] if e3 else '-'} | "
                 f"{_f(e3 and e3['mae'])} / {_f(e3 and e3['spearman'], 3)} | {_f(e4 and e4['mae'])} / {_f(e4 and e4['spearman'], 3)} |")
    L.append("")

    # 3. Pooled
    L += ["## 3. Pooled (reference contests with complete labels, full pool, slate-clustered 95% interval)", "",
          "| format | horizon | source selector | slates | version | slot | subset | n | MAE [95%] | bias [95%] | RMSE | mean Spearman [95%] |",
          "|---|---|---|---:|---|---|---|---:|---|---|---:|---|"]
    for r in sorted(result["pooled"], key=lambda r: (r["format"], r["horizon"], r["source"], r["slot"], r["design_tag"], r["forecast_version"])):
        m = r["metrics"]
        p, ci = m["pooled"], m["bootstrap_95"]
        if not p["slates"]:
            continue
        L.append(f"| {r['format']} | {r['horizon']} | {r['source']} | {p['slates']} | {_short(r['forecast_version'])} | {r['slot']} | "
                 f"{r['design_tag']} | {p['n']} | {_f(p['mae'])} {_ci(ci.get('mae'))} | {_f(p['bias'], 2, True)} {_ci(ci.get('bias'))} | "
                 f"{_f(p['rmse'])} | {_f(p['spearman_mean'], 3)} {_ci(ci.get('spearman_mean'), 3)} |")
    L.append("")

    # Source disagreement
    L += ["### Source disagreement at the canonical horizon (same slate, same version)", "",
          "| wk | slate | slot | version | " + " | ".join(["pool", "run", "ghost"]) + " |", "|---:|---|---|---|---|---|---|"]
    allc = [r for r in result["rows"] if r["row_kind"] == "contest" and r["contest_role"] == "reference"
            and r["horizon"] == "canonical" and r["population"] == "full_pool" and r["forecast_version"] in (V3, V4)]
    by = defaultdict(dict)
    for r in allc:
        by[(r["week"], r["slate_key"], r["slot"], r["forecast_version"])][r["source"]] = r
    for (wk, key, slot, v), d in sorted(by.items()):
        if len(d) < 2:
            continue
        cells = []
        for s in ("pool_capture", "optimizer_run", "ghost_freeze"):
            r = d.get(s)
            cells.append(f"{str(r['captured_at'])[11:16]}: {_f(r['mae'])} / {_f(r['spearman'], 3)}" if r else "-")
        L.append(f"| {wk} | {_slate_label(result, key)} | {slot} | {_short(v)} | " + " | ".join(cells) + " |")
    L.append("")

    # 4. Diagnostics
    L += ["## 4. Diagnostics (outcome-defined, never a gate) — reference, canonical, primary, summed over slates", ""]
    diag = [r for r in result["rows"] if r["row_kind"] == "contest" and r["contest_role"] == "reference" and r["horizon"] == "canonical"
            and r["details"].get("source_rank") == "primary" and r["population"].startswith("diag:") and r["labels_complete"]
            and r["forecast_version"] in (V3, V4) and r["n"]]
    agg = defaultdict(lambda: {"n": 0, "abs": 0.0, "sum": 0.0, "slates": 0})
    for r in diag:
        k = (r["format"], r["slot"], r["population"], r["forecast_version"])
        a = agg[k]
        a["n"] += r["n"]; a["abs"] += r["mae"] * r["n"]; a["sum"] += r["bias"] * r["n"]; a["slates"] += 1
    L += ["| format | slot | population | v3 n | v3 MAE | v3 bias | v4 MAE | v4 bias |", "|---|---|---|---:|---:|---:|---:|---:|"]
    keys = sorted({(f, s, p) for f, s, p, _ in agg})
    for f, s, p in keys:
        a3, a4 = agg.get((f, s, p, V3)), agg.get((f, s, p, V4))
        L.append(f"| {f} | {s} | {p[5:]} | {a3['n'] if a3 else '-'} | {_f(a3 and a3['abs'] / a3['n'])} | {_f(a3 and a3['sum'] / a3['n'], 2, True)} | "
                 f"{_f(a4 and a4['abs'] / a4['n'])} | {_f(a4 and a4['sum'] / a4['n'], 2, True)} |")
    L += ["", "Note: the predicted-chalk rows are selected on each version's own prediction, so their n differ by version.", ""]

    # Calibration aggregated
    L += ["### Calibration by predicted bin (summed over reference slates with complete labels, canonical, primary)", "",
          "| format | slot | version | bin | n | mean predicted | mean actual |", "|---|---|---|---|---:|---:|---:|"]
    cal = defaultdict(lambda: {"n": 0, "p": 0.0, "a": 0.0})
    for r in ref_rows(result):
        if not r["labels_complete"] or r["forecast_version"] not in (V3, V4):
            continue
        for b in r["metrics"]["calibration"]:
            if b["n"]:
                c = cal[(r["format"], r["slot"], r["forecast_version"], b["bin"])]
                c["n"] += b["n"]; c["p"] += b["mean_pred"] * b["n"]; c["a"] += b["mean_actual"] * b["n"]
    order = {b: i for i, b in enumerate(["0-1", "1-5", "5-10", "10-20", "20-35", "35-50", "35+", "50+"])}
    for (f, s, v, b), c in sorted(cal.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], order.get(kv[0][3], 9))):
        L.append(f"| {f} | {s} | {_short(v)} | {b} | {c['n']} | {c['p'] / c['n']:.2f} | {c['a'] / c['n']:.2f} |")
    L.append("")

    # Allocation
    L += ["### Allocation by position (predicted sum vs recorded sum, canonical, primary, reference)", "",
          "| wk | slate | slot | position | actual | v3 | v4 |", "|---:|---|---|---|---:|---:|---:|"]
    rr = defaultdict(dict)
    for r in ref_rows(result):
        if r["forecast_version"] in (V3, V4):
            rr[(r["week"], r["slate_key"], r["slot"])][r["forecast_version"]] = r
    for (wk, key, slot), d in sorted(rr.items()):
        a3 = d.get(V3, {}).get("metrics", {}).get("allocation", {})
        a4 = d.get(V4, {}).get("metrics", {}).get("allocation", {})
        for pos in sorted(a3):
            L.append(f"| {wk} | {_slate_label(result, key)} | {slot} | {pos} | {a3[pos]['actual']:.1f} | {a3[pos]['pred']:.1f} | "
                     f"{a4.get(pos, {}).get('pred', float('nan')):.1f} |")
    L += ["", "Recorded sums cover gate-class labels only (a synthesized captain zero is excluded, so a stat-api captain "
          "sum can fall short of 100).", ""]

    # 5. Questions
    L += _questions(result)

    # 7. Reproduction
    L += ["## 7. Reproduction checks", "",
          "- **WAS@SF (contest 196786695), owner run `21bf2279` (canonical, created 23:13 UTC, T-62):** flex v3 MAE "
          + _was(result, "optimizer_run") + ". The pre-grade result (flex v3 5.11 / 0.895, v4 5.05 / 0.860; captain "
          "v3 1.11 / 0.916, v4 1.27 / 0.898) is reproduced from this source.",
          "- **Same contest, canonical POOL capture (23:03 UTC, the upload-instant capture, code 1963fcd7, v3):** "
          + _was(result, "pool_capture") + ". It differs because at 23:03 Mike Evans and Stefon Diggs carried no Q "
          "status (the run at 23:13 has Q for both) and Brandin Cooks / Jacob Cowing were not yet market-anchored; "
          "v3 then put Evans at 67.3% flex (run: 43.9%, actual 26.8%). The owner resolved the source question on "
          "2026-10-20 (scorecard v3): the canonical forecast is the latest capture of any source, here the run. "
          "The v1/v2 preference pick is still pooled as `preferred`.",
          "- **Week-4 Millionaire (196151357), frozen stored v2 forecast:** 183 observed-label players including 2 "
          "OUT in the input (Case Keenum 0.69%, DeVonta Smith 0.05% actual) give MAE 3.540 (the review's 3.538); "
          "the scorecard's active-player population is 181 at MAE " + _wk4(result) + ". The contest's labels are not "
          "complete (mass 0.99493 < 0.995), so it is not gate-eligible; the 629-player 1.065 figure zero-fills 446 "
          "unresolved labels and is not reproduced as a gate number.",
          "- **Recomputation vs stored ownership:** " + _repro(result), ""]
    return "\n".join(L) + "\n"


def _was(result, source):
    out = []
    for slot in ("FLEX", "CPT"):
        for v in (V3, V4):
            r = next((r for r in result["rows"] if r["row_kind"] == "contest" and r["contest_id"] == "196786695"
                      and r["horizon"] == "canonical" and r["source"] == source and r["population"] == "full_pool"
                      and r["slot"] == slot and r["forecast_version"] == v), None)
            if r:
                out.append(f"{slot.lower()} {_short(v)} {_f(r['mae'])} / {_f(r['spearman'], 3)}")
    return ", ".join(out)


def _wk4(result):
    r = next((r for r in result["rows"] if r["row_kind"] == "contest" and r["contest_id"] == "196151357"
              and r["horizon"] == "canonical" and r["population"] == "full_pool" and r["forecast_version"].startswith("stored")
              and r["details"].get("source_rank") == "primary"), None)
    return f"{r['mae']:.3f} (n {r['n']})" if r else "-"


def _repro(result):
    seen = {}
    for r in result["rows"]:
        if r["row_kind"] != "contest" or r["contest_role"] != "reference":
            continue
        rep = r["details"].get("reproduction") or {}
        if "max_abs_diff" in rep:
            seen[(r["slate_key"], r["source"], r["horizon"])] = (rep.get("stored"), rep.get("compared_to"), rep["max_abs_diff"])
    bad = {k: v for k, v in seen.items() if v[2] is None or v[2] > 0.0101}
    return (f"{len(seen)} frozen inputs carried a stored forecast of a recomputable version; the largest absolute "
            f"difference on any player was {max((v[2] or 0) for v in seen.values()):.3f} percentage points"
            + ("." if not bad else f"; {len(bad)} input(s) differ: {bad}."))


def _questions(result) -> list[str]:
    L = ["## 5. The owner's four questions (descriptive; reference contest, canonical horizon, LATEST capture)", "",
          "These use the latest frozen capture at or before the canonical cutoff (for WAS@SF the owner run that "
          "reproduces the pre-grade; for week-3 Classic the run at T-28 rather than the 17-hour-old pool capture), "
          "because the questions are about what the forecast knew closest to lock. Slates whose reference labels are "
          "incomplete (weeks 4 and 6 Classic) are included on their observed labels and marked.", ""]
    fr = frames(result, selector="latest")
    early = {(f["slate"], f["source"]): f["df"] for f in result["frames"] if f["horizon"] == "early"}
    # Q1
    L += ["### Q1. Value chalk: actual >= predicted + 10 points (flex in Showdown, overall in Classic)", "",
          "| wk | slate | player | pos | salary | actual | v3 | v4 | absence commit | DK status (canonical / early) | availability | ourProj − DK avg | design (v4) |",
          "|---:|---|---|---|---:|---:|---:|---:|---|---|---|---:|---|"]
    hits = []
    by_slate = defaultdict(dict)
    for f in fr:
        if f["slot"] in ("FLEX", "overall") and f["version"] in (V3, V4):
            by_slate[(f["slate"], f["week"], f["label"], f["source"])][f["version"]] = f
    shape = defaultdict(int)
    for (key, wk, label, source), d in sorted(by_slate.items(), key=lambda kv: kv[0][1]):
        f3, f4 = d.get(V3), d.get(V4)
        if f3 is None:
            continue
        g = f3["frame"].set_index("normalized_name")
        g4 = f4["frame"].set_index("normalized_name")["pred"] if f4 is not None else pd.Series(dtype=float)
        df = f3["df"].set_index("normalized_name")
        edf = early.get((key, source))
        edf = edf.set_index("normalized_name") if edf is not None else None
        for name, row in g.iterrows():
            p4 = g4.get(name, np.nan)
            if row["actual"] - row["pred"] >= 10 or (np.isfinite(p4) and row["actual"] - p4 >= 10):
                info = df.loc[name]
                st_c = info["dk_status"] or "-"
                st_e = (edf.loc[name]["dk_status"] or "-") if (edf is not None and name in edf.index) else "n/a"
                gap = (info["proj"] - info["dk_avg"]) if (info["proj"] is not None and info["dk_avg"] is not None
                                                          and np.isfinite(info["proj"]) and np.isfinite(info["dk_avg"])) else None
                commit = "yes" if info["absence_commit"] else "no"
                hits.append((wk, label, row["name"], row["position"], row["salary"], row["actual"], row["pred"], p4, commit,
                             f"{st_c} / {st_e}", info["availability_status"] or "-", gap, f3["design"]))
                cheap = (row["salary"] or 0) < (5000 if f3["fmt"] == "classic" else 4500)
                news = info["absence_commit"] or (gap is not None and gap >= 2) or (st_c != st_e and st_e != "n/a")
                shape[(cheap, bool(news))] += 1
    for h in hits:
        L.append(f"| {h[0]} | {h[1]} | {h[2]} | {h[3]} | {int(h[4] or 0):,} | {h[5]:.1f} | {h[6]:.1f} | {_f(h[7], 1)} | {h[8]} | {h[9]} | "
                 f"{h[10]} | {_f(h[11], 1, True)} | {sc.design_tag('v4', 'classic' if 'classic' in h[1] else 'showdown', h[0])} |")
    tot = sum(shape.values())
    pos_count = defaultdict(int)
    for h in hits:
        pos_count[h[3]] += 1
    n_commit = sum(1 for h in hits if h[8] == "yes")
    n_status = sum(1 for h in hits if h[9].split(" / ")[0] != h[9].split(" / ")[1] and "n/a" not in h[9])
    n_gap = sum(1 for h in hits if h[11] is not None and h[11] >= 2)
    L += ["", f"{len(hits)} player-slates by position: " + ", ".join(f"{k} {v}" for k, v in sorted(pos_count.items()))
          + f". With an absence commit {n_commit}; DK status changed between early and canonical {n_status}; "
          f"ourProj at least 2 above DK average {n_gap}."]
    L += ["", f"Shape count over {tot} player-slates: cheap (< $5,000 Classic / < $4,500 Showdown) AND a news/role marker "
          f"(absence commit, ourProj at least 2 above DK average, or DK status changed between early and canonical) "
          f"{shape[(True, True)]}; cheap without a marker {shape[(True, False)]}; not cheap with a marker "
          f"{shape[(False, True)]}; neither {shape[(False, False)]}. The marker is a crude proxy: role growth that the "
          "projection did not absorb (e.g. Cooks/Ekeler on WAS@SF, ourProj close to DK average) does not show in it.", ""]

    # Q2 captain by position
    L += ["### Q2. Showdown captain share by position (sum of captain %, gate-class labels only)", "",
          "| wk | slate | position | actual | v3 | v4 | tag (v4) |", "|---:|---|---|---:|---:|---:|---|"]
    tot2 = defaultdict(lambda: [0.0, 0.0, 0.0])
    cpt = defaultdict(dict)
    for f in fr:
        if f["slot"] == "CPT" and f["version"] in (V3, V4):
            cpt[(f["week"], f["label"], f["slate"])][f["version"]] = f["frame"]
    for (wk, label, key), d in sorted(cpt.items()):
        a = d[V3].groupby("position")["actual"].sum()
        p3 = d[V3].groupby("position")["pred"].sum()
        p4 = d[V4].groupby("position")["pred"].sum() if V4 in d else pd.Series(dtype=float)
        for pos in a.index:
            L.append(f"| {wk} | {label} | {pos} | {a[pos]:.1f} | {p3.get(pos, 0):.1f} | {p4.get(pos, float('nan')):.1f} | "
                     f"{sc.design_tag('v4', 'showdown', wk)} |")
            t = tot2[(pos, "all")]
            t[0] += a[pos]; t[1] += p3.get(pos, 0); t[2] += p4.get(pos, 0)
            if wk >= 6:
                t = tot2[(pos, "week6")]
                t[0] += a[pos]; t[1] += p3.get(pos, 0); t[2] += p4.get(pos, 0)
    n_sd = len(cpt)
    L += ["", f"Totals over {n_sd} Showdown slates (sum of captain % across slates; divide by slates for a per-slate share):", "",
          "| position | scope | actual | v3 | v4 |", "|---|---|---:|---:|---:|"]
    for (pos, scope), t in sorted(tot2.items()):
        L.append(f"| {pos} | {scope} | {t[0]:.1f} | {t[1]:.1f} | {t[2]:.1f} |")
    L.append("")

    # Q3 kickers
    L += ["### Q3. Showdown kickers, flex ownership", "", "| wk | slate | kicker | salary | actual | v3 | v4 | tag (v4) |",
          "|---:|---|---|---:|---:|---:|---:|---|"]
    kd = []
    for f in fr:
        if f["slot"] == "FLEX" and f["version"] == V3 and f["fmt"] == "showdown":
            f4 = next((x for x in fr if x["slate"] == f["slate"] and x["slot"] == "FLEX" and x["version"] == V4), None)
            p4 = f4["frame"].set_index("normalized_name")["pred"] if f4 is not None else pd.Series(dtype=float)
            for _, r in f["frame"][f["frame"]["position"] == "K"].iterrows():
                kd.append((r["actual"], r["pred"], p4.get(r["normalized_name"], np.nan)))
                L.append(f"| {f['week']} | {f['label']} | {r['name']} | {int(r['salary']):,} | {r['actual']:.1f} | {r['pred']:.1f} | "
                         f"{_f(p4.get(r['normalized_name'], np.nan), 1)} | {sc.design_tag('v4', 'showdown', f['week'])} |")
    if kd:
        a = np.array(kd, float)
        L += ["", f"{len(kd)} kickers: mean actual {a[:, 0].mean():.1f}, v3 {a[:, 1].mean():.1f}, v4 {np.nanmean(a[:, 2]):.1f}; "
              f"MAE v3 {np.mean(np.abs(a[:, 1] - a[:, 0])):.1f}, v4 {np.nanmean(np.abs(a[:, 2] - a[:, 0])):.1f}.", ""]

    # Q4 concentration
    L += ["### Q4. Concentration: top-5 predicted vs everyone else (mean predicted − actual per player)", "",
          "| wk | slate | slot | version | top-5 pred sum | top-5 actual sum | top-5 mean bias | rest mean bias |",
          "|---:|---|---|---|---:|---:|---:|---:|"]
    sign = defaultdict(lambda: [0, 0])
    for r in sorted(ref_rows(result, selector="latest"), key=lambda r: (r["week"], r["slate_key"], r["slot"], r["forecast_version"])):
        if r["forecast_version"] not in (V3, V4):
            continue
        c = r["metrics"].get("concentration_top5") or {}
        if not c:
            continue
        L.append(f"| {r['week']} | {_slate_label(result, r['slate_key'])} | {r['slot']} | {_short(r['forecast_version'])} | "
                 f"{c['top_pred_sum']:.1f} | {c['top_actual_sum']:.1f} | {c['top_bias_mean']:+.2f} | {_f(c['rest_bias_mean'], 2, True)} |")
        s = sign[(r["format"], r["slot"], r["forecast_version"])]
        s[0] += 1
        s[1] += c["top_bias_mean"] > 0 and (c["rest_bias_mean"] or 0) < 0
    L += ["", "Slates where the top 5 were over-predicted AND the rest under-predicted on average:", ""]
    for (fm, slot, v), s in sorted(sign.items()):
        L.append(f"- {fm} {slot} {_short(v)}: {s[1]} of {s[0]}")
    L.append("")
    return L
