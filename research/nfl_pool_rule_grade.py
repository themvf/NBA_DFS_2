"""Grade the pre-registered paired NFL DFS studies from saved research arms (read-only).

Studies (docs/nfl-classic-te-wr-pool-rule-study.md, docs/nfl-showdown-kicker-rule-study.md,
docs/nfl-portfolio-objective-study.md): each saved pair is two `research_*`
optimizer runs for one slate. For every pair this reads the flagship contest
linked to the slate (score curve), the week's actual DraftKings points, and
computes the registered per-slate metrics for each arm. At a milestone (8, then
16 slates in a format) it computes the paired statistics and names the verdict
the registration defines; before that it reports descriptively and says so.

Scoring follows the attribution studies: a player's points come from the
contest's own stored numbers, then the weekly results table; a player found in
neither leaves the lineup UNSCORED and is counted, never summed as 0.

    python -m research.nfl_pool_rule_grade [--study classic|showdown|portfolio|all] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_dk_pool_match import normalize_name
from research.nfl_pool_rule_study import STUDIES
from research.nfl_showdown_attribution_export import contest_points

ROOT = Path(__file__).resolve().parents[1]
MILESTONES = (8, 16)
BOOTSTRAP = 4000


def curve_at(curve: list, fraction: float, entries: int) -> float:
    rank = max(1, math.ceil(fraction * entries))
    best = None
    for r, score in curve:
        if r <= rank:
            best = score
    return float(best if best is not None else curve[0][1])


def rank_of(curve: list, score: float) -> int:
    """Rank a score would take in the stored field (entries strictly above it, plus one)."""
    above = 0
    for r, s in curve:
        if s > score:
            above = r
    return above + 1


def results_by_name(db, season: int, week: int) -> dict[str, float]:
    out = {}
    for r in db.execute(
            """SELECT DISTINCT ON (r.player_id) f.canonical_name, r.actual_dk_fpts FROM nfl_dfs_player_week_results r
               JOIN ff_players f ON f.id = r.player_id WHERE r.season=%s AND r.week=%s AND r.actual_dk_fpts IS NOT NULL
               ORDER BY r.player_id, r.scoring_version DESC, r.computed_at DESC""", (season, week)):
        out.setdefault(normalize_name(r["canonical_name"]), float(r["actual_dk_fpts"]))
    return out


def load_pairs(db, version: str) -> list[dict]:
    rows = db.execute(
        """SELECT o.run_id, o.upload_id, o.status, o.generated_lineups, o.requested_lineups, o.settings, o.created_at,
                  u.slate_signature, u.format, u.games
           FROM nfl_dfs_optimizer_runs o JOIN nfl_dfs_slate_uploads u ON u.upload_id=o.upload_id
           WHERE o.settings->'researchStudy'->>'version' = %s ORDER BY o.created_at""", (version,))
    pairs: dict[str, dict] = {}
    for r in rows:
        study = r["settings"]["researchStudy"]
        p = pairs.setdefault(study["pairId"], {"pairId": study["pairId"], "season": study["season"], "week": study["week"],
                                               "format": r["format"], "games": r["games"], "upload_id": r["upload_id"],
                                               "slate_signature": r["slate_signature"], "firstKickoff": study.get("firstKickoff"), "arms": {}})
        lineups = db.execute("SELECT lineup_number, player_ids, slots FROM nfl_dfs_lineups WHERE run_id=%s ORDER BY lineup_number", (r["run_id"],))
        p["arms"][study["arm"]] = {"run_id": str(r["run_id"]), "status": r["status"], "created_at": r["created_at"],
                                   "requested": r["requested_lineups"], "built": r["generated_lineups"], "study": study,
                                   "lineups": [{"n": l["lineup_number"], "ids": l["player_ids"], "slots": l["slots"]} for l in lineups]}
    return [p for p in pairs.values() if set(p["arms"]) == {"A", "B"}]


def contest_for(db, pair: dict) -> dict | None:
    """The flagship contest of this slate: largest stored contest with a curve and top entries, same slate signature."""
    rows = db.execute(
        """SELECT c.contest_id, c.entry_count, c.score_curve, c.winning_score, c.median_score,
                  (SELECT count(*) FROM nfl_dfs_field_top_entries t WHERE t.contest_id=c.contest_id) AS top
           FROM nfl_dfs_field_contests c JOIN nfl_dfs_slate_uploads u ON u.upload_id=c.slate_upload_id
           WHERE u.slate_signature=%s AND c.score_curve IS NOT NULL ORDER BY c.entry_count DESC""", (pair["slate_signature"],))
    rows = [r for r in rows if r["top"] > 0] or list(rows)
    if not rows:
        return None
    c = dict(rows[0])
    c["curve"] = c["score_curve"] if isinstance(c["score_curve"], list) else json.loads(c["score_curve"])
    c["top1"] = curve_at(c["curve"], 0.01, c["entry_count"])
    c["top01"] = curve_at(c["curve"], 0.001, c["entry_count"])
    c["all_contest_ids"] = [r["contest_id"] for r in db.execute(
        "SELECT c.contest_id FROM nfl_dfs_field_contests c JOIN nfl_dfs_slate_uploads u ON u.upload_id=c.slate_upload_id WHERE u.slate_signature=%s",
        (pair["slate_signature"],))]
    return c


def score_arm(arm: dict, points: dict[str, float], contest: dict, positions: dict[str, str]) -> dict:
    scored, unscored = [], 0
    kicker, excluded_use = 0, 0
    rule_excluded = {e["dkPlayerId"] for e in arm["study"].get("ruleExclusions") or []}
    for l in arm["lineups"]:
        total, ok = 0.0, True
        for s in l["slots"]:
            pts = points.get(normalize_name(s.get("name") or ""))
            if pts is None:
                ok = False
                break
            total += pts * (1.5 if s.get("slot") == "CPT" else 1)
        if ok:
            scored.append(round(total, 2))
        else:
            unscored += 1
        if any(positions.get(normalize_name(s.get("name") or "")) == "K" for s in l["slots"]):
            kicker += 1
        if any(s.get("dkPlayerId") in rule_excluded for s in l["slots"]):
            excluded_use += 1
    out = {"run_id": arm["run_id"], "status": arm["status"], "requested": arm["requested"], "built": arm["built"],
           "scored": len(scored), "unscored": unscored, "kickerLineups": kicker, "ruleExcludedLineups": excluded_use}
    if scored:
        out.update({"median": statistics.median(scored), "best": max(scored),
                    "top1Pct": sum(s >= contest["top1"] for s in scored), "top01Pct": sum(s >= contest["top01"] for s in scored),
                    "bestRank": rank_of(contest["curve"], max(scored))})
    fc = (arm["study"].get("forecast") or {}).get("successTop1Pct")
    if fc is not None and scored:
        event = 1.0 if out["top1Pct"] > 0 else 0.0
        out.update({"forecastTop1Pct": fc, "eventTop1Pct": event, "brier": round((fc - event) ** 2, 4)})
    return out


def top_entries(db, contest_id: str, positions: dict[str, str], rule_excluded: set[int], slate_names: dict[str, int]) -> dict:
    rows = db.execute("SELECT entry_id, players FROM nfl_dfs_field_top_entries WHERE contest_id=%s", (contest_id,))
    n = len(rows)
    with_k = with_excluded = 0
    for r in rows:
        players = r["players"] if isinstance(r["players"], list) else json.loads(r["players"] or "[]")
        names = [normalize_name(p.get("name") or "") for p in players]
        with_k += any(positions.get(nm) == "K" for nm in names)
        with_excluded += any(slate_names.get(nm) in rule_excluded for nm in names)
    return {"entries": n, "withKicker": with_k, "withRuleExcluded": with_excluded}


def paired_summary(slates: list[dict], key: str) -> dict:
    diffs = [s["B"][key] - s["A"][key] for s in slates if key in s["A"] and key in s["B"]]
    if not diffs:
        return {"n": 0}
    rng = random.Random(20261014)
    means = []
    for _ in range(BOOTSTRAP):
        draw = [diffs[rng.randrange(len(diffs))] for _ in diffs]
        means.append(sum(draw) / len(draw))
    means.sort()
    return {"n": len(diffs), "mean": round(sum(diffs) / len(diffs), 3), "positive": sum(d > 0 for d in diffs),
            "ci95": [round(means[int(0.025 * BOOTSTRAP)], 3), round(means[int(0.975 * BOOTSTRAP)], 3)], "diffs": diffs}


def verdict(study: str, slates: list[dict]) -> dict:
    n = len(slates)
    milestone = max((m for m in MILESTONES if n >= m), default=None)
    if milestone is None:
        return {"status": "no verdict", "reason": f"{n} graded slates; first look at {MILESTONES[0]}", "milestone": None}
    looked = slates[:milestone]
    med = paired_summary(looked, "median")
    top1 = paired_summary(looked, "top1Pct")
    total_a = sum(s["A"].get("top1Pct", 0) for s in looked)
    total_b = sum(s["B"].get("top1Pct", 0) for s in looked)
    if study == "portfolio":
        supported = total_b > total_a and top1["positive"] >= 6 and med["ci95"][0] >= 0
        rejected = total_b < total_a or med["ci95"][1] < 0
    else:
        supported = med["positive"] >= 6 and med["ci95"][0] > 0 and total_b >= total_a
        rejected = med["ci95"][1] < 0
    status = "B supported" if supported else "B rejected" if rejected else "inconclusive"
    return {"status": status, "milestone": milestone, "medianPaired": med, "top1Paired": top1,
            "top1Total": {"A": total_a, "B": total_b}, "lookedAt": [s["pairId"] for s in looked]}


def grade(db, study: str) -> dict:
    version = STUDIES[study]["version"]
    pairs = load_pairs(db, version)
    out = {"study": study, "version": version, "gradedAt": date.today().isoformat(), "pairs": len(pairs), "formats": {}}
    by_format: dict[str, list] = defaultdict(list)
    for pair in sorted(pairs, key=lambda p: (p["season"], p["week"], p["firstKickoff"] or "")):
        contest = contest_for(db, pair)
        row = {"pairId": pair["pairId"], "season": pair["season"], "week": pair["week"], "games": pair["games"], "format": pair["format"]}
        if contest is None:
            row["skipped"] = "no stored flagship contest with a score curve for this slate"
            by_format[pair["format"]].append(row)
            continue
        slate_players = {normalize_name(r["name"]): (r["dk_player_id"], r["position"]) for r in db.execute(
            "SELECT dk_player_id, name, position FROM nfl_dfs_slate_players WHERE upload_id=%s", (pair["upload_id"],))}
        positions = {k: v[1] for k, v in slate_players.items()}
        slate_names = {k: v[0] for k, v in slate_players.items()}
        points = contest_points(db, contest["all_contest_ids"])
        for k, v in results_by_name(db, pair["season"], pair["week"]).items():
            points.setdefault(k, v)
        row["contest"] = {"id": contest["contest_id"], "entries": contest["entry_count"], "top1": contest["top1"], "top01": contest["top01"],
                          "winning": float(contest["winning_score"]), "median": float(contest["median_score"])}
        for arm in ("A", "B"):
            row[arm] = score_arm(pair["arms"][arm], points, contest, positions)
        rule_excluded = {e["dkPlayerId"] for e in pair["arms"]["A"]["study"].get("ruleExclusions") or []}
        row["topEntries"] = top_entries(db, contest["contest_id"], positions, rule_excluded, slate_names)
        if study == "portfolio":
            row["fieldModelCheck"] = {"impliedTop1": (pair["arms"]["B"]["study"].get("impliedField") or {}).get("top1Pct", {}).get("p50"),
                                      "realizedTop1": contest["top1"]}
        by_format[pair["format"]].append(row)
    for fmt, rows in by_format.items():
        graded = [r for r in rows if "A" in r and "scored" in r["A"] and r["A"]["scored"] and r["B"]["scored"]]
        out["formats"][fmt] = {"slates": rows, "graded": len(graded), "verdict": verdict(study, graded)}
    return out


def render(report: dict) -> str:
    lines = [f"# {report['study']} ({report['version']}) graded {report['gradedAt']}", ""]
    for fmt, block in report["formats"].items():
        v = block["verdict"]
        lines += [f"## {fmt}: {block['graded']} graded slates, verdict: {v['status']}" + (f" ({v.get('reason')})" if v.get("reason") else ""), "",
                  "| Week | Slate | Contest | Arm | Built | Scored | Median | Best (rank) | Top 1% | Top 0.1% | Forecast | Brier | K lineups | Rule-excl. lineups |",
                  "|---:|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|"]
        for r in block["slates"]:
            if "skipped" in r:
                lines.append(f"| {r['week']} | {','.join(r['games'])} | - | - | - | - | - | - | - | - | - | - | - | skipped: {r['skipped']} |")
                continue
            for arm in ("A", "B"):
                a = r[arm]
                lines.append(f"| {r['week']} | {','.join(r['games'])[:30]} | {r['contest']['id']} | {arm} | {a['built']}/{a['requested']} | {a['scored']} | "
                             f"{a.get('median', '-')} | {a.get('best', '-')} ({a.get('bestRank', '-')}) | {a.get('top1Pct', '-')} | {a.get('top01Pct', '-')} | "
                             f"{a.get('forecastTop1Pct', '-')} | {a.get('brier', '-')} | {a['kickerLineups']} | {a['ruleExcludedLineups']} |")
            te = r["topEntries"]
            lines.append(f"| | | top-100 entries | field | {te['entries']} | | | | | | | | {te['withKicker']} | {te['withRuleExcluded']} |")
        if v.get("milestone"):
            lines += ["", f"Milestone {v['milestone']}: median B-A mean {v['medianPaired']['mean']} CI {v['medianPaired']['ci95']}, positive {v['medianPaired']['positive']}/{v['medianPaired']['n']}; "
                      f"top-1% totals A {v['top1Total']['A']} vs B {v['top1Total']['B']}."]
        lines.append("")
    lines.append("Unscored lineups contain a player with no recorded score and are excluded from the metrics, never summed as 0. Before a milestone the tables are descriptive.")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--study", choices=[*sorted(STUDIES), "all"], default="all")
    ap.add_argument("--out", type=Path, default=ROOT / "artifacts" / "nfl-pool-rule-grades" / date.today().isoformat())
    args = ap.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    args.out.mkdir(parents=True, exist_ok=True)
    for study in (sorted(STUDIES) if args.study == "all" else [args.study]):
        report = grade(db, study)
        (args.out / f"{study}.json").write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
        text = render(report)
        (args.out / f"{study}.md").write_text(text, encoding="utf-8")
        print(text)


if __name__ == "__main__":
    main()
