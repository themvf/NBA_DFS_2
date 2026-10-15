"""Empirical field baseline from the stored DraftKings flagship contests (read-only).

For every stored contest with a score curve: the realized winning, top-0.1%,
top-1%, top-20% and median scores; the payout table reconstructed from the
top users' lineups (stat-api gives each of their entries' rank and payout, so
the brackets are observed at the ranks those entries occupied and nowhere
else); and what roster coverage exists. Writes
`artifacts/nfl-field-baseline/<date>/baseline.json` and `baseline.md`.

What this is and is not, stated once here and in the output:
- The score curves are complete (every entry's score), so threshold and payout
  EVALUATION of a lineup is exact; the payout table is complete only at the
  observed ranks and is stepped between them (DraftKings brackets are
  piecewise constant, so a step between two observed ranks in the same
  bracket is exact; a step across an unobserved bracket edge is not).
- Rosters exist only for the top-100 entries and the top users' builds: they
  are selected on success and describe winning constructions, never the
  field's unconditional lineup process. Nothing here fits a field model to
  them.

    python -m research.nfl_field_baseline
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

from config import load_config
from db.database import DatabaseManager

ROOT = Path(__file__).resolve().parents[1]
FRACTIONS = {"top01Pct": 0.001, "top1Pct": 0.01, "top20Pct": 0.2}


def curve_at(curve: list, fraction: float, entries: int) -> float:
    rank = max(1, math.ceil(fraction * entries))
    best = None
    for r, score in curve:
        if r <= rank:
            best = score
    return float(best if best is not None else curve[0][1])


def payout_table(db, contest_id: str) -> dict:
    """Observed (rank, payout) pairs from the top users' entries, collapsed to brackets."""
    rows = db.execute("""SELECT (l->>'rank')::int AS rank, (l->>'payout')::float AS payout
                         FROM nfl_dfs_field_user_builds b, jsonb_array_elements(b.lineups) l
                         WHERE b.contest_id=%s AND l->>'rank' IS NOT NULL""", (contest_id,))
    seen = sorted({(r["rank"], r["payout"]) for r in rows})
    brackets, last = [], None
    for rank, payout in seen:
        if last is not None and last["payout"] == payout:
            last["rankTo"] = rank
            last["observedRanks"] += 1
        else:
            last = {"rankFrom": rank, "rankTo": rank, "payout": payout, "observedRanks": 1}
            brackets.append(last)
    paid = [b for b in brackets if b["payout"] > 0]
    return {"brackets": brackets, "observedRanks": len(seen), "lastPaidObservedRank": max((b["rankTo"] for b in paid), default=None),
            "minObservedPayout": min((b["payout"] for b in paid), default=None),
            "note": "Brackets are observed only at ranks the top users' entries occupied; a payout between two observed ranks "
                    "with the same amount is exact (DraftKings brackets are constant), an unobserved bracket edge is not."}


def payout_for_scores(scores: list[float], curve: list, entries: int, brackets: list[dict]) -> dict:
    """Tie-aware payout of lineups with these scores entered into the stored field (research evaluation).

    The curve lists (rank, score) at 300 points; between points the field's score is taken as the
    next listed one (a step), which is exact for the ranks listed and conservative between them.
    """
    def payout_at(rank: int) -> float | None:
        for b in brackets:
            if b["rankFrom"] <= rank <= b["rankTo"]:
                return b["payout"]
        return None
    total, detail, unknown = 0.0, [], 0
    for s in scores:
        above = 0
        tied = 0
        prev_rank = 0
        for rank, score in curve:
            if score > s:
                above = rank
            elif score == s:
                tied += rank - prev_rank
            prev_rank = rank
        first, last = above + 1, above + max(1, tied) + (1 if tied else 0)
        pool, missing = 0.0, False
        for r in range(first, last + 1):
            p = payout_at(r)
            if p is None:
                missing = True
                break
            pool += p
        share = None if missing else pool / (last - first + 1)
        unknown += missing
        total += share or 0
        detail.append({"score": s, "rankFrom": first, "rankTo": last, "payout": share})
    return {"total": round(total, 2), "lineups": detail, "unknownBracket": unknown}


def main() -> None:
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out_dir = ROOT / "artifacts" / "nfl-field-baseline" / date.today().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = db.execute("""SELECT c.contest_id, c.contest_name, c.season, c.week, c.format, c.entry_count, c.winning_score, c.median_score,
                                c.score_curve, c.slate_upload_id,
                                (SELECT count(*) FROM nfl_dfs_field_top_entries t WHERE t.contest_id=c.contest_id) AS top_entries,
                                (SELECT count(*) FROM nfl_dfs_field_user_builds b WHERE b.contest_id=c.contest_id) AS top_users,
                                (SELECT coalesce(sum(jsonb_array_length(b.lineups)),0) FROM nfl_dfs_field_user_builds b WHERE b.contest_id=c.contest_id) AS user_lineups
                         FROM nfl_dfs_field_contests c WHERE c.score_curve IS NOT NULL ORDER BY c.season NULLS LAST, c.week, c.format, c.contest_id""")
    contests = []
    for r in rows:
        curve = r["score_curve"] if isinstance(r["score_curve"], list) else json.loads(r["score_curve"])
        n = r["entry_count"]
        thresholds = {k: curve_at(curve, f, n) for k, f in FRACTIONS.items()}
        table = payout_table(db, r["contest_id"])
        contests.append({"contestId": r["contest_id"], "name": r["contest_name"], "season": r["season"], "week": r["week"],
                         "format": r["format"], "entries": n, "linkedSlate": r["slate_upload_id"] is not None,
                         "realized": {"winning": float(r["winning_score"]), **thresholds, "median": float(r["median_score"])},
                         "coverage": {"scoreCurvePoints": len(curve), "topEntriesWithRosters": r["top_entries"],
                                      "topUsersWithBuilds": r["top_users"], "topUserLineups": r["user_lineups"],
                                      "rosterShareOfField": round(r["user_lineups"] / n, 4) if n else None},
                         "payoutTable": table})
    by_format = {}
    for fmt in ("classic", "showdown"):
        big = [c for c in contests if c["format"] == fmt and c["season"] and c["entries"] >= 10000 and c["coverage"]["topEntriesWithRosters"]]
        vals = {k: [c["realized"][k] for c in big] for k in ("winning", "top01Pct", "top1Pct", "median")}
        sd = lambda xs: round((sum((x - sum(xs) / len(xs)) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5, 2) if len(xs) > 1 else None
        by_format[fmt] = {"flagships": len(big), **{k: {"values": v, "mean": round(sum(v) / len(v), 2) if v else None, "sd": sd(v)} for k, v in vals.items()}}
    result = {"version": "nfl-field-baseline-v1", "computedAt": date.today().isoformat(), "contests": contests, "byFormat": by_format,
              "limits": ["Score curves are complete; payout tables are observed only at ranks the top users' entries occupied.",
                         "Rosters exist for top finishers only and are outcome-selected; they are not used to fit a field model.",
                         "Flagship spreads use contests with >= 10,000 entries and stored top entries (the stat-api flagships)."]}
    (out_dir / "baseline.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    lines = ["# NFL DFS field baseline (stored flagship contests)", "", f"Computed {result['computedAt']} from {len(contests)} stored contests. "
             "Score curves are complete; payout tables are observed only where top users' entries landed; rosters are top finishers only (outcome-selected).", "",
             "| Week | Format | Contest | Entries | Win | Top 0.1% | Top 1% | Top 20% | Median | Last paid rank seen | Rosters (top entries / user lineups) |",
             "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for c in contests:
        rz, cv, pt = c["realized"], c["coverage"], c["payoutTable"]
        lines.append(f"| {c['week'] or '-'} | {c['format']} | {c['contestId']} | {c['entries']:,} | {rz['winning']} | {rz['top01Pct']} | {rz['top1Pct']} | {rz['top20Pct']} | {rz['median']} | {pt['lastPaidObservedRank'] or '-'} | {cv['topEntriesWithRosters']} / {cv['topUserLineups']} |")
    lines += ["", "## Week-to-week spread of realized thresholds (flagships)", "", "| Format | n | Top 1% mean | Top 1% SD | Top 0.1% mean | Top 0.1% SD | Winning mean | Winning SD | Median mean | Median SD |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for fmt, b in by_format.items():
        lines.append(f"| {fmt} | {b['flagships']} | {b['top1Pct']['mean']} | {b['top1Pct']['sd']} | {b['top01Pct']['mean']} | {b['top01Pct']['sd']} | {b['winning']['mean']} | {b['winning']['sd']} | {b['median']['mean']} | {b['median']['sd']} |")
    (out_dir / "baseline.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[-6:]))
    print(f"wrote {out_dir}")


if __name__ == "__main__":
    main()
