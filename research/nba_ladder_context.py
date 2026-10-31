"""Reference scores and minutes context for the stored NBA points-ladder grades (`nba-ladder-context-v1`).

Implements the "First delivery" of docs/nba-points-tail-validation-spec.md as a
read-only report over `nba_points_calibration`. It never regrades, rewrites a
probability, changes eligibility, or touches the registered ladder-v2 study.

Per (grade_version, ladder_version, rung), using exactly the rows that carry a
probability and a resolved outcome for that rung (legacy convention: `graded`
rows only; known `push` rows are counted as omitted):

  brier                  mean((p - y)^2)
  fitted_constant_brier  q(1 - q), q = the same rows' hit rate  ("same-sample constant":
                         fitted to the evaluation outcomes, a yardstick, not a forecast)
  descriptive_skill      1 - brier / fitted_constant_brier; null with constant_has_zero_loss
                         when every row agrees

Also: a prior-only constant per date ((prior_hits + 1) / (prior_n + 2) over strictly
earlier dates, at least 100 prior rows), reported as a RETROSPECTIVE reconstruction,
never prospective, because `graded_at` is restamped on every regrade and so cannot
prove when an outcome was first available; fixed main-line bands (<15, 15-20, 20-25,
25+); fixed reliability bins; and minutes strata kept separate from grading status:
played_under_15, played_15_plus, played_minutes_unknown, not_applicable.

    python -m research.nba_ladder_context                     # stored grade-v1 rows, all dates
    python -m research.nba_ladder_context --from 2026-10-20 --through 2026-10-30 --json
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import json
import math
from typing import Any, Iterable

VERSION = "nba-ladder-context-v1"
RUNGS: tuple[str, ...] = ("over", "20", "25", "30", "35", "40")
PROB_FIELD = {"over": "p_over", "20": "p20", "25": "p25", "30": "p30", "35": "p35", "40": "p40"}
LINE_BANDS: tuple[tuple[str, float, float], ...] = (("<15", -math.inf, 15.0), ("15-20", 15.0, 20.0), ("20-25", 20.0, 25.0), ("25+", 25.0, math.inf))
RELIABILITY_BINS: tuple[tuple[float, float], ...] = ((0.0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.35), (0.35, 0.50), (0.50, 0.75), (0.75, 1.0000001))
SHORT_MINUTES = 15.0
PRIOR_MIN_ROWS = 100
STRATA = ("played_under_15", "played_15_plus", "played_minutes_unknown")


def hit(row: dict[str, Any], rung: str) -> bool:
    pts = float(row["actual_points"])
    return pts > float(row["line"]) if rung == "over" else pts >= int(rung)


def scored(rows: Iterable[dict[str, Any]], rung: str) -> list[tuple[dict[str, Any], float, int]]:
    """Legacy cohort for one rung: graded rows with a probability and actual points."""
    f = PROB_FIELD[rung]
    return [(r, float(r[f]), 1 if hit(r, rung) else 0) for r in rows
            if r.get("status") == "graded" and r.get(f) is not None and r.get("actual_points") is not None]


def brier_reference(pairs: list[tuple[dict[str, Any], float, int]]) -> dict[str, Any]:
    n = len(pairs)
    if n == 0:
        return {"n": 0, "hits": 0, "meanStated": None, "observedRate": None, "brier": None,
                "fittedConstantBrier": None, "descriptiveSkill": None, "skillNote": None}
    hits = sum(y for _, _, y in pairs)
    q = hits / n
    brier = sum((p - y) ** 2 for _, p, y in pairs) / n
    const = q * (1 - q)
    skill, note = (None, "constant_has_zero_loss") if const == 0 else (1 - brier / const, None)
    return {"n": n, "hits": hits, "meanStated": sum(p for _, p, _ in pairs) / n, "observedRate": q, "brier": brier,
            "fittedConstantBrier": const, "descriptiveSkill": skill, "skillNote": note}


def prior_only_constant(pairs: list[tuple[dict[str, Any], float, int]]) -> dict[str, Any]:
    """Each row scored against (prior_hits+1)/(prior_n+2) from strictly earlier dates, when >= PRIOR_MIN_ROWS exist."""
    by_date: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for r, p, y in pairs:
        by_date[str(r["game_date"])].append((p, y))
    prior_n = prior_hits = 0
    covered = []
    first_value: dict[str, float] = {}
    for d in sorted(by_date):
        if prior_n >= PRIOR_MIN_ROWS:
            ref = (prior_hits + 1) / (prior_n + 2)
            first_value[d] = ref
            covered.extend((p, y, ref) for p, y in by_date[d])
        prior_n += len(by_date[d])
        prior_hits += sum(y for _, y in by_date[d])
    n = len(covered)
    return {"coveredRows": n, "uncoveredRows": len(pairs) - n,
            "brier": sum((p - y) ** 2 for p, y, _ in covered) / n if n else None,
            "referenceBrier": sum((ref - y) ** 2 for _, y, ref in covered) / n if n else None,
            "frozenValueByDate": first_value, "prospectiveEligible": False,
            "note": "Retrospective reconstruction: nba_points_calibration.graded_at is restamped on regrade, so it cannot "
                    "prove an outcome was available before a later date's recording."}


def line_bands(pairs: list[tuple[dict[str, Any], float, int]]) -> list[dict[str, Any]]:
    out = []
    for label, lo, hi in LINE_BANDS:
        cell = [(p, y) for r, p, y in pairs if lo <= float(r["line"]) < hi]
        n = len(cell)
        out.append({"band": label, "n": n, "meanStated": sum(p for p, _ in cell) / n if n else None,
                    "observedRate": sum(y for _, y in cell) / n if n else None})
    return out


def reliability(pairs: list[tuple[dict[str, Any], float, int]]) -> list[dict[str, Any]]:
    out = []
    for lo, hi in RELIABILITY_BINS:
        cell = [(p, y) for _, p, y in pairs if lo <= p < hi]
        n = len(cell)
        out.append({"bin": f"[{lo:.2f},{min(hi, 1.0):.2f}{']' if hi > 1 else ')'}", "n": n, "hits": sum(y for _, y in cell),
                    "meanStated": sum(p for p, _ in cell) / n if n else None, "observedRate": sum(y for _, y in cell) / n if n else None})
    return out


def minutes_stratum(row: dict[str, Any]) -> tuple[str, bool]:
    """(stratum, invalid_minutes_flag). Participation context only; grading status is untouched."""
    if row.get("status") not in ("graded", "push") or row.get("actual_points") is None:
        return "not_applicable", False
    m = row.get("minutes")
    if m is None:
        return "played_minutes_unknown", False
    try:
        m = float(m)
    except (TypeError, ValueError):
        return "played_minutes_unknown", True
    if not math.isfinite(m) or m < 0:
        return "played_minutes_unknown", True
    return ("played_under_15", False) if m < SHORT_MINUTES else ("played_15_plus", False)


def minutes_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = defaultdict(int)
    invalid = 0
    for r in rows:
        s, bad = minutes_stratum(r)
        counts[s] += 1
        invalid += bad
    by_stratum = {}
    for s in STRATA:
        members = [r for r in rows if minutes_stratum(r)[0] == s]
        by_stratum[s] = {rung: brier_reference(scored(members, rung)) for rung in RUNGS}
    played = sum(counts[s] for s in STRATA)
    return {"counts": {s: counts.get(s, 0) for s in (*STRATA, "not_applicable")}, "invalidMinutes": invalid,
            "reconciles": played + counts.get("not_applicable", 0) == len(rows), "byStratum": by_stratum,
            "label": "low-minute appearance, not an injury exit; the reason is unknown unless separately sourced"}


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    versions = sorted({(r["grade_version"], r["ladder_version"]) for r in rows})
    if len(versions) > 1:
        raise ValueError(f"mixed grade/ladder versions {versions}: report one version at a time")
    dates = sorted({str(r["game_date"]) for r in rows})
    statuses: dict[str, int] = defaultdict(int)
    for r in rows:
        statuses[r["status"]] += 1
    rungs = {}
    for rung in RUNGS:
        pairs = scored(rows, rung)
        pushes = sum(1 for r in rows if r.get("status") == "push" and r.get(PROB_FIELD[rung]) is not None)
        rungs[rung] = {"sameSample": brier_reference(pairs), "pushRowsOmitted": pushes, "priorOnly": prior_only_constant(pairs),
                       "lineBands": line_bands(pairs), "reliability": reliability(pairs)}
    return {"version": VERSION, "gradeVersion": versions[0][0] if versions else None, "ladderVersion": versions[0][1] if versions else None,
            "dates": {"from": dates[0] if dates else None, "through": dates[-1] if dates else None, "count": len(dates)},
            "rows": len(rows), "statuses": dict(statuses), "rungs": rungs, "minutes": minutes_summary(rows)}


def load_rows(grade_version: str, start: date | None, through: date | None) -> list[dict[str, Any]]:
    from config import load_config
    from db.database import DatabaseManager, fetch_rows

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT grade_version, ladder_version, game_date, event_id, player_key, status, line, actual_points, minutes,
                p_over, p20, p25, p30, p35, p40 FROM nba_points_calibration
            WHERE grade_version = %s AND (%s::date IS NULL OR game_date >= %s::date) AND (%s::date IS NULL OR game_date <= %s::date)
            ORDER BY game_date, event_id, player_key""", (grade_version, start, start, through, through))
        return fetch_rows(cur)


def _f(x: Any, pct: bool = False) -> str:
    return "-" if x is None else (f"{100 * x:.1f}%" if pct else f"{x:.5f}")


def print_report(rep: dict[str, Any]) -> None:
    d = rep["dates"]
    print(f"{rep['version']}: {rep['gradeVersion']} / {rep['ladderVersion']}, {d['from']}..{d['through']} ({d['count']} dates), "
          f"{rep['rows']} rows, statuses {rep['statuses']}")
    print(f"\n{'rung':<6}{'n':>6}{'hits':>6}{'stated':>9}{'observed':>10}{'Brier':>10}{'const':>10}{'skill':>9}"
          f"{'push omit':>10}{'prior n':>9}{'prior ref':>11}")
    for rung, x in rep["rungs"].items():
        s, po = x["sameSample"], x["priorOnly"]
        print(f"{rung:<6}{s['n']:>6}{s['hits']:>6}{_f(s['meanStated'], True):>9}{_f(s['observedRate'], True):>10}{_f(s['brier']):>10}"
              f"{_f(s['fittedConstantBrier']):>10}{_f(s['descriptiveSkill']):>9}{x['pushRowsOmitted']:>10}{po['coveredRows']:>9}"
              f"{_f(po['referenceBrier']):>11}")
    print("  const = same-sample constant (fitted to these outcomes); prior ref = prior-only constant, retrospective only.")
    m = rep["minutes"]
    print(f"\nMinutes: {m['counts']} (reconciles: {m['reconciles']}; invalid minutes {m['invalidMinutes']}). {m['label']}.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--grade-version", default="nba-points-grade-v1")
    ap.add_argument("--from", dest="start", type=date.fromisoformat)
    ap.add_argument("--through", type=date.fromisoformat)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    rep = build_report(load_rows(args.grade_version, args.start, args.through))
    if args.json:
        print(json.dumps(rep, indent=2, default=str))
    else:
        print_report(rep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
