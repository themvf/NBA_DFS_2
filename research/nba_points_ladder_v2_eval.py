"""Evaluator for the pre-registered NBA points ladder v2 (power de-vig) study.

Implements ``docs/nba-points-ladder-v2-registration.md`` exactly; nothing here
changes it. Read-only: it SELECTs from ``prop_odds_history``,
``nba_box_score_games``/``nba_box_score_players`` and ``nba_points_calibration``
and writes nothing.

What a run does:

1. For every Eastern date from 2026-10-27 (the discovery nights 10-20..10-26 are
   excluded entirely) through the latest night the nightly grade has stored, it
   re-reads each game's last pre-tip capture (main ``player_points`` plus
   ``player_points_alternate``) with BOTH readers, ``nba-points-ladder-v1``
   (proportional) and the candidate ``nba-points-ladder-v2`` (power, one-sided
   rungs only), and grades them against the box score with
   ``model/nba_points_calibration.py``'s own matching and status rules. Only
   ``graded`` rows count; short nights stay in.
2. Integrity check, before any number: the recomputed v1 reading must equal
   the stored ``nba-points-grade-v1`` row for every graded player-game (line,
   P(over), floor, ceiling, P(20+)..P(40+), to 1e-9), in both directions. A
   mismatch stops the run: it is a reader bug, not a result.
3. Floors: at least 30 prospective nights and at least 1,000 graded
   player-games carrying a 35+ value under both readers, counting nights no
   later than the 2027-04-12 deadline.
4. Primary metric (mean paired Brier v2 - v1 over 25+/30+/35+/40+ pairs where
   both readers have a value; 95% interval from a bootstrap over whole nights,
   10,000 resamples, seed 20261026; PASS iff the upper bound is below 0) is
   WITHHELD until both floors are met. Below the floors only the floor status is
   printed for it. "Computed once, when both floors are met, and frozen" is
   implemented as: the verdict is computed on the nights from 2026-10-27 through
   the FIRST night at which both floors are met, so a later rerun reproduces
   the same verdict instead of a new look on more data.
5. Secondaries, gating nothing (the registration's "reported always"): per-rung
   stated vs happened and Brier for both readers, floor/ceiling coverage,
   skill against the line-only reference (the panel's leave-one-night-out
   guess), one-sided and skipped rung counts, and ``k`` by book.

Usage:
    python -m research.nba_points_ladder_v2_eval            # floor status + secondaries
    python -m research.nba_points_ladder_v2_eval --json     # same, as JSON
    python -m research.nba_points_ladder_v2_eval --check-integrity-from 2026-10-20
        # integrity check ONLY (v1 recompute vs stored rows) from that date; prints
        # match counts, never a v2 number or an outcome. For checking the machinery.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
from typing import Any

from model.nba_points_calibration import (
    GRADE_VERSION,
    SHORT_MINUTES,
    GradeRow,
    grade_captures,
    load_box_scores,
    load_pretip_captures,
)
from model.nba_points_ladder import LADDER_V2_VERSION, LADDER_VERSION, THRESHOLDS, name_key, read_player

STUDY = "nba-points-ladder-v2-registration"
REGISTRATION = "docs/nba-points-ladder-v2-registration.md"
START_DATE = date(2026, 10, 27)
DEADLINE = date(2027, 4, 12)
MIN_NIGHTS = 30
MIN_ROWS_35 = 1000
PRIMARY_RUNGS: tuple[int, ...] = (25, 30, 35, 40)
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20261026
INTEGRITY_TOL = 1e-9
# The panel's line-only reference (web/src/lib/nba-points-calibration-charts.ts).
LINE_BAND = 3
BASELINE_PRIOR_ROWS = 10
STORED_FIELDS = ("line", "p_over", "floor", "ceiling", "p20", "p25", "p30", "p35", "p40")
READERS = ("v1", "v2")


class FloorsNotMet(RuntimeError):
    """The primary metric was asked for before both registered floors were met."""


@dataclass
class EvalRow:
    """One graded player-game read by both readers from the same capture."""
    game_date: date
    event_id: str
    player_key: str
    line: float
    actual: float
    minutes: float | None
    v1: GradeRow
    v2: GradeRow
    one_sided: dict[str, int]
    skipped: dict[str, int]
    book_k: dict[str, float | None]

    def stated(self, reader: str, rung: str) -> float | None:
        row = self.v1 if reader == "v1" else self.v2
        return row.p_over if rung == "over" else row.at_least.get(int(rung))

    def hit(self, rung: str) -> bool:
        return self.actual > self.line if rung == "over" else self.actual >= int(rung)


# ---------------------------------------------------------------- pure: rows


def pair_rows(v1_rows: list[GradeRow], v2_rows: list[GradeRow],
              counters: dict[tuple[str, str], dict[str, Any]] | None = None) -> list[EvalRow]:
    """Graded rows read by both readers. The two lists come from the same captures and box scores,
    so their keys and statuses must agree; anything else is a bug and raises."""
    by_key = {(r.event_id, r.player_key): r for r in v2_rows}
    if len(by_key) != len(v2_rows) or len({(r.event_id, r.player_key) for r in v1_rows}) != len(v1_rows):
        raise ValueError("duplicate (event, player) rows in a grade")
    if set(by_key) != {(r.event_id, r.player_key) for r in v1_rows}:
        raise ValueError("v1 and v2 graded different player-games from the same captures")
    out: list[EvalRow] = []
    for a in v1_rows:
        b = by_key[(a.event_id, a.player_key)]
        if a.status != b.status or a.line != b.line or a.actual_points != b.actual_points:
            raise ValueError(f"v1/v2 disagree on status or line for {a.event_id}/{a.player_key}")
        if a.status != "graded" or a.actual_points is None:
            continue
        c = (counters or {}).get((a.event_id, a.player_key), {})
        out.append(EvalRow(
            game_date=a.game_date, event_id=a.event_id, player_key=a.player_key, line=a.line,
            actual=a.actual_points, minutes=a.minutes, v1=a, v2=b,
            one_sided=dict(c.get("one_sided", {"v1": 0, "v2": 0})),
            skipped=dict(c.get("skipped", {"v1": 0, "v2": 0})),
            book_k=dict(c.get("book_k", {})),
        ))
    return out


def _close(a: float | None, b: float | None, tol: float = INTEGRITY_TOL) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=0, abs_tol=tol)


def integrity_check(recomputed: list[GradeRow], stored: list[dict[str, Any]]) -> list[str]:
    """Every graded recomputed v1 row must equal its stored v1 row, and every stored graded row
    must be recomputed as graded. Returns one message per mismatch; empty means pass."""
    stored_by_key = {(str(s["event_id"]), str(s["player_key"])): s for s in stored}
    mismatches: list[str] = []
    graded_keys: set[tuple[str, str]] = set()
    for r in recomputed:
        if r.status != "graded":
            continue
        key = (r.event_id, r.player_key)
        graded_keys.add(key)
        s = stored_by_key.get(key)
        if s is None:
            mismatches.append(f"{r.game_date} {key}: graded on recompute, no stored v1 row")
            continue
        if s["status"] != "graded":
            mismatches.append(f"{r.game_date} {key}: graded on recompute, stored status {s['status']}")
            continue
        mine = {"line": r.line, "p_over": r.p_over, "floor": r.floor, "ceiling": r.ceiling,
                **{f"p{n}": r.at_least.get(n) for n in THRESHOLDS}}
        for f in STORED_FIELDS:
            if not _close(mine[f], s[f]):
                mismatches.append(f"{r.game_date} {key}: {f} recomputed {mine[f]!r} vs stored {s[f]!r}")
        if not _close(r.actual_points, s.get("actual_points")):
            mismatches.append(f"{r.game_date} {key}: actual {r.actual_points!r} vs stored {s.get('actual_points')!r}")
    for key, s in stored_by_key.items():
        if s["status"] == "graded" and key not in graded_keys:
            mismatches.append(f"{s.get('game_date')} {key}: stored graded, not graded on recompute")
    return mismatches


# ---------------------------------------------------------------- pure: floors


def _has_both(row: EvalRow, n: int) -> bool:
    return row.v1.at_least.get(n) is not None and row.v2.at_least.get(n) is not None


def floor_status(rows: list[EvalRow], *, today: date | None = None) -> dict[str, Any]:
    """Cumulative nights and 35+ rows (nights after the deadline never count), and the first night
    at which both floors are met, which closes the verdict population."""
    eligible = [r for r in rows if START_DATE <= r.game_date <= DEADLINE]
    nights = sorted({r.game_date for r in eligible})
    per_night_35: dict[date, int] = defaultdict(int)
    for r in eligible:
        if _has_both(r, 35):
            per_night_35[r.game_date] += 1
    cutoff: date | None = None
    cum = 0
    for i, night in enumerate(nights, start=1):
        cum += per_night_35[night]
        if i >= MIN_NIGHTS and cum >= MIN_ROWS_35:
            cutoff = night
            break
    total_35 = sum(per_night_35.values())
    met = cutoff is not None
    today = today or datetime.now(timezone.utc).date()
    state = "met" if met else ("inconclusive" if today > DEADLINE else "not_met")
    return {
        "nights": len(nights), "minNights": MIN_NIGHTS, "nightsMet": len(nights) >= MIN_NIGHTS,
        "rows35Both": total_35, "minRows35": MIN_ROWS_35, "rows35Met": total_35 >= MIN_ROWS_35,
        "firstNight": nights[0].isoformat() if nights else None,
        "lastNight": nights[-1].isoformat() if nights else None,
        "deadline": DEADLINE.isoformat(),
        "verdictCutoffNight": cutoff.isoformat() if cutoff else None,
        "state": state,
    }


def verdict_population(rows: list[EvalRow], status: dict[str, Any]) -> list[EvalRow]:
    """The rows the one-time verdict is computed on: start through the first night both floors were met."""
    if status["verdictCutoffNight"] is None:
        raise ValueError("floors not met: the verdict population does not exist yet")
    cutoff = date.fromisoformat(status["verdictCutoffNight"])
    return [r for r in rows if START_DATE <= r.game_date <= cutoff]


# ---------------------------------------------------------------- pure: primary metric


def paired_brier_diffs(rows: list[EvalRow]) -> dict[date, list[float]]:
    """Brier_v2 - Brier_v1 for every (graded row, rung) pair in 25+/30+/35+/40+ where both readers
    have a value, grouped by night."""
    out: dict[date, list[float]] = defaultdict(list)
    for r in rows:
        for n in PRIMARY_RUNGS:
            if not _has_both(r, n):
                continue
            y = 1.0 if r.actual >= n else 0.0
            p1 = r.v1.at_least[n]
            p2 = r.v2.at_least[n]
            out[r.game_date].append((p2 - y) ** 2 - (p1 - y) ** 2)  # type: ignore[operator]
    return dict(out)


def night_bootstrap(per_night: dict[Any, list[float]], *, resamples: int = BOOTSTRAP_RESAMPLES,
                    seed: int = BOOTSTRAP_SEED) -> dict[str, Any]:
    """Pooled mean over pairs and its 95% percentile interval from resampling whole nights."""
    import numpy as np

    keys = sorted(per_night)
    if not keys:
        raise ValueError("no paired values")
    sums = np.array([sum(per_night[k]) for k in keys], dtype=float)
    counts = np.array([len(per_night[k]) for k in keys], dtype=float)
    mean = float(sums.sum() / counts.sum())
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(keys), size=(resamples, len(keys)))
    stats = sums[idx].sum(axis=1) / counts[idx].sum(axis=1)
    lo, hi = np.percentile(stats, [2.5, 97.5])
    return {"mean": mean, "lower": float(lo), "upper": float(hi), "pairs": int(counts.sum()),
            "nights": len(keys), "resamples": resamples, "seed": seed}


def primary_metric(rows: list[EvalRow], status: dict[str, Any]) -> dict[str, Any]:
    """The registered decision. Refuses to run unless both floors are met."""
    if status.get("state") != "met":
        raise FloorsNotMet("primary metric withheld: both floors must be met first")
    population = verdict_population(rows, status)
    result = night_bootstrap(paired_brier_diffs(population))
    result["verdict"] = "PASS" if result["upper"] < 0 else "FAIL"
    result["population"] = {"from": START_DATE.isoformat(), "through": status["verdictCutoffNight"],
                            "gradedRows": len(population)}
    return result


# ---------------------------------------------------------------- pure: secondaries

RUNG_KEYS: tuple[str, ...] = ("over",) + tuple(str(n) for n in THRESHOLDS)


def rung_table(rows: list[EvalRow]) -> list[dict[str, Any]]:
    """Per rung and reader: rows with a value, mean stated, hit rate, Brier; plus rows both readers cover."""
    out = []
    for rung in RUNG_KEYS:
        entry: dict[str, Any] = {"rung": "over the line" if rung == "over" else f"{rung}+"}
        for reader in READERS:
            scored = [(r.stated(reader, rung), r.hit(rung)) for r in rows if r.stated(reader, rung) is not None]
            n = len(scored)
            entry[reader] = {
                "n": n,
                "stated": sum(p for p, _ in scored) / n if n else None,  # type: ignore[misc]
                "happened": sum(1 for _, h in scored if h) / n if n else None,
                "brier": sum((p - (1.0 if h else 0.0)) ** 2 for p, h in scored) / n if n else None,  # type: ignore[operator]
            }
        entry["pairedN"] = sum(1 for r in rows if r.stated("v1", rung) is not None and r.stated("v2", rung) is not None)
        out.append(entry)
    return out


def coverage(rows: list[EvalRow]) -> dict[str, Any]:
    """Where the actual landed against each reader's floor (P10) and ceiling (P90), over rows with both
    ends; same rule as the panel's coverageData (below: actual < floor; above: actual > ceiling)."""
    out: dict[str, Any] = {"target": {"below": 0.1, "inside": 0.8, "above": 0.1}}
    for reader in READERS:
        below = inside = above = 0
        for r in rows:
            g = r.v1 if reader == "v1" else r.v2
            if g.floor is None or g.ceiling is None:
                continue
            if r.actual < g.floor:
                below += 1
            elif r.actual > g.ceiling:
                above += 1
            else:
                inside += 1
        n = below + inside + above
        out[reader] = {"n": n, "below": below / n if n else None, "inside": inside / n if n else None,
                       "above": above / n if n else None}
    return out


def line_only_skill(rows: list[EvalRow], reader: str, rung: str) -> dict[str, Any]:
    """Port of skillRow's line-only part: each row's guess is the hit share of rows in the same
    LINE_BAND-point line band on OTHER nights, shrunk toward the other nights' pooled share with
    BASELINE_PRIOR_ROWS pseudo-rows. Skill = 1 - reader Brier / line-only Brier on the same rows."""
    scored = [(r, r.stated(reader, rung), 1 if r.hit(rung) else 0) for r in rows if r.stated(reader, rung) is not None]
    n = len(scored)
    hits_all = 0
    band_all: dict[int, list[int]] = defaultdict(lambda: [0, 0])
    night_all: dict[date, list[int]] = defaultdict(lambda: [0, 0])
    night_band: dict[tuple[date, int], list[int]] = defaultdict(lambda: [0, 0])
    band = lambda line: math.floor(line / LINE_BAND)  # noqa: E731
    for r, _, y in scored:
        hits_all += y
        for c in (band_all[band(r.line)], night_all[r.game_date], night_band[(r.game_date, band(r.line))]):
            c[0] += 1
            c[1] += y
    line_n = 0
    sum_paired = 0.0
    sum_line = 0.0
    for r, p, y in scored:
        night = night_all[r.game_date]
        n_other = n - night[0]
        if n_other == 0:
            continue
        pool = (hits_all - night[1]) / n_other
        b = band(r.line)
        nb = band_all[b][0] - night_band[(r.game_date, b)][0]
        hb = band_all[b][1] - night_band[(r.game_date, b)][1]
        guess = (hb + BASELINE_PRIOR_ROWS * pool) / (nb + BASELINE_PRIOR_ROWS)
        line_n += 1
        sum_paired += (p - y) ** 2  # type: ignore[operator]
        sum_line += (guess - y) ** 2
    brier_paired = sum_paired / line_n if line_n else None
    brier_line = sum_line / line_n if line_n else None
    skill = (1 - brier_paired / brier_line) if brier_paired is not None and brier_line else None
    return {"lineN": line_n, "brierPaired": brier_paired, "brierLine": brier_line, "skillVsLine": skill}


def skill_table(rows: list[EvalRow]) -> list[dict[str, Any]]:
    return [{"rung": "over the line" if rung == "over" else f"{rung}+",
             **{reader: line_only_skill(rows, reader, rung) for reader in READERS}} for rung in RUNG_KEYS]


def rung_counts(rows: list[EvalRow]) -> dict[str, Any]:
    return {reader: {"oneSidedRungs": sum(r.one_sided.get(reader, 0) for r in rows),
                     "skippedRungs": sum(r.skipped.get(reader, 0) for r in rows)} for reader in READERS}


def _quantile(values: list[float], q: float) -> float:
    s = sorted(values)
    pos = (len(s) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def k_by_book(rows: list[EvalRow]) -> list[dict[str, Any]]:
    """The power exponent of each book's main line across graded player-games (v2 reader)."""
    values: dict[str, list[float]] = defaultdict(list)
    unsolved: dict[str, int] = defaultdict(int)
    for r in rows:
        for book, k in r.book_k.items():
            if k is None:
                unsolved[book] += 1
            else:
                values[book].append(k)
    out = []
    for book in sorted(set(values) | set(unsolved)):
        v = values.get(book, [])
        out.append({
            "book": book, "n": len(v), "unsolvable": unsolved.get(book, 0),
            "min": min(v) if v else None, "p10": _quantile(v, 0.1) if v else None,
            "median": _quantile(v, 0.5) if v else None, "p90": _quantile(v, 0.9) if v else None,
            "max": max(v) if v else None,
        })
    return out


def secondaries(rows: list[EvalRow]) -> dict[str, Any]:
    return {
        "gradedRows": len(rows),
        "shortNights": sum(1 for r in rows if r.minutes is not None and r.minutes < SHORT_MINUTES),
        "rungs": rung_table(rows),
        "coverage": coverage(rows),
        "lineOnlySkill": skill_table(rows),
        "rungCounts": rung_counts(rows),
        "kByBook": k_by_book(rows),
    }


def evaluate(rows: list[EvalRow], *, today: date | None = None) -> dict[str, Any]:
    """Floor status, the primary metric only if both floors are met, and the secondaries."""
    status = floor_status(rows, today=today)
    report: dict[str, Any] = {"study": STUDY, "registration": REGISTRATION, "gradeVersion": GRADE_VERSION,
                              "readers": {"v1": LADDER_VERSION, "v2": LADDER_V2_VERSION}, "floors": status}
    if status["state"] == "met":
        report["primary"] = primary_metric(rows, status)
        population = verdict_population(rows, status)
    else:
        report["primary"] = {"withheld": True, "reason": "floors not met" if status["state"] == "not_met"
                             else "deadline passed without the floors: INCONCLUSIVE, v1 stays"}
        population = [r for r in rows if START_DATE <= r.game_date <= DEADLINE]
    report["secondaries"] = secondaries(population)
    return report


# ---------------------------------------------------------------- database


def _counters(captures, ladders) -> dict[tuple[str, str], dict[str, Any]]:
    """Per (event, player): one-sided and skipped rungs under each reader and the v2 k by book."""
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for cap in captures:
        key = (cap.event_id, name_key(cap.player))
        ladder = ladders.get(key)
        a = read_player(cap.books, ladder)
        b = read_player(cap.books, ladder, "power")
        if a is None or b is None:
            continue
        da, db_ = a.distribution, b.distribution
        out[key] = {
            "one_sided": {"v1": da.one_sided_rungs if da else 0, "v2": db_.one_sided_rungs if db_ else 0},
            "skipped": {"v1": da.skipped_rungs if da else 0, "v2": db_.skipped_rungs if db_ else 0},
            "book_k": dict(db_.book_k) if db_ else {},
        }
    return out


def graded_dates(db, first: date) -> list[date]:
    """Dates from ``first`` with stored v1 grade rows (the nightly grade's coverage)."""
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_date FROM nba_points_calibration WHERE grade_version = %s AND game_date >= %s "
                    "ORDER BY game_date", (GRADE_VERSION, first))
        return [r["game_date"] for r in fetch_rows(cur)]


def ungraded_capture_dates(db, first: date, graded: list[date]) -> list[date]:
    """Dates with stored pre-tip NBA points captures but no stored v1 grade: not evaluated, and said so."""
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_date FROM prop_odds_history WHERE sport = 'nba' AND market = 'player_points' "
                    "AND game_date >= %s AND commence_time IS NOT NULL AND captured_at < commence_time ORDER BY game_date",
                    (first,))
        dates = [r["game_date"] for r in fetch_rows(cur)]
    have = set(graded)
    return [d for d in dates if d not in have]


def load_stored(db, target: date) -> list[dict[str, Any]]:
    from db.database import fetch_rows

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT game_date, event_id, player_key, status, actual_points, {', '.join(STORED_FIELDS)} "
                    "FROM nba_points_calibration WHERE grade_version = %s AND game_date = %s", (GRADE_VERSION, target))
        return fetch_rows(cur)


def load_night(db, target: date) -> tuple[list[GradeRow], list[GradeRow], dict[tuple[str, str], dict[str, Any]]]:
    captures, ladders = load_pretip_captures(db, target)
    games = load_box_scores(db, target)
    v1 = grade_captures(target, captures, ladders, games)
    v2 = grade_captures(target, captures, ladders, games, devig="power")
    return v1, v2, _counters(captures, ladders)


def _fmt(x: Any, pct: bool = False, digits: int = 4) -> str:
    if x is None:
        return "-"
    return f"{100 * x:.1f}%" if pct else f"{x:.{digits}f}"


def print_report(report: dict[str, Any]) -> None:
    f = report["floors"]
    print(f"{report['study']} ({report['registration']})")
    print(f"readers: v1 {report['readers']['v1']} vs v2 {report['readers']['v2']}; grade {report['gradeVersion']}")
    print(f"\nFloors (nights {START_DATE}..{f['deadline']}): "
          f"nights {f['nights']}/{f['minNights']} [{'met' if f['nightsMet'] else 'not met'}], "
          f"35+ rows under both {f['rows35Both']}/{f['minRows35']} [{'met' if f['rows35Met'] else 'not met'}]; "
          f"nights {f['firstNight'] or '-'}..{f['lastNight'] or '-'}; state {f['state']}")
    p = report["primary"]
    if p.get("withheld"):
        print(f"Primary metric: WITHHELD ({p['reason']}).")
    else:
        print(f"Primary metric (frozen population {p['population']['from']}..{p['population']['through']}, "
              f"{p['pairs']} pairs over {p['nights']} nights): mean Brier v2 - v1 = {p['mean']:+.6f}, "
              f"95% [{p['lower']:+.6f}, {p['upper']:+.6f}] -> {p['verdict']}")
    s = report["secondaries"]
    print(f"\nSecondaries (gate nothing): {s['gradedRows']} graded player-games, {s['shortNights']} short nights")
    print(f"{'rung':<14}{'n v1':>7}{'said v1':>9}{'hit v1':>8}{'Brier v1':>10}{'n v2':>7}{'said v2':>9}{'hit v2':>8}"
          f"{'Brier v2':>10}{'paired':>8}")
    for r in s["rungs"]:
        a, b = r["v1"], r["v2"]
        print(f"{r['rung']:<14}{a['n']:>7}{_fmt(a['stated'], True):>9}{_fmt(a['happened'], True):>8}{_fmt(a['brier']):>10}"
              f"{b['n']:>7}{_fmt(b['stated'], True):>9}{_fmt(b['happened'], True):>8}{_fmt(b['brier']):>10}{r['pairedN']:>8}")
    c = s["coverage"]
    for reader in READERS:
        x = c[reader]
        print(f"coverage {reader}: n {x['n']}, below floor {_fmt(x['below'], True)}, inside {_fmt(x['inside'], True)}, "
              f"above ceiling {_fmt(x['above'], True)} (target 10/80/10)")
    print(f"\n{'line-only skill':<16}{'n v1':>7}{'skill v1':>10}{'n v2':>7}{'skill v2':>10}")
    for r in s["lineOnlySkill"]:
        print(f"{r['rung']:<16}{r['v1']['lineN']:>7}{_fmt(r['v1']['skillVsLine']):>10}"
              f"{r['v2']['lineN']:>7}{_fmt(r['v2']['skillVsLine']):>10}")
    rc = s["rungCounts"]
    print(f"\nrungs: one-sided v1 {rc['v1']['oneSidedRungs']} / v2 {rc['v2']['oneSidedRungs']}; "
          f"skipped v1 {rc['v1']['skippedRungs']} / v2 {rc['v2']['skippedRungs']}")
    if s["kByBook"]:
        print(f"{'k by book':<16}{'n':>6}{'unsolv.':>9}{'min':>8}{'p10':>8}{'median':>8}{'p90':>8}{'max':>8}")
        for k in s["kByBook"]:
            print(f"{k['book']:<16}{k['n']:>6}{k['unsolvable']:>9}{_fmt(k['min'], digits=3):>8}{_fmt(k['p10'], digits=3):>8}"
                  f"{_fmt(k['median'], digits=3):>8}{_fmt(k['p90'], digits=3):>8}{_fmt(k['max'], digits=3):>8}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--check-integrity-from", metavar="YYYY-MM-DD",
                        help="run ONLY the v1 integrity check from this date (any date); prints no v2 number or outcome")
    args = parser.parse_args()

    from config import load_config
    from db.database import DatabaseManager

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    db.require_tables(["prop_odds_history", "nba_box_score_games", "nba_box_score_players", "nba_points_calibration"])
    first = date.fromisoformat(args.check_integrity_from) if args.check_integrity_from else START_DATE
    integrity_only = args.check_integrity_from is not None

    rows: list[EvalRow] = []
    checked = 0
    with db.reuse_connection():
        nights = graded_dates(db, first)
        not_graded = ungraded_capture_dates(db, first, nights)
        for night in nights:
            if integrity_only:
                captures, ladders = load_pretip_captures(db, night)
                v1 = grade_captures(night, captures, ladders, load_box_scores(db, night))
                v2 = counters = None
            else:
                v1, v2, counters = load_night(db, night)
            problems = integrity_check(v1, load_stored(db, night))
            if problems:
                print(f"INTEGRITY CHECK FAILED on {night}: {len(problems)} mismatch(es); stopping, no metric is reported.")
                for line in problems[:20]:
                    print(f"  {line}")
                return 1
            checked += sum(1 for r in v1 if r.status == "graded")
            if not integrity_only:
                rows.extend(pair_rows(v1, v2, counters))  # type: ignore[arg-type]

    if integrity_only:
        if not_graded:
            print(f"no stored grade for: {', '.join(d.isoformat() for d in not_graded)}")
        print(f"integrity check: {checked} graded player-games over {len(nights)} nights from {first} "
              f"match the stored {GRADE_VERSION} rows to {INTEGRITY_TOL:g}")
        return 0
    report = evaluate(rows)
    report["integrity"] = {"gradedPlayerGames": checked, "nights": len(nights), "tolerance": INTEGRITY_TOL, "passed": True}
    report["notYetGraded"] = [d.isoformat() for d in not_graded]
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(f"integrity check passed: {checked} graded player-games over {len(nights)} nights match the stored v1 rows")
        if not_graded:
            print(f"not evaluated (pre-tip captures, no stored {GRADE_VERSION} rows yet): "
                  f"{', '.join(d.isoformat() for d in not_graded)}")
        print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
