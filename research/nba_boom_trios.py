"""Boom trio alternatives beside the baseline trio (`nba-boom-trios-v1`).

Registered in docs/nba-boom-trios.md before it was run on any slate. Pure
selection rules over a frozen `nba-points-ghost-v2` snapshot, plus grading from
that snapshot's ghost grade rows. Nothing here changes a forecast.

Three trios per slate:

  baseline     the three highest market lines (exactly the existing
               `market_line` trio of research.nba_leader_evaluation: same
               eligibility, same tie rule).
  boom         keep baseline #1 and #2; replace #3 with the strongest boom
               candidate whose line is at most ONE_SWAP_MAX_DROP below #3's.
  aggressive   keep baseline #1; the other two seats go to the pair of boom
               candidates with the highest combined boom score whose lines sum
               to at most TWO_SWAP_MAX_DROP below the baseline total. A
               baseline player who is himself a boom candidate may keep his seat.

The boom score never touches projected points. It only ranks replacements
inside the points limit. It is rising shot volume: true shot attempts
(FGA + 0.44 * FTA) per game over the last 3 games minus the last 20, from the
snapshot's own frozen features (shots_growth, ft_growth). A boom candidate has
ready history and a boom score of at least BOOM_MIN. A day with no candidate
inside the limit has no alternative; it is reported, never filled.

Every constant here is part of the registration. Changing one is a new version.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from itertools import combinations
import json
import math
from pathlib import Path
import random

from collections import Counter

# Pool, ranking and identity mirror research.nba_leader_evaluation (nba-leader-evaluation-v1)
# exactly, copied so this module stands on its own: one quote per player, a finite
# line, not reported Out; predicted points descending, market line descending,
# player key, event id.


def finite(value) -> bool:
    try:
        return value is not None and not isinstance(value, bool) and math.isfinite(float(value))
    except (ValueError, TypeError):
        return False


def identity(row: dict) -> tuple[str, str]:
    return (str(row["event_id"]), row["player_key"])


def eligible(candidates: list[dict], method: str) -> list[dict]:
    counts = Counter(c["player_key"] for c in candidates)
    return [c for c in candidates if counts[c["player_key"]] == 1 and finite(c["predictions"].get(method))
            and str(c.get("availability", {}).get("status", "")).lower() != "out"]


def ranked(candidates: list[dict], method: str) -> list[dict]:
    return sorted(candidates, key=lambda c: (-c["predictions"][method], -c["predictions"]["market_line"], c["player_key"], str(c["event_id"])))

VERSION = "nba-boom-trios-v1"
BASELINE_METHOD = "market_line"
FT_WEIGHT = 0.44
BOOM_MIN = 2.0
ONE_SWAP_MAX_DROP = 3.0
TWO_SWAP_MAX_DROP = 6.0
# Rules frozen at this instant; only snapshots created after it count.
REGISTERED_AT = "2026-10-31T19:30:00+00:00"
REVIEW_AFTER_DATES = 30
BOOTSTRAP_DRAWS = 5000
# The only candidate fields these rules read. The web page
# (web/src/lib/nba-boom-trios.ts) pulls just these from the snapshot.
FEATURE_FIELDS = ("shots_growth", "ft_growth", "fga3", "fta3")
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "tests/fixtures/nba_boom_trios_fixture.json"


def slim(candidate: dict) -> dict:
    f = candidate.get("features")
    return {"event_id": candidate["event_id"], "player_key": candidate["player_key"], "player": candidate["player"],
            "history_status": candidate.get("history_status"), "availability": {"status": (candidate.get("availability") or {}).get("status")},
            "predictions": {"market_line": (candidate.get("predictions") or {}).get("market_line")},
            "features": None if f is None else {k: f.get(k) for k in FEATURE_FIELDS}}


def boom_score(candidate: dict) -> float | None:
    f = candidate.get("features") or {}
    if candidate.get("history_status") != "ready" or not finite(f.get("shots_growth")) or not finite(f.get("ft_growth")):
        return None
    return float(f["shots_growth"]) + FT_WEIGHT * float(f["ft_growth"])


def _line(c: dict) -> float:
    return float(c["predictions"][BASELINE_METHOD])


def _shots(c: dict) -> tuple[float, float] | None:
    """(last 3, last 20) true shot attempts per game, for the explanation."""
    f = c.get("features") or {}
    try:
        recent = float(f["fga3"]) + FT_WEIGHT * float(f["fta3"])
        longer = (float(f["fga3"]) - float(f["shots_growth"])) + FT_WEIGHT * (float(f["fta3"]) - float(f["ft_growth"]))
    except (KeyError, TypeError, ValueError):
        return None
    return recent, longer


def _member(c: dict, role: str) -> dict:
    shots = _shots(c)
    return {"event_id": c["event_id"], "player_key": c["player_key"], "player": c["player"], "role": role,
            "line": _line(c), "boom": boom_score(c),
            "shots_last3": None if shots is None else round(shots[0], 2),
            "shots_last20": None if shots is None else round(shots[1], 2)}


def _why(added: list[dict], dropped_points: float) -> str:
    parts = []
    for m in added:
        if m["shots_last3"] is not None:
            parts.append(f"{m['player']} has taken {m['shots_last3'] - m['shots_last20']:.1f} more shot attempts a game over his "
                         f"last 3 games than his last 20 ({m['shots_last3']:.1f} vs {m['shots_last20']:.1f})")
    return f"Projected total drops by {dropped_points:.1f} points, but " + " and ".join(parts) + "."


def select(snapshot: dict) -> dict:
    """The three trios for one frozen snapshot. Deterministic."""
    pool = ranked(eligible(snapshot["candidates"], BASELINE_METHOD), BASELINE_METHOD)
    out = {"version": VERSION, "game_date": snapshot["game_date"], "snapshot_sha256": snapshot.get("sha256"),
           "snapshot_created_at": snapshot.get("created_at"), "snapshot_mode": snapshot.get("mode"),
           "counts": counts_toward_rule(snapshot), "pool": len(pool),
           "rules": {"boom_min": BOOM_MIN, "ft_weight": FT_WEIGHT, "one_swap_max_drop": ONE_SWAP_MAX_DROP,
                     "two_swap_max_drop": TWO_SWAP_MAX_DROP, "baseline": BASELINE_METHOD}}
    if len(pool) < 3:
        out.update(baseline=None, boom=None, aggressive=None, note="fewer than three eligible players")
        return out
    base = pool[:3]
    base_total = sum(_line(c) for c in base)
    out["baseline"] = {"members": [_member(c, "baseline") for c in base], "projected_total": round(base_total, 2)}
    candidates = [c for c in pool if (boom_score(c) or -math.inf) >= BOOM_MIN]
    order = lambda c: (-(boom_score(c) or 0.0), -_line(c), c["player_key"], str(c["event_id"]))

    # Boom: one seat, #3's.
    pick = sorted((c for c in candidates if c not in base and _line(base[2]) - _line(c) <= ONE_SWAP_MAX_DROP + 1e-9), key=order)
    if pick:
        c = pick[0]
        drop = _line(base[2]) - _line(c)
        added = _member(c, "boom")
        out["boom"] = {"members": [_member(base[0], "baseline"), _member(base[1], "baseline"), added],
                       "replaced": _member(base[2], "baseline"), "projected_total": round(base_total - drop, 2),
                       "projected_drop": round(drop, 2), "why": _why([added], drop)}
    else:
        out["boom"] = None

    # Aggressive: two seats beside #1, both boom candidates.
    best = None
    for a, b in combinations([c for c in candidates if c is not base[0]], 2):
        total = _line(base[0]) + _line(a) + _line(b)
        if base_total - total > TWO_SWAP_MAX_DROP + 1e-9:
            continue
        key = (-((boom_score(a) or 0) + (boom_score(b) or 0)), -total, a["player_key"], b["player_key"])
        if best is None or key < best[0]:
            best = (key, a, b, total)
    if best is None:
        out["aggressive"] = None
    else:
        _, a, b, total = best
        pair = sorted([a, b], key=lambda c: -_line(c))
        added = [_member(c, "boom") for c in pair if c not in base]
        kept = [c for c in pair if c in base]
        drop = base_total - total
        out["aggressive"] = {"members": [_member(base[0], "baseline"), *[_member(c, "baseline") for c in kept], *added],
                             "replaced": [_member(c, "baseline") for c in base[1:] if c not in pair],
                             "projected_total": round(total, 2), "projected_drop": round(drop, 2),
                             "why": _why(added, drop) if added else "Both boom candidates were already in the baseline."}
    return out


def counts_toward_rule(snapshot: dict) -> bool:
    created = snapshot.get("created_at")
    return (snapshot.get("mode") == "prospective" and created is not None
            and datetime.fromisoformat(created) > datetime.fromisoformat(REGISTERED_AT))


def grade(selection: dict, rows: list[dict]) -> dict:
    """Actual totals from the ghost grade rows. A trio with an unresolved or DNP
    member is incomplete and left out of the comparison, never scored as 0."""
    outcomes = {identity(r): r for r in rows}

    def actual(m):
        r = outcomes.get((str(m["event_id"]), m["player_key"]))
        if not r or r.get("status") not in ("graded", "push") or not finite(r.get("actual_points")):
            return None
        return float(r["actual_points"])

    def p90_hit(m):
        r = outcomes.get((str(m["event_id"]), m["player_key"]))
        a = actual(m)
        if r is None or a is None or not finite(r.get("historical_p90")):
            return None
        return a > float(r["historical_p90"])

    def total(trio):
        if trio is None:
            return None
        values = [actual(m) for m in trio["members"]]
        return None if any(v is None for v in values) else sum(values)

    base_total = total(selection.get("baseline"))
    result = {"version": VERSION, "game_date": selection["game_date"], "counts": selection["counts"],
              "baseline_actual": base_total}
    for name in ("boom", "aggressive"):
        trio = selection.get(name)
        if trio is None:
            result[name] = {"status": "no_alternative"}
            continue
        t = total(trio)
        replaced = trio["replaced"] if isinstance(trio["replaced"], list) else [trio["replaced"]]
        added = [m for m in trio["members"] if m["role"] == "boom"]
        if t is None or base_total is None:
            result[name] = {"status": "incomplete"}
            continue
        delta = t - base_total
        result[name] = {"status": "graded", "actual": t, "delta": delta,
                        "outcome": "helped" if delta > 0 else "hurt" if delta < 0 else "tied",
                        "projected_drop": trio["projected_drop"],
                        "p90_added": [p90_hit(m) for m in added], "p90_replaced": [p90_hit(m) for m in replaced]}
    return result


def _bootstrap(values: list[float], seed: int = 20261031) -> tuple[float, float] | None:
    if len(values) < 2:
        return None
    rng = random.Random(seed)
    means = sorted(sum(rng.choice(values) for _ in values) / len(values) for _ in range(BOOTSTRAP_DRAWS))
    return means[int(0.025 * BOOTSTRAP_DRAWS)], means[int(0.975 * BOOTSTRAP_DRAWS) - 1]


def summarize(grades: list[dict], counted_only: bool = True) -> dict:
    """One row per slate date already (one snapshot per date), so the date is the bootstrap unit."""
    days = [g for g in grades if g["counts"] or not counted_only]
    out = {"version": VERSION, "dates": len(days), "counted_only": counted_only,
           "review_after_dates": REVIEW_AFTER_DATES, "verdict": None}
    for name in ("boom", "aggressive"):
        graded = [g[name] for g in days if g[name]["status"] == "graded"]
        deltas = [x["delta"] for x in graded]
        helped = [d for d in deltas if d > 0]
        hurt = [d for d in deltas if d < 0]

        def rate(key):
            hits = [h for x in graded for h in x[key] if h is not None]
            return {"n": len(hits), "hits": sum(hits), "rate": sum(hits) / len(hits) if hits else None}
        out[name] = {"graded": len(graded),
                     "no_alternative": sum(g[name]["status"] == "no_alternative" for g in days),
                     "incomplete": sum(g[name]["status"] == "incomplete" for g in days),
                     "mean_delta": sum(deltas) / len(deltas) if deltas else None, "ci95": _bootstrap(deltas),
                     "helped": len(helped), "hurt": len(hurt), "tied": len(deltas) - len(helped) - len(hurt),
                     "mean_gain_when_helped": sum(helped) / len(helped) if helped else None,
                     "mean_loss_when_hurt": sum(hurt) / len(hurt) if hurt else None,
                     "mean_projected_drop": sum(x["projected_drop"] for x in graded) / len(graded) if graded else None,
                     "p90_added": rate("p90_added"), "p90_replaced": rate("p90_replaced")}
    return out


# ── CLI: read the frozen snapshots and latest grade per snapshot, read-only ──

def _load(conn, since: str | None):
    with conn.cursor() as cur:
        cur.execute("""SELECT game_date, snapshot, snapshot_sha256 FROM nba_ghost_snapshots
            WHERE version = 'nba-points-ghost-v2' AND (%s::date IS NULL OR game_date >= %s::date)
            ORDER BY game_date, created_at""", (since, since))
        snaps = cur.fetchall()
        cur.execute("""SELECT DISTINCT ON (snapshot_sha256) snapshot_sha256, grade FROM nba_ghost_grades
            ORDER BY snapshot_sha256, graded_at DESC, id DESC""")
        grades = {r["snapshot_sha256"]: r["grade"] for r in cur.fetchall()}
    first = {}
    for r in snaps:  # one snapshot per date: the first prospective one, else the first
        s = r["snapshot"]
        d = str(r["game_date"])
        if d not in first or (first[d][0].get("mode") != "prospective" and s.get("mode") == "prospective"):
            first[d] = (s, grades.get(r["snapshot_sha256"]))
    return [first[d] for d in sorted(first)]


def _print_trio(label: str, trio: dict | None) -> None:
    if trio is None:
        print(f"  {label:10s} none inside the limit")
        return
    names = ", ".join(f"{m['player']} {m['line']:.1f}" for m in trio["members"])
    drop = f"  (-{abs(trio['projected_drop']):.1f})" if "projected_drop" in trio else ""
    print(f"  {label:10s} {trio['projected_total']:5.1f}{drop}  {names}")
    if trio.get("why"):
        print(f"             {trio['why']}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["show", "report", "fixture"])
    ap.add_argument("--date", default="2026-10-30", help="fixture: the slate to pin the web port against")
    ap.add_argument("--since", default=None)
    ap.add_argument("--all-dates", action="store_true", help="report: include dates that do not count (descriptive)")
    ap.add_argument("--output", default=None, help="report: also write JSON here (must not exist)")
    args = ap.parse_args(argv)
    from research.nfl_ownership_inventory import connect
    conn = connect()
    try:
        days = _load(conn, args.since)
    finally:
        conn.close()
    selections, grades = [], []
    for snapshot, ghost_grade in days:
        sel = select(snapshot)
        selections.append(sel)
        if ghost_grade:
            grades.append(grade(sel, ghost_grade["rows"]))
    if args.command == "fixture":
        snapshot = next(s for s, _ in days if s["game_date"] == args.date)
        slim_snap = {k: snapshot.get(k) for k in ("game_date", "sha256", "created_at", "mode")}
        slim_snap["candidates"] = [slim(c) for c in snapshot["candidates"]]
        expected = select(slim_snap)
        assert expected == {**select(snapshot), "snapshot_sha256": expected["snapshot_sha256"]}, "slim fields change the selection"
        FIXTURE_PATH.write_text(json.dumps({"snapshot": slim_snap, "expected": expected}, indent=1) + "\n", encoding="utf-8")
        print(f"  wrote {FIXTURE_PATH}")
        return 0
    if args.command == "show":
        for sel in selections:
            tag = "counts" if sel["counts"] else ("pre-registration" if sel["snapshot_mode"] == "prospective" else "retrospective")
            print(f"\n{sel['game_date']}  pool {sel['pool']}  [{tag}]")
            for label in ("baseline", "boom", "aggressive"):
                _print_trio(label, sel.get(label))
        return 0
    summary = summarize(grades, counted_only=not args.all_dates)
    print(f"{VERSION}: {summary['dates']} dates ({'counted only' if summary['counted_only'] else 'all, descriptive'}); "
          f"review after {REVIEW_AFTER_DATES} counted dates")
    for name in ("boom", "aggressive"):
        s = summary[name]
        ci = f"[{s['ci95'][0]:+.1f}, {s['ci95'][1]:+.1f}]" if s["ci95"] else "n/a"
        md = f"{s['mean_delta']:+.2f}" if s["mean_delta"] is not None else "n/a"
        print(f"  {name:10s} graded {s['graded']}  mean vs baseline {md} {ci}  helped {s['helped']} hurt {s['hurt']} tied {s['tied']}"
              f"  (no alternative {s['no_alternative']}, incomplete {s['incomplete']})")
        print(f"             P90 hits: added {s['p90_added']['hits']}/{s['p90_added']['n']}, replaced {s['p90_replaced']['hits']}/{s['p90_replaced']['n']}")
    for g in grades:
        if g["counts"] or args.all_dates:
            cells = "  ".join(f"{n} {g[n]['delta']:+.1f}" if g[n]["status"] == "graded" else f"{n} {g[n]['status']}" for n in ("boom", "aggressive"))
            print(f"    {g['game_date']}  baseline {g['baseline_actual']}  {cells}")
    if args.output:
        path = Path(args.output)
        with path.open("x", encoding="utf-8") as fh:
            json.dump({"computed_at": datetime.now(timezone.utc).isoformat(), "summary": summary,
                       "selections": selections, "grades": grades}, fh, indent=1, default=str)
        print(f"  written {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
