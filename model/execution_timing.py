"""D2 — Execution-timing study (pre-registered in CLAUDE.md 2026-07-08).

Implements EXACTLY the frozen spec — population, price basis, metrics,
slices, and kill criterion were committed (PR #94) before this script was
written or any trail price path examined.

  Population : moneyline ledger bets (status != 'void'), matchup with >= 5
               pre-commence CONSENSUS captures at/after 2026-07-02, side
               price present. One record per (sport, matchup, side) — the
               ledgers can hold multiple model versions of the same
               proposition; duplicates would double-count games.
  Price basis: consensus American -> decimal at each capture (timing only;
               per-book shopping is D1).
  M1 : oracle premium  = max(trail decimal)/close decimal - 1  (upper bound)
  M2 : where the max occurs (hours before commence, normalized position)
  M3 : fixed no-hindsight rules — first capture, T-24h/T-12h/T-6h/T-3h
       (latest capture at/before each horizon) vs close
  Kill: MLB median M1 < 1% -> D2 dropped. Min sample: 100 MLB bets.

Definitional detail not pinned by the spec, chosen before running and
stated here: favorite = closing side decimal <= 2.0, underdog otherwise.

Usage:
    python -m model.execution_timing
"""

from __future__ import annotations

import argparse
import logging
import statistics
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.best_price import _BETS_TBL, _MATCHUP_TBL, _bet_side, load_settled_bets
from model.soccer_bet_rating import american_to_decimal

logger = logging.getLogger(__name__)

_EPOCH = "2026-07-02"      # odds-fix + dense-cadence epoch (frozen)
_MIN_CAPTURES = 5          # frozen
_MIN_MLB_SAMPLE = 100      # frozen
_KILL_MEDIAN_M1 = 0.01     # frozen: MLB median oracle premium < 1% -> dead
_SIDE_COL = {"home": "home_ml", "away": "away_ml", "draw": "draw_ml"}
_HORIZONS_H = (24, 12, 6, 3)


def _load_bets(db: DatabaseManager, sport: str) -> list[dict]:
    """Moneyline ledger bets (any status except void), deduped to one per
    (matchup, side)."""
    matchup_col = "match_id" if sport == "tennis" else "matchup_id"
    side_col = "tb.side," if sport == "tennis" else "NULL AS side,"
    subj_col = "NULL AS subject_team_id," if sport == "tennis" else "tb.subject_team_id,"
    home_col = "NULL AS home_team_id" if sport == "tennis" else "m.home_team_id"
    rows = db.execute(
        f"""
        SELECT DISTINCT ON (tb.{matchup_col}, tb.selection_label)
               tb.id, tb.{matchup_col} AS matchup_id, tb.bet_type, tb.selection_label,
               tb.status, {side_col} {subj_col} {home_col},
               tb.inputs_json->>'side' AS ij_side,
               m.commence_time
        FROM {_BETS_TBL[sport]} tb
        JOIN {_MATCHUP_TBL[sport]} m ON m.id = tb.{matchup_col}
        WHERE tb.bet_type = 'moneyline' AND tb.status != 'void'
          AND m.commence_time IS NOT NULL
        ORDER BY tb.{matchup_col}, tb.selection_label, tb.id DESC
        """,
    )
    return rows


def _load_trails(db: DatabaseManager, sport: str, matchup_ids: list[int]) -> dict[int, list[dict]]:
    """{matchup_id: [captures ascending]} — pre-commence, epoch-filtered."""
    if not matchup_ids:
        return {}
    rows = db.execute(
        f"""
        SELECT h.matchup_id, h.captured_at, h.home_ml, h.away_ml, h.draw_ml,
               m.commence_time
        FROM game_odds_history h
        JOIN {_MATCHUP_TBL[sport]} m ON m.id = h.matchup_id
        WHERE h.sport = %s AND h.matchup_id = ANY(%s)
          AND h.captured_at >= %s
          AND m.commence_time IS NOT NULL AND h.captured_at <= m.commence_time
        ORDER BY h.matchup_id, h.captured_at ASC
        """,
        (sport, matchup_ids, _EPOCH),
    )
    trails: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        trails[r["matchup_id"]].append(r)
    return trails


def _side_decimal(cap: dict, side: str) -> float | None:
    price = cap.get(_SIDE_COL.get(side, ""))
    if price is None:
        return None
    try:
        return american_to_decimal(int(price))
    except (TypeError, ValueError):
        return None


def analyze_sport(db: DatabaseManager, sport: str) -> dict:
    bets = _load_bets(db, sport)
    trails = _load_trails(db, sport, sorted({b["matchup_id"] for b in bets}))

    recs = []
    n_short_trail = 0
    for b in bets:
        side = _bet_side(sport, b)
        if side is None:
            continue
        caps = [(c, _side_decimal(c, side)) for c in trails.get(b["matchup_id"], [])]
        caps = [(c, d) for c, d in caps if d is not None]
        if len(caps) < _MIN_CAPTURES:
            n_short_trail += 1
            continue
        close_cap, close_dec = caps[-1]
        commence = close_cap["commence_time"]
        # M1: oracle max over the trail
        best_idx = max(range(len(caps)), key=lambda i: caps[i][1])
        best_cap, best_dec = caps[best_idx]
        m1 = best_dec / close_dec - 1
        hours_before = (commence - best_cap["captured_at"]).total_seconds() / 3600
        norm_pos = best_idx / (len(caps) - 1)
        # M3 fixed rules
        rules: dict[str, float] = {"first": caps[0][1] / close_dec - 1}
        for h in _HORIZONS_H:
            eligible = [d for c, d in caps
                        if (commence - c["captured_at"]).total_seconds() >= h * 3600]
            if eligible:
                rules[f"T-{h}h"] = eligible[-1] / close_dec - 1  # latest at/before horizon
        recs.append({
            "m1": m1, "hours_before": hours_before, "norm_pos": norm_pos,
            "rules": rules, "n_caps": len(caps),
            "fav": close_dec <= 2.0,
        })
    return {"sport": sport, "recs": recs, "n_bets": len(bets), "n_short_trail": n_short_trail}


def _med_iqr(xs: list[float]) -> str:
    if not xs:
        return "n/a"
    xs = sorted(xs)
    med = statistics.median(xs)
    q1, q3 = xs[len(xs) // 4], xs[(3 * len(xs)) // 4]
    return f"median {med*100:+.2f}%  IQR [{q1*100:+.2f}%, {q3*100:+.2f}%]"


def report(db: DatabaseManager, sports: list[str]) -> None:
    print("=== D2 — Execution-timing study (graded against the frozen spec, PR #94) ===\n")
    for sport in sports:
        r = analyze_sport(db, sport)
        recs = r["recs"]
        n = len(recs)
        print(f"--- {sport.upper()} — {n} qualifying ML bets "
              f"(of {r['n_bets']} candidates; {r['n_short_trail']} excluded: <{_MIN_CAPTURES} epoch captures) ---")
        if n == 0:
            print("    (nothing qualifies)\n")
            continue

        m1s = [x["m1"] for x in recs]
        print(f"    M1 oracle premium (hindsight max vs close): {_med_iqr(m1s)}")
        if sport == "mlb":
            med = statistics.median(m1s)
            if n < _MIN_MLB_SAMPLE:
                print(f"    SAMPLE: n={n} < {_MIN_MLB_SAMPLE} pre-registered minimum — "
                      f"NO VERDICT yet; report is descriptive until the epoch accrues more bets.")
            elif med < _KILL_MEDIAN_M1:
                print(f"    KILL CRITERION MET: median {med*100:.2f}% < 1% — timing doesn't matter "
                      f"even with hindsight. D2 is dead per the frozen spec.")
            else:
                print(f"    Kill criterion NOT met (median {med*100:.2f}% >= 1%) — "
                      f"M2/M3 below are worth reading.")

        # M2 — where the max occurs
        hrs = [x["hours_before"] for x in recs]
        pos = [x["norm_pos"] for x in recs]
        early = sum(1 for p in pos if p <= 0.25)
        late = sum(1 for p in pos if p >= 0.75)
        print(f"    M2 when best occurs: median {statistics.median(hrs):.1f}h before commence; "
              f"trail position: {early/n*100:.0f}% in first quarter, {late/n*100:.0f}% in last quarter")

        # M3 — fixed rules
        print("    M3 fixed entry rules vs close (no hindsight):")
        for rule in ["first"] + [f"T-{h}h" for h in _HORIZONS_H]:
            vals = [x["rules"][rule] for x in recs if rule in x["rules"]]
            if vals:
                print(f"      {rule:6} n={len(vals):4}  {_med_iqr(vals)}")

        # Slice: favorite vs underdog at close
        for label, sel in (("favorites (close dec <= 2.0)", True), ("underdogs", False)):
            sub = [x["m1"] for x in recs if x["fav"] == sel]
            if sub:
                print(f"    M1 {label}: n={len(sub)}  {_med_iqr(sub)}")
        print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="D2 execution-timing study (pre-registered)")
    parser.add_argument("--sport", choices=["mlb", "soccer", "tennis", "all"], default="all")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    sports = ["mlb", "soccer", "tennis"] if args.sport == "all" else [args.sport]
    report(db, sports)
