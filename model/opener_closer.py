"""D3 — Opener-vs-closer study (pre-registered in CLAUDE.md 2026-07-09).

Implements EXACTLY the frozen spec — population, price/line basis, metrics,
kill criterion, and minimum sample were committed (PR #96) before this script
was written or any open/close price examined.

  Question   : does our RAW (unanchored) model disagreement with the market's
               OPEN predict which way the market moves by CLOSE? Distinct from
               D2 (fixed entry TIME vs close) — this tests whether our SIGNAL
               leads price discovery.
  Population : {mlb,soccer}_bets moneyline/total rows (status != 'void'), one
               per (matchup_id, bet_type); matchup has >= 2 pre-commence
               consensus game_odds_history captures at/after epoch 2026-07-02.
               Tennis excluded (already closed — tennis-moneyline-no-edge).
  Values     : home-referenced (ML) or total-line-referenced (totals) —
               NEVER bet-selection-referenced, to avoid sign-flip bugs.
               our_value = the RAW unanchored model output frozen in
               inputs_json (our_prob_home / our_total_pred for MLB;
               model_prob [home-side row only] / lambda for soccer) — NOT the
               ledger's anchored our_prob, which is half-built from the
               near-close market price at lock time (a mechanical confound
               caught while designing this spec; see CLAUDE.md D3 section).
  M1         : directional agreement rate — sign(movement) == sign(edge_open)
  M2         : Pearson r and Spearman rho of (edge_open, movement)
  Kill       : MLB ML and MLB totals graded INDEPENDENTLY. Dead if 95%
               bootstrap CI of M1 includes 50% AND 95% bootstrap CI of
               Pearson r includes 0. Min sample: 150 each.

Usage:
    python -m model.opener_closer
"""

from __future__ import annotations

import argparse
import logging
import random
import statistics
from collections import defaultdict

import numpy as np

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_decimal

logger = logging.getLogger(__name__)

_EPOCH = "2026-07-02"       # odds-fix + dense-cadence epoch (frozen, shared w/ D1/D2)
_MIN_CAPTURES = 2           # frozen — D3 only needs first + last, unlike D2's 5
_MIN_SAMPLE = 150           # frozen, MLB ML and MLB totals evaluated separately
_KILL_MEDIAN_M1 = 0.50      # frozen: directional-agreement CI must exclude this
_N_BOOT = 2000

_BETS_TBL = {"mlb": "mlb_bets", "soccer": "soccer_bets"}
_MATCHUP_TBL = {"mlb": "mlb_matchups", "soccer": "soccer_matchups"}
# Current model_version only (2026-07-09) — mlb_bets/soccer_bets also hold
# superseded versions (mlb-gameline-v1; gameline-v1/v2) whose anchoring
# formula differs. Mixing versions would violate this project's own
# "bump the version rather than silently mix" rule (see soccer_bets/
# mlb_bets model_version convention throughout CLAUDE.md).
_CURRENT_MODEL_VERSION = {"mlb": "mlb-gameline-v2", "soccer": "gameline-v3"}


def _load_bets(db: DatabaseManager, sport: str, bet_type: str) -> list[dict]:
    """One row per (matchup_id, bet_type), current model_version only.
    Soccer ML restricted to the home-side row (its inputs_json.model_prob is
    side-specific; only the home row is home-referenced). MLB ML has exactly
    one row per fixture already (mlb_game_bets picks a single side), so no
    side filter needed there. Requires the matchup to carry a commence_time
    (parity with D2's candidate-counting convention)."""
    bets_tbl = _BETS_TBL[sport]
    matchup_tbl = _MATCHUP_TBL[sport]
    extra_where = ""
    if sport == "soccer" and bet_type == "moneyline":
        extra_where = "AND tb.inputs_json->>'side' = 'home'"

    if bet_type == "moneyline":
        val_col = ("tb.inputs_json->>'our_prob_home'" if sport == "mlb"
                   else "tb.inputs_json->>'model_prob'")
    else:
        val_col = ("tb.inputs_json->>'our_total_pred'" if sport == "mlb"
                   else "tb.inputs_json->>'lambda'")

    rows = db.execute(
        f"""
        SELECT DISTINCT ON (tb.matchup_id)
               tb.id, tb.matchup_id, tb.status,
               {val_col} AS our_value_raw
        FROM {bets_tbl} tb
        JOIN {matchup_tbl} m ON m.id = tb.matchup_id
        WHERE tb.bet_type = %s AND tb.status != 'void'
          AND tb.model_version = %s AND m.commence_time IS NOT NULL {extra_where}
        ORDER BY tb.matchup_id, tb.id DESC
        """,
        (bet_type, _CURRENT_MODEL_VERSION[sport]),
    )
    out = []
    for r in rows:
        if r["our_value_raw"] is None:
            continue
        try:
            r["our_value"] = float(r["our_value_raw"])
        except (TypeError, ValueError):
            continue
        out.append(r)
    return out


def _load_trails(db: DatabaseManager, sport: str, matchup_ids: list[int]) -> dict[int, list[dict]]:
    """{matchup_id: [captures ascending]} — pre-commence, epoch-filtered."""
    if not matchup_ids:
        return {}
    rows = db.execute(
        f"""
        SELECT h.matchup_id, h.captured_at, h.home_ml, h.vegas_total_raw, h.vegas_total,
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


def _home_prob(cap: dict) -> float | None:
    if cap.get("home_ml") is None:
        return None
    try:
        return 1.0 / american_to_decimal(int(cap["home_ml"]))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _total_line(cap: dict) -> float | None:
    if cap.get("vegas_total_raw") is not None:
        return float(cap["vegas_total_raw"])
    if cap.get("vegas_total") is not None:
        return float(cap["vegas_total"])
    return None


def analyze(db: DatabaseManager, sport: str, bet_type: str) -> dict:
    bets = _load_bets(db, sport, bet_type)
    trails = _load_trails(db, sport, sorted({b["matchup_id"] for b in bets}))
    value_fn = _home_prob if bet_type == "moneyline" else _total_line

    recs = []
    n_short_trail = 0
    for b in bets:
        caps = [(c, value_fn(c)) for c in trails.get(b["matchup_id"], [])]
        caps = [(c, v) for c, v in caps if v is not None]
        if len(caps) < _MIN_CAPTURES:
            n_short_trail += 1
            continue
        open_cap, open_val = caps[0]
        close_cap, close_val = caps[-1]
        edge_open = b["our_value"] - open_val
        movement = close_val - open_val
        recs.append({
            "edge_open": edge_open, "movement": movement,
            "close_val": close_val, "fav": (close_val >= 0.5) if bet_type == "moneyline" else None,
        })
    return {"sport": sport, "bet_type": bet_type, "recs": recs,
            "n_bets": len(bets), "n_short_trail": n_short_trail}


def _pearson(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2 or statistics.pstdev(xs) == 0 or statistics.pstdev(ys) == 0:
        return float("nan")
    return float(np.corrcoef(xs, ys)[0, 1])


def _rank(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[order[k]] = avg_rank
        i = j + 1
    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2:
        return float("nan")
    return _pearson(_rank(xs), _rank(ys))


def _hit_rate(recs: list[dict]) -> tuple[float, int, int]:
    """(rate, n_used, n_ties). Ties (edge_open == 0) excluded from the rate."""
    used = [r for r in recs if r["edge_open"] != 0]
    ties = len(recs) - len(used)
    if not used:
        return float("nan"), 0, ties
    hits = sum(1 for r in used if (r["movement"] > 0) == (r["edge_open"] > 0))
    return hits / len(used), len(used), ties


def _bootstrap_ci(recs: list[dict], stat_fn, n_boot: int = _N_BOOT, seed: int = 1234) -> tuple[float, float, float]:
    """(point_estimate, ci_low, ci_high) via resample-with-replacement."""
    rng = random.Random(seed)
    n = len(recs)
    point = stat_fn(recs)
    boots = []
    for _ in range(n_boot):
        sample = [recs[rng.randrange(n)] for _ in range(n)]
        v = stat_fn(sample)
        if v == v:  # not NaN
            boots.append(v)
    if not boots:
        return point, float("nan"), float("nan")
    boots.sort()
    lo = boots[int(0.025 * len(boots))]
    hi = boots[int(0.975 * len(boots)) - 1]
    return point, lo, hi


def _stat_hit_rate(recs: list[dict]) -> float:
    rate, n_used, _ = _hit_rate(recs)
    return rate


def _stat_pearson(recs: list[dict]) -> float:
    used = [r for r in recs if r["edge_open"] != 0]
    if len(used) < 2:
        return float("nan")
    return _pearson([r["edge_open"] for r in used], [r["movement"] for r in used])


def _med_iqr(xs: list[float]) -> str:
    if not xs:
        return "n/a"
    xs = sorted(xs)
    med = statistics.median(xs)
    q1, q3 = xs[len(xs) // 4], xs[(3 * len(xs)) // 4]
    return f"median {med:+.4f}  IQR [{q1:+.4f}, {q3:+.4f}]"


def report(db: DatabaseManager, sports: list[str]) -> None:
    print("=== D3 — Opener-vs-closer study (graded against the frozen spec, PR #96) ===\n")
    for sport in sports:
        for bet_type in ("moneyline", "total"):
            r = analyze(db, sport, bet_type)
            recs = r["recs"]
            n = len(recs)
            unit = "home-prob" if bet_type == "moneyline" else "total-line"
            print(f"--- {sport.upper()} {bet_type} — {n} qualifying bets "
                  f"(of {r['n_bets']} candidates; {r['n_short_trail']} excluded: <{_MIN_CAPTURES} epoch captures) [{unit} units] ---")
            if n == 0:
                print("    (nothing qualifies)\n")
                continue

            rate, n_used, ties = _hit_rate(recs)
            pear = _stat_pearson(recs)
            used = [r2 for r2 in recs if r2["edge_open"] != 0]
            spear = _spearman([r2["edge_open"] for r2 in used], [r2["movement"] for r2 in used]) if len(used) >= 2 else float("nan")

            print(f"    M1 directional agreement: {rate*100:.1f}% (n={n_used}, {ties} ties excluded)")
            print(f"    M2 correlation(edge_open, movement): Pearson r={pear:+.4f}  Spearman rho={spear:+.4f}")
            print(f"    edge_open: {_med_iqr([x['edge_open'] for x in recs])}")
            print(f"    movement : {_med_iqr([x['movement'] for x in recs])}")

            is_gated = sport == "mlb"
            if is_gated:
                if n < _MIN_SAMPLE:
                    print(f"    SAMPLE: n={n} < {_MIN_SAMPLE} pre-registered minimum — "
                          f"NO VERDICT yet; descriptive only.")
                else:
                    _, m1_lo, m1_hi = _bootstrap_ci(recs, _stat_hit_rate)
                    _, r_lo, r_hi = _bootstrap_ci(recs, _stat_pearson)
                    m1_dead = m1_lo <= _KILL_MEDIAN_M1 <= m1_hi
                    r_dead = r_lo <= 0 <= r_hi
                    print(f"    M1 95% bootstrap CI: [{m1_lo*100:.1f}%, {m1_hi*100:.1f}%]"
                          f"{' (includes 50%)' if m1_dead else ' (excludes 50%)'}")
                    print(f"    M2 95% bootstrap CI (Pearson r): [{r_lo:+.4f}, {r_hi:+.4f}]"
                          f"{' (includes 0)' if r_dead else ' (excludes 0)'}")
                    if m1_dead and r_dead:
                        print(f"    KILL CRITERION MET: {sport.upper()} {bet_type} is dead per the frozen spec.")
                    else:
                        print(f"    Kill criterion NOT met for {sport.upper()} {bet_type} — signal may be real.")

            if bet_type == "moneyline":
                for label, sel in (("favorites (close home-prob >= 0.5)", True), ("underdogs (away favored)", False)):
                    sub = [x for x in recs if x["fav"] == sel]
                    if sub:
                        rr, nn, _ = _hit_rate(sub)
                        print(f"    slice {label}: n={len(sub)}  M1={rr*100:.1f}%")
            print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="D3 opener-vs-closer study (pre-registered)")
    parser.add_argument("--sport", choices=["mlb", "soccer", "all"], default="all")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    sports = ["mlb", "soccer"] if args.sport == "all" else [args.sport]
    report(db, sports)
