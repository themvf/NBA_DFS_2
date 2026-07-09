"""D5 — Calibration-drift governance layer (pre-registered in CLAUDE.md 2026-07-09).

Implements EXACTLY the frozen spec — the monitored-segment list, window
definition, and trigger thresholds were committed (PR #98) before this
script was written or any settled-bet data examined.

  Scope      : only bet families currently rated ABOVE the standing 2★ cap
               anywhere in the ledger. Today that is exactly one: soccer
               futures (outright_winner, group_winner), validated
               separately via its own Monte Carlo backtest. Every game-line
               market (soccer ML/totals/first-scorer, MLB ML/totals,
               tennis ML) is already hard-capped at 2★ — this mechanism has
               nothing to add there and does not touch them.
  Window     : one distinct model_version run per bet_type (the only
               natural boundary for futures — a new tournament cycle gets
               a new model_version per this project's "bump the version"
               rule, which IS the window boundary here).
  Per window : realized_win_rate = wins / n (won/lost settled bets only)
               expected_win_rate = avg(our_prob) over the same bets
               brier = avg((our_prob - outcome)^2)
  Trigger    : DOWNGRADE ONLY, to 3★ pending revalidation. Fires if the
               last 3 consecutive windows for a bet_type show either
               realized_win_rate < expected_win_rate - 0.05, OR
               brier > 2x the validated baseline (0.036 for group_winner;
               no outright_winner baseline was ever separately validated,
               so its brier check is reported but not gated — see below).
               Never auto-uncaps, never raises a threshold.

Usage:
    python -m model.calibration_guard
"""

from __future__ import annotations

import argparse
import logging
import statistics

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# Frozen monitored segments (today: only the one tier above the 2★ cap).
_MONITORED_SEGMENTS = [
    {"sport": "soccer", "bet_type": "outright_winner"},
    {"sport": "soccer", "bet_type": "group_winner"},
]
_BETS_TBL = {"soccer": "soccer_bets"}

_N_CONSECUTIVE = 3       # frozen
_WIN_RATE_GAP = 0.05     # frozen: realized < expected - 5pp
_BRIER_MULT = 2.0        # frozen: brier > 2x validated baseline
# Validated baselines at the time each bet_type was originally cleared for
# 4-5★ (CLAUDE.md "MLB Underdog..." sibling section / soccer futures
# write-up: "group-winner Brier .036 vs .188"). outright_winner (1 champion
# settled ever) has no separately-validated baseline — its brier is
# reported descriptively, never gated, until one exists.
_BRIER_BASELINE = {"group_winner": 0.036}


def _load_settled(db: DatabaseManager, sport: str, bet_type: str) -> list[dict]:
    return db.execute(
        f"""
        SELECT model_version, our_prob, status
        FROM {_BETS_TBL[sport]}
        WHERE bet_type = %s AND status IN ('won', 'lost')
        ORDER BY model_version, id
        """,
        (bet_type,),
    )


def _windows(rows: list[dict]) -> list[dict]:
    """One window per distinct model_version, in the order first seen."""
    by_version: dict[str, list[dict]] = {}
    order: list[str] = []
    for r in rows:
        v = r["model_version"]
        if v not in by_version:
            by_version[v] = []
            order.append(v)
        by_version[v].append(r)

    out = []
    for v in order:
        bets = by_version[v]
        n = len(bets)
        wins = sum(1 for b in bets if b["status"] == "won")
        realized = wins / n if n else float("nan")
        expected = statistics.mean(b["our_prob"] for b in bets) if n else float("nan")
        brier = (statistics.mean((b["our_prob"] - (1.0 if b["status"] == "won" else 0.0)) ** 2 for b in bets)
                 if n else float("nan"))
        out.append({"model_version": v, "n": n, "realized": realized,
                     "expected": expected, "brier": brier})
    return out


def evaluate_segment(db: DatabaseManager, sport: str, bet_type: str) -> dict:
    rows = _load_settled(db, sport, bet_type)
    wins_list = _windows(rows)
    baseline = _BRIER_BASELINE.get(bet_type)

    triggered = False
    reason = None
    if len(wins_list) >= _N_CONSECUTIVE:
        last3 = wins_list[-_N_CONSECUTIVE:]
        win_rate_fail = all(w["realized"] < w["expected"] - _WIN_RATE_GAP for w in last3)
        brier_fail = (baseline is not None
                      and all(w["brier"] > _BRIER_MULT * baseline for w in last3))
        if win_rate_fail:
            triggered = True
            reason = f"realized win rate < expected-{_WIN_RATE_GAP*100:.0f}pp in the last {_N_CONSECUTIVE} windows"
        elif brier_fail:
            triggered = True
            reason = f"brier > {_BRIER_MULT}x baseline ({baseline}) in the last {_N_CONSECUTIVE} windows"

    return {"sport": sport, "bet_type": bet_type, "windows": wins_list,
            "n_windows": len(wins_list), "triggered": triggered, "reason": reason,
            "baseline": baseline}


def report(db: DatabaseManager) -> None:
    print("=== D5 — Calibration-drift governance layer (graded against the frozen spec, PR #98) ===\n")
    for seg in _MONITORED_SEGMENTS:
        r = evaluate_segment(db, seg["sport"], seg["bet_type"])
        print(f"--- {seg['sport'].upper()} {seg['bet_type']} — {r['n_windows']} window(s) "
              f"(need >= {_N_CONSECUTIVE} for the trigger to be evaluable) ---")
        for w in r["windows"]:
            base_note = f"  (baseline {r['baseline']})" if r["baseline"] is not None else "  (no validated baseline — descriptive only)"
            print(f"    {w['model_version']:14} n={w['n']:4}  "
                  f"realized={w['realized']*100:5.1f}%  expected={w['expected']*100:5.1f}%  "
                  f"brier={w['brier']:.4f}{base_note}")
        if r["n_windows"] < _N_CONSECUTIVE:
            print(f"    NOT TRIGGERED — insufficient windows ({r['n_windows']} < {_N_CONSECUTIVE}). "
                  f"Descriptive only; no star rating affected.")
        elif r["triggered"]:
            print(f"    *** TRIGGERED: {r['reason']}. Downgrade {seg['sport']} {seg['bet_type']} "
                  f"to 3★ pending revalidation. ***")
        else:
            print(f"    NOT TRIGGERED — {_N_CONSECUTIVE}-window bar not met. No action.")
        print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="D5 calibration-drift governance layer (pre-registered)")
    parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    report(db)
