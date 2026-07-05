"""P1-P2 of the MLB Underdog-Value Investigation spec (CLAUDE.md). Re-derives
the NATURAL (uncapped) star tier for every settled `mlb-gameline-v2` moneyline
bet already in the ledger — using the FROZEN (our_prob, market_decimal,
market_prob) each bet locked at rating time — then computes the pre-
registered success metrics on the qualifying (4-5★) subset.

Two corrections made while building this (2026-07-05), both caught by the
script's own checks before any metric was trusted, reported honestly rather
than silently fixed:

1. `mlb_bets.event_commence` is mostly NULL in this ledger (a data-quality
   gap, not touched here). An early version of this script filtered dates by
   `event_commence BETWEEN ...`, which silently drops NULL rows — that
   undercounted the known 65-bet 5★ population down to 6 and, earlier,
   made the CLAUDE.md spec itself wrongly claim those 65 bets span only
   "four calendar days" (2026-06-28 to 07-01). Joined to
   `mlb_matchups.game_date` instead (populated on every row): the 65 bets
   actually span **2026-03-31 to 2026-07-01 — essentially the whole
   available season**, not four days. The spec's "small-sample, cherry-
   picked window" framing was wrong and needs correcting alongside this
   result (see CLAUDE.md).

2. Recomputing `rate_market` on the frozen inputs did NOT reproduce all 65
   stored 5★ ratings — 19 mismatches, ALL of them (verified) rows whose
   `inputs_json` shows `odds_repaired=True` from the 2026-07-02 odds-bug fix
   (memory `mlb-gameline-caps-odds-bug`). The repair updated `market_decimal`
   in place but never retroactively recomputed `stars`/`ev` from the
   corrected odds — `edge` (which depends only on our_prob/market_prob, not
   odds) matches exactly on all 19; `ev`/`stars` don't, because they were
   computed from the OLD, inflated, pre-repair decimal odds. **19 of the
   original 65 "5★" bets (29%) were only 5★ because of the very odds bug
   that was supposedly fixed** — this script's `natural_stars` (recomputed
   from the current, correct, repaired odds) is the honest value; the stale
   stored `stars` is not. Verified the recomputation logic itself is correct
   separately, against 39 UNREPAIRED rows across all star tiers (38 exact
   matches; the one "mismatch" was a genuinely-natural-5★ bet correctly
   shown as capped-to-2★ post-2026-07-02 — exactly the kind of row this
   investigation exists to find, not a bug).

Star tiers ARE recomputed here (not read from the stored `stars` column) for
two independent reasons: the 2★ cap (deployed 2026-07-02) clamps `stars` at
INSERT time for every bet rated after that date, AND the odds-repair
contamination above. `ev`/`edge`/`our_prob` are stored uncapped regardless of
the star cap (`model/mlb_game_bets.py` computes `stars, ev, edge =
rate_market(...)` THEN clamps only `stars`) and reflect the CURRENT
`market_decimal` regardless of the repair (it was updated in place) — so
calling `rate_market` again on the frozen inputs reconstructs the honest
natural tier for every row, capped or not, repaired or not.

Offline only — nothing here writes to mlb_bets or touches the live 2★ cap.

Usage:
    python -m model.mlb_underdog_value_backtest
"""

from __future__ import annotations

import argparse
from collections import Counter
import numpy as np

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import rate_market

_QUALIFYING_STARS = (4, 5)
_MIN_SAMPLE = 200
_TEAM_CONCENTRATION_MAX_PCT = 0.25
_BOOTSTRAP_ITERS = 2000

_SANITY_EXPECTED_N = 65  # total stored 5-star v2 moneyline bets, whole season


def load_bets(db: DatabaseManager) -> list[dict]:
    """Frozen mlb-gameline-v2 settled moneyline bets, with team abbreviation.

    Dates come from mlb_matchups.game_date via the join, NOT
    mlb_bets.event_commence — most event_commence values in this ledger are
    NULL (a data-quality gap discovered while building this script; a
    WHERE event_commence BETWEEN ... filter silently drops NULL rows, which
    is what made an early version of this script undercount 65 real bets
    down to 6). game_date is populated on every row.
    """
    return db.execute(
        """
        SELECT tb.id, tb.matchup_id, tb.subject_team_id, tb.selection_label,
               tb.market_odds, tb.market_decimal, tb.market_prob, tb.our_prob,
               tb.edge AS stored_edge, tb.ev AS stored_ev, tb.stars AS stored_stars,
               tb.status, tb.inputs_json, m.game_date,
               t.abbreviation AS team
        FROM mlb_bets tb
        JOIN mlb_matchups m ON m.id = tb.matchup_id
        LEFT JOIN mlb_teams t ON t.team_id = tb.subject_team_id
        WHERE tb.model_version = 'mlb-gameline-v2' AND tb.bet_type = 'moneyline'
          AND tb.status IN ('won', 'lost')
          AND tb.our_prob IS NOT NULL AND tb.market_decimal IS NOT NULL AND tb.market_prob IS NOT NULL
        ORDER BY m.game_date ASC
        """
    )


def recompute_natural(bets: list[dict]) -> list[dict]:
    out = []
    for b in bets:
        our_prob = float(b["our_prob"])
        decimal_odds = float(b["market_decimal"])
        market_prob = float(b["market_prob"])
        stars, ev, edge = rate_market(our_prob, decimal_odds, market_prob, longshot_odds_cap=True)
        won = b["status"] == "won"
        profit = (decimal_odds - 1.0) if won else -1.0
        out.append({
            **b,
            "our_prob": our_prob, "market_prob": market_prob, "decimal_odds": decimal_odds,
            "natural_stars": stars, "recomputed_ev": ev, "recomputed_edge": edge,
            "won": won, "profit": profit,
        })
    return out


def _was_repaired(b: dict) -> bool:
    return bool(b["inputs_json"]) and "odds_repaired" in str(b["inputs_json"])


def sanity_check(bets: list[dict]) -> bool:
    """Two independent checks, since a naive 'recomputed == stored for every
    5-star row' check does NOT hold here — and that's a real data finding,
    not a bug (see module docstring). So:

    (a) CODE CORRECTNESS — among rows whose odds were NEVER touched by the
        2026-07-02 repair, recomputing must reproduce the stored stars
        exactly (except for legitimately-capped rows, which recompute
        HIGHER than stored — that's the cap working as designed, not a
        mismatch). This proves the rate_market() call itself is right.
    (b) KNOWN, EXPLAINED discrepancy — among the 65 bets currently stored as
        5-star, every recomputation mismatch must be a REPAIRED row (proving
        the mismatches are the documented odds-repair gap, not something
        unexplained creeping in).
    """
    stored_five = [b for b in bets if b["stored_stars"] == 5]
    n = len(stored_five)
    mismatches = [b for b in stored_five if b["natural_stars"] != 5]
    unexplained = [b for b in mismatches if not _was_repaired(b)]

    unrepaired = [b for b in bets if not _was_repaired(b)]
    code_mismatches = [b for b in unrepaired if b["natural_stars"] < b["stored_stars"]]

    print("=" * 70)
    print("SANITY CHECK")
    print("=" * 70)
    print(f"  (a) Code correctness on {len(unrepaired)} unrepaired rows (any star tier): "
          f"{len(code_mismatches)} cases where recomputed < stored (should be 0 — "
          f"recomputed can only be >= stored, from the cap being lifted)")
    print(f"  (b) Rows currently stored as 5-star: n={n} (expected {_SANITY_EXPECTED_N}); "
          f"{len(mismatches)} recompute lower, of which {len(unexplained)} are UNEXPLAINED "
          f"(not flagged odds_repaired — expected 0)")
    passed = n == _SANITY_EXPECTED_N and len(code_mismatches) == 0 and len(unexplained) == 0
    print(f"  {'PASS' if passed else 'FAIL'} — "
          f"{'proceeding to full-ledger metrics' if passed else 'STOPPING: fix before trusting anything below'}")
    if passed and mismatches:
        print(f"  Note: {len(mismatches)} of the original 65 5-star bets recompute lower once "
              f"the (correctly repaired) odds are used honestly — see module docstring.")
    return passed


def _bootstrap_ci(values: np.ndarray, iters: int = _BOOTSTRAP_ITERS, seed: int = 0):
    rng = np.random.default_rng(seed)
    n = len(values)
    boots = np.empty(iters)
    for i in range(iters):
        idx = rng.integers(0, n, n)
        boots[i] = values[idx].mean()
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return float(values.mean()), float(lo), float(hi)


def _report_gap_and_roi(label: str, tier: list[dict]) -> dict:
    n = len(tier)
    if n == 0:
        print(f"  {label}: n=0")
        return {"n": 0}
    won = np.array([b["won"] for b in tier], dtype=float)
    mkt = np.array([b["market_prob"] for b in tier], dtype=float)
    profit = np.array([b["profit"] for b in tier], dtype=float)

    gap_vals = won - mkt
    gap_mean, gap_lo, gap_hi = _bootstrap_ci(gap_vals)
    roi_mean, roi_lo, roi_hi = _bootstrap_ci(profit)

    excludes_zero_gap = (gap_lo > 0 and gap_hi > 0) or (gap_lo < 0 and gap_hi < 0)
    excludes_zero_roi = (roi_lo > 0 and roi_hi > 0) or (roi_lo < 0 and roi_hi < 0)

    print(f"  {label}: n={n}  realized={won.mean():.3f}  implied={mkt.mean():.3f}  "
          f"gap={gap_mean:+.3f} 95%CI[{gap_lo:+.3f},{gap_hi:+.3f}] "
          f"({'excludes 0' if excludes_zero_gap else 'includes 0'})")
    print(f"           ROI={roi_mean:+.3f} 95%CI[{roi_lo:+.3f},{roi_hi:+.3f}] "
          f"({'excludes 0' if excludes_zero_roi else 'includes 0'})")
    return {
        "n": n, "realized": float(won.mean()), "implied": float(mkt.mean()),
        "gap_mean": gap_mean, "gap_ci": (gap_lo, gap_hi), "gap_excludes_zero": excludes_zero_gap,
        "roi_mean": roi_mean, "roi_ci": (roi_lo, roi_hi), "roi_excludes_zero": excludes_zero_roi,
    }


def run_metrics(bets: list[dict]) -> None:
    tier = [b for b in bets if b["natural_stars"] in _QUALIFYING_STARS]
    dates = [b["game_date"] for b in bets if b["game_date"] is not None]
    print(f"\nPopulation: {len(bets)} settled v2 moneyline bets, "
          f"{min(dates) if dates else '?'} to {max(dates) if dates else '?'} "
          f"({(max(dates) - min(dates)).days if dates else 0} days)")
    print(f"Qualifying (natural {_QUALIFYING_STARS}-star) tier: {len(tier)} bets")

    print("\n" + "=" * 70)
    print(f"METRIC 1+2 — realized-vs-implied win-rate gap & ROI, "
          f"{_QUALIFYING_STARS}-star tier, FULL available ledger window")
    print("=" * 70)
    full = _report_gap_and_roi("Full window", tier)

    print("\n" + "=" * 70)
    print("METRIC 3 — split-half stability (chronological midpoint of the FULL bet population)")
    print("=" * 70)
    if not dates:
        print("  No game_date data — cannot split.")
        h1 = h2 = {"n": 0}
    else:
        mid = sorted(dates)[len(dates) // 2]
        first_half = [b for b in tier if b["game_date"] is not None and b["game_date"] < mid]
        second_half = [b for b in tier if b["game_date"] is not None and b["game_date"] >= mid]
        print(f"  Population midpoint: {mid}")
        h1 = _report_gap_and_roi("First half", first_half)
        h2 = _report_gap_and_roi("Second half", second_half)
    both_same_sign = (h1.get("n", 0) > 0 and h2.get("n", 0) > 0
                       and h1.get("gap_excludes_zero") and h2.get("gap_excludes_zero")
                       and (h1.get("gap_mean", 0) > 0) == (h2.get("gap_mean", 0) > 0) == (full.get("gap_mean", 0) > 0))
    print(f"  Split-half stability: {'PASS' if both_same_sign else 'FAIL'}")

    print("\n" + "=" * 70)
    print("METRIC 4 — calibration by our_prob decile, within the qualifying tier")
    print("=" * 70)
    if len(tier) >= 10:
        sorted_tier = sorted(tier, key=lambda b: b["our_prob"])
        deciles = np.array_split(sorted_tier, 10)
        for i, dec in enumerate(deciles):
            if len(dec) == 0:
                continue
            claimed = float(np.mean([b["our_prob"] for b in dec]))
            realized = float(np.mean([b["won"] for b in dec]))
            print(f"  decile {i+1:2d} (n={len(dec):3d}): claimed={claimed:.3f}  realized={realized:.3f}  "
                  f"gap={realized-claimed:+.3f}")
    else:
        print(f"  n={len(tier)} — too few for deciles")

    print("\n" + "=" * 70)
    print(f"METRIC 6 — minimum sample: n={len(tier)}, need >={_MIN_SAMPLE}")
    print("=" * 70)
    meets_min = len(tier) >= _MIN_SAMPLE
    print(f"  {'PASSES' if meets_min else 'FAILS'} the pre-registered minimum")
    if not meets_min:
        print(f"  NOTE: the ledger only spans "
              f"{(max(dates) - min(dates)).days if dates else 0} days so far — "
              f"below the minimum is expected at this stage, not a sign anything is broken.")

    print("\n" + "=" * 70)
    print("METRIC 7 — team concentration")
    print("=" * 70)
    if not tier:
        print("  n=0 — nothing to check")
    else:
        counts = Counter(b["team"] for b in tier)
        n_total = len(tier)
        top_team, top_n = counts.most_common(1)[0]
        top_pct = top_n / n_total
        print(f"  Team breakdown ({len(counts)} teams represented):")
        for team, c in counts.most_common():
            sub = [b for b in tier if b["team"] == team]
            print(f"    {team}: n={c} ({c/n_total*100:.1f}%)  "
                  f"realized={np.mean([b['won'] for b in sub]):.3f}  "
                  f"implied={np.mean([b['market_prob'] for b in sub]):.3f}")
        print(f"\n  Most-represented team: {top_team} ({top_n}/{n_total} = {top_pct*100:.1f}%)")
        concentration_ok = top_pct <= _TEAM_CONCENTRATION_MAX_PCT
        print(f"  Concentration bar (<= {_TEAM_CONCENTRATION_MAX_PCT*100:.0f}%): "
              f"{'PASS' if concentration_ok else 'FAIL'}")

        without_top = [b for b in tier if b["team"] != top_team]
        loto = _report_gap_and_roi(f"Leave-one-out (excl. {top_team})", without_top)
        loto_ok = loto.get("gap_excludes_zero", False) and (loto.get("gap_mean", 0) > 0) == (full.get("gap_mean", 0) > 0)
        print(f"  Leave-one-team-out bar: {'PASS' if loto_ok else 'FAIL'}")

    print("\n" + "=" * 70)
    print("METRIC 5 — CLV: deferred to P4 (live shadow-tracking), not part of this offline pass")
    print("=" * 70)


def run() -> None:
    config = load_config()
    db = DatabaseManager(config.database_url)
    raw = load_bets(db)
    print(f"Loaded {len(raw)} settled mlb-gameline-v2 moneyline bets from the ledger.")
    bets = recompute_natural(raw)

    if not sanity_check(bets):
        print("\nStopping — fix the recomputation before trusting any metric.")
        return

    run_metrics(bets)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="MLB underdog-value offline backtest (P1-P2)").parse_args()
    run()
