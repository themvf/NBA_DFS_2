"""P2 of the Tennis Recency/Fatigue Model spec (CLAUDE.md): grid-search the
_RecencyElo half-life and freeze H1's form-gap subset — BEFORE any test of H1
itself (that's P3).

Deliberately stops short of evaluating whether the recency signal or the
form-gap subset predicts anything against the market. That's P3. Blurring
the two would mean the same data selects the hyperparameter AND grades the
hypothesis it enables — exactly the kind of leakage this project's discipline
exists to prevent (see CLAUDE.md "Non-negotiables").

What this script actually does:
  1. Reconstructs winner/loser per match from the existing corpus's
     fav_name/dog_name/y (already point-in-time labels — no new leakage).
  2. Replays _RecencyElo chronologically per tour, once per half-life
     candidate.
  3. Picks the half-life that minimizes standalone logloss of the recency-
     Elo-implied probability against actual outcomes — but ONLY on a TUNING
     period (matches before TUNING_CUTOFF). This is ordinary hyperparameter
     selection (choosing which version of a rating system to use), not a
     test of H1's market-disagreement claim.
  4. Freezes the "top-quartile-by-|form_gap|" subset definition using ONLY
     the tuning period's |form_gap| distribution (the 75th-percentile cutoff),
     then reports — descriptively, no outcome evaluation — how many matches
     in the RESERVED period (on/after TUNING_CUTOFF, untouched until P3)
     would qualify, to confirm P3 will clear the ≥200-match minimum.

Usage:
    python -m model.tennis_recency_calibration
"""

from __future__ import annotations

import math
from datetime import datetime

import pandas as pd

from ingest.tennis_training import _RecencyElo, _surname_initial_key, load_corpus

_HALF_LIFE_CANDIDATES = [60.0, 90.0, 180.0, 365.0, 730.0]
# Matches before this date: half-life + form-gap threshold selection (tuning).
# Matches on/after: RESERVED for P3's actual H1 test — never touched here.
_TUNING_CUTOFF = "2022-01-01"
_FORM_GAP_QUANTILE = 0.75  # top-quartile-by-|form_gap|


def _implied_prob(diff: float) -> float:
    """Standard Elo win-probability transform for a rating difference."""
    return 1.0 / (1.0 + 10 ** (-diff / 400.0))


def _logloss(p: float, y: int, eps: float = 1e-6) -> float:
    p = min(max(p, eps), 1 - eps)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def _replay_recency(df: pd.DataFrame, half_life_days: float) -> pd.Series:
    """Pre-match recency_elo_diff (fav-minus-dog), replayed chronologically
    per tour. Winner/loser reconstructed from fav_name/dog_name/y — those are
    already point-in-time labels from Phase 1, so this adds no leakage."""
    out = pd.Series(index=df.index, dtype=float)
    for _tour, g in df.groupby("tour"):
        g = g.sort_values("date")
        recency = _RecencyElo(half_life_days)
        for idx, row in g.iterrows():
            fav_key = _surname_initial_key(row["fav_name"])
            dog_key = _surname_initial_key(row["dog_name"])
            if not fav_key or not dog_key:
                out.loc[idx] = float("nan")
                continue
            d = datetime.strptime(row["date"], "%Y-%m-%d")
            fav_rating = recency.snapshot(fav_key, d)
            dog_rating = recency.snapshot(dog_key, d)
            out.loc[idx] = fav_rating - dog_rating
            winner_key, loser_key = (fav_key, dog_key) if row["y"] == 1 else (dog_key, fav_key)
            recency.update(winner_key, loser_key, d)
    return out


def run() -> None:
    df = load_corpus()
    tuning_mask = df["date"] < _TUNING_CUTOFF
    reserved_mask = ~tuning_mask
    print(f"Corpus: {len(df)} matches | tuning (<{_TUNING_CUTOFF}): {tuning_mask.sum()} | "
          f"reserved (>= {_TUNING_CUTOFF}, untouched until P3): {reserved_mask.sum()}")

    # Baseline for context: flat elo_diff's own tuning-period logloss.
    flat_probs = df.loc[tuning_mask, "elo_diff"].apply(_implied_prob)
    flat_loss = sum(_logloss(p, y) for p, y in zip(flat_probs, df.loc[tuning_mask, "y"])) / tuning_mask.sum()
    print(f"\nFlat elo_diff tuning-period logloss (reference, not a candidate): {flat_loss:.4f}")

    print("\nGrid search (tuning period only):")
    results: list[tuple[float, float]] = []
    diffs_by_hl: dict[float, pd.Series] = {}
    for hl in _HALF_LIFE_CANDIDATES:
        diffs = _replay_recency(df, hl)
        diffs_by_hl[hl] = diffs
        valid = diffs.notna() & tuning_mask
        probs = diffs[valid].apply(_implied_prob)
        loss = sum(_logloss(p, y) for p, y in zip(probs, df.loc[valid, "y"])) / valid.sum()
        results.append((hl, loss))
        for tour in ("ATP", "WTA"):
            tour_valid = valid & (df["tour"] == tour)
            tour_probs = diffs[tour_valid].apply(_implied_prob)
            tour_loss = (sum(_logloss(p, y) for p, y in zip(tour_probs, df.loc[tour_valid, "y"]))
                         / tour_valid.sum())
            print(f"  half_life={hl:6.0f}d  {tour}: logloss={tour_loss:.4f} (n={int(tour_valid.sum())})")
        print(f"  half_life={hl:6.0f}d  BOTH: logloss={loss:.4f} (n={int(valid.sum())})")

    best_hl = min(results, key=lambda r: r[1])[0]
    best_loss = min(r[1] for r in results)
    print(f"\nChosen half-life: {best_hl:.0f} days (tuning logloss {best_loss:.4f}, "
          f"vs flat-Elo reference {flat_loss:.4f})")

    best_diffs = diffs_by_hl[best_hl]
    form_gap = best_diffs - df["elo_diff"]
    tuning_form_gap = form_gap[tuning_mask & best_diffs.notna()]
    threshold = tuning_form_gap.abs().quantile(_FORM_GAP_QUANTILE)
    print(f"Frozen |form_gap| threshold (tuning-period {_FORM_GAP_QUANTILE:.0%}ile): {threshold:.1f} Elo points")

    reserved_valid = reserved_mask & best_diffs.notna()
    reserved_form_gap = form_gap[reserved_valid]
    qualifying = reserved_form_gap.abs() >= threshold
    qual_df = df.loc[reserved_valid][qualifying.values]
    print(f"\nReserved-period qualifying subset (|form_gap| >= {threshold:.1f}, "
          f"untouched until P3): {len(qual_df)} matches")
    for tour, g in qual_df.groupby("tour"):
        print(f"  {tour}: {len(g)} matches, {g['date'].min()} to {g['date'].max()}")
    print(f"\n{'PASSES' if len(qual_df) >= 200 else 'FAILS'} the >=200-match minimum "
          f"pre-registered for P3.")


if __name__ == "__main__":
    run()
