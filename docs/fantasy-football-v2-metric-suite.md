# Fantasy Football V2 definitive metric suite

**Version:** `ff-v2-metrics-v1`
**Status:** Preregistered validation contract; V2 remains shadow-only

`model/ff_v2_metrics.py` is the executable V2-005 contract. It consumes frozen
rolling-origin artifacts from `model/ff_v2_backtest.py`; it does not fit a
model or activate a projection.

## Frozen scoring policy

- Team opportunity reports MAE, signed bias, P10/P50/P90 empirical coverage,
  and the proper P10/P50/P90 weighted interval score for plays, pass attempts,
  allocatable targets, rush attempts, RB carries, RB targets, passing TDs, and
  rushing TDs.
- Player season totals report PPR Week 1–17 MAE and bias. Top-12 precision and
  recall use the draftable QB/RB/WR/TE universe within each season-position,
  `min(12, n)`, and stable player-ID tie breaking.
- Weekly spike recall is position-aware: QB 25, RB 20, WR 20, and TE 15 PPR
  points. A predicted spike requires probability at least 0.30.
- Best Ball counted points and non-negative oracle decision regret sum only
  Weeks 1–17 within each historical draft instance. Week 18 is never a product
  outcome.
- Small sample means at most five prior NFL games. Required overlapping slices
  are overall, QB/RB/WR/TE, rookie, changed-team, injury-affected, and small
  sample.

## Models and missing eligibility

The report always preserves separate labels for `champion`, the generic player
`simple_baseline`, team `simple_baseline:rolling_average` and
`simple_baseline:league_average`, `market_baseline`, `challenger`, and schedule,
correlation, availability, and roster-fit ablations. A market score is eligible only from an immutable
preseason snapshot available by the fold cutoff. A market point forecast does
not receive invented quantiles. Every absent label or cohort is emitted with
`n=0`, `eligible=false`, and a reason. In particular, historical Best Ball and
draft-decision metrics remain visibly zero-sample until real replay artifacts
exist.

## Historical market-baseline cohort

[`../artifacts/ff_v2_historical_bestball_cohort_2020_2025.json`](../artifacts/ff_v2_historical_bestball_cohort_2020_2025.json)
provides 72 nonzero counterfactual roster-seasons: 12 focal draft seats for
each season from 2020 through 2025, scored under exact DraftKings rules for
Weeks 1-17. Its digest is
`36f76b660edc88961b2234a9740a263de2d52671347e3cb9bf7d46bcfb954d4f`.
It records source hashes, 97.09%-99.44% eligible-player outcome-match coverage,
weekly maximum legal lineups, counted points and weeks, and conservative
same-position ex-post pick regret.

These are deterministic **synthetic focal-seat ADP baselines**, not observed
human drafts or full reconstructed rooms. Historical FFC boards contain only
146-206 eligible QB/RB/WR/TE rows, so the builder does not invent the missing
opponent selections needed for a 240-player room. The cohort is valid baseline
evidence and removes the all-zero outcome gap, but it cannot establish how real
historical drafters behaved. Champion/challenger roster comparisons remain
ineligible until their own predictions are replayed against this same frozen
cohort.

## Paired uncertainty

Every challenger comparison uses only exact shared artifact identities and a
deterministic 2,000-draw percentile paired bootstrap. Resampling units are game
for team opportunity, player-week for weekly metrics, player-season for season
errors, evaluation season for Top-12 metrics, and draft instance for Best Ball
and regret. Fewer than two paired units produces an explicit ineligible
interval rather than a false precision claim.

Frozen calibration tolerances are five percentage points for P10/P50/P90
coverage and five PPR points for overall season bias. Material-degradation
bounds are 2% for team MAE/WIS, 1% for season MAE, two percentage points for
Top-12/spike recall, 1% downside for counted points, and 1% upside for regret.
These thresholds must not change after V2 fitting without a versioned protocol
change.
