# DFS scenario and contest implementation evidence

The production v5 projections and saved lineups were not changed. The final
opponent comparison remains run `2d0db3bc8ce8c812b1fcd0a6cb3a78765ea1060d3baf4073d1765544315b4aba`,
paired with baseline `3ef96f66-7a8c-5070-a41e-6ae59b5712b0`.

## Implemented

- Audited original-seed production score banks, with missing/unreproduced
  forecasts excluded. Saved mean, P10/P50/P90 and per-stat means must reproduce
  within 0.00011 points/stat units. Independent fallback is explicitly marked
  and cannot support joint ceiling, field-win or ROI claims.
- Separately registered `nfl-coherent-matchup-research-v2`: shared empirical
  game opportunities, exact passing/receiving and opportunity accounting,
  opponent-derived sacks/turnovers/DST scores, shared FG/PAT kicker events,
  explicit unallocated roles/events, and canonical Showdown Captain scoring.
  This model changes marginal distributions and is unqualified research.
- Distinct selection/evaluation streams; legal equal-count portfolio comparison
  for single-entry, three-entry and 20-entry multi-entry examples; exact tie
  prize splitting when a qualified complete field and payout configuration are
  supplied. Missing field/payout data yields construction-only comparisons.
- Immutable source/config/code linkage, separate Classic and Showdown forward
  cohorts, original-baseline P25/P75 only from audited saved-draw reproduction,
  and 365 complete paired grading rows for today's research capture.
- Strict post-lock archive grading using previously imported contest files;
  missing actuals are withheld, CPT points are not multiplied twice, and a
  post-lock saved optimizer run is excluded.
- Optional shadow integration retains the exact production result when research
  inputs are unavailable. The hosted report publisher/reader and recurring
  wrapper are implemented by the companion integration slices.

## Final artifacts

Final manual v2 banks, summaries, ledgers and verification are under
`coherent-v2/final/`. The final compact report is
`coherent-v2/final/coherent-scenario-summary.json`; its matching portfolio report
is `coherent-v2/final/portfolio-coherent-comparison.json`.

The final v2 registration is `research/nfl_coherent_scenario_study_v2.json`,
registered at `2026-09-27T15:29:37.273146+00:00`, raw SHA256
`4adec3c62e44d07c81d30b91b6a37964110f7cccb740578da74d334860de35ec`.
The baseline hash explicitly covers the `{model_version,model_config}` envelope.
Earlier v1 files and pre-forward registration-convention corrections remain in
`coherent-v1/` and `coherent-v2/pin-audit/`; they are not pooled into v2 evidence.

## Verification

- Classic: 423 modeled players, 848 paired historical games, 300 draws per
  stream; exact Python/TypeScript score agreement within 2.5e-14. All 26
  conditional current-starter/lead-receiver pairs have positive dependence.
- Real saved NYG–LAR Showdown: 33 modeled players, 832 pre-target historical
  games, 300 draws per stream. Both kickers use the shared game ledger;
  canonical Captain salary and the 1.5 score multiplier pass. This is explicitly
  a retrospective mechanical replay, not a forward performance test.
- Three distinct saved pre-lock slates: one 670-player Classic pool and two
  53-player Showdown pools each produce 20 legal unique lineups with unchanged
  production results when optional research inputs are missing.
- 23 focused Python tests pass, matchup contest TypeScript tests pass, and the
  full web typecheck passes.
- v1/v2 Classic slate, optimizer inputs and every per-player statistic in all
  600 draws are exactly equal. Fixed candidate portfolios were independently
  rescored under the new manifest; expensive optimizer search was not repeated
  for this manual replay. The recurring wrapper also runs the full path.

## Keep the comparisons separate

`portfolio-attribution.json` and `.md` use the same inspected 29-lineup union in
both arms. Simulated average best portfolio score under policy-only versus
policy plus PFR is 156.67 vs 156.75 for one entry, 172.17 vs 172.06 for three,
and 186.71 vs 186.76 for twenty. This differs from the earlier 24-new-candidate
search. Large reselection changes are primarily a selection-policy effect;
they cannot be described as gains caused by PFR.

The separate coherent model gives 107.56 vs 146.71, 120.15 vs 159.21, and
140.73 vs 170.18 for the current/reselected portfolios under that same research
model. These are conditional construction sensitivities from a changed,
uncalibrated workload model, not increases in approved projections or win/ROI
estimates.

## Archived evaluation and remaining data gaps

`archived-contest-grades.json` reconciles the exact source hashes of contests
195648006 (317,082 entries, 846 observed player-slot ownership rows) and
195786073 (47,562 entries, 89 ownership rows). All 80 pre-lock saved lineups are
scored. Best scores are 137.82 for the Classic 20-entry portfolio, and 100.77 /
110.77 for the Showdown 20-/40-entry portfolios. A later post-lock Classic run
is excluded. These inspected outcomes are evaluation-only and never enter
pregame projections or today's ownership estimates.

Today's contest entry fees, payout tables, complete calibrated rival fields and
prospectively qualified ownership forecasts are still unavailable. Conditional
QB/K roles do not estimate injury or benching probabilities. Full sequential
possessions and designed-run/scramble decomposition are not supplied. Forward
performance remains NO_VERDICT until the registered weeks, sample sizes,
distribution accuracy and separate tournament gates pass.
