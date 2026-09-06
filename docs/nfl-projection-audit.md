# Audited situation adjustments

The NFL workspace (`/dfs/nfl`) now connects supported situation evidence to the experimental **Position workload** optimizer source. The historical source remains the default. Position switches still control QB/RB/WR/TE independently; saved settings without a `situations` field preserve their previous behavior.

## What a player explanation means

The starting baseline is the saved salary slate's `ourProj`, not the player's raw average. The explanation separates:

1. **Baseline snapshot alignment:** the difference between that saved projection and the dated, market-free workload reference. This must not be labeled an injury or recent-performance effect.
2. **Workload model:** QB/RB/TE expose the intercept and standardized baseline, history-count and prior-opportunity regression contributions, when they reconcile to the pinned forecast. WR exposes the reference-to-volume/share model change.
3. **Situation changes:** full-roster reference alignment, reviewed team run/pass profile, reviewed role shares, eligible injury redistribution and reviewed passing-efficiency assumptions. Each marginal change records before/after opportunities, points per opportunity, reason and point delta.
4. **Final estimate:** baseline plus all point deltas. No unsupported situational signal receives an invented coefficient.

The app includes baseline-versus-adjusted bars, a player selector, a calculation ledger, and JSON downloads. The live preview is separate from a completed run. Changing assumptions shows a regeneration notice; the completed run's forecasts and explanations remain frozen.

## Connections and limits

| Input | Optimizer effect |
|---|---|
| Current roster, team, position, kickoff and QB1 evidence | Eligibility checks; reread before generation |
| One officially confirmed inactive WR/TE | With an unchanged, resolved historical QB, redistribute 50% of removed prior target share proportionally among supported remaining full-roster recipients. This is an explicit, uncalibrated hypothesis. |
| Questionable status, partial injury, missing official report | No automatic absence or numeric severity adjustment |
| 2025 coach/caller and game-script profiles | Show provenance/continuity. An explicit reviewed profile assumption can change the team budget. Partial continuity is not treated as a validated current scheme forecast. |
| Reviewed target/carry shares | Apply the full-roster allocation, including players outside the DK pool; reject shares over 100%, unavailable recipients and unsupported fields. Manual shares replace automatic injury redistribution. |
| Rookies and arrivals | Never transfer prior-team shares. Unsupported baseline workload/split remains a disclosed limitation; this release does not manufacture a new rookie forecast. |
| Changed or uncertain QB | Block automatic injury redistribution. A reviewed passing-efficiency assumption can alter supported forecasts; the change is not inferred from the QB's name. |
| Defender injuries and scheme mismatches | Visible as not applied; no learned interaction coefficient exists yet. |

Team assumptions require a reason. Passing-efficiency multipliers are bounded to 0.5–1.5 and explicitly represent an assumed scoring-efficiency change, not additional targets or a provider injury fact. QB rushing remains at baseline; the role editor rejects QB rushing overrides.

## Opportunity and point arithmetic

Use the historical **all-game plays per game**, even when selecting a neutral/leading/trailing profile. Situation-specific play counts are not whole-game counts.

```
dropbacks = plays * (1 - designed_run_rate)
attempts = dropbacks * (1 - scramble_rate - sack_rate)
targets = attempts * target_rate
carries = plays * designed_run_rate + dropbacks * scramble_rate
```

The carry budget includes scrambles because historical carry shares include them. Unknown roles remain unassigned; no extra share is invented to fill a budget.

Point deltas use existing dated, shrunk efficiency rates linked to the same workload dataset, with the shared DK scoring constants:

```
points / target = catch_rate * (1 + receiving_yards_per_reception * .1 + receiving_td_rate * 6)
points / carry = rushing_yards_per_carry * .1 + rushing_td_rate * 6
points / attempt = completion_rate * (passing_yards_per_completion * .04 + passing_td_rate * 4)
                   - interception_rate
point change = (new opportunities - reference opportunities) * points / opportunity
```

RB's existing candidate combines carries and targets. The adapter uses known same-team shares to split that reference; it refuses an unresolved split rather than subtracting unrelated components. Full-roster reference alignment is a separately labeled model change. Passing-efficiency scaling is recorded as equivalent opportunities for the scoring calculation, while actual allocated opportunity counts remain unchanged.

Fumble rates, yardage bonuses and other scoring components remain fixed. **P10/P50/P90 receive the same point delta**; their spread is unchanged. These are uncalibrated scenario range shifts, not a new joint simulation. Changed candidates lose the old boom bonus. Cash/GPP optimization consumes the adjusted lower/upper player estimates; lineup tail sums remain search heuristics.

## Frozen audit

Each run saves raw forecasts, source timestamps, model/run IDs, full-roster team context, coaching/roster/recipe digests, efficiency inputs and sample sizes, settings, derived player explanations and selected lineup projections. Captain projections multiply the explained FLEX estimate by 1.5.

`Download frozen lineup explanations` reads the saved run and lineups from the database, rather than reconstructing them from current controls. From `nfl-dfs-ilp-v5-audited-situations`, `inputDigest` is SHA-256 over recursively key-sorted JSON of `{settings, inputSnapshot, optimizerVersion}`, so it remains verifiable after PostgreSQL JSONB reorders keys.

Verification: `npm run test:nfl-projection-audit`, existing workload/calibration/workspace suites, TypeScript, lint and a production build. The focused suite covers ledger reconciliation, explicit versus uncertain injury evidence, stale/future inputs, QB changes, role constraints, no mutation/compounding, all-position marginal scoring and real solver/CPT consumption. Browser smoke checks use a clearly labeled hypothetical scenario, not a fabricated official injury report.

This release establishes an auditable experimental connection. It does not establish improved tournament returns, calibrate injury/defender effects, implement a new joint-scenario optimizer, or add Kelly sizing.
