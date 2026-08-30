# Fantasy Football V2 Team Opportunity Model

**Version:** `ff-v2-team-opportunity-v1`
**Calibration:** `uncalibrated-shadow-v1`
**Status:** eligible shadow fit implemented; awaiting calibration; not active

This is the smallest interpretable V2-008 engine. It produces weekly
team-opportunity distributions through the V2-007 persistence contract. The
accepted shadow reconstruction trains on exact archived 2020-2021 play-by-play
and roster bytes published before the applicable simulated cutoffs. Current
nflverse release replacements still fail the cutoff gate. Calibration, baseline
comparison, and superiority claims remain V2-009.

## Chronological feature boundary

For evaluation season `Y`, training contains only archived facts from seasons
before `Y` whose `observed_at` is at or before the frozen V2-004 preseason
cutoff, and every exact consumed source snapshot must have `available_at` no
later than that cutoff. The held-out V2-003 fact supplies only season, week,
game, date, team, and opponent identity before the forecast is frozen. Its
opportunity counts and result context are not features.

Two immutable archive bundles are available. The 2021 fold can train on 2020;
the 2022-2025 folds can train on 2020-2021. No later historical season is
silently substituted. Each bundle verifies file SHA-256, Git blob identity where
applicable, row counts, all 32 teams, and zero unknown rusher/receiver positions.

Historical quarterback fields identify actual game starters from final
schedules and are not preseason-safe. Historical play-caller coverage is null.
The canonical reconstruction therefore marks both fields missing with
`no_eligible_as_of_source`. Because weekly stats, participation, schedule,
transactions, quarterback, and play-caller sources are also unavailable in the
archive, an explicit minimum Tier C is enforced even when team/opponent samples
would otherwise estimate Tier B. Head coach and realized score-state features
are excluded. Tier A is supported only when independently sourced QB and
play-caller values carry availability timestamps no later than the cutoff; it
has zero canonical historical rows and is not yet validated.

ADP, ECR, rankings, consensus, market data, current rosters, and held-out
outcomes never enter the model.

## Interpretable estimation

The parameter set is plays, dropback share, sack share, target-per-attempt
share, RB carry share, RB target share, total offensive touchdowns, and passing
touchdown share. Each begins at a recency-weighted league prior. Team, opponent,
quarterback, and play-caller evidence is shrunk back to that prior using explicit
effective samples and prior strengths of 8, 12, 20, and 28 games. The artifact
records raw means, priors, effective samples, reliability, shrunk means, and the
resulting estimate for every parameter.

## Joint game scripts

Each game receives one deterministic seed and shared pace, scoring, and script
draws. The two opponents receive complementary leading/neutral/trailing states
with declared probabilities 25%/50%/25%. Each team then uses its own stable
identity seed for this hierarchy:

```text
plays
  -> dropbacks + rush attempts
  -> dropbacks = sacks + pass attempts
  -> pass attempts -> allocatable targets -> RB targets
  -> rush attempts -> RB carries
total offensive touchdowns -> passing + rushing touchdowns
```

Every draw satisfies the equalities and subset constraints. A draw with no pass
or rush attempt cannot produce an offensive touchdown. Expected value,
population standard deviation, P10, P50, and P90 come from the same joint draws;
they are not independently sampled marginals.

## Fallback policy

- **A:** team and opponent history plus eligible QB and play-caller evidence;
  confidence `1.00`, latent uncertainty scale `1.00`.
- **B:** adequate team/opponent history with QB or play caller missing;
  confidence `0.80`, latent uncertainty scale `1.25`.
- **C:** fewer than four eligible team/opponent games or a declared core-source
  gap; league-dominant forecast, confidence `0.60`, latent uncertainty scale
  `1.70`.

The scale applies monotonically to shared play-volume and touchdown-rate latent
variance. All child pools inherit those parent draws. Because counts are
discrete and bounded subsets, a particular child's empirical P10–P90 width may
tie across adjacent tiers; the contract does not falsely require every child
quantile width to increase strictly. Tests demonstrate strict widening for the
continuous parent play-volume pool and verify the stored scales satisfy
`scale >= 1 / confidence` for Tier C.

## Canonical 2025 shadow reconstruction

The accepted representative reconstruction separates its eligible archived
training run from the current historical context run used only for held-out game
identity and evaluation. It remains shadow-only.

| Field | Value |
|---|---|
| Run ID | `ea73a8c7-65db-593a-b237-8c40ee645bfd` |
| Artifact digest | `587499356e7aedd0e979861b2bdf5f992d2c6be9c1ef9044640e51ed29f27b8d` |
| Evaluation season | 2025 |
| Training context run | `61cfdc92-3aac-5e3b-ab1d-2d0cbc7a063c` |
| Training seasons | 2020–2021 |
| Forecasts | 544 |
| Distributions | 4,352 |
| Draws per team-game | 4,000 |
| Feature snapshots | 5 |
| Fallback cohorts | 544 Tier C; 0 Tier A; 0 Tier B |

All 544 rows are Tier C. Every row records the six declared source gaps, the
sample-derived tier, the enforced minimum tier, and the effective uncertainty
scale. Exact rebuild and persisted-row verification reproduce the same run and
artifact digest.

The artifact freezes the root seed, identity seeds, draw count, cutoff, training
context run and seasons, source snapshot IDs and availability, feature
missingness, shrinkage evidence, fallback tier, distributions, and V2-007
artifact/run digests. A payload hash, archive identity, position-coverage, or
cutoff mismatch fails before fitting.

No runtime redraft, Best Ball, advisor, ranking, or display path reads this
artifact.
