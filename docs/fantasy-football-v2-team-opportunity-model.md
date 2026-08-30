# Fantasy Football V2 Team Opportunity Model

**Version:** `ff-v2-team-opportunity-v1`
**Calibration:** `uncalibrated-shadow-v1`
**Status:** shadow engine implemented; historical fit blocked; not active

This is the smallest interpretable V2-008 engine. It produces weekly
team-opportunity distributions through the V2-007 persistence contract. The
engine and its synthetic/adversarial tests are push-safe, but no historical fit
is accepted yet: the exact stored play-by-play and weekly-stat snapshots used by
the diagnostic reconstruction became available after the simulated 2025
preseason cutoff. The CLI now rejects those snapshots rather than treating game
completion time as source availability. Calibration and superiority comparisons
remain V2-009.

## Chronological feature boundary

For evaluation season `Y`, training contains only V2-003 facts from seasons
before `Y` whose `observed_at` is at or before the frozen V2-004 preseason
cutoff, and every exact consumed source snapshot must have `available_at` no
later than that cutoff. The held-out fact supplies only season, week, game,
date, team, and opponent identity before the forecast is frozen. Its opportunity
counts and result context are not features.

Historical quarterback fields identify actual game starters from final
schedules and are not preseason-safe. Historical play-caller coverage is null.
The canonical reconstruction therefore marks both fields missing with
`no_eligible_as_of_source` and uses Tier B. Head coach and realized score-state
features are also excluded. Tier A is supported only when independently sourced
QB and play-caller values carry availability timestamps no later than the
cutoff; it has zero canonical historical rows and is not yet validated.

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
- **C:** fewer than four eligible team or opponent games; league-prior forecast,
  confidence `0.60`, latent uncertainty scale `1.70`.

The scale applies monotonically to shared play-volume and touchdown-rate latent
variance. All child pools inherit those parent draws. Because counts are
discrete and bounded subsets, a particular child's empirical P10–P90 width may
tie across adjacent tiers; the contract does not falsely require every child
quantile width to increase strictly. Tests demonstrate strict widening for the
continuous parent play-volume pool and verify the stored scales satisfy
`scale >= 1 / confidence` for Tier C.

## Diagnostic reconstruction (not accepted evidence)

Before the source-availability defect was found, a representative 2025
diagnostic reconstruction was generated. It is intentionally not committed as a
canonical artifact and cannot close V2-008.

| Field | Value |
|---|---|
| Run ID | `c6721951-dccd-5702-82c0-de8cc4d83b29` |
| Diagnostic digest | `f82a3e270c86db407e6dd8b1c488a1c838149442df3dd8ebc3e0dc22618a7980` |
| Evaluation season | 2025 |
| Training seasons | 2020–2024 |
| Forecasts | 544 |
| Distributions | 4,352 |
| Draws per team-game | 4,000 |
| Feature snapshots | 21 |
| Fallback cohorts | 544 Tier B; 0 Tier A; 0 Tier C |
| File size | 11,263,275 bytes |

All 544 diagnostic rows are Tier B because neither historical quarterback nor
play-caller identity has a qualifying preseason source. This is an explicit
limitation, not missingness hidden by an aggregate.

The next eligible artifact must freeze the root seed, identity seeds, draw
count, cutoff, training seasons and digest, source snapshot IDs and availability,
feature missingness, shrinkage evidence, fallback tier, distributions, and
V2-007 artifact/run digests. Until eligible immutable snapshots are located,
the historical CLI fails closed before fitting.

No runtime redraft, Best Ball, advisor, ranking, or display path reads this
artifact.
