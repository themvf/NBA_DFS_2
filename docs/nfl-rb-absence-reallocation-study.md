# NFL RB-room absence reallocation, graded on points (pre-registered 2026-09-28)

Study id `nfl-rb-absence-reallocation-v2`. Registered and committed **before any
2020–2021 outcome was computed**. It follows the carries result of
[`nfl-absence-reallocation-v1`](nfl-absence-reallocation-study.md).
Implementation: `model/nfl_rb_absence_reallocation.py`.

## Why

In v1, handing a missing back's leftover carries to the remaining backs (the
fixed pie) predicted carries clearly better in 2024–2025 (−0.37 carries per
player, CI [−0.52, −0.21]). It did not improve points by MAE (−0.005,
CI [−0.11, +0.10]).

Discovery diagnostics on 2022–2025 (every season v1 has already read, now
declared discovery) found two reasons:

1. **Receiving work moves too.** In a material RB absence, the lead remaining
   back beat his own baseline by +2.53 carries, +0.41 targets and +2.7
   receiving yards; the other backs by +0.76 carries and +0.32 targets. v1
   moved only carries. His rushing TD rate followed his own rate (+0.08 TD on
   +2.5 carries), so no goal-line adjustment is needed.
2. **MAE is the wrong scale for this correction.** The baseline under-projects
   backs in these games by **+1.39 PPR points per player** (lead +2.04).
   Fantasy points are skewed, and MAE rewards the median. So a correct shift
   of the mean hardly moves MAE, while squared error, the proper score for a
   mean forecast, moves a lot. The app's projection is expected points, a mean
   the optimizer adds up.

**Disclosed:** the switch from MAE (v1) to squared error was decided after
seeing the discovery numbers above. The confirmation seasons below are
untouched, so the test of the frozen setting remains out of sample. MAE is
kept as a non-inferiority guard so the switch cannot be a free pass.

## Model (frozen)

For a team game with a **material RB absence** (an established RB/FB/WR/TE on
the week roster with status `INA` or `RES` and a baseline of ≥ 5 carries), two
fixed pies are run with v1's walk-forward machinery (8-game window, half-life
4, ≥ 2 active games to be established, full-strength reserve):

| pie | unit | team budget counts | donors | recipients |
|---|---|---|---|---|
| carries | carries | all non-QB players | absent RB/FB/WR/TE | active established RB/FB |
| RB-room targets | targets | RB/FB only | absent RB/FB | active established RB/FB with a target baseline |

Each pie: `V = φ · min(D, max(0, T̂ − A − R̂))`, split among recipients in
proportion to their own baseline, capped at 4× baseline.

Points: `pred = own PPR baseline + Δcarries × own rushing points per carry +
Δtargets × own receiving points per target` (PPR, DK bonuses excluded; per-unit
rates are the recipient's own weighted history).

**Frozen setting: φ_carries = 0.5, φ_targets = 0.5.** Selected on 2022–2025
points squared error among six settings, before registration:

| φ_carries, φ_targets | discovery points MSE | points MAE | bias (actual − pred) |
|---|---:|---:|---:|
| BASE (no change) | 44.80 | 4.564 | +1.385 |
| 0.5, 0 (v1 carries only) | 41.33 | 4.533 | +0.682 |
| **0.5, 0.5 (frozen)** | **40.54** | **4.544** | **+0.307** |
| 0.5, 1 | 40.96 | 4.604 | −0.045 |
| 1, 0 | 41.68 | 4.613 | +0.044 |
| 1, 0.5 | 42.02 | 4.660 | −0.331 |
| 1, 1 | 43.50 | 4.760 | −0.683 |

Discovery, frozen versus BASE (513 events, 1,293 rows): squared error −4.25
[−6.32, −2.35]; MAE −0.020 [−0.124, +0.083].

## Confirmation data

- nflverse weekly rosters carry gameday-inactive status (`INA`) only from
  **2019**; before that, inactives appear as `ACT` and cannot be separated.
  Every quantity an event depends on (its window, the reserve residuals and
  their own windows) must therefore lie in 2019 or later.
- **Confirmation: 2020 and 2021 regular seasons. Warm-up: 2019** (history
  only, no events graded).
- Neither season has been graded by any absence study. v1 and v2 discovery
  used 2021 games only as history for 2022 events; no 2021 absence event was
  ever scored.
- 2026 weeks with complete data: descriptive only.

## Metrics (confirmation, frozen setting versus BASE)

Event-clustered bootstrap (team games), 10,000 draws, seed 20260928.

- Points squared error delta and points MAE delta, per row.
- Mechanism: carries and RB-room targets squared error deltas.
- Reported, gating nothing: bias, by-season deltas, and v1 carries-only
  (φ 0.5, 0) points deltas for comparison.

## Gate (all required)

- **G1 points squared error:** 95% CI of the delta entirely below 0.
- **G2 points MAE non-inferiority:** 95% CI upper bound below **+0.15**
  points. The margin is set from the confirmation sample's expected precision
  (about ±0.14 at ~260 events). A +0.05 margin would have failed on discovery
  itself (upper bound +0.083) and would act as a hidden kill. +0.15 still
  rejects the kind of harm v1's additive transfer did (+0.63 in week 2).
- **G3 mechanism:** carries and RB-room-targets squared-error deltas both
  below 0 (point estimates).
- **G4 sample:** ≥ 200 events and ≥ 480 recipient rows.

Verdicts: `PROMOTE`, `NOT_PROMOTED` (G4 passes, another gate fails),
`INSUFFICIENT` (G4 fails).

## What a verdict licenses

- **PROMOTE** licenses a *live shadow* on the NFL DFS slate. For RB absences
  the slate ruled out, the adjusted estimate is computed and shown next to the
  baseline, then graded against DraftKings results each week. It does **not**
  change optimizer projections. That switch is a separate, explicit decision
  after forward evidence, per this repo's shadow-first rule for projection
  changes.
- **NOT_PROMOTED / INSUFFICIENT:** v3 behaviour stays (baseline retained).
  No retuning of φ, thresholds, window or metric on 2020–2021. A further
  variant is a new registration on data neither study has graded.

## Result (run 2026-09-28, after registration commit eafe710)

No deviations from the registration. Reproduce with
`python -m model.nfl_rb_absence_reallocation study`; full output and source
digests in `artifacts/nfl_rb_absence_reallocation_v2.json`.

**Verdict: NOT_PROMOTED.** Baselines stay for RB absences (v3 behaviour).

Confirmation 2020–2021, frozen setting (φ 0.5 / 0.5) versus BASE, 334 events,
946 rows:

| measure | delta | 95% CI | gate |
|---|---:|---|---|
| points squared error | −1.28 | [−3.66, +1.18] | G1 ✗ |
| points MAE | +0.107 | [−0.027, +0.248] | G2 ✗ (upper > +0.15) |
| carries squared error | −5.68 | [−7.52, −3.90] | G3 ✓ |
| RB-room targets squared error | −0.18 | [−0.38, +0.01] | G3 ✓ |
| sample | 334 events / 946 rows | | G4 ✓ |

| setting | points MSE | points MAE | bias (actual − pred) |
|---|---:|---:|---:|
| BASE | 43.90 | 4.663 | +1.250 |
| v1 carries only (0.5, 0) | 42.60 | 4.694 | +0.508 |
| v2 frozen (0.5, 0.5) | 42.62 | 4.770 | +0.060 |

By season (points squared error): 2020 −1.64 [−5.02, +1.42], 2021 −0.91
[−4.36, +2.64]. 2026 weeks 1–3 (15 events, 29 rows, descriptive): frozen
worse, MSE 37.2 versus 25.1.

### What it means

- **The carries allocation is real.** It is now confirmed in three separate
  periods: 2022–23 discovery, 2024–25 (v1) and 2020–21 (v2), with carries MAE
  −0.26 [−0.37, −0.14] here.
- **The baseline's average under-projection is real too.** Backs in
  RB-absence games beat their baseline by +1.25 points in 2020–21 and +1.39 in
  2022–25. v2 removes almost all of it (bias +0.06).
- **What does not survive is player-level points skill.** The squared-error
  gain (−1.28) is about what removing a +1.25 average bias is worth on its own
  (1.25² ≈ 1.56). Beyond that, spreading the points across individual backs
  adds as much error as it removes, and typical misses (MAE) get slightly
  worse. The discovery gain (−4.25) shrank to a third out of sample, which is
  also what selecting the best of six settings on discovery predicts.
- Per the registration, φ, thresholds, window and metric are not retuned on
  2020–2021. Two ideas are left, each a **new registration** on data neither
  study has graded (2026 onward):
  1. a backfield-level (team) correction of that average bias rather than a
     per-player split;
  2. showing the carries estimate as display-only volume information, which
     the carries evidence alone supports.
