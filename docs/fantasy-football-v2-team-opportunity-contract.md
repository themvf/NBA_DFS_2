# Fantasy Football V2 Team Opportunity Contract

Status: **shadow contract only; no forecast values have been fitted or
published.** This document implements V2-007. Model fitting, joint game-script
sampling, shrinkage, and uncertainty calibration belong to V2-008 and V2-009.

## Persisted contract

V2-003's `ff_v2_team_week_facts` remains the authoritative observed outcome
table. V2-007 adds three append-only, versioned forecast tables:

- `ff_v2_team_opportunity_forecast_runs` freezes the context run, contract,
  model and calibration versions, as-of time, eligible feature-source snapshot
  IDs, model configuration, row count, and artifact digest.
- `ff_v2_team_opportunity_forecasts` links one forecast envelope to one exact
  V2-003 game/team fact and freezes the fact digest, fallback tier, confidence,
  row-level feature provenance, source snapshots, and forecast digest.
- `ff_v2_team_opportunity_distributions` stores long-form expected value,
  dispersion, P10, P50, P90, family, parameters, and digest by opportunity pool.

The required forecast pools are plays, pass attempts, allocatable targets, RB
carries, RB targets, passing touchdowns, and rushing touchdowns. Team rush
attempts are an allowed optional distribution and remain a required observed
fact because they reconcile RB and non-RB rushing work.

The persistence API is `ingest/ff_v2_team_opportunity.py`. It accepts forecast
values produced elsewhere; it does not estimate, fill, or default any value.
Every input team/game must resolve to exactly one persisted V2-003 fact. An
unknown game, duplicate fact, duplicate forecast, incomplete distribution, or
conflicting digest fails before or during the same database transaction.
Repeating an identical artifact is an idempotent no-op. A materially different
artifact receives a different deterministic run identity and cannot overwrite
an earlier validation artifact.

## Count and reconciliation semantics

These rules are inherited from `ff-v2-context-v1` and persisted with each
V2-003 fact:

- `plays = pass_attempts + sacks + rush_attempts`. A sack is one dropback/play,
  not an official pass attempt and not a rush. `no_play` rows are excluded.
- `pass_attempts` are official attempts after removing sacks. A throwaway and a
  spike remain official attempts.
- `allocatable_targets` require a named receiver. Sacks, spikes, and throwaways
  have no receiving allocation, so `allocatable_targets <= pass_attempts`.
- Quarterback kneels are removed from both `rush_attempts` and `plays`. They
  cannot create player rushing opportunity or reduce the allocatable play pool.
- Quarterback scrambles remain `rush_attempts` and `plays`; they are not pass
  attempts or allocatable targets.
- `rb_carries` are the subset of non-kneel rush attempts credited to an `RB` or
  `FB` by same-season weekly stats, falling back to the same week's roster.
- Non-RB carries—including quarterback scrambles and designed quarterback, WR,
  or TE runs—remain in team `rush_attempts` but never enter `rb_carries`.
  Therefore `non_rb_carries = rush_attempts - rb_carries` and must be
  non-negative.
- `rb_targets` are the subset of named allocatable targets assigned to an `RB`
  or `FB`, so `rb_targets <= allocatable_targets`.
- Passing and rushing touchdowns exclude two-point attempts. They are separate
  team outcome pools; V2-008 must make their simulated game scripts jointly
  coherent rather than sampling each count independently.

The contract validates finite non-negative values, monotonic quantiles, the
complete required pool set, fallback tier A/B/C, and a confidence multiplier in
`(0, 1]`. It does not assert distributional quality; chronological calibration
is a later gate.

## Provenance boundary

The linked `context_fact_id` and `context_fact_digest` identify the held-out
outcome used for later scoring. They are not forecast features. Forecast feature
inputs are separately frozen in `source_snapshot_ids` and
`feature_provenance`, under the run's `as_of_at`. This separation prevents an
outcome snapshot from being mistaken for an eligible preseason input.

No V2-007 table is read by the current redraft or Best Ball display path.

