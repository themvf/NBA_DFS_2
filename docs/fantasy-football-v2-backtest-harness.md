# Fantasy Football V2 Rolling-Origin Backtest Harness

**Status:** implemented shadow validation infrastructure; not a fitted model
**Ledger target:** V2-004
**Harness:** `ff-v2-backtest-v1`

The harness freezes how roster-aware V2 experiments are split, what prediction
code may see, how held-out artifacts are paired for scoring, and what must be
persisted for replay. It does not define the definitive metric suite and does
not fit Team Opportunity.

## Canonical run

The canonical split artifact is
[`../artifacts/ff_v2_backtest_harness_2020_2025.json`](../artifacts/ff_v2_backtest_harness_2020_2025.json).

| Field | Value |
|---|---|
| Run ID | `3db2e15a-bbf0-57d4-bb5b-df684f671d3b` |
| Output digest | `4f61720a8e84e55daa90fcb29dc324df790a8cf1d82c09e71ee636444d7b4399` |
| Context run | `9077ad91-e258-5e47-beb8-f41b68c6651b` |
| Seed | `20260828` |
| Folds | 6 |
| Scorable seasons | 2021, 2022, 2023, 2024, 2025 |
| Source snapshots | 25 |

The 2020 fold is retained with an explicit
`no_prior_season_training_history` exclusion. It is the seed training season,
not a silently in-sample score.

## Frozen cutoffs

Each cutoff is 23:59:59 US Eastern on the day before that season's opener:

| Evaluation season | Preseason cutoff |
|---|---|
| 2020 | `2020-09-09T23:59:59-04:00` |
| 2021 | `2021-09-08T23:59:59-04:00` |
| 2022 | `2022-09-07T23:59:59-04:00` |
| 2023 | `2023-09-06T23:59:59-04:00` |
| 2024 | `2024-09-04T23:59:59-04:00` |
| 2025 | `2025-09-03T23:59:59-04:00` |

For evaluation season `Y`, the expanding training window contains only seasons
strictly before `Y`, and every training observation must have
`observed_at <= cutoff(Y)`. Prediction-time evaluation features must also be
available by the cutoff. Outcome-like keys are removed from the prediction
view, and held-out outcomes enter only after predictions are frozen.

## Artifact scoring contract

One identity-matched protocol supports:

- `team_week`
- `player_week`
- `season_total`
- `roster_simulation`

It rejects duplicate identities, post-cutoff predictions, pre-cutoff outcomes,
cross-season rows, non-finite values, and unmatched prediction/outcome sets.
The included point-error scorer is a protocol smoke test only. The implemented
V2-005 contract in
[`fantasy-football-v2-metric-suite.md`](fantasy-football-v2-metric-suite.md)
owns the preregistered MAE, bias, proper distribution scores, coverage, Top-12,
spike, Best Ball, regret, cohort, and paired-bootstrap metrics.

The historical context currently contains no captured historical draft-decision
or realized roster-simulation artifacts, so the canonical run records
`roster_simulation: 0` for every season. The harness can score that artifact
type, as its parameterized tests demonstrate, but it does not fabricate a
historical cohort. Populating or explicitly bounding that cohort belongs before
its metrics can be claimed in V2-005.

## Persistence

- `ff_v2_backtest_runs` stores the context run, harness/model/calibration
  versions, seed, evaluation seasons, exact cutoffs, 25 source snapshot IDs,
  cohort counts, configuration, artifact path, and output digest.
- `ff_v2_backtest_splits` stores each evaluation season's training seasons,
  cutoff, training/evaluation row counts, input digests, split digest, and
  scorable/exclusion state.

The canonical run is a hash-pinned retrospective reconstruction. This is
declared in configuration rather than represented as a contemporaneously
captured preseason feed. V2-006 owns the stronger automated leakage and feature
eligibility audits before any challenger fitting is allowed.

## Build and replay

```powershell
python -m model.ff_v2_backtest `
  --context-artifact artifacts/ff_v2_historical_context_2020_2025.json `
  --artifact artifacts/ff_v2_backtest_harness_2020_2025.json

python -m model.ff_v2_backtest `
  --context-artifact artifacts/ff_v2_historical_context_2020_2025.json `
  --artifact artifacts/ff_v2_backtest_harness_2020_2025.json `
  --verify
```

Replay rebuilds every fold from the persisted immutable context run and checks
every deterministic artifact field, every persisted database-run field, and
every field and digest on all six persisted split rows. The V2-006 row-level
feature and protocol gate is documented in
[`fantasy-football-v2-validation-audits.md`](fantasy-football-v2-validation-audits.md).
