# NFL scenario ranking harness

First implementation slice, 2026-09-05. This is a command-line research
tool, not a production projection model, portfolio optimizer, or DK entry
exporter. The user waived Jira synchronization for this implementation.

## Run the synthetic experiment

From `web`:

```powershell
npm run demo:nfl-scenarios
node --import tsx scripts/compare-nfl-scenarios.ts --demo --format classic --count 100 --draws 2000 --output ../artifacts/nfl-scenario-classic-p0.json
node --import tsx scripts/compare-nfl-scenarios.ts --demo --format showdown --count 100 --draws 2000 --output ../artifacts/nfl-scenario-showdown-p0.json
```

The first command prints a summary using 1,000 draws per bank. The other
commands save full reports with 2,000 draws per bank. Output files are
created exclusively: use a new filename if a report already exists.
Parent directories must exist. Use the direct `node` invocation for flags
on PowerShell installations whose npm wrapper consumes `--` arguments.

`--seed` defaults to `20260905`. Selection, evaluation, candidate search,
and independence-ablation streams use separate documented seeds. The
report records model/scorer/PRNG versions, snapshot/cutoff metadata,
input/candidate SHA-256 digests, runtime, observed heap usage, and search
status. Runtime/heap fields are measurements and are not reproducible
content; the experiment result and its digests are deterministic under
the same code, settings, inputs, and runtime behavior.

The demo generates synthetic passing, rushing, interception, sack,
field-goal and extra-point events. Passing yards/TDs reconcile with
receivers; interceptions reconcile with opposing DST; pick-sixes are not
charged to the throwing team's DST, while ensuing extra points are.
Showdown includes kickers. Classic's off-pool kicking events still affect
points allowed. Unmodeled categories are explicitly zero **in this toy
fixture only**. Its rates are arbitrary and must never be used as fitted
NFL projections. Injury uncertainty, fitted TD probabilities, residual
off-pool offense, and the full production game model are not implemented.

## Use supplied scenario files

```powershell
node --import tsx scripts/compare-nfl-scenarios.ts --salary-csv path/to/DKSalaries.csv --selection-bank path/to/selection.json --evaluation-bank path/to/evaluation.json --target 150 --count 100 --output path/to/new-report.json
```

The target is a fantasy-score threshold, not a cash line. With external
files, the salary parser determines format and the banks determine draw
counts. Do not pass `--format` or `--draws`. Parser warnings are retained
in the full report.

The JSON contract is `NflScenarioBank` in
[`scenarios.ts`](../web/src/lib/nfl-dfs/scenarios.ts). Required metadata:

| Field | Contract |
|---|---|
| `schemaVersion` | `1` |
| `runId`, `streamId` | Nonempty identities, different between selection and evaluation |
| `modelVersion`, `snapshotId` | Same fitted model and immutable input snapshot for both banks |
| `decisionAt`, `inputsCapturedAt` | Timestamp with explicit timezone; captured no later than decision; same across banks |
| `source` | `synthetic` or `model`; a source label is not a calibration certificate |
| `seed` | Unsigned 32-bit integer; selection and evaluation must differ |
| `sampling` | `iid` for equal-weight independent draws, or `weighted` |
| `scenarios` | At least two records, each containing unique `id`, positive finite `weight`, and complete `stats` |

Each scenario's `stats` object is keyed by the underlying `dkPlayerId`
from the parsed salary pool. Every player must occur exactly once in
every scenario. A Showdown CPT purchase ID is not an additional player.
Scenario IDs must not overlap between selection and evaluation banks.

Every stat key in `NFL_STAT_KEYS` for the player's position is mandatory.
Count stats must be nonnegative integers. Yardage may be negative. Unknown
keys, omitted stats (including DST points allowed), NaNs, and mismatched
player sets fail clearly. `emptyNflStats()` is a convenience for fixtures;
do not use it to conceal missing real data. Negative fantasy scores are
retained. Lineup scoring accumulates integer hundredths to preserve exact
target boundaries and CPT multipliers.

The standalone scorer supports positive nonuniform weights and weighted
inverse-CDF quantiles. The current **three-way comparison requires equal
weights** because its independence ablation permutes player columns.
Weighted resampling is not yet implemented. Do not relabel weighted draws
as IID to get a report or confidence interval.

## What is evaluated

1. Generate up to N legal, canonical-unique candidates using a bounded,
   seeded randomized search. It honors eligibility, OUT status, salaries,
   Classic's two-game rule, Showdown's both-team rule and role pricing.
   A `search-limit` result is neither a complete portfolio nor proof that
   the full problem is infeasible. Exposure/stack preferences are not yet
   candidate-search constraints.
2. Evaluate selection-bank candidates using summed player P90, independent
   lineup draws, joint lineup draws, and joint target probability.
   Independence is an intentional ablation that destroys football event
   accounting while preserving the supplied player marginals.
3. Select each policy's winner using only its selection objective, with
   canonical identity breaking ties. Assess every selected winner on the
   same separate joint evaluation bank. Per-candidate evaluation metrics
   are included for inspection, but are not read by the selection policy.
4. Report mean, inverse-CDF P10/P50/P90, and inclusive `P(score >= target)`.
   For supplied IID joint draws, report a Wilson 95% interval describing
   Monte Carlo sampling noise only. No interval is fabricated for the
   permutation ablation or non-IID weights. These intervals do not
   describe model error, historical uncertainty, or repeated-study risk.

The boundary checks metadata and scoring inputs. It cannot prove that a
file's declared streams are independent, its timestamps are truthful, or
its supplied correlations conserve real football events. Those require
generator/source evidence and the later model-validation gates.

## Verification and current limits

```powershell
npm run test:nfl-scenarios
npm run test:nfl-scoring
npm run test:nfl-dk-csv
npx tsc --noEmit --incremental false
```

Tests cover both formats, 100 unique candidates, deterministic replay,
salary/eligibility/identity failures, complete aligned stat inputs,
weighted quantiles, negative scores, exact threshold equality, CPT
scoring, scenario/source cutoff failures, selection/evaluation isolation,
preserved ablation marginals and synthetic football accounting. A
controlled counterexample has summed player P90 = 60 but lineup P90 = 30.

No historical NFL salary/scenario/standings snapshots were found in the
repository inspection. The generated reports are therefore **synthetic
mechanics evidence only**. Historical evaluation remains pending verified
point-in-time data. Contest fields, calibrated ownership, tie/payout
evaluation, joint portfolio selection, late swap, Kelly sizing and UI
remain later slices. Kelly stays disabled until its payout and bankroll
validation prerequisites are implemented.
