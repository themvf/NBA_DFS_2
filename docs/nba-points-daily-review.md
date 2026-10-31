# Daily NBA leader review

This report evaluates full-game points, including overtime, from immutable ghost snapshots. It adds measurements and postgame diagnostics without changing forecast parameters or publishing production predictions.

## Acceptance and interpretation

- Recompute the snapshot digest before grading. Preserve the original file and original snapshot database row.
- Select three distinct players using frozen predictions, descending; break forecast ties by market line, canonical name and event ID. New v2 snapshots record this policy before their digest is calculated. Older snapshots explicitly label selections as reconstructed from frozen predictions: the selection policy was added later.
- Evaluate each model against the market on the same eligible player pool. Exclude ambiguous canonical identities and players reported out in the frozen evidence. Missing model predictions cannot enter that model's pool.
- Preserve selected DNP or unresolved players; never replace them using final scores or invent a zero. Incomplete trios have no scored total or gap. The best verified trio is a hindsight benchmark among resolved eligible players, provisional while other outcomes remain unresolved. It does not establish contest legality, rank or profitability.
- Top-three capture counts tied boundary positions fractionally. For example, three players tied for the last remaining slot each receive one-third slot credit.
- Reconcile source game identity, date, teams, individual shot/point arithmetic and both final team/player point totals. Regulation and overtime splits require the entire scoring-play feed to reconcile to every played player's points. Missing or inconsistent play feeds leave period splits unknown, including overtime.
- Display minutes, field-goal attempts, three-point attempts, free-throw attempts, personal fouls and point contributions from twos/threes/free throws. Six personal fouls are an observed limit note, not a causal estimate of lost points.
- Compare opportunities only with values recorded in the frozen snapshot. New snapshots also preserve the unchanged opportunity model's conversion rates and expected point components. At actual attempts, frozen rates permit an arithmetic attempt-mix difference and conversion residual; the two sum to actual minus forecast. These are postgame diagnostics, overlapping with overtime/minutes, not independent predictive effects. Old snapshots lacking these inputs remain unavailable.
- Accuracy trends include only prospective dates from the same version and fitted artifact. Market comparisons use identical model-specific paired appearances. Historical P90, experimental conditional P90 and market ceiling approximations remain separate. Exceedances use strict greater-than; missing evidence is not zero. Watch scores remain uncalibrated rankings.

## Run

From the research checkout:

```powershell
python -m research.nba_points_daily --date 2026-10-25 --output-dir artifacts/nba-daily/20261025-review
python -m research.nba_points_daily --snapshot artifacts/nba-points-ghost/20261024-early-ghost-v1.json --output-dir artifacts/nba-daily/20261024-v1-review
```

`--date` accepts only an existing prospective v2 database snapshot; it never substitutes retrospective predictions. `--snapshot` verifies the explicit v1/v2 file. Existing final boxes are graded; missing summaries are retrieved from the free ESPN summary endpoint and saved. `--summaries-dir PATH` reuses captured summaries. `--no-fetch` permits offline operation with explicit diagnostic gaps. `--apply` appends an additional grade only for an existing v2 snapshot, without updating that snapshot. v1 stays in separate local files.

Every invocation requires a new output directory and writes the source snapshot copy, raw source summaries, grade, diagnostics, compact chart data, readable review and interactive report fragment. The template is `research/templates/nba_points_daily.html`. The local daily automation runs at 11:35 AM Eastern while the app and computer are available; it uses the preceding finalized dates and keeps v1 separate. No deployment is part of this reporting command.

The daily review shows leader ranks, frozen trio outcomes, paired point errors, ceiling/watch denominators and observed scoring mix. Pending outcomes remain pending. Opening the report or passing tests establishes mechanical/reporting correctness, not improved predictive accuracy; evaluate the prospective trial before changing projections.
