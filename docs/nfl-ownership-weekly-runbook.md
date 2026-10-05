# NFL projected ownership v1

This local pipeline learns NFL player ownership from historical DraftKings
contest entries and pregame salary/projection snapshots. It produces ownership
for every player in a new frozen salary slate. It is an **experimental,
uncalibrated model**. The initial evidence contains one Classic contest and one
Showdown contest; neither has an earlier same-format training contest for a
forward accuracy test. No tournament-return or duplication claim is supported.

## Model and audit contract

- `model/nfl_ownership.py`: pure model, normalization, chronological evaluation,
  content digests and immutable output writer. Requires NumPy already listed in
  the repository requirements.
- `research/nfl_ownership.py`: read-only database snapshot, strict contest import,
  training, prediction, CSV export and grading CLI. Database capture additionally
  uses the existing psycopg2/python-dotenv dependencies. No schema migration.
- `tests/test_nfl_ownership.py`: cutoff leakage, repeated sources, roster totals,
  slot handling, recount, audit integrity and weekly workflow regression tests.
- `artifacts/nfl-ownership-v1/manifest.json`: exact initial input, model,
  prediction and evaluation files for integration and next-week continuation.

Each file has an `artifact_digest`: SHA-256 of the JSON object without that key,
serialized with sorted keys and compact separators. Names contain the full hash.
Repeated identical writes are idempotent; different bytes are never overwritten.
Training pins the feature recipe and model implementation hash. Changing the
model code requires a new fit, preserving earlier artifacts as historical records.
Source hashes normalize line endings so Git's Windows checkout conversion does
not invalidate a checkpoint. Raw contest file hashes preserve the original bytes.
Models and forecasts also record actual wall-clock creation time. The forecast
CLI refuses new output after lock even if passed an earlier `--as-of`. Historical
evaluation is explicitly marked as a retrospective replay. The decision cutoff
must never be presented as proof that a later reconstruction existed pregame.

Historical inputs use the optimizer's immutable **pregame input snapshot**, not
mutable player rows read after the contest. Current capture freezes the saved
salary slate in a read-only, repeatable-read database transaction. Its provenance
contains the salary file digest, projection run, update time and source as-of.
The snapshot command does **not** refresh injury news or projections: refresh the
app's saved slate first, then capture again when news changes.

Raw contest files are counted in full, checked against the known entry count,
and reconciled with the displayed `%Drafted` values within 0.011 percentage
points (two-decimal rounding). Every roster must be complete and have distinct
players. Duplicate entries and ambiguous player identities fail import. Entrant
IDs are used transiently for duplicate checks; entrant names/IDs, prizes and
fantasy results are not written to the training artifacts.

Empty zero-point entries are counted separately. Training ownership is the
fraction of **complete lineups** containing each player. The audit records both
that denominator and the all-entry denominator of published ownership. A zero
target is established by no appearances in the verified complete field. An
absent leaderboard row alone does not establish zero ownership.

Classic base-slot and FLEX appearances are added into one `OVERALL` label per
player. Showdown `CPT` and `FLEX` remain separate. Normalized snapshot names plus
position join identities; names and player IDs are not model features. If a name
cannot resolve uniquely, supply `--identity-overrides` with an explicit mapping
such as `{"normalizedname":{"player_id":123,"reason":"Reviewed DK identity"}}`.
Never infer an ambiguous match from projection or eventual points.

## Abstraction and constraints

Separate Classic overall, Showdown Captain and Showdown FLEX ridge regressions
learn log ownership from salary, our saved projection, DK average points,
missingness flags, within-position salary/projection/value ranks, position pool
size, position and any saved backup-role flag. This allows transfer to previously
unseen players by observable characteristics. Each contest receives equal total
training weight. Hyperparameters are fixed in the versioned recipe, not selected
on these two contests. No game result, actual fantasy points, rank, winning lineup
membership, opponent outcome or current-slate observed ownership is a feature.

Classic marginal totals are QB 100%, DST 100%, and the mandatory RB/WR/TE counts
plus one FLEX slot. FLEX shares learn from past completed lineups, shrunk toward
the disclosed RB/WR/TE prior of 35%/55%/10% with three prior contests. Each player
is capped at 100%. Showdown totals are Captain 100% and FLEX 500%, with each
player's combined Captain+FLEX ownership capped at 100%. A salary penalty is
applied only if necessary to bring expected roster salary within $50,000.
These are marginal consistency checks; they do not simulate legal joint lineups.

Explicitly out players receive zero with reason `explicitly_out`; unknown
projection values have a missingness flag. With no same-format training history,
the output explicitly uses `salary_prior_no_matching_history` (salary squared,
then roster/salary normalization). This fallback never claims historical model
support. Rows retain raw feature contributions, their units, source support and
the shared normalization settings so the final estimate can be reproduced.

The initial Sunday capture includes 629 players but inherits a salary/projection
snapshot last updated October 3. It is an audit checkpoint, not confirmation of
October 4 inactives. Re-freeze current inputs before using new forecasts in entries.

## Repeatable weekly workflow

Commands below run from the worktree/repository root. Substitute paths returned
by each command; do not rename content-addressed files or edit them in place.

1. After refreshing the saved slate, freeze its full pregame pool:

   ```powershell
   python -m research.nfl_ownership snapshot --upload-id <saved-upload-id> --env-file <local-env-file> --output-dir artifacts/nfl-ownership-week5
   ```

2. Fit using all reconciled history available at the decision time. `--as-of`
   must include a timezone. The model excludes labels whose recorded availability
   is later than this cutoff. Fit each week as new evidence becomes available:

   ```powershell
   python -m research.nfl_ownership fit --history <week3-history.json> <week4-history.json> --as-of <decision-UTC> --output-dir artifacts/nfl-ownership-week5
   python -m research.nfl_ownership forecast --model <model.json> --snapshot <snapshot.json> --as-of <decision-UTC> --output-dir artifacts/nfl-ownership-week5
   ```

   Forecasting requires snapshot capture and model as-of no later than the
   decision, and decision strictly before first kickoff. It refuses a slate
   already used in training. Save both returned JSON and CSV. Regeneration after
   news produces a new snapshot and a new forecast digest; old forecasts remain.

3. After the contest, import the actual full standings against the exact snapshot
   used by the saved prediction. Use the verified field entry count and the time
   the results actually became available; never backdate label availability:

   ```powershell
   python -m research.nfl_ownership import-contest --snapshot <snapshot.json> --standings <standings.csv> --contest-id <id> --expected-entries <count> --labels-available-at <observed-UTC> --output-dir artifacts/nfl-ownership-week4
   python -m research.nfl_ownership grade --forecast <forecast.json> --history <history.json> --contest-id <id> --output-dir artifacts/nfl-ownership-week4
   python -m research.nfl_ownership evaluate --history <week3-history.json> <week4-history.json> --output-dir artifacts/nfl-ownership-evaluation
   ```

   `grade` measures the actual frozen forecast. `evaluate` refits chronologically
   by slate, with all same-slate contests excluded together and label availability
   respected. It reports percentage-point MAE, bias, RMSE, top-ten overlap and
   ownership-bin calibration against a salary-only baseline. Prior-only folds
   are labeled and do not count as trained forward tests. New history files are
   additive; identical repeated imports deduplicate, conflicting contest IDs fail.

## Integration handoff

The implementation is isolated to new files. It does not change the generator,
LineStar ingestion, database schema, schedules or deployment. The integration
agent can call the CLI or import `fit` / `forecast`; use `--help` for the contract.

Consume rows by `(slate_id, player_id, slot)` using `OVERALL` for Classic.
`ownership_pct` is **0–100**, so divide by 100 for APIs expecting a fraction.
Showdown Captain uses the same underlying player identity; the frozen snapshot
also carries the Captain roster-entry ID when available. Keep the two slots
separate. Do not overwrite vendor ownership or treat missing model output as zero.
Retain model digest, forecast digest, as-of, snapshot digest and source on the
saved optimizer run. Require the input salary slate and IDs to match; a new slate
needs a new forecast. A later projection/availability snapshot must not silently
reuse an old forecast. Do not call the historical forecast generator after lock
for a late-swap slate; that needs a separately frozen remaining-game contract.

The output explicitly supplies `ownership_capability=heuristic_uncalibrated` and
`validated_ownership_enabled=false`. Existing optimizer gates for validated
ownership must not be bypassed by changing that label. Wiring a user-enabled
experimental ownership objective is an integration task; predictive qualification
requires future grades, not just 900% roster totals. Neither opportunity chips
nor opponent experiments are changed by this ownership branch.

Run `python -m pytest tests/test_nfl_ownership.py -q` before integrating. No extra
package installation is required in the current Python environment.
