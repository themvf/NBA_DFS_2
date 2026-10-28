# NBA points v3 research challenger

## Contract and acceptance criteria

Full-game points, including overtime, are the target. All scoring probabilities are conditional on appearing. Reported Out removes eligibility; unknown availability is never Active. Do not invent scenario probabilities or minutes restrictions. V2 coefficients, snapshots, grades and schedules remain unchanged. V3 writes new local directories only, makes no paid captures, and has no deployment or promotion path.

Before treating a result as useful:

1. Audit player/team coverage, scoring reconciliation, identity uniqueness, date boundaries and ingestion times. A date-ordered replay of late-downloaded historical logs is retrospective research, not decision-time verified evidence.
2. Generate every feature using strictly earlier dates. Freeze fitting, calibration and evaluation periods separately. Historical actual teammate absences may inform conditional scenario research but are never smuggled into unconditional pregame forecasts.
3. Compare point error, opportunity error, proper distribution scores, P90 loss and threshold calibration on identical resolved cohorts; show sample sizes. Fit residual distributions only on the calibration period. Evaluation outcomes cannot alter an artifact.
4. Diagnose minutes versus attempt intensity versus conversion algebraically. Actual-opportunity substitutions are retrospective diagnostics, never predictive inputs or independent causal effects.
5. Verify probability monotonicity, scoring identities, integer thresholds, seed reproducibility, empirical distribution support, joint block provenance, ties and unresolved-player treatment. Simulation count is not empirical validation.
6. A prospective v3 output requires the fitted artifact, source snapshot, source ingestion and creation time to precede the full slate's first scheduled tip. Historical source snapshots cannot become prospective v3 forecasts.

## Available implementation

The CLI `python -m research.nba_points_v3` provides `collect`, `audit`, `train`, `evaluate`, `forecast`, `record`, `diagnose`, and `grade`. It consumes existing player/team logs and digest-protected v2 snapshots; it never mutates them. `--input` uses the existing `{players, teams, team_names}` research bundle. `audit --database` inventories actual database coverage through a read-only connection. `collect` also retains raw free ESPN scoreboards and source-timed recent boxes. Current boxes lack offensive rebounds; their attempt allowances are usable but their possessions remain missing, not substituted with FGA.

The challenger forecasts minutes and per-minute shot/FT intensity, plus the three-point attempt fraction. Features include prior workload, rest, home/away, team pace and opponent box-score allowances. Prior team rates are adjusted for overtime using team player-minute totals where available. Separate context and history-only challengers are evaluated. Shooting conversion is shrunk using development-period rates.

Scoring distributions resample complete player residual vectors from calibration games, preserving observed teammate/opponent dependence through historical home/away scoring-role ranks. They are empirical residual distributions, not a possession simulator. Quoted subsets do not constitute complete rotations; no 240-minute allocation or shot-budget claim is made. Small or unmatched block roles use an explicitly reported independent cohort fallback. The output is conditional on appearance and covers only the eligible, sufficiently historical quoted pool. Leader/trio probabilities never claim coverage of unquoted players.

Observed same-team participant absences can generate empirical opportunity scenarios when a timestamped reported-Out observation exists and a usable historical analogue is supplied. Such results are descriptive, small-sample conditional comparisons, not causal injury bonuses. No unknown/questionable scenario gets an invented weight.

## Data gaps

Current database coverage has one historical regular season and a short current-season box archive. There are no historical NBA substitution/stint tables, full tracking feeds, or complete decision-time injury archives. Thus lineup-stint-dependent rotations are unavailable. Player/team box context is an explicitly coarser first challenger. Raw historical inputs were downloaded later than their games, so replay results are exploratory and cannot validate a pregame information advantage over sportsbooks. Prior season holdouts have already been inspected in other research.

## Operation

Use a new output directory for every command. `train` records input hash, fixed feature/specification settings, fitted JSON coefficients, calibration residual blocks and evaluation. `forecast --snapshot ... --model ... --input ...` creates an independent v3 snapshot and probabilities. It refuses to label late work prospective. `grade --forecast ... --diagnostics ...` appends a local report of matched outcomes; missing/DNP players are not assigned zero.

Promotion requires future prospective evidence against v2 and same-time market baselines. This implementation is not evidence of improvement.

## Fixed local artifact and daily procedure

`research/artifacts/nba_ghost_v3_manifest.json` pins the local model file and digest. The 8 MB model is retained under the separate research artifact directory, rather than added to Git. It contains coefficients and January residuals, not executable serialization. Do not train again in the daily follow-up. Verify its digest before every record. Future specification changes need a new model pin and a separate trial.

Run commands from the existing research worktree with `C:\Python313\python.exe`. Use a new dated directory for every command below; the example names are placeholders, not overwrite targets.

1. `python -m research.nba_points_v3 collect --date YYYY-MM-DD --output-dir NEW_INPUT_DIR`
2. `python -m research.nba_points_v3 record --date YYYY-MM-DD --input NEW_INPUT_DIR/inputs.json --model PINNED_MODEL_PATH --injury-archive INJURY_ARCHIVE --output-dir NEW_RECORD_DIR`
3. For an existing genuine v3 prospective recording, `python -m research.nba_points_daily --snapshot ORIGINAL_RECORD_DIR/source-snapshot.json --injury-archive INJURY_ARCHIVE --output-dir NEW_SOURCE_REVIEW_DIR`. Do not use `--apply`: this source review is not another original v2 trial snapshot.
4. `python -m research.nba_points_v3 grade --forecast ORIGINAL_RECORD_DIR/forecast.json --diagnostics NEW_SOURCE_REVIEW_DIR/diagnostics.json --output-dir NEW_GRADE_DIR`

The recorder checks the whole slate's first scheduled tip, captures another free official injury observation, and invokes the existing v2 source collector **without `--apply`**. That creates local input evidence only; it does not replace/insert the original v2 trial snapshot. A per-date/model marker in `prospective-index/` preserves the first successful v3 recording. Later attempts skip. A missing market archive or passed cutoff produces an explicit receipt; never backfill a prospective forecast. Free collection can fail; leave the corresponding evidence unknown.

Use `forecast --snapshot ORIGINAL_V2_SNAPSHOT --input INPUT_BUNDLE --model PINNED_MODEL --output-dir NEW_DIR` only for explicit reconstructions or controlled tests. Mode is determined from actual creation time; old sources cannot become prospective forecasts. `diagnose --snapshot ORIGINAL_V2_SNAPSHOT --diagnostics MATCHED_DIAGNOSTICS --output-dir NEW_DIR` reports minutes, attempt intensity/mix, and conversion contributions with exact arithmetic reconciliation.

`evaluate --input ORIGINAL_TRAINING_INPUT --model PINNED_MODEL --draws 1000 --output-dir NEW_DIR` evaluates the actual joint sampler over the historical evaluation period. Keep that audit distinct from the artifact's marginal distribution scores and prospective grading. The eligible historical pool consists of observed appearances, so it does not validate pregame availability decisions. No market comparison is claimed for historical dates without corresponding decision-time quotes.

## Initial evidence, October 28

Audited historical coverage: 26,547 appearances in 1,225 reconciled games. Splits: 6,173 fitting, 4,782 calibration, 10,145 evaluation appearances across 64 evaluation dates. Two invalid/no-appearance rows and 102 player/team join disagreements are excluded. Five recent games fail full player-stat reconciliation and are not used for team context.

The first historical challenger improves all-player point MAE modestly; high-scorer improvement is negligible. Joint sampling still understates rare 40+ outcomes in the inspected sample. October 27 reconstruction is worse than the frozen market baseline. These results do not justify promotion or automatically adjusting the fitted model. The first October 28 morning record attempt skipped because no archived quotes for that slate were present; first scheduled tip was verified as 7 PM Eastern.
