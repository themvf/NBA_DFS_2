# CFB drive and market research v4

Acceptance contract, fixed before first publication:

- Reconcile offensive, non-offensive and overtime scoring with final totals.
  Do not impute missing scoring evidence. Remove kneel-only and kickoff-only
  drives, and report garbage-time exclusions separately.
- Select prior strength and recency on chronological development folds; no
  coefficient may depend on a future result or a later odds capture.
- Fit a challenger to the contemporaneous market residual with zero model
  weight allowed. Cover, push, total and moneyline probabilities are separate.
  Distribution calibration uses held-out errors, not in-sample residuals.
- Freeze exact source IDs, sample counts, model parameters, context gaps and
  pregame quotes. Keep old forecasts intact. Refuse post-kickoff enrollment.
- Registration is a database-server timestamp plus immutable configuration
  hash and source commit. Only later first eligible decisions enter the new
  forward cohort. Inspected history and the Thursday/Friday misses are development.
- Surface missing QB/OL/availability, continuity/staff and weather coverage.
  No made-up injury/weather point adjustments. Numerical effects need their
  own timestamped training population before activation.
- Mechanically sound estimates remain research only. Report matched market
  comparisons, coverage, calibration, price-based returns and date-clustered
  uncertainty. Passing code checks never grants betting permission.

The machine-readable protocol is `cfb-drive-market-v4-protocol.json`.


## Operations and source map

The canonical joins are game `cfb_matchups.id`, team `cfb_teams.team_id`,
`cfb_drives.game_id` / `cfb_plays.game_id` and the composite game plus CFBD
source drive ID. Historical classification follows the canonical stored FBS
population; this is reconstructed development data, not historical availability
proof. The model uses only previous calendar-day games with at least six hours
between their kickoff and that day's boundary, avoiding simultaneous-game finals.
Market development uses the latest accepted DraftKings paired quote at T-180,
no older than 15 minutes, with bookmaker update within five minutes of capture.
Game dates for temporal splits use the canonical Eastern game date.

`ingest/cfb_plays.py` now refreshes scores and clocks as well as result labels,
even when a previous importer already advanced the payload hash. Contradictory
or unresolved offensive scoring drives exclude their game. Linked scoring-play
text may recover a touchdown's points only when the conversion is explicit.
A final-score remainder is labeled nonoffensive **or unattributed**, never
silently certified as defensive scoring. Bad data is visible in audit artifacts.

The recurring `refresh_cfb_plays.yml` registers the protocol first, refreshes
source data, captures weather, appends v4 forecasts and first primary decisions,
and grades the forward cohort. Manual `repair_historical=true` refetches 2022–25
play/drive evidence; `bootstrap=true` repairs all completed current-season weeks.
Run these once for the importer fix. This uses CFBD requests and may take longer
than an ordinary refresh. The existing paid odds capture is unchanged.

`cfb_v4_protocols`, `cfb_v4_context`, `cfb_v4_runs`, `cfb_v4_snapshots` and
`cfb_v4_decisions` are additive, append-only tables with database mutation guards.
Registration fails if the stored protocol digest differs. A new definition needs
a new version. The server guards kickoff, source quote age and the scheduled
boundary at publication. Historical predictions and prior ledgers are untouched.

Weather comes from https://open-meteo.com/en/docs : future hourly temperature,
precipitation and wind/gusts, captured with units, coordinates, response digest
and UTC forecast times. Roof status remains unknown unless sourced separately.
A missing or failed provider response is an explicit capture status.

Availability input is supplied with `python -m ingest.cfb_v4_context --team-input
PATH`. The JSON list requires canonical game/team IDs, a published HTTPS source,
its timestamp, and reviewed fields. The collector downloads and hashes the source
and records server capture time. Confirmed QB records require a player source
identity. Annual CFBD roster references remain separate from game-day confirmation.
Human transcriptions require source review; missing fields are never inferred.

Per-game v4 results appear below existing forecasts at `/cfb/analytics/games/ID`.
`/cfb/analytics/research` shows registration and development/forward coverage.
All numerical forecasts and probabilities remain exploratory. An empty calibration
sample produces unavailable probabilities, not a manufactured 50% estimate.

Every run uploads source audits, aggregate source features, model coefficients,
temporal split dates, calibration residuals and the retrospective miss audit as
workflow artifacts. Review the `prior_miss_audit` before assigning a cause:
possession, offensive-rate and other-scoring contributions are arithmetic
attributions, not causal estimates of turnover luck. Keep those artifacts with
the run IDs for longitudinal comparisons.

Numerical context effects cannot be trained honestly before a timestamped
historical population exists. The capture/gating path is implemented now;
validated availability/weather effects and prospective confirmation remain
empirical requirements, not an implementation assertion.

CFB refresh commands check required tables without rerunning application-wide
migrations. This removes a verified pregame failure mode: unrelated schema
changes held locks until the scheduled forecast job timed out. Missing required
tables fail explicitly and require the normal schema migration process.

Weather capture first refreshes CFBD `/venues` coordinates through provider
venue IDs matched to canonical venue IDs. Each weather observation preserves
the coordinate source URL, capture time, response hash and both venue identities.
Absent or invalid coordinates remain missing. This adds one CFBD venue request
per refresh, alongside free Open-Meteo calls.
