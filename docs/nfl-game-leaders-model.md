# Single-game player leaders — implementation contract

Open repair work and acceptance checks are tracked in
[nfl-game-leaders-backlog.md](nfl-game-leaders-backlog.md), including the
2026 Dallas fumble/lateral discrepancies and incomplete-history forecast guard.

### Count-only reception analysis

`prepare(..., reconciliation_fields=('targets','receptions'))` admits only games
whose player targets and catches reconcile, preserving Dallas Weeks 3–4 without
pretending the unresolved fumble/lateral yardage is correct. Each accepted game
records `validated_fields`. `forecast(..., outcomes=('receptions',))` restricts
output to the validated outcome; a yardage forecast rejects count-only history.
Named reception rows expose a discrete `count_probabilities` distribution.
Default full-outcome reconciliation and existing saved forecasts remain intact.
This is a scoped count analysis, not completion of the yardage repair backlog.

The evening 2026-10-08 local analysis is frozen in
`artifacts/nfl-game-leaders/receptions-forecast-20261008-evening.json`, with a fresh
dual-provider request and all four current Dallas games. Its price comparison
is reproducible with `research.nfl_receptions_analysis`; supplied screenshot
prices are explicitly timestamp-unknown observations, not a live odds feed.
Compare tie-split leader credit with offered-price break-even credit, not
first-or-tied probability. The comparison does not de-vig an incomplete field.

### Stat-credit repair and coverage gate (2026-10-08)

Capture now preserves primary `receiving_yards`, `rushing_yards`, final completion
and attempt status, and lateral recipient/yardage fields. Raw play type supplies
the final no-play flag when the feed lacks a separate `no_play` column. Final
status takes precedence over earlier text in replay descriptions or a nullified
touchdown that leaves credited yardage. Lateral yards are separate events with no
invented target, reception or carry. Their forecast incidence uses weighted
historical lateral credits per team opportunity, separately from per-catch gains.
This empirical rare-event assumption is recorded, not a calibrated coefficient.

All 880 player-box games reconcile against the separate same-provider team
aggregate feed; 871 event games fully reconcile. Nine games have primary-source
disagreements and contribute workload only. Catch rates and target budgets use
verified boxes; per-touch yard distributions use reconciled events only.
Forward capture times apply to primary stat enrichment as well as labels/boxes.

Acceptance gate: require all latest six team games, using current-season games
within that window when present (otherwise prior-season games). Canonical expected
IDs come from the complete schedule, never the already-filtered training set.
Missing recent workload blocks forecasting. Recent event quarantine blocks
yardage forecasting. Pure research calls without expected IDs are explicitly
unverified; the publisher rejects them. The page shows coverage or marks older
snapshots unverified. No old forecast file is overwritten.

The repaired 2026 retrospective run grades 60 games and blocks four Week 1 games
whose recent 2025 event history is unresolved. These are examined-week reruns,
not untouched validation. See the repair backlog for source and test evidence.

## Requested outcome and acceptance criteria
For each canonical NFL game, estimate the full-field leader in rushing yards,
receptions, and receiving yards, including overtime and all offensive positions.
Passing yards and touchdowns are different questions. Do not use sportsbook odds.

Before presenting an actionable probability: conserve team carries and targets in
every draw, model receptions and receiving yards jointly, split ties by actual
individuals before grouping unresolved players, retain zero/negative yard outcomes,
and compare chronological predictions with independent nflverse player box totals.
Report Brier score, fractional-winner log loss, top-choice accuracy, calibration,
mean-stat baseline, all skipped games, and coverage. Simulation count alone is not
validation. Initial outputs are exploratory; deployment requires a separate release.

## Design
Canonical schedule joins, deduplicated PBP role identities, and independently
captured nflverse weekly stats underpin the fit. Team budgets are sampled as paired
historical game blocks. Roles use recent team games (including zeros) and a shared
Dirichlet allocation. Efficiency distributions retain empirical long gains and
losses; catch counts and yardage share the same opportunities. Opponent effects
compare each offense against a defense with its other prior games and shrink sparse
samples. Recent-season evidence receives more weight than older seasons.

No reliable workload is inferred from depth rank. Explicit request role shares can
represent a documented scenario; they remain assumptions with provenance. Unresolved
current availability blocks a current actionable claim. Surprise contributors are
separate latent identities before their leader credits are grouped into residuals.

Historical reconstructed rosters and corrected records are not archived pregame
availability. Development backtests must disclose that limitation and preserve a
later untouched evaluation period. No tuning to the latest missed game.

## Files and commands
- `model/nfl_game_leaders.py`: pure joint simulator and result scorer.
- `research/nfl_game_leaders.py`: read-only capture, per-game evidence requests,
  forecast/batch, result grading, chronological evaluation and sensitivity.
- `research/nfl_game_leaders_publish.py`: reviewed static page publication.
- `/nfl/game-leaders`: per-game selector and all three outcome tables. The saved
  batch is manual, independent of DraftKings uploads, and never changes optimizer
  points. Kickoff changes the page to a saved-estimate warning, not a live result.

```powershell
python -m research.nfl_game_leaders capture --output artifacts/nfl-game-leaders/new-capture.json.gz
python -m research.nfl_game_leaders week-requests --input artifacts/nfl-game-leaders/new-capture.json.gz --season 2026 --week 5 --output artifacts/nfl-game-leaders/new-requests.json
python -m research.nfl_game_leaders batch --input artifacts/nfl-game-leaders/new-capture.json.gz --season 2026 --week 5 --decision-at <timezone-aware-time-after-request-captures> --requests artifacts/nfl-game-leaders/new-requests.json --output artifacts/nfl-game-leaders/new-batch.json
python -m research.nfl_game_leaders_publish --batch artifacts/nfl-game-leaders/new-batch.json --evaluation artifacts/nfl-game-leaders/final-evaluation-2025.json --evaluation artifacts/nfl-game-leaders/final-evaluation-2026.json --output web/src/data/game-leaders.json
```

Outputs use exclusive creation. Preserve the previous page snapshot before
refreshing it; never overwrite frozen requests, forecasts or evaluation reports.
Single-game commands are `evidence-request --game ID`, `forecast --request PATH
--sensitivity`, and `grade --forecast PATH`. `request` produces a usage-only draft
when current provider research is unavailable. `week-requests` retains capture
errors; batch skips those games instead of silently replacing provider evidence.

## Source audit and initial coverage
The fantasy database (`ff_player_week_stats`) is filtered through a current player
universe and omits some historical contributors. It is NOT a full-field grading
source. The model downloads complete nflverse weekly player stats directly,
retaining URL, SHA-256, and capture time. Canonical game ID is checked against a
unique season/week/team/opponent schedule match. No name-only identity joins.
QB kneels count as official rushes and can produce negative yards. Incompletions
are targets when a receiver is attributed; sacks and unassigned throwaways are not.
All periods, including overtime, are included. Receiving yards use caught air
yards plus YAC, falling back to recorded play yards only when air yards are absent.
Each complete game is reconciled per player and all five counts/totals; discrepancies
exclude that training game and block automated result grading.

The first complete source capture has 880 source games, 17,078 player-game rows,
and 786 reconciled games (248 in 2023, 237 in 2024, 245 in 2025, 56 in 2026).
94 games require stat reconciliation. This exclusion may bias evaluation and is
not a passed source-quality gate. Capture provenance and rejected IDs are saved.

## Assumptions that remain explicit
Recent role shares use up to six prior team games with a three-game half life.
Team opportunity means shrink toward league means using three pseudo-games.
Opponent volume, catch, and yard effects compare the same offense in other prior
games, then shrink; volume effects cap at +/-20%, yard shifts at +/-2 per event.
These caps and priors are chosen assumptions. Dirichlet concentrations are 55 for
carries and 40 for targets, not fitted estimates of role variance. Thirty peer
efficiency events and prior-season weight 0.35 smooth sparse player samples.
Shared historical catch luck makes teammates' catch totals correlated. Paired
historical volumes preserve cross-team carry/target dependence, but do not simulate
score states, drives, weather, or clock sequences.

Other contributors are simulated as distinct historical identities and three
newcomer slots, then grouped AFTER resolving winners. Their combined yard totals
never compete against a named individual. Confirmed out-player opportunities stay
in unresolved slots until an explicitly documented full-field role scenario is
supplied. Neither a depth rank nor an injury label establishes replacement usage.

An availability-verified request must include both depth sources and week-matched
FantasyPros injuries, with source references and captures at/before cutoff, no
older than 48 hours, no conflicts, and no unresolved statuses. This freshness
threshold is an operational assumption. Official inactive imports, when present,
are retained for review; absent imports remain missing coverage. Even matching
providers do not establish game-day confirmation. No raw provider report is
converted into a fitted availability probability in this version.

## Evaluation and next promotion gate
Every target is excluded before fitting. Reports include selected, graded and
skipped games, identities, Brier score, fractional-winner log loss, calibration
bins, mean-projection errors, and recent-average first-choice results. Bootstrap
intervals resample whole games, not correlated player rows. Zero simulated shares
are floored at 1e-12 only for finite log-loss reporting, not as a calibrated
probability. Output ranges are simulated percentiles, not calibration evidence.

2024 Weeks 5–8 is development. The initial 2025 Weeks 5–8 and 2026 Weeks 1–4
checks were examined during development; they are not untouched holdouts.
After adding opponent opportunity-volume terms (an initially specified missing
factor), evaluate 2025 Weeks 9–12 without selecting parameters on those results.
The 2026 rerun is descriptive because earlier 2026 outcomes were already examined.
Raw pre-change and final reports are retained, not overwritten. These records use
later corrections and reconstructed usage rosters; they cannot prove pregame
availability accuracy. No calibration, betting edge or superiority is claimed.

Before promotion: independently reconcile the remaining source games, fit role
variance and replacement scenarios on development data, pre-register untouched
seasons/weeks, add a distributional baseline, evaluate game-level proper scores
and calibration with uncertainty, then collect genuinely frozen forward requests,
final inactives and actual results. Maintain the baseline alongside the challenger
until improved decision quality is demonstrated.

## Local review results (2026-10-08)
The opportunity-volume version graded 49 development games, 50 later 2025 games,
and 56 current-season games. On the 56 current-season games, model vs baseline
fractional first-choice credit was 44.6% vs 51.8% rushing, 20.2% vs 24.7%
receptions, and 23.2% vs 26.8% receiving yards. This does not establish superiority.
The later 2025 sample also did not beat the baseline consistently. Bootstrap
intervals include no improvement; no family has passed a promotion gate.

A final identity review found two opposing-team transfer cases in reconstructed
2026 rosters. Only those affected games were replayed with the corrected identity
handling; the prior reports remain frozen. `reviewed-evaluation-2026.json` retains
the changed game IDs and before/after implementation hashes. The Thursday replay
is unchanged; all 15 weekly games have no cross-team identity conflict. Provider
coverage is retained in `week5-requests.json`; no weekly game is labeled
availability-verified, and no official Week 5 inactive imports were present in
this morning's capture. Zero imports is missing coverage, not proof an external
list does not exist.

The 15-game public snapshot uses `final-week5-forecast.json`, reviewed historical
evaluation, and `reviewed-page-snapshot.json`. Raw source capture is
`full-capture-20261008.json.gz`; the initial fantasy-DB capture is preserved only
locally as diagnostic evidence and is not the full-field source.

## Failed-model investigation (2026-10-08)

The complete Weeks 1–4 review covers all 64 games. Full player boxes reconcile
to the separately published nflverse team feed for all five statistics; this
is a second aggregation from the same provider, not independent provider evidence.
Target PBP discrepancies still exclude those games from event-model training.
Original forecasts remain frozen; eight previously excluded target games were
forecast without changing settings and graded against the reconciled boxes.

First-choice fractional credit, model versus six-game average:

| Outcome | Model | Average baseline |
| --- | ---: | ---: |
| Rushing yards | 43.8% | 50.0% |
| Receptions | 17.7% | 22.4% |
| Receiving yards | 25.0% | 25.0% |

When model and baseline differed, rushing selections earned 6 versus 10 games
of credit across 23 disagreements; receptions earned 2.83 versus 5.83 across 30;
receiving yards earned 5 versus 5 across 29. These demonstrate harmful overrides,
not a proven diagnosis that any specific opportunity or efficiency term caused them.

`research/nfl_game_leaders_challenger.py` tests a separate conditional ranking
model fitted directly to first-place outcomes, with split tie labels and an
OTHER winner category for contributors absent from prior usage. OTHER is not
a pooled yardage competitor and receives zero primary named-pick credit.
Features include prior 1/3/6-game workload and production, team share, position,
and prior opponent totals. Historical candidates use only earlier team usage;
zero-touch games are retained in averages. Unobserved opposing-team transfers
cannot receive credit under an obsolete team role. This is a ranking experiment,
not a calibrated probability replacement for the joint simulator.

Train: 2023 plus 2024 Weeks 1–10 (408 games after history warm-up).
Selection: 2024 Weeks 11–18 (120 games), choosing among three registered
regularization strengths and 1/3/6-game average baselines.
Untouched evaluation: 2025 Weeks 13–18 (94 games). All 816 source games from
2023–2025 reconcile to separately published team totals. This verified box
history is broader than the event model's reconciled PBP training history;
the two model results must not be presented as identical historical experiments.

| Outcome | Challenger | Selected average baseline | Improvement 95% interval |
| --- | ---: | ---: | ---: |
| Rushing yards | 36.7% | 38.8% | −9.6 to +5.3 percentage points |
| Receptions | 28.8% | 31.3% | −10.8 to +5.9 percentage points |
| Receiving yards | 27.7% | 26.6% | −6.4 to +8.5 percentage points |

Intervals resample paired games. All three categories failed the registered
promotion gate. The initial report is retained; the reviewed rerun corrected
transfer-label accounting and source-scope reporting without changing settings
or any outcome scores. These 2025 weeks are now examined and cannot serve as
an untouched holdout for subsequent tuning. No 2026 outcomes selected settings.
Historical roster reconstruction and later stat corrections still limit
pregame validity. Live forecasts remain unchanged and experimental.

Reproduce the reviewed experiment from frozen sources with:
`python -m research.nfl_game_leaders_challenger --output NEW_REPORT.json`.
Existing outputs and the study registration are protected against overwrite.
Preserved evidence lives under `artifacts/nfl-game-leaders/challenger-*`;
the plain-language review is `model-quality-review.md` in that directory.
