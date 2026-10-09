# Matchup forecast registration, grading, and freeze health

Implemented locally on 2026-09-27. No production forecast policy is activated
by a registration or a study verdict.

## Registered prospective studies

`docs/nfl-matchup-studies.json` is the discovery index. The original immutable
registrations and implementation pins live in
`artifacts/nfl_matchup_registrations/`. DFS pressure, contact, and combined
configurations were registered after fitting 2023-2025 retrospective
development data. Pick'em has its own combined market-offset model and
registration; player-efficiency coefficients are not substituted for a game
winner model. Pressure-only and contact-only pick'em declarations remain drafts.

The six declared family members share a Bonferroni familywise correction.
The new-family fixed evaluation endpoint is 2026-12-01; the registered row,
game, week, position, uncertainty, materiality, and harm gates still apply.
A missing cohort or an incomplete registration cannot yield PASS. A verdict
does not update context qualifications, policy pointers, or production rows.

The three DFS baseline-amendment-1 records preserve the original baseline
hash and explicitly register production's `availability_qb_transfer_enabled:
true` configuration. The amendment was recorded at
2026-09-27T14:27:52.602603+00:00 before a new pregame capture. Earlier trial
captures are excluded; their hashes and timestamps are not rewritten.

Implementation pins bind the extractor, source and identity selection,
forecast adapter, and protected baseline. Pick'em pins its own fitted
trainer/consumer instead of the DFS baseline. The original pins remain
available for auditing old runs. The current grader qualifies only captures
at or after the latest pin. An implementation or configuration change
requires a new, prospective pin/registration; do not edit immutable files
in place. Pin 2 was registered at 2026-09-27T14:45:49.075027+00:00. It uses
SHA256 over LF-normalized source bytes so Windows and Linux agree. The fixed
December 1 endpoint and statistical gates are unchanged.

## Commands

```powershell
python -m research.nfl_matchup_study context --season 2026
python -m research.nfl_matchup_study health --season 2026 --week 4
python -m research.nfl_matchup_study validate
python -m research.nfl_matchup_study freeze-pickem --season 2026
python -m research.nfl_matchup_study prospective --season 2026 --capture-results --output artifacts/nfl-matchup-forward-report.json
python -m research.nfl_matchup_study coherent --season 2026 --output artifacts/nfl-coherent-forward-report.json
python -m research.nfl_matchup_scenario_refresh --dry-run
python -m research.nfl_matchup_scenario_refresh --persist
```

`context` reads the explicit protected shadow pin, verifies its stored output
digest, deduplicates last accepted pregame player-weeks, and applies the
2026-09-22 context-variant registration. It never mixes study pins. A whole
NFL week must finish before it counts toward the eight scorable-week floor.
The report card also displays all five context streams on the same last
shadow capture as its baseline, with no resurrection of an older missing
variant. Its display population may contain multiple original study pins;
only the pinned study grader can produce the registered verdict.

`prospective` reads persisted pick'em and DFS snapshots and exact outcome
revisions. With `--capture-results`, it first appends observations of completed
game scores. Changed scores produce a new result revision, including a
reversion to an earlier score. DFS outcomes use the existing exact scoring
ledger. Missing outcomes remain pending; no stat row is not a zero.

Both arms use the same player/game population. A missing matchup feature
uses the exact saved production distribution for both DFS arms; it does not
use an unreproduced resample. Pick'em's fresh-market, missing-feature cases
use the identical market probability for both arms. Covered and complete
deployment populations are reported separately. All rows carry original
source, configuration, code, and outcome identities.

Each forward report retains the exact selected forecast IDs and outcome
revision IDs/digests, whether each pair entered scoring, and a population
digest. A later score correction creates a different report population;
it does not silently overwrite the evidence behind an earlier report.

## Cadence and operational checks

The existing `refresh_nfl_dfs_projections.yml` now refreshes published PFR
evidence before production projections. Research steps freeze the registered
DFS and pick'em challengers, grade completed observations, and retain the
forecast/grading artifacts. They use committed fitted artifacts and never
refit on the daily refresh. A missing current salary slate is a coverage
state, not permission to recycle an old slate as a new one.

The existing pipeline-health monitor now includes
`nfl_context_variant_freeze`: zero eligible context rows by Saturday 21:35
UTC is a failure. Started games missing from the current pin are separately
reported. Future weeks before the deadline are pending. Unpublished PFR
sections remain missing and use the registered fallback.

These workflow changes take effect after deployment. The separately configured
Monday/Tuesday/Friday 7 a.m. Eastern local automation was updated on September
27 to run the frozen matchup forecasts and graders. Neither scheduler refits
the registered artifacts or activates a challenger.

`prospective` also grades immutable published coherent reports. The separate
`coherent` action runs that portion alone. It uses exact original-seed baseline
P10/P25/P50/P75/P90 values only after the saved baseline is reproduced; absent
quantiles remain excluded. WIS uses the registered 50% and 80% intervals with
the standard weights and normalization. Classic and Showdown are distinct
registered cohorts, with K included only in Showdown. Each applies its own
Holm correction and position floors. Week 3 mechanics cannot qualify the
week-4-forward study. A version or configuration change cannot pool with the
old pin, and no coherent verdict activates a model or bypasses the GPP gate.

The coherent scenario cycle requires Python dependencies from `requirements.txt`
and Node 22 with the `web` dependencies installed. Run it immediately after the
current-slate matchup freeze. It generates the separately named research bank,
checks Python/TypeScript scoring agreement, compares illustrative 1-, 3-, and
20-entry portfolios, and publishes the compact report to
`nfl_matchup_research_reports`. It never changes production projections or
submits contest entries. Missing contest fields remain construction-only.

`--dry-run` validates the current comparison and writes the planned steps only.
If the current comparison is missing, stale, empty, or already started, a live
run checks for a saved upcoming Showdown slate with exact identity and lock
constraints. If neither format is available, it returns an explicit
`no_current_pregame_slate` status. The workflow passes its start boundary via
`--not-before`, so a failed/missing refresh cannot reuse an older saved file.
Kickoff is checked again between stages and by the publisher. Failed stages
retain logs and `scenario-refresh-status.json`, stop dependent publication,
and fail the workflow. The raw bank, both event ledgers, verification, portfolio
comparison, and compact report are retained alongside that status for 90 days.
Local runs use unique `scenario-runs/<UTC-time>-<id>/` directories with a copy
of the exact comparison and an SHA256 archive manifest. A same-day rerun never
overwrites the earlier bank, ledgers, or report referenced by an immutable DB
record. Each stage must produce fresh outputs before the next stage can use
them. The status file at the date directory is only the latest-run pointer.
