# Matchup forecast registration, grading, and freeze health

Prepared for the main release on 2026-09-27. The scheduled pipeline captures
research forecasts and grades their forward evidence. No production forecast
policy is activated by a registration or a study verdict.

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

The three DFS `outcome-amendment-1` records register the merged main scorer,
`nfl-dk-realized-v3`, at 2026-09-27T15:41:34.524290+00:00. They pin the exact
scorer, historical scoring function, DST component helper, and team aliases.
The original v2 registrations and captures remain immutable. New mean-study
captures must follow this amendment; no coefficients were refitted, baseline
changed, or earlier outcomes reinterpreted. A changed scorer implementation
fails the declared pin check instead of silently changing the evaluation.

The protected context study has a separate compatibility declaration in
`research/nfl_dfs_context_scoring_compatibility.json`, recorded at
2026-09-27T15:45:13.637239+00:00. AST comparison verifies that the skill-position
scorer, field mapping, and DraftKings point function are unchanged between
v2 and v3. Thus later v3 QB/RB/WR/TE outcome revisions can grade the original
context forecasts without restarting its eight-week window, changing its
shadow pin, or changing the registered carries formula or PASS/kill gates.
V3 DST outcomes are excluded from that context study because their component
semantics changed; unknown outcome versions are rejected. Version counts and
compatibility identity remain visible in every context report.

Coherent distribution research retains its immutable v2 registration and
introduces a separate `nfl-coherent-matchup-research-v3` registration for the
merged scorer and its exact dependency bundle. Its first full forward week
remains week 4; today's week-3 construction checks cannot qualify it. No
old coherent report is relabeled under the v3 contract.

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
evidence before production projections, then freezes the registered DFS
matchup forecasts the page reads for defensive adjustments. Since 2026-09-28
the remaining research steps run in `refresh_nfl_dfs_research.yml`, after each
green production run: they freeze the pick'em challengers and coherent
scenarios, grade completed observations, and retain the forecast/grading
artifacts. They use committed fitted artifacts and never
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

## September 28 coherent registration repair

The current coherent registration is `nfl-coherent-matchup-v4`, recorded at 2026-09-28T11:50:46.075313+00:00. The opponent processing repair changed the pressure/contact coverage predicate in `model/nfl_matchup_projection.py`; v3 correctly rejected that changed implementation. v4 pins the current implementation and gives new banks a distinct model identity. The immutable v3 manifest remains in `research/nfl_coherent_scenario_study_v3.json`; v4 is archived alongside it. Historical compact reports keep their original registrations and are graded separately.

The baseline configuration, fitted models, scoring contracts, independent simulation streams, draw counts, first full forward week (2026 Week 4), December 1 endpoint, eight-week evidence floor, and distribution/portfolio gates are unchanged. There is no refit or production promotion. Captures must occur after the new registration and before kickoff. A missing current salary slate remains a separate coverage blocker.

Run `python -m pytest tests/test_nfl_coherent_study.py tests/test_nfl_matchup_scenarios.py tests/test_nfl_matchup_scenario_refresh.py tests/test_nfl_matchup_projection.py -q` to verify the current code pin, historical cohort isolation, simulation invariants, and refresh guards. Then use the existing scheduled scenario refresh with a fresh comparison and a saved upcoming salary slate.

## September 29: realized-v4 outcome scorer and capture-identity repair

Main's realized scorer moved to `nfl-dk-realized-v4` (reliability B6): play-by-play DST components v2 credit special-teams fumble recoveries and read only the ruling that stands. Only DST points change. The QB/RB/WR/TE scorer, field mapping and DraftKings point function are AST-identical to v3; the AST hashes reproduce the v3 declaration exactly. Every re-registration below was recorded at 2026-09-29T01:57:29.430760+00:00, is forward-only, and adds a new file (or a new version key) without editing an earlier registration.

- **DFS mean studies (pressure, contact, combined).** `outcome-amendment-2` moves them from v3 to v4 and pins the exact scorer. As with every outcome amendment, the holdout restarts at the amendment. `implementation-pin-5` repairs a separate defect: pin 4 bound nine files while captures recorded hashes for six, and the grader requires an exact match, so no capture after pin 4 could qualify. Captures now record `research.nfl_matchup_implementation.IMPLEMENTATION_FILES`, and pin 5 binds exactly that set: pin 4's nine files plus `research/nfl_saved_upload_selection.py` (#295).
- **Registered scope.** The pins register `capture: saved_current_week_Classic_salary_pool`. Since #295, Showdown captures land in the same tables, and the grader keeps the latest capture per player/game, so the grader now enforces the registered scope from each capture's recorded `format`. It rejects out-of-scope rows before selection, so a Showdown capture can never displace a Classic one, and it refuses an unrecognized scope rather than guessing. This enforces the registered scope; it does not change it.
- **Coherent study.** `nfl-coherent-matchup-v5` (`research/nfl_coherent_scenario_study_v5.json`, copied to the current path) pins the new scorer hashes and grades outcomes as `nfl-dk-realized-v4`. The simulation, scenario scoring, baseline, draws, week-4 start, endpoint and gates are unchanged. No forward week had been graded, so none is lost. v1-v4 captures stay under their own registrations.
- **Context study.** `research/nfl_dfs_context_scoring_compatibility.json` gains an `nfl-dk-realized-v4` key: QB/RB/WR/TE only (DST excluded, as for v3), with its own registration time, AST proof and scorer hashes. The v2 and v3 declarations are untouched. Only v4 outcomes computed after the declaration can enter.
- **Pick'em studies.** `ingest/nfl_dfs_weekly.py` changed after the latest pick'em pins (6a07cc0, 44fd1cd). As a result, `freeze-pickem` refused every freeze with "Registration model/code pin mismatch" from the #292 merge onward. New pins (combined pin 6, pressure-v2 and contact-v2 pin 3) re-bind the same file set at current hashes. No feature, coefficient or gate changed.

`tests/test_nfl_matchup_study.py` and `tests/test_nfl_coherent_study.py` now fail if any pinned file drifts from its latest pin, if the capture file list and the DFS pin disagree, or if a Showdown capture could enter a Classic-scoped study.
