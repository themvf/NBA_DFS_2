# NFL DFS + line movement — implementation handoff (2026-09-22)

For the agent that implements the fixes recommended by
[the 2026-09-21 review handoff](nfl-dfs-redzone-review-handoff-2026-09-21.md).
That document records what was found and why; this one says what to build,
in what order, in which files, and how to prove each item is done. Read the
review handoff sections 2–5 first; this file does not repeat the evidence.

Nothing below has been started. No code, no registrations, no DB writes.

## 0. What this work will and will not buy

Asked directly before this handoff was written: does the work improve DFS
projections, and does it give insight into closing lines even if it cannot
beat them? The honest split:

| Package | DFS projections | Closing-line insight | Expected size |
|---|---|---|---|
| WP1 zero-history prior | **Yes, directly.** Wrong on screen today: +14 QB / +5 RB / +5 WR / +4.5 TE on prior rows | none | Largest lever in this file |
| WP2 CLV instrument | none | **Makes the measurement honest.** Every NFL line-CLV figure today is graded at a mean line no book posts | Corrects the instrument; creates no edge |
| WP3 blind-study guard | none | Prevents the fade study from being read early | Governance |
| WP4 schema lock + stale market columns | Small (showdown favourite detection reads a 2-week-old line) | Restores the survivor refresh | Reliability |
| WP5 promotion gate | Enables an eventual gain from the five shadow candidates | none | Governance, prerequisite |
| WP6 environment / opponent tests | Possible, unproven; carries term is worth ~0.07 team points | none | Small |
| WP7 intervals | Better p10/p90 for deep-history players | none | Calibration, not means |
| WP8 share study v2 | Unknown; expect null | none | Low expected value |
| WP9 cadence / detector health | none | Tells you which detectors can fire at all | Diagnostic |
| WP10 hygiene | none | none | Prevents rework |

The two pipelines share no inputs. Nothing here feeds Vegas lines into DFS
projections or DFS data into line detection. Do not promise the user
otherwise. The repo's record against closing lines is a clean zero across
four sports; the expected outcome of WP2 is a trustworthy negative.

## 1. Ground rules (read before touching anything)

1. **A construction change is a new version.** Any change to
   `model/nfl_dfs_historical.py::MODEL_CONFIG` or `project_player` alters
   the baseline hash that `ingest/nfl_dfs_shadow.py:64` pins. The shadow job
   will then fail with `Baseline implementation drifted from the pinned
   study; rerun research before shadow`, which is exactly what killed the
   projection job Sept 17–20. Either bump `MODEL_VERSION` and re-pin per the
   2026-09-20 handoff's "Replacement research pin", or build the change as a
   separate candidate the way the five opportunity candidates were built.
   Never edit v3 in place.
2. **Pre-register before scoring.** Every item marked *study* below gets
   its hypothesis, population, metric, minimum sample and kill rule committed
   before the grading script runs. The review handoff section 5 already
   drafts each registration; copy it, do not loosen it.
3. **Append-only ledgers.** `line_alerts` grades go through
   `_append_grade_history`; prediction snapshots and shadow rows are never
   rewritten. Corrections are new rows with a new `grading_version` or
   `model_version`.
4. **Do not re-slice the 2026-09-21 studies.** Not the attempts/targets
   opponent terms, not the trips baseline, not the west-coast study. Section
   6's do-not list still applies.
5. **The delivery contract applies.** Each package closes with the
   requirement-to-evidence table in section 4, states whether live DB state
   was mutated, and lists the commands run with their pass/fail output.
6. **Weeks 1–2 of 2026 are inspected.** They cannot appear in any
   confirmation window for WP1, WP5, WP6 or WP7.

## 2. Work packages, in order

Order is by dependency then by value. WP0 and WP1 are the ones the user
cares about first; WP2–WP4 are independent of WP1 and can run in parallel
with it in a separate worktree.

### WP0 — Correct the record (no code, ~15 minutes)

**Why first:** `CLAUDE.md` currently misattributes the share study's failure,
and a completed study's result lives only in a commit message. Anyone who
reads the current text will re-litigate the share study on a false premise.

Edits:

1. `CLAUDE.md` section "NFL DFS — Player Red-Zone Touch Share (2026-09-21)"
   (around line 6943). Replace the "Why" paragraph. Dominant causes, in
   order: (a) shares are built only from player-games with a red-zone touch,
   so touch-only EWMA shares already sum to 1.38 per team-week before any
   prior (p90 1.79); (b) the team TD budget sums only 2026-roster survivors
   (2023 recovers 73% of team TDs, 2024 86%, 2025 97%), which partially
   cancelled (a), so the true bias exceeds the recorded +0.166; (c) the
   position-mean prior is a secondary +0.07. Keep the "new study, do not
   patch" instruction. Add that "touch" counts targets, not receptions
   (39.5% of 2025 receiver credits were not catches), and that 32% of
   passing / 16% of rushing TDs are snapped outside the 20.
2. Same file, sections "Red-Zone Trips vs Touchdown History" and "Opponent
   Term Study": relabel the baseline as "workload-config team TD EWMA
   (research construct; production has no team TD budget)", state the
   opponent study's family as 6 kill tests (~4 effective), and add the
   translation "0.107 carries ≈ 0.07 DK team points of MAE".
3. `docs/nfl-line-movement-study.md`: append a `## Result (2026-09-08)`
   section transcribed from `git log -1 --format=%B 7f15a63` (H1/H2/H3 all
   die; H3's control-arm incidental finding recorded as a candidate, not a
   result; the event re-keying bug and the matchup-keyed fix). Add a
   three-line `CLAUDE.md` entry pointing at it.

Check:
```bash
grep -n "Result (2026-09-08)" docs/nfl-line-movement-study.md
grep -n "touch-only\|survivor" CLAUDE.md | grep -i "red-zone"
```

### WP1 — Projection v4: zero-history prior (study, then ship)

**The defect.** `model/nfl_dfs_historical.py::_peer_rows` (line 208) builds a
player's fallback from the 400 most recent stat rows of the position. Stat
rows exist only for players who recorded stats, so the "position prior" is a
starter prior. `project_player` then sets `player_strength = 0.0` for
`status == "position_prior"`, so a backup with no history is projected as an
average starter. Prior population means QB 14.5 / RB 8.4 / WR 6.3 / TE 5.8;
walk-forward first-game outcomes average 4.6 / 3.1 / 3.7 / 2.8 (medians 1.5 /
1.2 / 1.7 / 1.1). Run 75dabb4a carries 31 QB / 89 RB / 174 WR / 79 TE such
rows. The web QB1 gate (`web/src/lib/nfl-dfs/availability.ts`) hides some QB
rows on slates but the stored projection and the report card are still wrong.

**Two deliverables, in this order.**

*1a. A slate-scoped report-card stream (needed to grade 1b).* The existing
report card (`model/nfl_dfs_reportcard.py`, `missing_policy`: "No stat row is
not evidence of DNP or zero") leaves 689 of 1,078 week-1 forecasts unscored,
which is precisely the population the prior is wrong about. Add a second
stream, do not change the first:
- population = `nfl_dfs_slate_players` rows of completed slates;
- convention = DraftKings' (listed on the slate, game completed, no stat
  line → 0 points);
- emits paired MAE and bias per position with a weeks-clustered bootstrap
  CI, and separately for cohorts hist = 0, hist 1–5, hist ≥ 6;
- persisted under its own version string so the two streams cannot be
  mixed.
Check: the week-2 classic slate reproduces the review's +14.2 / +5.3 / +5.1 /
+4.5 position-prior bias figures within rounding.

*1b. The v4 prior (study).* Register before grading:
- **Population for the prior:** the walk-forward "first appearance" cohort:
  for each cutoff, the outcome of every player's first recorded game in that
  position. Optionally stratify by Sleeper depth order when a capture
  fresher than 7 days exists; fall back to the unstratified cohort
  otherwise and record which was used.
- **1–5 game players:** shrink toward the same cohort mean with
  `w = n / (n + k)`; k is the only tunable and is fixed at 4 (the existing
  `prior_equivalent_games`) for this registration. Do not grid-search it.
- **Draw mechanics:** keep the resampling structure of v3 (peer game lines,
  not a point estimate) so p10/p90 stay meaningful; the change is WHICH
  rows form the peer set.
- **Grading:** stream 1a, weeks 4–10 of 2026, paired v3-vs-v4 MAE and bias
  per position with weeks-clustered CI, plus a hist ≥ 6 cohort that must
  show no regression (CI on the paired delta not above zero).
- **Kill:** hist-0 paired MAE CI not below zero, or hist ≥ 6 CI above zero
  → v4 not promoted.
- **Ship path:** new `MODEL_VERSION = "nfl-dfs-historical-v4"`, re-pin the
  shadow study per the 2026-09-20 handoff, and only then flip production.
  The shadow ledger cannot grade this (it skips players with < 2 games at
  `ingest/nfl_dfs_shadow.py:111`); stream 1a is the grader.
- **Deadline:** registration committed before week-4 kickoff
  (2026-09-24 ET) or the window starts at week 5.

Files: `model/nfl_dfs_historical.py` (new prior builder, version bump),
`model/nfl_dfs_reportcard.py` or a sibling module for stream 1a,
`ingest/nfl_dfs_projections.py` (unchanged unless the version flips), tests
in `tests/test_nfl_dfs_historical.py` and `tests/test_nfl_dfs_reportcard.py`.

Check: `python -m pytest tests/test_nfl_dfs_historical.py tests/test_nfl_dfs_reportcard.py tests/test_nfl_dfs_shadow.py -q` passes; a unit test asserts a zero-history RB projects below the position's hist-0 cohort p75 and above zero; the registration doc exists under `docs/` with a commit date before the first graded week.

### WP2 — Fix the NFL CLV instrument

**The defect.** The NFL insert path in `model/line_alerts.py` (lines
~1265–1284, the `if sport == "nfl":` block) never writes
`details["entry_home_line"]`. `_settle_nfl_line_alerts` (line 2583) then
falls back to `details.get("entry_home_line", trigger_line)`, and
`trigger_line` is a `_consensus_book_line` mean. The CFB path immediately
below (lines ~1296–1301) does it correctly. 271 NFL alerts, 0 with
`entry_home_line`; 58 settled rows are graded at non-half-point lines.

Changes:
1. In the NFL block, after `priced = freeze_execution_price(...)`, set
   `details["entry_home_line"]` exactly as the CFB block does (negate
   `exec_line` for the away side on spreads; totals take the line as-is).
2. Bump the NFL `grading_version` to `"nfl-lines-v2"` in
   `_settle_nfl_line_alerts` (two places, lines ~2681 and ~2690). The stamps
   at lines ~756 and ~775 are written at scan time; confirm whether they
   describe grading or signal definition before touching them, and never
   change `signal_version`.
3. Regrade all settled NFL line alerts under v2, append-only via
   `_append_grade_history`. For legacy rows without `entry_home_line`,
   derive it from `details["exec_line"]` and `side` using the same rule; if
   `exec_line` is absent, leave the row at v1 and count it.
4. `report()` (line 2712): add line-CLV columns (n, mean, beat-close share)
   sourced from `grading_json.line_clv`, shown as n/a where not applicable.
   The web side already reads `grading_json->>'line_clv'` at
   `web/src/db/queries.ts:10355` and `:11159`; verify those pick up the
   regraded rows without change.

Check:
```bash
python -m model.line_alerts --report   # NFL steam/walking rows show n_lineclv 24/30/14/14, not 0
```
and a SQL check that, for every row graded `nfl-lines-v2`,
`grading_json->>'line_clv'` equals the exec-line recomputation
(`_nfl_line_clv(market, side, entry_home_line, close_home_line)`) on 100% of
rows. Expected post-regrade means, from the review: spread_steam +0.125,
total_steam −0.117, spread_walking +0.071, total_walking −0.429. Won/lost
outcomes must be unchanged on all 159 settled rows. Tests:
`tests/test_line_alert_execution_price.py` plus a new test that an NFL spread
alert on the away side stores the negated home line.

### WP3 — Guard the blind fade study

`model/nfl_walking_fade_study.py` is sealed. Its negative is currently
readable from `report()` and the two public backtest queries
(`getLineAlertBacktest` at `queries.ts:11153` and the walking-family
aggregate at `:11053`), which print the flagged side's regular-season record.

Change: exclude `sport='nfl' AND alert_type='total_walking' AND
season_type='regular'` from those three surfaces until the study's floors
(n ≥ 100 and ≥ 40 games) are met, with a one-line note in the UI and report
saying the row is sealed and why. Do not touch the study script. Do not
exclude preseason or CFB rows.

Check: the row is absent from `python -m model.line_alerts --report` and from
the `/nfl` alerts backtest; `tests/test_nfl_walking_fade_study.py` still passes.

### WP4 — Schema-init lock failure and the stale market columns

`db/database.py::_ensure_schema` (line 96) runs every table, migration and
index statement under an advisory lock with a 30s `lock_timeout` on every
process start. `refresh_nfl_survivor.yml` (cron `20 13 * * 2,4`, actually
firing ~17:00–17:40 UTC) failed 4 of its last 5 runs on `LockNotAvailable`,
so `nfl_season_games.market_*` has been frozen at 2026-09-08.

Changes, smallest first:
1. Add a skip-schema construction path (the `--existing-schema` flag already
   exists on `model/line_alerts.py:2951`; generalise the pattern) and use it
   in read-mostly scheduled jobs, starting with the survivor refresh. Do not
   remove schema init from ingestion jobs that create tables.
2. Re-run the survivor refresh manually and confirm `market_captured_at`
   advances.
3. `web/src/app/dfs/nfl/actions.ts:250`: the showdown favourite detection
   reads `nfl_season_games.market_home_ml`. Point it at `nfl_matchups` or
   the latest `game_odds_history` consensus, which are fresh. Leave the
   survivor model's fallback alone until step 2 is confirmed.

Check: next two scheduled survivor runs succeed;
`SELECT MAX(market_captured_at) FROM nfl_season_games WHERE season=2026` is
within a week of now; a showdown slate for a game with a moved line resolves
the favourite from the fresh source. The wider one-time-migration refactor
described in `CLAUDE.md` ("Known residual issue") stays out of scope.

### WP5 — Forward promotion gate for the five opportunity candidates

`artifacts/nfl_dfs_shadow_config.json` holds only
`"production_promotion": false`. Spec §7.7's numeric gate was never written,
so nothing can ever promote a candidate or say it failed.

Write the gate into the config and enforce it in `ingest/nfl_dfs_shadow.py`
or a sibling `model/nfl_dfs_shadow_gate.py`:
- window: 2026 weeks 3–12 (weeks 1–2 inspected, excluded);
- per-position minimum forward n (state it; 150 player-weeks is a
  reasonable floor given ~470 rows per two weeks);
- pass = paired MAE CI below zero AND p10–p90 coverage not worse than
  baseline AND boom Brier not worse, each position graded independently;
- graded once at the end of the window; no interim efficacy stop.

Check: a test loads the config and refuses to promote on a synthetic ledger
where any one of the three conditions fails; the config records
`inspected_weeks: [1, 2]`.

### WP6 — Environment and opponent terms (two studies)

`ingest/nfl_dfs_projections.py:218` builds `ProjectionContext` with only
`team_implied_total`; `opponent_factor` is never set, so
`opponent_exponent` (0.35) is dead code and `feature_snapshot.opponent_factor`
is NULL on all rows. The team factor is `implied / 22.5` clamped to
[0.8, 1.2], which binds on 7 of 32 teams in a typical week.

Prerequisite: the shadow ledger freezes rows with `ProjectionContext()` (no
context, `ingest/nfl_dfs_shadow.py:114`), so no environment change is
gradeable there. Add a context-bearing shadow variant first, keyed by its own
`shadow_version`.

Then two separately registered studies, one variant each:
(i) team factor relative to the team's own trailing scoring instead of the
22.5 constant, clamp widened to [0.7, 1.3];
(ii) an opponent term on rushing lines only, using the surviving allowed-
carries mechanism from `model/nfl_dfs_workload_opponent.py` (weight 0.5,
not re-fitted).
Both graded on the context-bearing shadow ledger, ≥ 8 forward weeks, paired
MAE with weeks-clustered CI. Either remove `opponent_exponent` from
`MODEL_CONFIG` or populate it; do not leave it dead.

### WP7 — Intervals

hist 17–34 players have p10–p90 coverage 0.73 (below-p10 0.15); hist < 2
have 0.97. The shadow opportunity candidate's residual-quantile p10 is
already calibrated (0.09–0.16). Register: residual-quantile p10/p90 by
position × history bucket, plus a higher `prior_equivalent_games`, graded on
the shadow ledger's existing p10/p90/boom fields over ≥ 8 forward weeks.
Boom calibration is fine and must not regress.

### WP8 — Share study v2 (optional; expect a null)

Only if the user still wants it after WP1. One variant, family 1, per review
section 5 item 9: team budget from DST `raw_team_stats` rush+rec TDs (assert
2024 sum == 1,320); share = player RZ opportunities / team RZ opportunities
for EVERY active player-game with 0 when absent; targets-vs-receptions stated;
`MIN_SHARE_GAMES` on games played; team-filtered history; shrink toward zero
with `w = n/(n+4)`; normalise within team-week by `max(1, Σ shares)`;
kill = paired MAE CI below zero AND |bias| ≤ 0.02 TD/game. State the 68% /
84% inside-the-20 ceiling in the registration.

### WP9 — Cadence and detector health

On the detector-health page (`web/src/app/vegas/detector-health-panel.tsx`
and `getDetectorHealth`): report distinct final-2h captures and
closing-candidate hit rate per game; add an "eligible captures" denominator
for the four elapsed-bound detectors (reversal, reference_led,
price_pressure, market_convergence) so a detector starved by cadence reads
`no_opportunity`, not `dead`. Populate `nfl_matchups.week` (NULL on all 272
rows). Store `interval_minutes` on every NFL steam row; either bound it at
30 minutes like the shared detector or rename the type. Do not lower the
bound to make dead detectors fire.

### WP10 — Study hygiene

- Each study script emits `dataset_digest`, `max_labelled_week`, `git_sha`
  (reuse `ingest/nfl_dfs_workload.py:56`).
- `model/nfl_dfs_workload_opponent.py --tonight` (line ~212): apply V1 to
  carries only and enforce the targets ≤ attempts cap `build()` uses.
- Deactivate FantasyPros-only `ff_players` rows that have a gsis twin (Nacua
  id 34 vs 560 is the only one in run 75dabb4a) and assert no duplicate
  (name, team, position) in `build_week`.

## 3. Suggested sequencing for one agent

| Day | Do | Parallelisable with |
|---|---|---|
| 1 | WP0, WP3, WP2 | — |
| 1–2 | WP1a (slate-scoped stream), WP4 | WP2 |
| 2 | WP1b registration committed (deadline 2026-09-24 ET), WP5 | WP4 |
| 3+ | WP6 shadow-context variant, WP10 | WP9 |
| after week 10 | grade WP1b, WP5 | — |
| after 8 forward weeks | grade WP6, WP7 | — |

## 4. Evidence table to return (per the delivery contract)

| Package | Requirement | Canonical implementation | Automated test | Command run | Result | Live state mutated? | Limitation |
|---|---|---|---|---|---|---|---|

One row per check listed above. `PASS`, `FAIL` or `BLOCKED` only. A study
that is registered but not yet graded is `Built` and `Tested`, never
`Backtested` or `Prospectively validated`.

## 5. Commands

```bash
python -m pytest tests/test_nfl_dfs_historical.py tests/test_nfl_dfs_reportcard.py tests/test_nfl_dfs_shadow.py tests/test_line_alert_execution_price.py tests/test_nfl_walking_fade_study.py -q
python -m model.line_alerts --report
python -m model.line_alerts --sport nfl
python -m ingest.nfl_dfs_projections            # after any version flip
cd web && npm run lint && npm run build         # copy web/.env.local into a fresh worktree first
```

## 6. Do not

- Edit `nfl-dfs-historical-v3` in place, or tune any constant in WP1/WP6/WP7
  against the forward weeks it will be graded on.
- Re-run or re-slice the three 2026-09-21 studies or the west-coast study.
- Lower the 30-minute steam bound.
- Promote the carries term without a new workload version and a shadow
  cycle.
- Add any public-stat model against closing lines.
- Report a package as complete while any check above is `FAIL` or `BLOCKED`.
