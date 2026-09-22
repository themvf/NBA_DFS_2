# NFL DFS red-zone studies, projections, and line movement — review handoff (2026-09-21)

Written 2026-09-22 for whoever picks this up next. It records what was reviewed,
what was verified against the live database, what was found, and what to do
next. Nothing below changes production; no study was re-run with altered
constants; the database was read only.

## 0. Read this first

- **Scope of the review.** The three pre-registered studies merged on
  2026-09-21 (`model/nfl_dfs_workload_opponent.py`,
  `model/nfl_dfs_redzone_trips.py`, `model/nfl_dfs_redzone_share.py`), the
  production projection pipeline (`nfl-dfs-historical-v3`), the NFL line
  movement program, and the NYG @ LAR (week 2, Monday 2026-09-21) read.
- **How it was done.** Orchestrator scouting against the live DB, then a
  six-lens agent workflow. Four lenses completed: statistical methodology,
  code correctness, projection pipeline, line movement. **Two lenses never
  ran (football game-read via the `nfl-analyst` agent, and data integrity)
  and the adversarial verification stage did not run** — the session hit its
  usage limit twice. Every finding below is therefore a single reviewer's
  claim with cited evidence, cross-checked where the orchestrator's own
  queries overlapped. Treat them as strong leads, not adjudicated facts;
  section 7 says which ones were independently confirmed.
- **The one-line verdict.** The approach (pre-registration, walk-forward,
  weeks-clustered bootstrap, kill rules, stop-rather-than-patch) is correct
  and should be kept. Two of the three studies were mis-specified in ways the
  recorded verdicts do not describe, and the "production-style" baselines
  are research constructs production never computes. The biggest projection
  problem is not red-zone usage; it is the zero-history position prior and
  the missing matchup term. The biggest line-movement problem is the
  measuring instrument (CLV graded at a mean line no book posts), not the
  detectors.

## 1. What was verified directly (orchestrator, live DB, 2026-09-21)

| Item | Result |
|---|---|
| Share study omits zero-touch games | LAR 2025: 169 active RB/WR/TE player-games, only 89 (53%) had a red-zone touch; the other 80 never enter as 0-share observations. 12 qualifying LAR players' shrunk shares sum to 2.15 at cutoff 2026w2. |
| Position-mean share priors | Computed over the same touch-only games: RB 0.313, WR 0.174, TE 0.164, QB 0.182 — also inflated. |
| QB1 gate (commit 6ace9f8) | Lives only in `web/src/lib/nfl-dfs/availability.ts` (`applyTeamQbContext`), wired into `workspaceSlate`. Python run rows keep position-prior numbers (Ty Simpson 16.28, Jake Haener 13.44). |
| Duplicate Puka Nacua | `ff_players` id 34 (FantasyPros-only, gsis NULL, fetched 2026-08-01) vs id 560 (nflverse+Sleeper, gsis 00-0039075). Only duplicate in run 75dabb4a. DK slates resolve to 560 by gsis, so no money effect. Nacua was QUESTIONABLE. |
| West-coast 1pm line study | Run 2026-09-08, commit 7f15a63: H1, H2, H3 all die. Result exists only in the commit message; `docs/nfl-line-movement-study.md` still ends at "Cost" and CLAUDE.md has no entry. |
| `nfl_season_games.market_*` | Frozen at 2026-09-08 for every week-2 row (NYG@LAR 8.5 / 49.0 vs live 6.5 / 47.5). Writer `refresh_nfl_survivor.yml` failed on 4 of its last 5 runs (Sept 3, 10, 15, 17), all `LockNotAvailable` in `db/database.py::_ensure_schema`. Projection environment reads `nfl_matchups` (fresh), pick'em evidence prefers `game_odds_history` (fresh); only the DFS showdown favourite detection (`web/src/app/dfs/nfl/actions.ts:250`) and the survivor model's fallback read the stale columns. |
| Projection job failures Sept 17–20 | All "Baseline implementation drifted from the pinned study" — fixed in the 2026-09-20 handoff; every run since 2026-09-20 15:14Z succeeded. |
| Forward interval calibration (weeks 1–2, production, mean ≥ 5) | Share of actuals inside [p10,p90]: DST 0.76, QB 0.74, RB 0.82, TE 0.72, WR 0.79 (nominal 0.80). Boom probability vs realized: 0.089/0.095, 0.080/0.091, 0.097/0.093, 0.076/0.101, 0.066/0.064. Median bias −0.3 to −0.9; mean bias positive. Spread is fine; the misses are in the means. |
| Forward MAE weeks 1–2 | W1: DST 4.49, QB 6.89, RB 7.55, TE 5.49, WR 5.98. W2: DST 4.81, QB 7.12, RB 5.05 (bias +2.75), TE 5.32, WR 6.04. Worse than the 2025 diagnostic on every position except QB; n is two weeks. |
| total_walking fade study | Blind accrual n 24/100, games 23/40. |
| Pick'em archetype tags for NYG@LAR (real `tagSeason`) | LAR: OFF_BLOWOUT_LOSS, IN_PRIMETIME, REST_EDGE. NYG: OFF_PRIMETIME_WIN, IN_PRIMETIME, CROSS_COUNTRY, MARQUEE_BRAND. `narrativeRead` = favouriteHeat −2, underdogHeat +3, verdict **contrarian**. |

Corrections to earlier suspicions: the "479 rows for 421 players" duplicate in
`nfl_dfs_player_week_results` was NOT reproduced by the projection lens (421
exact rows under `nfl-dk-realized-v2`), and the LA/LAR report-card mismatch was
NOT confirmed (LAR week-1 rows exist under `LAR`).

## 2. Findings — the three studies

### 2.1 Share study (`model/nfl_dfs_redzone_share.py`) — verdict "dead" is a measurement artifact, and the recorded cause is incomplete

1. **Zero-touch omission dominates (high).** `Prior.__init__` lines 117–122
   append a share only for players present in the `rz` map for that
   team-week. Raw touch-only EWMA shares already sum to **1.38** per
   team-week before any prior (p90 1.79); the position-mean prior adds only
   ~+0.07. `MIN_SHARE_GAMES=2` then selects on outlier games: 450 of 8,088
   rows have exactly 2 touch-games and a mean v1 of 0.413 TD/game vs actual
   0.270. Across 2023–25: RB touch-games 57% of active games, WR 35%, TE 36%.
   The CLAUDE.md note "shrink toward zero" alone would fail again.
2. **Team TD budget is a survivor-only sum (high).** `team_td` sums TDs over
   rows joined through `ff_players`, which holds only the 2026 roster (1,087
   rows). Recovered team TDs: 2023 73%, 2024 86%, 2025 97%. Complete figures
   exist in DST `raw_team_stats` (rush+rec TDs 1,224/1,320/1,321) and in pbp
   TD drives. The target population is also selected on 2026 roster survival.
   This partially cancelled the share inflation, so the true bias was larger
   than the recorded +0.166.
3. **"Touch" is a target (medium).** Receiver credits include incompletions
   and interceptions: 39.5% of 2025 red-zone receiver credits were not
   catches. WR/TE shares are inflated relative to RB.
4. **Shares are not team-specific (low).** History keyed by player only;
   Likely's BAL share (0.155, 17 games) is multiplied by NYG's budget.
   `model/nfl_dfs_workload.py::player_shares` already filters by team.
5. **Structural ceiling never stated (medium).** 32% of passing TDs and 16%
   of rushing TDs (2024–25) are snapped outside the 20; a red-zone share
   cannot address them.
6. **Feature windows asymmetric (low).** `FIRST_SEASON=2022` for pbp, but
   `ff_player_week_stats` starts 2023. No leakage found in either direction.

### 2.2 Trips study (`model/nfl_dfs_redzone_trips.py`)

7. **The baseline is not production (medium).** "Production-style" is a team
   TD EWMA; production derives TDs from per-player resampled lines times a
   team-implied-total factor (`environment_td_exponent` 1.0) and has no team
   TD budget anywhere. corr(baseline, actual) 0.27; a constant league mean
   scores MAE 1.155 vs baseline 1.090. The null holds a fortiori (failing a
   weak baseline implies failing the market), but "relabelling" overstates
   it: corr(v1, baseline) 0.79, mean |v1−baseline| 0.25 TD drives.

### 2.3 Opponent workload study (`model/nfl_dfs_workload_opponent.py`)

8. **Carries survivor holds (medium, positive).** Reproduced −0.107;
   Bonferroni-6 [−0.184, −0.032], Bonferroni-9 [−0.192, −0.030]; block-4 and
   block-8 week bootstraps agree; within-team lag-1 autocorrelation 0.016.
   By season it decays: 2024 −0.158, 2025 −0.074, 2026 +0.047 (n=62).
   Attempts' upper bound now sits at +0.0002 on the live 1,150-row
   population — do not read that as "nearly real".
9. **Registration understated its family (medium).** Docstring says 2 tests;
   it runs 6 kill tests (attempts/targets correlate 0.888, so ~4 effective).
10. **Effect size (medium).** 0.107 carries ≈ **0.07 DK team points** of MAE;
    the adjustment averages 0.89 carries (3.3% of budget, p90 1.76), so a
    bell-cow RB moves ≤ 0.35 points typically, ~0.7 at p90, against forward RB
    MAE of 5–7.5. Real, small, low priority.
11. **`--tonight` applies V1 to dead fields (low).** Line 212 uses v1 for
    attempts and targets too, and skips the targets ≤ attempts cap `build()`
    enforces.
12. **No dataset digest (low, all three studies).** Recorded n=1,136 vs 1,150
    on today's data (week-2 Sunday games labelled after the morning run);
    verdicts unchanged. `ingest/nfl_dfs_workload.py:56` already computes a
    digest the studies could reuse.

### 2.4 Was stopping the right call?

Yes. The repo rule is that a construction change is a new study. But the
recorded reason must be corrected in CLAUDE.md ("Player Red-Zone Touch
Share"): the dominant defects are the conditional-on-touch share series and
the survivor team budget, with the prior a secondary contributor. A v2
registration is in section 5.

## 3. Findings — production projections (`nfl-dfs-historical-v3`)

13. **Zero-history "position prior" is a starter prior applied to backups
    (high).** `model/nfl_dfs_historical.py:239` builds the prior from the 400
    most recent stat rows of the position — rows that exist only for players
    who recorded stats. Prior population averages QB 14.5, RB 8.4, WR 6.3, TE
    5.8; the walk-forward outcome of a player's FIRST recorded game (2024–25)
    averages 4.6/3.1/3.7/2.8 (medians 1.5/1.2/1.7/1.1), an upper bound. On the
    completed week-2 classic slate, position-prior rows over-projected by
    +14.2 (QB), +5.3 (RB), +5.1 (WR), +4.5 (TE) under DK's no-stat-row = 0
    rule. Run 75dabb4a carries 31 QB / 89 RB / 174 WR / 79 TE such rows. The
    web QB1 gate hides some of it on slates; the projection number itself is
    still wrong and the report card scores it.
14. **Forward accuracy excludes the population the model is most wrong about
    (medium).** Report-card policy "no stat row is not evidence of zero"
    leaves 689 of 1,078 week-1 forecasts unscored. A slate-scoped stream with
    DK's convention (listed, game completed, no stat line = 0) is needed;
    even historical QBs on the week-2 slate show +5.15 bias under it.
15. **Intervals are mis-scaled by history depth (medium).** hist 17–34: p10–p90
    coverage 0.73, below-p10 0.15; hist < 2: coverage 0.97. The shadow
    opportunity candidate's residual-quantile p10 is already calibrated
    (0.09–0.16 vs baseline 0.15–0.31). Boom calibration is fine. Week-2 RB
    bias +2.75 is a slate-level down week (Barkley 16.1→3.0, Bijan 22.5→11.1),
    not a systematic finding.
16. **Opponent factor is dead code; environment factor is clamped and
    league-relative (medium).** `build_week` never sets
    `ProjectionContext.opponent_factor`; `feature_snapshot.opponent_factor` is
    NULL on all 1,086 rows. The clamp at 0.8/1.2 binds on 7 of 32 teams
    tonight (LAR 27.0 → 1.2 on TDs; NYG 20.5 → 0.911). LAR's 2025 defense
    (EPA allowed −0.062, 25.9% three-and-outs) enters nowhere. Forward data
    (n=474) show no detectable relationship between the factor and bias, so
    the double-count is not demonstrated — test it, do not patch it. The
    shadow ledger freezes with no context at all
    (`ingest/nfl_dfs_shadow.py:114`), so environment changes are currently
    ungradeable there.
17. **No forward promotion gate exists for the five opportunity candidates
    (medium).** `artifacts/nfl_dfs_shadow_config.json` holds only
    `production_promotion:false`; spec §7.7's numeric gate was never written.
    Week-2 forward cohorts: WR −0.119 [−0.224, −0.014] (only CI excluding
    zero), QB −0.151, RB −0.087, TE +0.062, DST −0.014. Weeks 1–2 are now
    inspected and must be excluded from any confirmation window.
18. **Duplicate Nacua identity (low)** — see section 1.

## 4. Findings — line movement

19. **NFL spread/total alerts are CLV-graded at a mean-consensus line no book
    posts (high).** The NFL insert path (`model/line_alerts.py:1265–1284`)
    never writes `entry_home_line`, so settlement falls back to
    `_consensus_book_line` (a mean); the CFB path does it right. 271 NFL line
    alerts, 0 with `entry_home_line`; 58 settled rows graded at non-half-point
    lines. Recomputed at the frozen exec line: spread_steam +0.236 → +0.125,
    total_steam −0.033 → −0.117, spread_walking +0.129 → +0.071,
    total_walking −0.454 → −0.429. Won/lost outcomes unchanged (0 of 159);
    0 voids exist because a mean line cannot push.
20. **NFL "steam" has no time bound (medium).** Median trigger interval ~6
    hours (spread_steam 355 min, total_steam 361, moneyline steam 730); the
    shared detector requires ≤ 30 min. `interval_minutes` is not stored.
21. **`report()` prints CLV n=0 for the four highest-volume NFL types
    (medium).** It aggregates `clv_pp` only; line CLV lives in
    `grading_json.line_clv` (82 of 100 settled regular-season alerts).
22. **Final-2h five-minute capture targets are met at ~half resolution
    (medium).** Week-2 Sunday games: 8–12 distinct captures in the final two
    hours against 24 targets; `closing_candidate` missed on all 15 completed
    week-2 games; only 6 of 30 regular-season closes are quality A; only 16%
    of consecutive pre-kickoff capture gaps are ≤ 30 min. `nfl_matchups.week`
    is NULL on all 272 rows, so completeness cannot be bucketed by week.
23. **Four structure detectors are dead since deployment (medium).**
    reversal, reference_led, price_pressure, market_convergence: 0 alerts in
    15 eligible days; the first three require ≤ 30-min predecessors, which
    the cadence rarely supplies. Do not lower the bound; report the eligible
    denominator.
24. **West-coast study unrecorded (medium, process).** See section 1. The
    incidental control-arm H3 finding (openers beat closers by 0.16 for all
    Sunday-1pm road teams, CI barely excluding zero) is a candidate for its
    own registration on free 2026 captures, not a result.
25. **total_walking fade study leaks through the general ledger (medium).**
    The flagged side's regular-season record (4-10, line CLV −0.454, n=14) is
    printed by `report()` and the Alerts backtest — the exact negative of the
    sealed fade-side CLV. The study also uses an all-book mean as "close"
    rather than `verified_clv_closes`, and inverts the flagged side's exec
    price instead of using the fade side's own quote. Do not touch the sealed
    v1 script; guard the public surfaces and register a v2 close definition.
26. **What the ledger supports (medium).** Regular season, game-clustered:
    steam +2.14pp [+0.65, +3.83] on 10 games; spread_steam +0.236
    [+0.024, +0.516] on 17 games (biased basis, +0.125 at exec line);
    everything else includes zero or is n ≤ 7 (dk_value +74% ROI is n=7,
    key_cross 3-0 is n=3). No type meets a 30-settled / 25-game floor.
27. **Walking measured against a thin opener (low).** 34 of 41 walking alerts
    measured drift from a 3-book first capture a median 27 h earlier, against
    a 6–7 book trigger set, with no overlap matching.

## 5. Recommended next work, in order

Each item has a binary check so the next agent can close it.

1. **Correct the record (no code).** Amend CLAUDE.md "Player Red-Zone Touch
   Share" (dominant cause = zero-touch omission + survivor budget), relabel
   the trips/share baselines "workload-config team TD EWMA (research)", fix
   the opponent study's family count (6, ~4 effective), add the DK-point
   translation (~0.07 team points), and append a "Result (2026-09-08)"
   section to `docs/nfl-line-movement-study.md` from commit 7f15a63 plus a
   CLAUDE.md entry. Check: `grep -n "Result (2026-09-08)" docs/nfl-line-movement-study.md`.
2. **Fix the CLV instrument.** Write `entry_home_line` from
   `priced['exec_line']` in the NFL branch of `model/line_alerts.py` exactly
   as CFB does; bump `grading_version` to `nfl-lines-v2`; regrade append-only
   via `_append_grade_history`. Add line-CLV columns to `report()` and the
   web backtest query (n/a where not applicable). Check: stored `line_clv`
   equals the exec-line recomputation on 100% of newly graded rows; report
   shows n_lineclv 24/30/14/14 instead of 0.
3. **Guard the blind study.** Exclude `sport='nfl' AND alert_type='total_walking' AND season_type='regular'`
   from the two public backtest queries and `report()` until n ≥ 100 / 40
   games. Check: the row is absent from `python -m model.line_alerts --report`.
4. **Fix the schema-init lock failure.** `db/database.py::_ensure_schema` runs
   DDL on every invocation and now kills `refresh_nfl_survivor.yml` 4 of 5
   runs (13:20 UTC cron, actually firing ~17:00–17:40 UTC). Move schema init
   to a one-time migration or skip it in read-mostly jobs; then re-run the
   survivor refresh so `nfl_season_games.market_*` is current. Check: the
   next two scheduled runs succeed and `market_captured_at` advances. Also
   point the showdown favourite detection (`actions.ts:250`) at
   `nfl_matchups` or `game_odds_history`.
5. **Register projection v4, zero-history prior.** Prior population = the
   walk-forward "first appearance" cohort (players with 0 prior rows at that
   cutoff), optionally stratified by Sleeper depth when a fresh capture
   exists; 1–5 game players matched on a shrunk mean. Graded on a new
   **slate-scoped** report-card stream (population = `nfl_dfs_slate_players`
   of completed slates, DK convention no stat row = 0), weeks 4–10, paired
   MAE and bias with weeks-clustered CI, plus a hist ≥ 6 cohort to prove no
   regression. The shadow ledger cannot grade this (it skips < 2-game
   players). Commit the registration before week-4 kickoff (2026-09-24 ET).
6. **Write the forward promotion gate for the five opportunity candidates**
   into `artifacts/nfl_dfs_shadow_config.json` (window weeks 3–12 2026,
   per-position minimum n, paired MAE CI below zero AND no worse interval
   coverage AND no worse boom Brier, graded once). Record that weeks 1–2 are
   inspected and excluded.
7. **Register two market-free environment tests** (separately): (i) team
   factor relative to the team's own trailing scoring instead of the 22.5
   constant, clamp widened; (ii) an opponent term starting with the surviving
   allowed-carries mechanism on rushing lines only. Add a context-bearing
   shadow variant first, otherwise neither is gradeable. Remove or populate
   `opponent_exponent`.
8. **Intervals.** Register residual-quantile p10/p90 by position × history
   bucket (the shadow candidate already does this) and a higher
   `prior_equivalent_games`; grade on the shadow ledger's existing
   p10/p90/boom fields, minimum 8 forward weeks.
9. **Share study v2 (only if still wanted; low expected value).** One
   variant, family 1: target unchanged; team budget = shrunk EWMA of DST
   `raw_team_stats` rush+rec TDs (assert 2024 sum == 1,320); share = player
   RZ opportunities / team RZ opportunities for EVERY active player-game with
   0 when absent, state targets-vs-receptions explicitly, MIN_SHARE_GAMES on
   games played, team-filtered history, shrink w = n/(n+4) toward ZERO,
   normalise within team-week by max(1, Σ shares); kill = paired MAE CI below
   zero AND |bias| ≤ 0.02 TD/game; state the 68%/84% inside-the-20 ceiling.
   Given the team-level trips result, expect a null.
10. **Cadence and detector health.** Report distinct final-2h captures and
    closing-candidate hit rate per game on the detector-health page; add an
    "eligible captures" denominator for elapsed-bound detectors; populate
    `nfl_matchups.week`. If the 5-minute worker cannot beat ~12-minute
    effective resolution, move the final-2h trigger to a Vercel cron bridge
    as MLB did. Store `interval_minutes` on every NFL steam row and either
    bound it or relabel the type.
11. **Study hygiene.** Emit `dataset_digest`, `max_labelled_week`, `git_sha`
    from each study script; fix the opponent `--tonight` path (v1 only on
    carries, apply the targets ≤ attempts cap); deactivate FantasyPros-only
    `ff_players` rows that have a gsis twin and assert no duplicate
    (name, team, position) in `build_week`.

Do NOT: re-slice any of the day's studies; re-run the west-coast study;
lower the 30-minute steam bound to make dead detectors fire; promote the
carries term without a new workload version and a shadow cycle; add any
public-stat model against closing lines.

## 6. NYG @ LAR — what the evidence said before kickoff

Kickoff 2026-09-21 8:15pm ET, SoFi (dome). The game has been played by the
time you read this; grade it rather than re-read it.

- **Market.** Opened LAR −7 / 48.5 (p_home 0.751, 2026-09-14), closed around
  −6.5 / 47.5 / −308/+248 (0.724). Four prospective NFL alerts fired, all
  toward NYG and the under (moneyline walking, spread_steam, key_cross 7→6.5,
  total_walking under). None of those types meets its evidence floor; this
  was an observation to log, not a position. Check after settlement:
  `grading_json.line_clv` for the four alerts against the verified close, and
  whether `closing_candidate` for `nfl_matchups.id=34` was captured (a
  standalone primetime game with no slate contention is the cleanest test of
  the cadence finding).
- **Archetypes (pick'em).** Room read is "contrarian": NYG carries
  OFF_PRIMETIME_WIN + MARQUEE_BRAND (loud, toward) and CROSS_COUNTRY
  (moderate, against); LAR carries OFF_BLOWOUT_LOSS (loud, against) and a
  quiet REST_EDGE (11 vs 8 days). Every measured gap has a CI including zero
  except CROSS_COUNTRY (+8.0pp, unconfirmed, one survivor of fifteen). The
  correct pick'em action was the straight favourite at ~72%; the room liked
  the dog and the market had already moved half a point toward it, so a flip
  was cheap by price and not differentiating.
- **Football read (from play-by-play; the football-analyst lens did not
  run).** 2025: LAR offense elite (3.71 TD drives/g, 4.35 RZ trips/g, EPA
  +0.145/play, explosive 8%) and LAR defense good (EPA allowed −0.062, 25.9%
  three-and-outs, turnovers on 24.1% of drives, 32% pressure); NYG offense
  average-to-poor (2.53 TD/g, EPA +0.01, 2.7 sacks taken/g, 21.5% no-huddle)
  and NYG defense poor (EPA allowed +0.084, blitz 30%). Week 1 was one game
  each way: LAR 1 TD drive and 4 turnovers vs SF (pass rate 17 points under
  expectation); NYG 4 TD drives on 6 possessions vs DAL. Red-zone work is
  concentrated in LAR (Kyren 63 of 2025 touches, Adams 32) and spread in NYG
  (Skattebo/Tracy/Singletary/Dart 27/26/24/22).
- **Production projections (run 75dabb4a).** Nacua 25.3 (QUESTIONABLE, floor
  13.2, boom .49), Stafford 22.3, Kyren 17.3, Adams 16.7; Dart 18.2 (floor
  1.0), Nabers 16.1, Skattebo 15.0, Likely 8.4. These are 2025 lines scaled
  by 1.2/1.066 (LAR, clamped) and 0.911/0.968 (NYG) with no defensive term.
  Structure that followed: LAR favourite-onslaught with Nacua/Stafford/Kyren
  as the correlated core; NYG exposure through Skattebo (only NYG player with
  a consistent 2025 red-zone role and a 29.0 ceiling) rather than Dart. The
  week-1 Likely miss (6.5 → 27.8 on two red-zone TDs) is the variance
  resampled lines cannot forecast, not evidence for a red-zone feature the
  studies already failed to find. No showdown slate was uploaded for this
  game, so nothing was optimized.

## 7. Confidence map

Independently confirmed by two sources (orchestrator + a lens, or two
lenses): zero-touch omission (1); survivor team budget partially (2, lens
only, but the `ff_players` season distribution was seen by two lenses);
production baseline mismatch (7); carries reproduction (8); west-coast study
status (24); duplicate Nacua (18); stale `market_*` and the survivor job
failure cause (orchestrator, four run logs). Single-lens with quantitative
evidence, unverified: 3–6, 9–17, 19–23, 25–27. The refutation stage did not
run; a follow-up agent should re-check 13, 16 and 19 first, since they carry
the largest implications.

## 8. Artifacts and how to reproduce

- Orchestrator brief (data pulled 2026-09-21, read-only):
  `C:/Users/joshb/AppData/Local/Temp/claude/C--Docs--AI-Python-Projects-NBADFS-v2--claude-worktrees-dreamy-nobel-b4d726/a6c000eb-d943-426a-8cfb-3751c9f794b1/scratchpad/review-brief.md`
  (session-local; the substance is in sections 1 and 6 above).
- Workflow journal with the four completed lenses' full findings:
  `C:/Users/joshb/.claude/projects/C--Docs--AI-Python-Projects-NBADFS-v2--claude-worktrees-dreamy-nobel-b4d726/a6c000eb-d943-426a-8cfb-3751c9f794b1/subagents/workflows/wf_47c024dd-e8e/journal.jsonl`.
- Archetype tagging for the game was reproduced with
  `web/src/lib/nfl/pickem-archetypes.ts::tagSeason` over the real 2026
  schedule (team ids from `nfl_teams`; pHome 0.7243 from `nfl_game_win_probs`).
- Commits under review: ee2b262 (opponent), 825edf7 (trips, PR #225),
  a44afa8 (share, PR #226), 6ace9f8 (QB1 gate), 7f15a63 (west-coast result).
- All study scripts run unchanged from the worktree root; `--tonight` modes
  write nothing.
