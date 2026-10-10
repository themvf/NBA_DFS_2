# NFL Team Identity: source and presentation map

## Local longest-touchdown research consumer

`model/nfl_longest_touchdown.py` and `research/nfl_longest_touchdown.py` implement
an exploratory scrimmage-only TD-distance/leader simulator. See
`docs/nfl-longest-touchdown-model.md` for definitions, commands and limits.
The read-only capture verifies `nfl_season_games.nflverse_game_id = game_id`,
season, week and home/away identities through `nfl_teams`. Receiver/rusher
participants are deduplicated by role and GSIS before aggregation to one play.
Positions resolve by `(season, week, player_gsis_id)` in `ff_v2_roster_weeks`
when unambiguous, then `(season, gsis_id)` in `ff_players`; missing positions
remain UNKNOWN. No name-only attribution or inferred scoring from gains alone.

Touchdowns require a counted run/pass description, a unique scorer identity and
yardage agreeing with distance to the goal. Complex/voided/unverified scoring
rows are excluded with counts. Hazards use all attributed opportunities within
field-position bins; defense adjustments compare current offenses against that
defense with their other games. Strict fitting gates kickoff/label time before
the decision; retrospective walk-forward studies permit later labels and must
remain explicitly exploratory. Participants/roster corrections have no immutable
play-level pregame availability guarantee. Output is frozen local JSON with
input/request/implementation digests, not a production projection or page metric.
No market inputs or production writes are introduced by this consumer.

The v2 research capture additionally requires PBP `quarter`, `out_of_bounds`,
canonical schedule `completed`, final scores and schedule capture time. Local
`game_coverage` records captured play count and regulation-end evidence (Q4 zero
clock, or an overtime END GAME marker). Forward and historical grading require
that final metadata and coverage agree; incomplete sources remain unknown.
Training, simple baselines and scoring all use regulation scrimmage TDs; overtime
plays are explicitly excluded. Legacy snapshots without quarters must be
recaptured. Original frozen forecasts remain immutable and can be graded on a
new complete actuals capture. The optional unexpected-scorer reserve is also
applied to the comparison baseline. See the model guide for the remaining scope
and clock approximations and the reproducible development-comparison command.



**Status:** The Team Identity page is implemented at `/nfl/team-identity` and computes its profile on each request. A persisted team-profile summary does not exist. Source coverage below was verified against the local database on 2026-10-01; recheck live coverage before relying on counts.

The page's team directory links to every active NFL team and uses the same season, game, market, and settlement queries for each one. On the Monday and Tuesday `refresh_nfl_pbp_archetypes.yml` runs, `ingest.nfl_season_schedule` first refreshes the current season's completed games, scores, and matchup links from nflverse; `ingest.nfl_pbp_archetypes --relabel-stale` then labels newly published games. The page is dynamic, so its rolling summary and postgame rows advance on the next request after both steps succeed. The scheduled runs are not live during a game. A quote appears only when a verified pre-kickoff odds capture exists for that matchup; missing market data remains marked unavailable.

This is the starting point for agents maintaining cross-season NFL team archetypes. The existing `/nfl/pbp` page is the game and play evidence view. The Team Identity page summarizes a team's choices and results across games, then links back to the exact games and plays. Keep source facts in their existing tables; a future profile table or view should store only derived measures and provenance.

## Source ownership and keys

| Question | Source of truth | Join or use | Important constraint |
| --- | --- | --- | --- |
| Play choices and outcomes | `nfl_pbp_archetypes` | `game_id`, `play_id`; offense `posteam`, defense `defteam` | Filter `season_type = 'REG'` for regular-season comparisons. Use `play_type IN ('run','pass')` for scrimmage rates and explicitly state other denominators. |
| Drive outcomes | `nfl_pbp_archetypes` | One observation per `(game_id, posteam, drive)`, then use `drive_archetype` | Do not count the drive label once per play. Inspect clock-ending/kneel drives when setting a denominator. |
| Players on a play | `nfl_pbp_play_participants` | `(game_id, play_id)` | One play has multiple participant rows; aggregate before joining to avoid multiplying plays. |
| DFS player opportunity chips | `nfl_pbp_play_participants` joined to `nfl_pbp_archetypes` | Play on `(game_id, play_id)`; participant `player_id` (GSIS) to the season's `ff_players.gsis_id`, then `ff_players.id` to `nfl_dfs_slate_players.ff_player_id` | Use receiver/rusher roles only, distinct role-play rows, the previous three regular-season weeks of the same season, and `labelled_at <= saved projection as-of`. Unknown identity or insufficient games yields no chip, not zero opportunity. Participant rows lack an as-of timestamp, so a reconstructed past slate can reflect later participant corrections; the saved optimizer input snapshot records the chips used for generated lineups. `web/src/db/nfl-dfs-player-signals.ts` and `web/src/lib/nfl-dfs/player-signals.ts` own the query and thresholds. |
| Upload-free rolling player projections | `nfl_dfs_projection_runs`, `nfl_dfs_player_projections`, `nfl_season_games`, `nfl_teams` | Select the latest immutable run for an upcoming regular-season week, then match each player team and opponent to that week's unstarted canonical game | `/nfl/projections` reads the scheduled model runs through `web/src/db/nfl-rolling-projections.ts`; no salary upload is required. The page omits a player if its saved opponent no longer matches the schedule or kickoff has passed. It shows the frozen run `as_of_at` separately from live market/PbP read time. |
| Rolling QB matchup assessments | `nfl_pbp_archetypes`, `nfl_season_games`, `nfl_teams`, `game_odds_history` | Prior completed regular-season games join on `nflverse_game_id = game_id`, season, week, and verified home/away teams; current game uses `nfl_season_games.matchup_id` for the last quote before read time and kickoff | `web/src/db/nfl-qb-matchup-context.ts` reads the facts; `web/src/lib/nfl-dfs/qb-matchup-context.ts` computes the versioned summary. Prior games require `kickoff < read time` and `labelled_at <= read time`. These are descriptive diagnostics and never alter the frozen player projections. |
| Game, opponent, venue, rest, and market bridge | `nfl_season_games` | `nflverse_game_id = nfl_pbp_archetypes.game_id`; `home_team_id`/`away_team_id` reference `nfl_teams.team_id` | This is the canonical bridge from the nflverse game ID to market matchup ID. Verify uniqueness and team/date agreement. |
| Team identity | `nfl_teams` | Numeric `team_id` to `abbreviation` (`NYJ`, `PIT`, etc.) | Do not guess numeric IDs or join on display names. |
| Current NFL game markets | `nfl_matchups` | `nfl_season_games.matchup_id = nfl_matchups.id` | Its lines are the latest convenience values, not an as-of history. |
| Timestamped NFL odds | `game_odds_history` | `sport = 'nfl'` and `matchup_id = nfl_season_games.matchup_id` | Use a snapshot captured before the relevant kickoff or decision cutoff; retain `id`, `captured_at`, and book/consensus definition. Do not call the first captured quote the true opener. |
| Frozen closing quote | `event_closing_lines` | `sport = 'nfl'`, `matchup_id` | Use the recorded boundary and quality/eligibility fields. Do not substitute a later quote. See `docs/event-driven-closing-lines.md`. |
| Historical 2025 lines | `nfl_line_snapshots` | Resolve event by verified season, kickoff, and both teams; validate against `nfl_season_games` | Historical `snapshot_at` is the market time; `captured_at` reflects a later archive import. These rows have spread and moneyline, but no comparable game total. Apply `snapshot_at < commence_time`. |
| Injuries and roster context | `ff_player_injury_observations`, `ff_source_snapshots`, and eventually `ff_v2_roster_weeks` | Resolve player/team identity through the repository's identity mappings; use effective and available times | Raw 2026 weekly-roster snapshots exist, but `ff_v2_roster_weeks` currently has no 2026 rows. Do not join on player name alone or infer participation from an injury listing. See `docs/fantasy-football-v2-source-contracts.md`. |
| Weather and venue on plays | `nfl_pbp_archetypes` (`roof`, `surface`, `temp`, `wind`) and `nfl_season_games` | Game-level context | Show field coverage; missing weather is unknown, not zero. |

The labeling code is `model/nfl_play_archetypes.py` and `model/nfl_drive_archetypes.py`; `ingest/nfl_pbp_archetypes.py` persists their versions. `web/src/db/queries.ts` supplies the game explorer at `web/src/app/nfl/pbp/`. The explorer's selector loads the 60 most recent games, and `web/src/db/nfl-team-identity.ts::getNflArchetypeGameById` resolves older direct links. The Team Identity query, options, and game joins live in that same DB module; rolling summaries and denominators live in `web/src/lib/nfl/team-identity.ts`; the server-rendered page is `web/src/app/nfl/team-identity/page.tsx`.

The experimental `AIR_MATCHUP` DFS chip joins the existing player `AIR_VOLUME` chip to the opponent's prior three regular-season weeks of unique pass plays in `nfl_pbp_archetypes`. Defense is `defteam`; non-null `air_yards` counts targeted pass plays including incompletions, and `labelled_at` must be at or before the linked projection cutoff. Canonicalize nflverse `LA` to DK `LAR`. A defense needs two games and 40 measured targets; at least 16 defenses must qualify. Rank air yards per target after shrinkage toward the same-window league mean with 60 pseudo-targets, and tag the top quartile. The database reader and threshold live in `web/src/db/nfl-dfs-player-signals.ts` and `web/src/lib/nfl-dfs/player-signals.ts`. The opt-in Classic GPP minimum percentage rounds up to a lineup count and forces a tagged player when the remaining slots require one; it does not adjust fantasy-point or ownership projections. Saved optimizer inputs freeze the chip and its sample details, and run settings freeze the requested percentage. Historical participant reconstructions retain the as-of limitation above.

The player-level shadow air-matchup evidence also uses prior-three-week unique receiver-target pass plays, aggregated by `posteam` for the team target denominator. Its player GSIS join is the same participant join above. Projected team pass attempts come from the linked run's active QB `statMeans.attempts`; absent attempts make the feature unavailable. The opponent depth baseline uses the eligible defense sample above and the same 60-target shrinkage; the bounded 0.8–1.2 factor is a research sensitivity, not an estimated scoring adjustment. Market context joins `nfl_season_games` through its `matchup_id` to the latest `game_odds_history` row with `sport = 'nfl'`, `captured_at <= projection as-of`, and `captured_at < kickoff`; keep quote ID/time, team spread, total, and moneyline. A missing quote remains missing. `web/src/lib/nfl-dfs/air-matchup-evidence.ts` builds the versioned record, which is shown on the player explanation and frozen in optimizer input snapshots. The shadow script `web/scripts/simulate-nfl-air-matchup.ts` uses this evidence for paired target-air-yard sensitivities; it does not model complete games or lineups. Participant rows still have no as-of timestamp.

The separate `/nfl/projections` page reads the existing twice-daily scheduled baseline and presents DK-scored forecasts for all positions without requiring salary. It shows QB implied points, spread/total, close-state dropback rate (run/pass scrimmage plays with score within seven), opponent-adjusted pass EPA, and opponent touchdown drives per competitive drive. The EPA comparison weights each opponent offense's dropbacks against this defense versus its other same-season games; positive means the defense allowed more passing EPA. Require at least two comparable games and 60 dropbacks, 40 close-state plays, and 15 labelled competitive drives; unavailable evidence stays explicitly unavailable. Count each `(game_id, posteam, drive)` once and exclude kneel/clock-expired endings. The frozen model time and later context read time are separate; new odds do not rewrite the model. Context is not a calibrated fantasy-point adjustment.

## Safe joins and calculations

1. Start with `nfl_season_games`, using its `nflverse_game_id` for PbP and `matchup_id` for 2026 odds. Check `season`, `week`, home team, away team, and kickoff before accepting a join. The 2025 `nfl_season_games` records have `nflverse_game_id` but currently no `matchup_id`; use the separately archived `nfl_line_snapshots` only after event-level validation.
2. Filter odds to observations known before kickoff, or before an earlier requested as-of time. For postgame explanation, identify the exact quote selected. For a historical prediction, never use a closing quote if the prediction cutoff was earlier.
3. Compute play metrics from play rows, drive metrics from unique drives, and player participation from aggregated participant rows. Keep offense and defense denominators separate. A touchdown drive rate must state whether clock-ending and kneel drives are included.
4. Compare the same season type and week window first (for example, 2025 Weeks 1–3 against 2026 Weeks 1–3). Show the prior full season as a second baseline, not as the matched sample. Show play/game counts and missingness beside rates. Attribute an observed difference to a coaching regime only as an association; opponents, roster, quarterback, and game state can also change.
5. Separate **decision features** (neutral early-down dropback rate, pass rate over expectation, throw depth, fourth-down choices) from **outcomes** (EPA, success, drive endings) and **context** (pregame odds, opponent, venue, availability). Do not treat existing mutually exclusive play/drive outcome labels as a complete schematic or coaching taxonomy.

At this check, both 2025 and 2026 PbP rows use `nfl-play-archetype-v10` and `nfl-drive-archetype-v9`. The database has 272 regular-season 2025 games and 48 completed 2026 games through Week 3. Formation, personnel grouping, and pressure are populated for 2025 Pittsburgh offense but absent for its 2026 offense; do not present a year-over-year scheme claim from those fields. The participation feed may arrive only after the postseason. Recheck these facts when later weeks or source releases arrive.

## Example: New York Jets

For 2026 Weeks 1–3 the canonical games are `2026_01_NYJ_TEN`, `2026_02_GB_NYJ`, and `2026_03_NYJ_DET`. They join through `nfl_season_games.nflverse_game_id` to three populated `nfl_matchups` rows and 67, 65, and 60 NFL odds-history captures, respectively. The matched 2025/2026 Jets offensive samples contain 172/194 run-pass plays. Neutral early-down dropback rates are 27/59 (45.8%) and 54/95 (56.8%); EPA per play is -0.067 and +0.044. These are examples as of 2026-10-01, not hard-coded profile values.

## Team Identity page contract

- Keep `/nfl/team-identity` as the team-season summary and `/nfl/pbp` as the detailed game/drive/play evidence view.
- At the top show team, season-through-week, matched prior-season window, sample size, provisional status, and a one-sentence observed profile. Never imply a causal coach effect from three games.
- Present offense, defense, and drive outcomes side by side with the same definitions and denominators each year. A week-by-week chart should reveal whether the apparent shift is consistent or driven by one game.
- Add a context band with opponent, home/away, rest, pregame market spread/total/implied points where available, weather coverage, and key availability events. Clearly distinguish pregame expectations from postgame results.
- In each completed game card, explain the selected pregame moneyline, team spread, and total beside the final result. Moneyline outcome is the outright winner; spread edge is `(team_score - opponent_score) + team_spread`; total edge is `(team_score + opponent_score) - pregame_total`. Exactly zero is a push. The source's `vegas_prob_home` is a vig-free win chance derived from both moneylines; flip it for the away team. Missing quotes produce an unavailable result, never a synthetic line. `web/src/lib/nfl/team-identity.ts::settleNflIdentityMarket` owns the result rules, with push/over/under checks in `web/scripts/test-nfl-team-identity-market.ts`.
- Provide game cards and links to exact games in `/nfl/pbp?game=<game_id>`. Direct lookup supports older games beyond the current 60-game selector; exact drive/play URL anchors are future work.
- If persisting a derived profile, key it by team, season, season type, through-week, comparison window, metric-definition version, and as-of time. Retain source and label versions, selected odds snapshot IDs, sample counts, missingness, and generation time. Recompute on corrected PbP/source data rather than overwriting provenance.

## Verification before shipping a new source or metric

- Check game and team key coverage for both comparison seasons, including unmatched rows and duplicates.
- Check quote timestamps against kickoff and requested as-of time, and state whether a quote is first captured, selected pregame, or frozen close.
- Check non-null coverage by season/week for every compared feature; suppress a difference when one side lacks the field.
- Check denominator logic for run/pass plays, neutral early downs, unique drives, and participant joins against a hand-inspected game.
- Check that a 2025 game link opens its exact PbP game even if it is outside the 60 most recent games.

## Single-game player leader model

### Local leader-page formatting and context audit, 2026-10-09

The weekly `/nfl/game-leaders` view remains the original independent leader
model. The new joint candidate on `/nfl/game-model` is a separate frozen research
replay and has not replaced the weekly batch. The page now identifies this
distinction and its broad opponent adjustment versus absent scheme inputs and
score-state simulation. Percentage displays use one decimal with `<0.1%` for
positive smaller shares; range endpoints use at most one decimal. Displayed
columns are not renormalized. First-or-tied counts ties in full; leader share
splits them. Zero sampled wins do not establish an impossible outcome.

A read-only canonical audit of current database rows for DET/ARI Weeks 1-4 found
no populated 2026 man/zone, coverage, blitz or pressure fields across 316 measured
dropbacks (ARI 124; DET 192). All 515 run/pass rows have score differential and
remaining game clock. Matched 2025 Weeks 1-4 have populated scheme fields on 317
dropbacks. This is current retrospective coverage, not frozen forecast evidence.
The latest 2026 schedule refresh follows the saved forecast cutoff; use preserved
snapshots to reconstruct that decision rather than treating current table state
as historical availability. Coverage artifacts live under
`artifacts/nfl-joint-outcomes/defense-style-*-coverage-20261009.json`.

Missing scheme fields cannot be treated as no blitz or assumed unchanged 2025
styles. Score/clock-conditioned modeling, scheme-to-player-role effects and the
weekly joint-candidate publisher remain separate implementation tasks. See
`docs/nfl-game-leaders-backlog.md` for the updated distinction between implemented
research mechanisms, unavailable evidence and unfinished integrations.

The game-leader capture additionally reads primary nflverse PBP player stat
credits via `research/nfl_game_leaders_source.py`: join on canonical game ID and
play ID, verify season/week/home/away and frozen description, and retain source
URL, cache digest and capture time. Official receiving/rushing credit and lateral
recipient GSIS fields supplement the archetype participants. Later stat captures
cannot enter a forward decision. Final source play status controls replay/penalty
counting; a nullified touchdown does not automatically erase credited yardage.
Box workload is separately reconciled against the same provider's published team
aggregates. Unresolved event games can supply verified workload, but never invented
per-touch yardage. Each calculation records its coverage. Forecasts and publication
require canonical latest-game coverage; recent unresolved yardage events block
yardage forecasts. See the model contract and repair backlog for exact gates.

`docs/nfl-game-leaders-model.md` is the model and operation contract.
`research/nfl_game_leaders.py` reuses the canonical PBP capture and independently
reads the COMPLETE nflverse weekly player-stat source. Do not substitute
`ff_player_week_stats`: the current-universe filter omits historical players.
GSIS identities join only after unique canonical season/week/team/opponent and
source game ID agreement. Reconcile per-player carries, targets, receptions,
rushing yards and receiving yards before fitting/grading a complete game.
All periods and official QB kneels count. Missing/ambiguous attribution and source
corrections quarantine a game and remain visible in the report.

Strict forecasts require kickoff, PBP label, canonical schedule capture and box
fetch timestamps before the decision boundary. Historical later corrections are
explicitly retrospective; participant rows still lack independent availability
times. Each weekly game's provider capture has its own cutoff. Sleeper and
FantasyPros depth evidence, week injuries and official inactive import coverage
are retained in immutable requests. Depth positions are not projected workload.
No odds enter this model. `/nfl/game-leaders` is a manually published independent
view, not an optimizer scoring path or a slate-upload dependency. Forecasts,
requests, source digests, excluded games and evaluation reports belong under
`artifacts/nfl-game-leaders/`; publication data lives in
`web/src/data/game-leaders.json` (force-stage because the broad data ignore applies).
The dynamic page also checks current availability through `ff_players.gsis_id`,
the latest same-week FantasyPros injury observation, Sleeper injury status, and
week-matched `nfl_official` observations. It compares exact GSIS identities to
the saved forecast. Fresh confirmed-out players still present in that forecast
block its entire probability table until a new model run is published; the page
does not delete one row or renormalize saved probabilities. Missing or stale
provider snapshots block pregame tables.


### Total yards from scrimmage (local v2)

`total_yards` means official rushing yards plus receiving yards for an individual
player across the full game, including overtime. Passing, return and fantasy
bonus yardage are excluded. The model adds both components within the SAME
simulation draw before comparing every individual and splitting tied leaders.
It does not add component leader probabilities or component percentiles.
Recent-average baselines and grading derive the same sum from reconciled boxes;
no new source field or player join is required. Both components must meet the
existing reconciliation and recent-event coverage gates. The three source
metrics remain separate from the four forecast outcomes.

Original frozen forecasts remain unchanged. The v2 local Thursday example reruns
the saved 2026-10-08 decision inputs, not a new availability capture. Older page
snapshots explicitly mark total yards as not calculated, and historical summaries
without that outcome show "Not evaluated". The registered three-outcome
challenger experiment retains its original outcome scope. Mechanical verification
of this addition does not establish predictive accuracy for total yards.


### Shared workload and DFS expansion (local v3)

See `docs/nfl-shared-simulation-expansion.md` for acceptance criteria and current
scope. The fixed-dispersion leader baseline remains the default. An explicit
`--role-dispersion empirical` candidate estimates prior team-season share variance
with finite-count noise removed; all sample counts, fallbacks and bounds are
reported. `--export-draws` emits aligned individual GSIS draws, including separate
unresolved individuals, with source and implementation digests. Replacement role
requests now require timestamped evidence and a complete allocation.

`model/nfl_role_dispersion.py` supplies common role-share sampling and interval
scoring. `model/nfl_shared_dfs_efficiency.py` and
`model/nfl_shared_matchup_scenarios.py` are separate development copies of the
pinned efficiency-v3 / coherent-v5 engine, with an optional common dispersion
report. Originals and registered study hashes are preserved. New full-model
inputs are frozen keyword inputs to `research.nfl_shared_dfs_export`; keep source
capture time, canonical games, history, roster identities, salaries and source
manifest. Later source replays require explicit retrospective mode.

`web/src/lib/nfl-dfs/shared-game-model.ts` scores partial production components
and complete supplied DFS banks through canonical scoring/scenario utilities.
Partial production is never advertised as complete DFS points or a lower bound.
The complete consumer preserves scenario order, salary comparisons, legal lineups
and captain multipliers. Leaders derived from a salary pool are explicitly scoped
to that modeled slate field, not full-game market probabilities. No candidate
replaces optimizer points. The local view is `/nfl/game-model`; its saved data is
`web/src/data/shared-game-model.json`, with frozen reports under the leader
artifact directory. No DK upload is needed for the partial per-game view.

Routes/snaps, role-specific defensive effects, sequential score states, early
exits, fitted replacements, calibrated ranges and ownership are not implemented
by this first expansion. Original example inputs remain frozen; no newly captured
availability is implied. Historical range checks include zero projections and
correlated player-game rows, so their pooled coverage is only descriptive.

### Joint outcome candidate (local, 2026-10-09)

`docs/nfl-joint-outcomes-implementation.md` records executable local fitting,
forecasting, exact-set scoring, evidence timing, capture budgets and limitations.
`model/nfl_joint_outcomes.py` adapts the existing reconciled leader source; ambiguous
role/credit sequence joins leave target depth unknown because original prepared
events omit play IDs. No name-only attribution or new database join is introduced.
Non-exit role fitting, adjudicated segment exits and empirical gain profiles can
also enter the separate complete event candidate through
`model/nfl_joint_full_dfs.py`. Its production view shares actual event scenario IDs;
unallocated contributors keep its leader scope explicitly incomplete.

`research/nfl_alt_capture.py` adds local immutable raw/normalized quote evidence,
including each ladder rung, side, bookmaker, provider identity, observation and
publication time. It defaults to no calls/no credits. Canonical event/player
mappings remain explicit, paired probabilities require matching book/line/time,
and integer-push conditioning is preserved. No new production table or scheduled
capture is enabled. Market-free, game-market and player-market branches remain
distinct. The local page summary is `web/src/data/joint-outcomes.json`.

These are exploratory implementations. Historical exit/ownership/alt-line data,
chronological qualification and a future forward period are not implied by their
availability as code. Existing exclusions and protected registered engines remain.
