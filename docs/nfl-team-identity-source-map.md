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

This is the starting point for agents maintaining cross-season NFL team archetypes. The existing `/nfl/pbp` page is the game and play evidence view. The Team Identity page summarizes a team's choices and results across games, then links back to the exact games and plays. Keep source facts in their existing tables; a future profile table or view should store only derived measures and provenance.

## Source ownership and keys

| Question | Source of truth | Join or use | Important constraint |
| --- | --- | --- | --- |
| Play choices and outcomes | `nfl_pbp_archetypes` | `game_id`, `play_id`; offense `posteam`, defense `defteam` | Filter `season_type = 'REG'` for regular-season comparisons. Use `play_type IN ('run','pass')` for scrimmage rates and explicitly state other denominators. |
| Drive outcomes | `nfl_pbp_archetypes` | One observation per `(game_id, posteam, drive)`, then use `drive_archetype` | Do not count the drive label once per play. Inspect clock-ending/kneel drives when setting a denominator. |
| Shadow DFS game scripts | `nfl_pbp_archetypes` plus `nfl_season_games` | `research/nfl_pbp_script_export.py` verifies `nflverse_game_id = game_id`, season, week, and both teams, mapping nflverse `LA→LAR` and `WAS→WSH` only for the canonical schedule join; drive teams retain Showdown `LAR` and `WAS` codes. `model/nfl_pbp_script_bank.py` groups `(game_id, posteam, drive)` once | For a pregame bank require `kickoff < decision_at` and `labelled_at <= decision_at`; retrospective later labels are mechanics-only. Script transport is not a qualified forecast or a production projection. |
| Players on a play | `nfl_pbp_play_participants` | `(game_id, play_id)` | One play has multiple participant rows; aggregate before joining to avoid multiplying plays. |
| DFS player opportunity chips | `nfl_pbp_play_participants` joined to `nfl_pbp_archetypes` | Play on `(game_id, play_id)`; participant `player_id` (GSIS) to the season's `ff_players.gsis_id`, then `ff_players.id` to `nfl_dfs_slate_players.ff_player_id` | Use receiver/rusher roles only, distinct role-play rows, the previous three regular-season weeks of the same season, and `labelled_at <= saved projection as-of`. Unknown identity or insufficient games yields no chip, not zero opportunity. Participant rows lack an as-of timestamp, so a reconstructed past slate can reflect later participant corrections; the saved optimizer input snapshot is the record of chips used for generated lineups. `web/src/db/nfl-dfs-player-signals.ts` and `web/src/lib/nfl-dfs/player-signals.ts` own the query and thresholds. |
| DFS QB matchup evidence | `nfl_pbp_archetypes`, `nfl_season_games`, `nfl_teams`, `game_odds_history` | Prior completed regular-season games join on `nflverse_game_id = game_id`, season, and week; slate game uses `nfl_season_games.matchup_id` for the last odds capture before both saved projection as-of and kickoff | `web/src/db/nfl-qb-matchup-context.ts` reads the facts; `web/src/lib/nfl-dfs/qb-matchup-context.ts` computes versioned summaries. It uses only prior weeks with `kickoff < as_of` and `labelled_at <= as_of`. It is descriptive and does not alter fantasy points. The optimizer input snapshot retains the selected odds ID/time, sample counts, and source game IDs. |
| Rolling player projections | `nfl_dfs_projection_runs`, `nfl_dfs_player_projections`, `nfl_season_games`, `nfl_teams` | Select the latest immutable run for an upcoming regular-season week, then match each player team and opponent to that week's future canonical game | `/nfl/projections` reads scheduled runs directly through `web/src/db/nfl-rolling-projections.ts`; a DK salary upload is never required. The page excludes a player if its saved opponent no longer matches the schedule or kickoff has passed. It shows the run's frozen `as_of_at` separately from the live market/PbP read time. It never presents current odds as having been in the model's frozen inputs. |
| Game, opponent, venue, rest, and market bridge | `nfl_season_games` | `nflverse_game_id = nfl_pbp_archetypes.game_id`; `home_team_id`/`away_team_id` reference `nfl_teams.team_id` | This is the canonical bridge from the nflverse game ID to market matchup ID. Verify uniqueness and team/date agreement. |
| Team identity | `nfl_teams` | Numeric `team_id` to `abbreviation` (`NYJ`, `PIT`, etc.) | Do not guess numeric IDs or join on display names. |
| Current NFL game markets | `nfl_matchups` | `nfl_season_games.matchup_id = nfl_matchups.id` | Its lines are the latest convenience values, not an as-of history. |
| Timestamped NFL odds | `game_odds_history` | `sport = 'nfl'` and `matchup_id = nfl_season_games.matchup_id` | Use a snapshot captured before the relevant kickoff or decision cutoff; retain `id`, `captured_at`, and book/consensus definition. Do not call the first captured quote the true opener. |
| Frozen closing quote | `event_closing_lines` | `sport = 'nfl'`, `matchup_id` | Use the recorded boundary and quality/eligibility fields. Do not substitute a later quote. See `docs/event-driven-closing-lines.md`. |
| Historical 2025 lines | `nfl_line_snapshots` | Resolve event by verified season, kickoff, and both teams; validate against `nfl_season_games` | Historical `snapshot_at` is the market time; `captured_at` reflects a later archive import. These rows have spread and moneyline, but no comparable game total. Apply `snapshot_at < commence_time`. |
| Injuries and roster context | `ff_player_injury_observations`, `ff_source_snapshots`, and eventually `ff_v2_roster_weeks` | Resolve player/team identity through the repository's identity mappings; use effective and available times | Raw 2026 weekly-roster snapshots exist, but `ff_v2_roster_weeks` currently has no 2026 rows. Do not join on player name alone or infer participation from an injury listing. See `docs/fantasy-football-v2-source-contracts.md`. |
| Weather and venue on plays | `nfl_pbp_archetypes` (`roof`, `surface`, `temp`, `wind`) and `nfl_season_games` | Game-level context | Show field coverage; missing weather is unknown, not zero. |

The labeling code is `model/nfl_play_archetypes.py` and `model/nfl_drive_archetypes.py`; `ingest/nfl_pbp_archetypes.py` persists their versions. `web/src/db/queries.ts` supplies the game explorer at `web/src/app/nfl/pbp/`. The explorer's selector loads the 60 most recent games, and `web/src/db/nfl-team-identity.ts::getNflArchetypeGameById` resolves older direct links. The Team Identity query, options, and game joins live in that same DB module; rolling summaries and denominators live in `web/src/lib/nfl/team-identity.ts`; the server-rendered page is `web/src/app/nfl/team-identity/page.tsx`.

## Safe joins and calculations

1. Start with `nfl_season_games`, using its `nflverse_game_id` for PbP and `matchup_id` for 2026 odds. Check `season`, `week`, home team, away team, and kickoff before accepting a join. The 2025 `nfl_season_games` records have `nflverse_game_id` but currently no `matchup_id`; use the separately archived `nfl_line_snapshots` only after event-level validation.
2. Filter odds to observations known before kickoff, or before an earlier requested as-of time. For postgame explanation, identify the exact quote selected. For a historical prediction, never use a closing quote if the prediction cutoff was earlier.
3. Compute play metrics from play rows, drive metrics from unique drives, and player participation from aggregated participant rows. Keep offense and defense denominators separate. A touchdown drive rate must state whether clock-ending and kneel drives are included.
4. Compare the same season type and week window first (for example, 2025 Weeks 1–3 against 2026 Weeks 1–3). Show the prior full season as a second baseline, not as the matched sample. Show play/game counts and missingness beside rates. Attribute an observed difference to a coaching regime only as an association; opponents, roster, quarterback, and game state can also change.
5. Separate **decision features** (neutral early-down dropback rate, pass rate over expectation, throw depth, fourth-down choices) from **outcomes** (EPA, success, drive endings) and **context** (pregame odds, opponent, venue, availability). Do not treat existing mutually exclusive play/drive outcome labels as a complete schematic or coaching taxonomy.

For DFS chips, a target is a receiver role on a pass play; a catch requires non-null `yards_after_catch`. Caught air yards sum `air_yards` only on catches. Expected YAC sums `xyac_mean_yardage` on catches. An inside-5 carry is a rusher role on a run play from `yardline_100 <= 5`, excluding QB dropbacks. Chips describe observed opportunity. The default Classic GPP rule requires one player with a user-selected signal type in each lineup; the user can turn it off. It does not change fantasy-point or ownership forecasts. Saved optimizer inputs retain the chips and selected signal types used for construction.

The DFS QB panel at `/dfs/nfl` shows the historical projection per $1,000 salary beside the team's implied points, team spread and game total, its prior close-state dropback rate (score within seven; run/pass scrimmage plays), an opponent-adjusted defensive pass EPA difference, and opponent touchdown drives per competitive drive. The EPA difference compares each offense's dropbacks against the upcoming defense with its other games in the same season, weighted by dropbacks; positive means the defense allowed higher EPA. Require two comparable games and 60 dropbacks for that estimate, 40 close-state plays, and 15 labelled competitive drives; missing data is explicitly unavailable. Competitive drives are unique `(game_id, posteam, drive)` and exclude clock-expired/kneel endings. The panel groups expected starters, unresolved roles, then backups. These are small-sample diagnostics and must remain outside the optimizer scoring path until a frozen forward test demonstrates a useful, calibrated fantasy-point adjustment.

`/nfl/projections` is the upload-free companion. The existing twice-daily `refresh_nfl_dfs_projections.yml` workflow creates the immutable baseline; the page selects the latest run for an upcoming week and rereads live context on request or refresh. It presents DK-scored projections for all positions and QB matchup assessments, ordered by projected points rather than salary value. Its live odds/PbP timestamps are separate from the baseline run time, so a later odds movement cannot be mistaken for a model input. After every game has started, it reports that no upcoming games remain instead of reusing those rows as pregame forecasts.

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
