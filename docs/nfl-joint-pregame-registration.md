# NFL joint outcomes, pregame: two pre-registered studies (2026-10-09)

Registered before any outcome under either study is examined. Both apply to
the joint outcome candidate on `codex/nfl-game-leaders` (`nfl-joint-outcomes-v1`,
commit fd777631) and to the weekly game-leader page it feeds. Both are
**pregame** studies: the decision time is a frozen cutoff before kickoff, and
nothing observed after kickoff enters a forecast. Mid-game exits are out of
scope. A mid-game exit hazard is pregame knowledge in principle (a full-game
distribution includes the chance a player leaves early), but the exit code on
the branch stays disabled and this registration does not touch it.

| Study | ID | Question | Baseline |
|---|---|---|---|
| 1 | `nfl-joint-score-state-v1` | Does the pregame market line, through the game states it implies, improve team opportunity totals and the leader probabilities built on them? | Current paired-block sampling |
| 2 | `nfl-joint-kickoff-availability-v1` | Does a derived kickoff availability and role label, built from sources already captured, improve forecasts for games with uncertain players? | Current replay: `availability_verified = false`, status taken as given |

Everything below names tables that were checked against the live database on
2026-10-09. Row counts are in the inventory section at the end.

---

## Study 1: `nfl-joint-score-state-v1`

### What the candidate does today

Each scenario samples one historical game from the pooled 880-game history,
assigns its two sides to the two current teams, and scales each side's
targets and carries by the current team's budget. Both teams therefore share a
game script, which is right, but the script is drawn without regard to this
game's expected closeness or pace. A 14-point favourite and a pick'em draw from
the same bank.

### Mechanism, in two fitted parts, no sequential simulation

Score and clock are not observable pregame. What is observable is the market's
expectation of them. The model therefore has two fitted pieces and one draw:

- **Part A, state profile given the line.** For every historical team-game,
  compute the state-time profile: seconds spent leading by 1-7, 8-14, 15+,
  tied, trailing by the same bands, from `score_differential` and
  `game_seconds_remaining` on `nfl_pbp_archetypes`. Fit the distribution of
  that profile conditional on the pregame `spread_line` and `total_line`
  (closing lines, carried on every row). Implementation: nearest-neighbour
  resampling of historical profiles by standardised (spread, total) distance,
  k=40, which keeps the joint shape rather than a parametric one.
- **Part B, opportunity given the profile.** Fit team pass attempts and carries
  as a function of the profile (Poisson log-linear on the time shares, ridge
  10, fitted per season window). This is where "teams run when leading" lives,
  as a coefficient, not a stated prior.
- **Draw.** At pregame, read this game's market line, draw a profile from Part
  A, then draw team totals from Part B. The two teams share one profile with
  opposite signs, so their totals stay coherent. Player allocation, gains and
  tie rules are unchanged from the candidate.

Two variants are registered. Neither may be re-tuned after the evaluation.

| Variant | Description | Why it is in |
|---|---|---|
| V1 market-weighted blocks | Keep the candidate's block sampler but weight historical blocks by (spread, total) similarity, k=40 | The cheap version; isolates whether the line alone helps |
| V2 state-profile model | Parts A and B above | The version that uses the score and clock data the backlog asked for |

### Data and cutoffs

| Input | Source | Point-in-time rule |
|---|---|---|
| Play state | `nfl_pbp_archetypes.score_differential`, `game_seconds_remaining`, `play_type`, `posteam`, 2016-2025 (33k-36k scrimmage plays a season, all populated) | Historical fit only |
| Historical line | `nfl_pbp_archetypes.spread_line`, `total_line` | nflverse closing line; pregame by construction |
| Current line | `nfl_season_games.market_spread_line`, `market_total_line`, `market_captured_at`; fallback `game_odds_history` where `sport='nfl'` | `market_captured_at < decision_at < kickoff`. A line captured after the cutoff is not used, and a game with no pregame line falls back to the unweighted sampler and says so |
| Team budgets, player shares, gains | Unchanged from the candidate | Unchanged |

Do not use nflverse `xpass` or `wp` as features. Both are outputs of models
nflverse trained on later seasons, so a row from 2017 carries information from
2023. Fit the state effect directly.

### Walk-forward and populations

- Fit on 2016-2023. Develop on 2024 (choose k, ridge, the band edges). Freeze.
- Evaluate once on 2025, every regular-season game with complete history
  coverage (272 scheduled; report the count actually eligible).
- 2026 weeks 1-4 are exposed. They were examined while the candidate was
  built and are ineligible as a holdout. Weeks 5 onward are the prospective
  set: forecasts frozen at the weekly cutoff, graded as games complete.

### Metrics and gates (frozen)

Primary: paired difference in `exact_set_log_loss` for top-three sets, the
candidate's registered primary metric, over the four families (receptions,
receiving yards, rushing yards, total yards), game-clustered bootstrap, 2,000
resamples.

Secondary, reported always, gating nothing: team attempts and carries CRPS
against realized counts; leader-share Brier for the named leader in each
family; P10-P90 coverage.

| Gate | Threshold |
|---|---|
| Promote V1 or V2 to the weekly page | Upper bound of the 95% CI on the primary loss difference below zero, in the variant's favour, on 2025 |
| Floor | At least 200 eligible 2025 games |
| Guard | Relative CRPS degradation on team totals no worse than 2%; P90 absolute exceedance error no worse than 3 pp (the study JSON's existing gates) |
| Kill | CI includes zero for both variants: not promoted, and no re-slicing by spread magnitude, favourite or underdog, or family. Any variant of this idea is a new registration |

If V1 passes and V2 does not, ship V1. If both pass, ship V2 only if its CI
excludes V1's point estimate; otherwise ship V1, the simpler one.

### Development result (2026-10-09, before any 2025 outcome was examined)

Built: `model/nfl_joint_score_state.py` (Parts A and B, both variants, the
`conditioned_volumes` hook), `research/nfl_joint_score_state.py` (database
build, fit, development grid), `tests/test_nfl_joint_score_state.py`, and the
`block_weights` hook in `model/nfl_game_leaders.py::forecast` plus method
dispatch in `model/nfl_joint_outcomes.py::generate`.

Population: 2,639 regular-season games 2016-2025 from `nfl_pbp_archetypes`,
438,075 plays. Kickoffs come from `nfl_season_games` for 2023 onward (816
games) and from the nflverse schedule file for 2016-2022 (1,823 games); the
source is recorded per game. The first build silently covered 2023-2025 only
because the database has no kickoff before 2023; it was discarded.

Development grid on the volume layer only, trained 2016-2023 (2,095 games),
developed on 2024 (272 games, 544 team-games), team-total CRPS versus a
control that uses the same machinery with every training game as the
neighbourhood (`artifacts/nfl-joint-score-state/dev-grid-2024-v2.json`):

| | targets CRPS delta vs control | carries CRPS delta vs control |
|---|---|---|
| range over the 9 grid points | -0.047 to +0.007 | -0.108 to -0.167 |
| selected k=40, ridge=100 | -0.047 | -0.167 |
| control CRPS | 4.53 | 4.08 |

Reading: the line helps carries by about 3-4% of CRPS and helps pass attempts
barely at all, the same asymmetry the opponent workload study found. No
confidence interval is quoted for development numbers on purpose; they chose
k and ridge and nothing else.

**Frozen for the 2025 evaluation:** V2 `state_profile` k=40, ridge=100
(`fit-v2-state-profile-eval2025.json`); V1 `market_weighted_blocks` k=40
(`fit-v1-market-weighted-eval2025.json`); cutoff 2025-09-04T00:00:00Z, one day
before the first 2025 kickoff (a cutoff equal to the opener's kickoff rejected the
opener itself as "fit after decision" in the smoke run); 2,367 training games. Both fit files carry their training
set and digests. The primary exact-set evaluation on 2025 has NOT been run.

### Honest prior

Small gain expected. This repo's own results say play-by-play team tendencies
rarely beat box-score history: the opponent workload study kept only allowed
carries, red-zone trips were null, and the market line already encodes most of
what the game state will be. The reason to run it anyway is that the market
line is free, already captured, and currently ignored.

---

## Study 2: `nfl-joint-kickoff-availability-v1`

### What the candidate does today

The frozen TB at DAL replay reports `availability_verified = false`. A player
is either `status = 'out'` (removed entirely) or assumed fully active. There is
no probability of playing, no role tier for a player returning from injury,
and the three absence studies this repo already ran are not applied.

### The derived label

For each `(team, game, decision_at)`, one append-only label per candidate
player:

```
identity, game_id, decision_at
p_active            probability the player is active at kickoff
status_source       dk_pool | sleeper | fantasypros | roster_weekly
status_observed_at  the observation's own timestamp, must be <= decision_at
status_tag          the raw tag that produced p_active (O, D, Q, P, IR, ACT ...)
days_to_kickoff     at observation
role_share_targets  prior-4-game share, active games only
role_share_carries  same
confidence          'derived'
label_version       'kickoff-availability-v1'
source_refs         snapshot ids / response hashes of every observation used
```

`p_active` is not a stated prior. It is fitted on 2025 as the realized active
rate per `(status_tag, days_to_kickoff bucket, position)`, with a Beta(2, 2)
prior toward the tag's pooled rate. Realized active means on the roster with
status `ACT` in nflverse `roster_weekly_{season}.csv` **and** at least one
scrimmage role in `nfl_pbp_play_participants` that game; `INA` or `RES`, or
`ACT` with zero snaps, is absent. That is the same definition the three
absence studies used.

### Sources, all already captured

| Source | Table | What it gives | 2026 coverage checked |
|---|---|---|---|
| DraftKings pool status | `nfl_dfs_dk_pool_player_status` via `nfl_dfs_dk_pool_snapshots` | O, Q, D, IR tags per snapshot | 503 snapshots since 2026-09-24 |
| Sleeper | `ff_player_injury_observations` where `source='sleeper'` | status, practice status, `observed_at` | 372k rows, 2026-08-26 to today |
| FantasyPros | same table, `source='fantasypros'` | status, `observed_at` | 81k rows |
| Truth, history | nflverse `roster_weekly_{season}.csv` (`INA`, `RES`, `ACT`) + `nfl_pbp_play_participants` snaps | realized active or absent | 2019-2025 true status; 2014-2018 proxy already validated at 98% recall |
| Truth, 2026 | `nfl_pbp_play_participants` (2026 weeks 1-4 loaded) + the weekly roster file | same | 64 games so far |

`nfl_official_inactive_imports` exists and is empty. A hand audit replaces it
for this study; do not treat the table as a source until rows exist.

### Two tiers of label, one gate

The branch's fits accept only `confidence == 'adjudicated'`. This study adds
`'derived'` and a gate: a derived label may enter a fit only after the labeller
passes its audit (below) for that season. The fit records which tier it used.
Hand-adjudicated labels remain the higher tier and override derived ones for
the same player-game.

### Audit of the labeller (before any model comparison)

- **Calibration.** On 2025, Brier score of `p_active` against realized active,
  per tag and per days-to-kickoff bucket, versus the base rate. Required:
  Brier below base rate in every bucket with at least 50 observations, and
  reliability within 5 pp in each decile with at least 100 observations.
- **Hand audit.** 100 team-games from 2025, stratified by weekday of the
  decision cutoff, checked against the team's published inactive list. Required:
  no player labelled `p_active >= 0.9` who was listed inactive, and no player
  labelled `p_active <= 0.1` who played 10+ snaps, in more than 3 of 100.
- **Source agreement.** Report the disagreement rate between DK, Sleeper and
  FantasyPros tags at the same cutoff. Disagreement is surfaced on the page,
  never resolved silently. (Memory rule: when a user fact and a feed disagree,
  say so and check a third source.)

### How the label enters the forecast

- Per scenario, draw presence for each candidate as Bernoulli(`p_active`)
  across the whole game. No segment hazard; this is kickoff availability only.
- When absent, redistribute with the **budget rule** the absence studies
  established: remaining active teammates keep their own prior shares, and only
  the leftover volume goes to the unresolved slot. The additive transfer was
  measured worse three times and is not used.
- The one promoted exception is applied as registered: in the first game a
  material back misses, the lead remaining back gets the `nfl-rb-first-game-blind-v1`
  pie, shadow only, and the page shows both numbers. The WR and TE breakout
  flags stay display-only.

### Walk-forward and populations

- Labeller fitted on 2024 observations where the injury tables have history;
  if 2024 observation depth is too thin (fewer than 2,000 flagged
  player-games), fit on 2025 weeks 1-9 and evaluate on weeks 10-18, and say so.
- Model comparison on 2025 evaluation weeks: candidate with labels versus
  candidate without, same scenarios, same seeds.
- 2026 weeks 5 onward prospective, labels frozen at each weekly cutoff.

### Metrics and gates (frozen)

Primary: paired difference in `exact_set_log_loss`, restricted to team-games
with at least one candidate whose `p_active` is between 0.1 and 0.9 at the
cutoff (the games where the label can matter). Game-clustered bootstrap.

Secondary: leader-share Brier for flagged players; P10-P90 coverage for the
teammates of flagged players; the count of forecasts that would have been
blocked under the old binary rule.

| Gate | Threshold |
|---|---|
| Labeller accepted | All three audit checks pass |
| Model change promoted | Primary CI upper bound below zero on the flagged population, at least 120 flagged team-games |
| Kill | CI includes zero: the labels still ship to the page as displayed availability (they are better than "unverified"), but the forecast is not changed by them. No re-slicing by position or tag |

### Honest prior

The display value is near-certain and the forecast value is uncertain. The
absence studies found the budget rule fixes a bias without placing points on
the right player, and this study inherits that limit. The win here is a
forecast that knows who is doubtful, states it, and stops assuming full
participation for a player DraftKings has tagged.

---

## Shared rules

- Version bump on any change to a threshold, feature, cutoff or population.
- Game-clustered bootstrap everywhere; players on one team in one game are
  not independent observations.
- Both studies report `n`, the date span, and the exposed-week exclusions in
  every output, and the weekly page shows which study version produced a number.
- Out of scope, each needing its own registration: mid-game exits, defensive
  scheme response, player alternate ladders (paid), ownership (owned by
  `docs/nfl-ownership-model.md`), reconciliation to production projections.

## Inventory used for this registration (live database, 2026-10-09)

| Table | Coverage |
|---|---|
| `nfl_pbp_archetypes` | 2016-2025 complete; 2026 weeks 1-4, 10,550 plays, score and clock on every scrimmage play, scheme fields empty for 2026 |
| `nfl_pbp_play_participants` | 2016-2025 about 118k rows a season; 2026 27k rows, 64 games |
| `nfl_season_games` | market spread, total, moneylines with `market_captured_at` |
| `game_odds_history` (`sport='nfl'`) | 1,686 / 3,383 / 1,601 rows for Aug / Sep / Oct 2026 |
| `ff_player_injury_observations` | Sleeper 372,517 rows, FantasyPros 81,387 rows, 2026-08-26 to 2026-10-09 |
| `nfl_dfs_dk_pool_snapshots` | 503 snapshots, 2026-09-24 to 2026-10-09 |
| `nfl_dfs_player_week_results` | 2023-2026 weekly DK points, the grading source |
| `nfl_official_inactive_imports` | empty |
