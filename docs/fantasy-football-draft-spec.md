# Fantasy Football Draft Assistant

Status: Proposed
Date: 2026-08-01
Owner: NBA DFS v2
Target surface: `/fantasy-football/draft`

## 1. Summary

Build a dedicated season-long fantasy football section whose first product is
a live draft assistant. The tool should help one user configure a league,
import or generate rankings, track every selection, and continuously recommend
the best players for that user's next pick.

This is separate from the `/nfl` game-odds page and from DraftKings daily
fantasy. Phase 1 is a draft-room companion; it does not host a league or submit
picks to a fantasy platform.

## 2. Product Decisions

Phase 1 assumes:

- season-long redraft leagues;
- snake drafts with 8–14 teams;
- one controlled team per draft session;
- 1QB and superflex configurations;
- standard, half-PPR, PPR, and custom scoring;
- manual pick entry as the universal workflow;
- optional read-only Sleeper synchronization;
- desktop-first design with a usable tablet/mobile fallback;
- one application user initially, with ownership fields reserved for auth.

Phase 1 excludes:

- auction drafts;
- dynasty, rookie, and keeper drafts;
- third-party draft write-back;
- simultaneous multi-user drafting inside this application;
- IDP leagues;
- best-ball portfolio optimization;
- weekly start/sit, waiver, and trade tools.

## 3. Goals and Success Criteria

The user must be able to:

1. Create a draft in under two minutes.
2. Configure league size, draft slot, roster slots, scoring, and draft order.
3. Import rankings/projections from CSV or use a stored baseline player pool.
4. Record any pick in no more than two interactions.
5. Undo an incorrect pick without corrupting draft history.
6. See best available players, tiers, roster needs, bye weeks, ADP value, and
   value over replacement after every pick.
7. Know who is on the clock and how many picks remain until the user's turn.
8. Resume after refresh or browser restart without losing state.
9. Optionally attach a Sleeper draft ID and follow selections read-only.
10. Export the completed board and controlled roster.

Operational targets:

- pick entry reflected in recommendations within 300 ms at p95;
- refresh restores the complete draft state;
- no player may be actively drafted twice;
- recommendation inputs and components are visible and reproducible;
- external-source failures never block manual drafting.

## 4. Routes and Navigation

Add `Fantasy Football` to product navigation without replacing `/nfl`.

```text
/fantasy-football                 -> landing page and recent drafts
/fantasy-football/draft/new       -> setup wizard
/fantasy-football/draft/[draftId] -> live draft room
/fantasy-football/rankings        -> player pool, tiers, and imports
```

Future routes, outside Phase 1:

```text
/fantasy-football/waivers
/fantasy-football/start-sit
/fantasy-football/trades
```

## 5. Draft Setup

The wizard collects:

- draft name;
- NFL season, defaulting to the upcoming/active season;
- team count, default 12;
- controlled draft slot, default 1;
- roster slots;
- scoring preset or custom scoring;
- ranking source;
- optional Sleeper draft ID;
- optional team names and custom order.

Default roster:

```text
QB  RB  RB  WR  WR  TE  FLEX  K  DST  BN x6
```

Supported slots:

```text
QB | RB | WR | TE | FLEX | SUPERFLEX | K | DST | BN
```

`FLEX` accepts RB/WR/TE. `SUPERFLEX` accepts QB/RB/WR/TE. Bench slots accept
all positions.

The setup flow is:

```text
League -> Roster -> Scoring -> Rankings -> Review
```

The review step must show total rounds, total selections, draft order, roster
requirements, final numeric scoring settings, and ranking source.

## 6. Rankings and CSV Import

The user can choose an application baseline, a saved ranking set, or CSV.

Required CSV columns:

```text
player_name,position,nfl_team
```

Optional columns:

```text
external_id,overall_rank,position_rank,tier,adp,projected_points,
projection_low,projection_high,bye_week,injury_status,notes
```

Import rules:

- normalize punctuation, suffixes, accents, and team abbreviations;
- exact-match external IDs first;
- then exact normalized name plus position;
- offer manual resolution for ambiguous or unmatched rows;
- never silently merge two players;
- show matched, inserted, ambiguous, and rejected counts before saving;
- preserve the original row and import timestamp.

Phase 1 must not depend on scraping commercial ranking sites. Baseline data
must be licensed/open or explicitly uploaded by the user. ADP is source-specific
and must always be labeled by source.

## 7. Live Draft Room

The room shows:

- full team-by-round draft board;
- current pick, round, selecting team, and picks until the user's next turn;
- controlled roster with filled and open slots;
- available-player table;
- recommendation panel;
- position/tier filters and player search;
- recent picks and undo controls;
- Sleeper sync health when connected.

Manual selection:

1. Search or select an available player.
2. Choose `Draft player`.
3. Persist the pick and recompute the board atomically.
4. Confirm only destructive corrections or material pick replacements.

Keyboard support:

- `/` focuses search;
- arrows move through results;
- Enter selects the highlighted player;
- `U` opens undo for the latest pick;
- position shortcuts filter the pool.

Completion occurs when every configured slot is filled or the user ends the
draft early. Exports include all-picks CSV, controlled-roster CSV, printable
board, and a JSON settings/rankings snapshot.

## 8. Draft Order Engine

For `N` teams:

```text
odd round owner  = draft slots ascending 1..N
even round owner = draft slots descending N..1
overall pick     = (round - 1) * N + pick within round
```

Materialize every board slot when the draft is created. Persist the resolved
owner so later setting changes cannot rewrite historical ownership.

Required behavior:

- standard snake order;
- custom team names;
- manually adjustable future pick owner;
- pause/resume;
- undo latest pick;
- correct an earlier pick using reversal events;
- commissioner correction for wrong player or team;
- advance only after a pick transaction commits.

Third-round reversal and traded picks can be represented through custom future
owners, but do not require dedicated Phase 1 controls.

## 9. Scoring

Store explicit numeric weights rather than only a preset name.

Initial scoring keys:

```text
passing_yards
passing_touchdowns
passing_interceptions
rushing_yards
rushing_touchdowns
receptions
receiving_yards
receiving_touchdowns
fumbles_lost
two_point_conversions
field_goals_0_39
field_goals_40_49
field_goals_50_plus
extra_points
```

Presets set receptions to 0, 0.5, or 1.0. Custom mode exposes every field.
DST can use imported fantasy-point projections in Phase 1; a complete DST
event model is deferred.

When projected stats exist:

```text
projected_fpts = sum(projected_stat_i * scoring_weight_i)
```

If only source fantasy points exist, retain the source scoring profile and
warn when it differs from the league. Never label an unconverted projection as
customized.

## 10. Player Valuation

### 10.1 Replacement level

Calculate replacement separately for QB, RB, WR, TE, K, and DST:

1. Count mandatory starters league-wide.
2. Allocate FLEX/SUPERFLEX demand to the best eligible projected players.
3. Add bench demand, initially 35% of league bench slots distributed using
   position demand.
4. Set replacement to the next player after estimated positional demand.

```text
VOR = player_projected_fpts - replacement_projected_fpts(position)
```

Persist assumptions and expose the replacement rank/player behind each value.

### 10.2 Recommendation score

Initial normalized score:

```text
recommendation_score =
    VOR                 * 0.35
  + roster_need         * 0.20
  + scarcity            * 0.15
  + ADP_value           * 0.15
  + next_turn_urgency   * 0.10
  - risk_penalty        * 0.05
```

- `VOR`: value over positional replacement.
- `roster_need`: open required slots and remaining rounds.
- `scarcity`: projection drop to the next tier.
- `ADP_value`: how far the player fell versus source ADP.
- `next_turn_urgency`: likelihood the player is gone before the user's turn.
- `risk_penalty`: supplied injury, role, suspension, or projection-width risk.

Weights are versioned configuration. Each recommendation exposes its component
values and a short explanation, for example:

```text
RB tier ends in 2 players; 18 picks until your turn; fills RB2; +11 ADP value.
```

The tool must not claim optimality or win probability.

### 10.3 Constraints and preferences

Recommendations must:

- exclude drafted, inactive, retired, and manually hidden players;
- stop recommending positions with no compatible active/bench slot;
- understand FLEX and SUPERFLEX;
- flag but not prohibit bye-week concentration;
- flag QB/TE/K/DST reaches relative to source ADP;
- support target, fade, and exclude preferences.

## 11. Data Sources

### Sleeper

Use Sleeper for optional player identity, league settings, draft order, and
draft picks. Its public API is read-only and requires no token, so the app must
not imply that it submits picks.

During an active connected draft, poll picks every five seconds only while the
page is visible. Stop while paused, hidden, or completed. Cache the full NFL
player payload daily rather than per page view, and stay below Sleeper's
published 1,000-calls-per-minute guidance.

Reference: [Sleeper API documentation](https://docs.sleeper.com/).

### nflverse

Use nflverse for stable player crosswalks, rosters, and historical NFL stats
when projection modeling is added. Cache datasets and record release/version;
never download large datasets during a live draft.

References: [nflverse](https://nflverse.nflverse.com/) and
[nflreadpy loaders](https://nflreadpy.nflverse.com/api/load_functions/).

### Existing application data

Reuse `nfl_teams` for team identity and display. Do not couple the draft room
to The Odds API. Game lines and Polymarket deltas may become future enrichment
but must never block drafting.

### FantasyPros

FantasyPros is the primary managed source for draft rankings, tiers, projected
statistics, player metadata, and draft-relevant injury context. It is an input
to this application's valuation model, not a live draft platform: the API does
not provide league draft state, picks, or pick submission.

Use the current public v2 API:

```text
Base URL: https://api.fantasypros.com/public/v2/json
Auth:     x-api-key: FANTASYPROS_API_KEY
Method:   GET for every endpoint used here
```

`FANTASYPROS_API_KEY` is server-only. It is stored in GitHub Secrets and Vercel
environment variables, must never use a `NEXT_PUBLIC_` name, and must never be
returned in a server-action error or request log.

#### Draft endpoint contract

| Dataset | Request | Draft use |
|---|---|---|
| Player directory | `GET /nfl/players?ecr=included&show=pos_rank&external_ids=yahoo:espn:cbs:nfl:mfl:draftkings` | Canonical FantasyPros ID, name, team, positions, rookie flag, platform crosswalks, and lightweight ECR/ADP fields |
| Draft ECR | `GET /nfl/{season}/consensus-rankings?position=ALL&type=DRAFT&scoring={STD\|HALF\|PPR}` | Overall/position rank, tier, bye week, expert count, and source freshness for each scoring format |
| ADP | `GET /nfl/{season}/consensus-rankings?position=ALL&type=ADP&scoring={STD\|HALF\|PPR}` | FantasyPros ADP market rank; store separately from ECR |
| Rank distribution | `GET /nfl/{season}/rankings?week=0&range=true&rankstats=true` | ECR minimum, maximum, average, standard deviation, ADP, and disagreement/risk signals when available |
| Projections | `GET /nfl/{season}/projections?week=0&positions=QB:RB:WR:TE:K:DST` | Full-season projected stat lines and FantasyPros STD/HALF/PPR point totals |
| Injuries | `GET /nfl/injuries?year={season}&week=0&include_probabilities=true` | Status, injury type/comment, update date, IR weeks, and practice/probability fields when available |
| News | `GET /nfl/news?limit=100&order_by=updated` | Recent injury, transaction, rumor, and breaking context; deduplicate by news item ID |
| Experts | `GET /nfl/{season}/rankings/experts?position=ALL&type=DRAFT&scoring={format}&include_overall=true` | Optional provenance and expert-pool audit; not required for every refresh |
| Compare players | `GET /nfl/compare-players?players={2-4 FP IDs}&position={position}&ranking_type=draft&details=all` | Optional diagnostics only; the live comparison UI should normally use stored rankings |

Do not infer that `rank_ecr` in an ADP response is draft-expert consensus. The
response's `ranking_type_name` and requested `type` determine the metric. Persist
FantasyPros ECR, FantasyPros ADP, and every external ADP source as distinct
records with visible labels.

The player endpoint does not document a Sleeper ID. Match Sleeper through other
stable crosswalks where possible, then normalized name + position + team, and
queue ambiguous matches for manual resolution. Never match from rank or ADP.

#### Projection mapping and custom scoring

For QB projections, persist at least:

```text
pass_att, pass_cmp, pass_yds, pass_tds, pass_ints,
rush_att, rush_yds, rush_tds, fumbles, ret_tds, 2pt_tds
```

For RB/WR/TE projections, persist at least:

```text
rush_att, rush_yds, rush_tds, rec_rec, rec_yds, rec_tds,
fumbles, ret_tds, 2pt_tds
```

Also retain `points`, `points_half`, and `points_ppr` as source reference values.
For custom league scoring, calculate points locally from the projected stat
line. This lets a six-point passing-TD or reception-premium league diverge from
FantasyPros' three canned scoring formats without pretending that source PPR
points are customized.

Important limitations:

- the documented projection field is `fumbles`, while the setup model scores
  `fumbles_lost`; do not apply a lost-fumble penalty unless the field semantics
  are verified or a separate source supplies fumbles lost;
- kicker projections expose field goals made/attempted and extra points, but
  not documented 0-39/40-49/50+ buckets, so distance-based custom kicker scoring
  cannot be reproduced exactly from this endpoint;
- DST projections expose sacks, interceptions, touchdowns, safety, fumble
  events, return touchdowns, and points-allowed buckets; use the supplied DST
  fantasy-point total unless the local scoring model maps every required bucket;
- projection payload types have historically mixed strings and numbers. Parse
  defensively, accept the documented `stats` array and observed legacy object
  shape, preserve the raw payload, and reject non-finite values;
- do not use player image URLs. FantasyPros states those images are separately
  licensed by Sportradar and are not included in normal API rights.

#### How FantasyPros powers the page

| Page surface | FantasyPros behavior |
|---|---|
| `/fantasy-football` | Show a compact data-health card with current season, latest ECR/projection snapshot, match coverage, and stale/error state |
| `/fantasy-football/draft/new` | Default to the newest compatible FantasyPros DRAFT snapshot; map Standard/Half-PPR/PPR to STD/HALF/PPR and preview its timestamp |
| `/fantasy-football/rankings` | Explore ECR, tiers, ADP, projections, deltas, injuries, filters, and snapshot history; allow CSV overlay without overwriting source data |
| `/fantasy-football/draft/[draftId]` | Read the draft's pinned snapshot and use its values in best-available ordering and explainable recommendations |

The Rankings page shows a source selector and scoring tabs for STD, HALF, and
PPR. For each player it displays:

```text
FantasyPros ECR | position rank | tier | FantasyPros ADP | ECR-vs-ADP delta
projected points | VOR | expert disagreement | injury | last updated
```

The live draft room uses the stored FantasyPros snapshot to:

1. Seed the available-player order with scoring-specific ECR and tiers.
2. Recalculate projected fantasy points from projected stats for custom scoring.
3. Calculate replacement level, VOR, positional scarcity, and tier cliffs.
4. Calculate value versus FantasyPros ADP and versus the independent market ADP.
5. Estimate next-turn availability from ADP and the number of picks until the
   controlled team selects again.
6. Apply a transparent risk modifier from rank dispersion and injury status.
7. Explain recommendations with source values, such as `ECR 18, ADP 27 (+9
   value), final player in Tier 3, 21 picks until your next turn`.

The recommendation model uses FantasyPros as features, not as an answer to copy:

```text
baseline_rank       = FantasyPros scoring-specific draft ECR
source_tier         = FantasyPros tier
fp_adp              = FantasyPros ADP
market_adp          = median(FantasyPros ADP, FFC ADP, MFL ADP)
ecr_adp_delta       = fp_adp - baseline_rank
market_delta        = market_adp - baseline_rank
rank_risk           = normalize(ECR standard deviation or rank range)
projected_fpts      = local_score(FantasyPros projected stats, league scoring)
```

Positive deltas mean the player is available later in drafts than the ECR
baseline. The UI must show both the sign and labels so a user does not have to
guess which direction is favorable.

The recommendation panel must keep the current source snapshot fixed for an
active draft. A refresh may create a newer ranking set for future drafts, but
must not silently reorder an in-progress board. The user can explicitly adopt
a newer snapshot, which creates a `settings_changed` event and preserves the
previous ranking-set ID in the event payload.

#### Outperforming the FantasyPros baseline

FantasyPros is the market baseline, not the application's final opinion. Its
consensus is valuable because it aggregates many informed views, but consensus
can react slowly, compress genuine uncertainty, inherit shared assumptions, and
miss league-specific value. This application should outperform by combining the
FantasyPros prior with independently modeled opportunity, efficiency,
availability, team context, and draft-price timing.

```text
play-by-play + participation + rosters + transactions + coaches + rookies
        -> effective-dated player/team facts
        -> as-of-date feature snapshots
        -> opportunity, efficiency, and availability distributions
        -> league-specific projections and VOR
        -> our rank, market deltas, and live-pick recommendations
        -> realized results and walk-forward calibration
```

The product's core question is not simply `Who will score the most points?` It
is:

```text
Given this league's scoring and roster rules, this player's range of outcomes,
the players already drafted, and the probability he survives until our next
pick, which selection creates the most incremental roster value now?
```

##### Independent projection model

Build the player forecast from components rather than applying an unexplained
adjustment to FantasyPros projected points:

```text
expected_season_points =
    expected_games_active
  x expected_opportunities_per_game
  x expected_points_per_opportunity
  x scoring_and_role_adjustments

our_rank = rank_by(
    VOR distribution,
    roster construction,
    positional scarcity,
    downside/ceiling preference,
    probability available at next pick
)
```

FantasyPros ECR and projections enter as Bayesian priors and comparison
features. They should stabilize sparse estimates, especially early in the
offseason, but their weight falls as the application gains reliable evidence.
Never define `our projection` as FantasyPros plus a subjective bump. Persist
every component and explanation so the opinion can be reproduced and tested.

##### Last season and prior-year performance

For returning players, ingest play-by-play and participation data and derive:

- fantasy points per game and per opportunity;
- games active, games missed, starts, snaps, routes, and route participation;
- target share, targets per route, air-yard share, first-read targets, and
  red-zone/end-zone opportunities;
- rush share, goal-line share, yards before/after contact, explosive-play rate,
  and receiving usage for running backs;
- quarterback attempts, designed rushes, scramble rate, pressure response,
  touchdown rate, interception rate, and supporting-cast efficiency;
- team pace, neutral-situation pass rate, pass rate over expectation, scoring
  rate, and position-level opportunity shares;
- full-season, final-eight-game, and final-four-game form without treating a
  small hot streak as a new permanent talent level.

Use per-route, per-snap, and per-opportunity measures alongside raw totals.
Totals alone underrate players who earned an expanded late-season role and
overrate players whose production came primarily from unusually high playing
time or touchdown luck. Regress unstable efficiency and touchdown rates toward
position, age, and role priors. Use multiple seasons with recency weighting when
the player's role and health are comparable.

##### Games played and availability

Separate weekly ability from the probability of being available:

```text
points_per_active_game != expected_season_points / scheduled_games
```

Estimate `expected_games_active` from recent games missed, current injury and
recovery information, age, position, suspension/PUP status, recurring versus
one-time injury classifications, and expected role. Show both projections:

```text
active-game projection | expected games active | season projection
```

Do not blindly label an injury-prone player or penalize every missed game
equally. A resolved fracture, a recurring soft-tissue issue, a suspension, and
being a healthy inactive are different signals. Keep availability uncertainty
separate from performance uncertainty so users can choose floor or ceiling.

##### Rookie translation

Rookies have no NFL game history, so they require a separate model rather than
missing values or arbitrary veteran comparisons. Candidate features include:

- NFL draft capital and whether the team traded up;
- age, early-declare status, breakout age, and career production trajectory;
- college target share, dominator rating, yards per route, air yards, missed
  tackles forced, receiving ability, and production against strong competition;
- athletic testing and size adjusted for position;
- quarterback quality and passing/rushing style;
- landing-spot depth chart, vacated opportunities, offensive-line quality,
  competition for touches, and coaching history with rookies;
- contract and roster signals that indicate an expected early role.

Translate college production to an NFL distribution using historical
comparables by position, draft capital, age, and usage—not a single player
comparison. Begin with a broad uncertainty interval and update it through camp,
preseason participation, depth-chart movement, and credible role information.
Preseason box-score production should carry less weight than snaps with the
first team, route participation, usage type, and coach behavior.

##### Trades and free agency

Maintain an effective-dated transaction ledger. When a player changes teams,
do not carry last season's volume forward unchanged. Rebuild his projection
from the destination context:

- vacated targets, carries, routes, red-zone work, and quarterback attempts;
- competition from the existing depth chart and other incoming players;
- quarterback accuracy/style and offensive-line quality;
- expected personnel grouping, pace, pass rate, and committee tendencies;
- contract length, guarantees, draft investment, and timing as role evidence;
- downgrade to the players losing opportunity as well as an upgrade to the
  incoming player.

Team opportunity must reconcile. Projected targets, carries, touchdowns, and
snaps cannot grow independently for every player. Run team-level allocation
checks so the sum of player forecasts stays within a plausible offensive total.
Surface unresolved competitions as scenarios rather than choosing a winner with
false precision.

##### Coach and scheme movements

Track head coach, offensive coordinator, play caller, quarterback coach, and
material position-coach changes by season and effective date. Attribute scheme
features to the actual play caller where known:

- neutral pass rate and pass rate over expectation;
- seconds per snap and total play volume;
- early-down aggressiveness and fourth-down behavior;
- 11/12/21 personnel rates, motion, play action, and shotgun usage;
- running-back committee concentration and target usage;
- slot, tight-end, deep-target, and red-zone distribution;
- quarterback rushing design and historical rookie playing-time tendencies.

Coach history should be opponent- and personnel-adjusted and shrunk toward the
league mean. A coordinator with one season and an elite quarterback should not
be assigned that offense's entire performance. When the play caller changes but
the head coach retains control, weight continuity accordingly.

##### Scenarios, uncertainty, and analyst vision

Produce a distribution, not only a point estimate. At minimum store P10, median,
P90, probability of beating FantasyPros projection, and probability of beating
the next positional tier. Explicit scenarios can include:

```text
wins starting role | committee persists | injury recurrence | rookie earns role
offense improves under new coach | offense remains near prior baseline
```

The application's human/analyst view can change scenario probabilities or flag
information the structured model cannot yet represent. Every override must have
an author, timestamp, rationale, affected component, previous value, and expiry.
This preserves the product's vision without turning the model into an
unreviewable collection of manual bumps.

##### Page presentation

The rankings and draft-room tables should place the independent opinion beside
the market:

```text
Our Rank | FP ECR | FP ADP | Market ADP | Our-vs-FP Delta | Our-vs-ADP Delta
Our Median | P10 | P90 | Expected Games | Tier | Confidence | Updated
```

Selecting a player opens an evidence panel showing the largest positive and
negative drivers. Example:

```text
Our Rank 31 vs FP ECR 46 (+15)
+ 24% target share after Week 10 role change
+ 118 vacated team targets
+ new coordinator has above-average slot usage
- availability model expects only 13.2 active games
- rookie quarterback increases passing-volume uncertainty
```

Add focused views for `Our Buys`, `Our Fades`, `Rookies`, `Team Changers`,
`New Coaches`, `High Uncertainty`, and `ADP Movers`. A delta without its drivers
is not sufficient; every material disagreement needs an explanation.

##### Special indicators and badges

Use compact indicators to make the board scannable, but do not turn every stat
into a badge. Indicators fall into four clearly different classes:

```text
FACT        = what happened or is officially designated
ROLE        = projected depth-chart/usage assignment
RISK        = availability or uncertainty warning
MODEL       = this application's derived opinion
```

Use distinct shapes or prefixes in addition to color. Every badge opens a
tooltip or evidence drawer containing its definition, season, source, value,
league rank/percentile, confidence, and last update. Show at most three badges
in a normal player row; prioritize contextually important badges and place the
complete set in the player drawer.

Do not use an ambiguous `WR1` or `RB1` label. Keep these concepts separate:

```text
TEAM WR1       = projected first wide receiver in that NFL team's target order
TEAM WR2       = projected second wide receiver in that NFL team's target order
ECR WR12       = FantasyPros positional rank
OUR WR8        = application positional rank
TEAM RB1/RB2   = projected backfield role, not fantasy positional rank
```

Initial high-priority indicators:

| Group | Indicators | Definition/use |
|---|---|---|
| Experience | `ROOKIE`, `YEAR 2`, `VETERAN` | Rookie badge includes draft round/pick and model confidence; Year 2 highlights common role-change candidates |
| Health | `QUESTIONABLE`, `PUP`, `IR`, `SUSPENDED`, `RECOVERY`, `DURABILITY RISK` | Separate official designations from model-estimated availability; include expected games and update date |
| Receiver role | `TEAM WR1`, `TEAM WR2`, `SLOT`, `X RECEIVER`, `DEEP THREAT`, `RED-ZONE TARGET` | Describe projected deployment rather than fantasy rank |
| Running-back role | `TEAM RB1`, `TEAM RB2`, `DIRECT HANDCUFF`, `HIGH-VALUE HANDCUFF`, `COMMITTEE`, `THIRD-DOWN BACK`, `GOAL-LINE BACK` | Explain how the back earns touches and what event changes his value |
| Other roles | `TE1`, `PASS-CATCHING TE`, `RUSHING QB`, `QB COMPETITION`, `ROOKIE STARTER` | Surface structurally important positional usage |
| Movement | `NEW TEAM`, `NEW QB`, `NEW PLAY CALLER`, `ROLE UP`, `ROLE DOWN`, `DEPTH-CHART RISER` | Effective-dated context changes with a short reason |
| Opportunity | `VACATED TARGETS`, `VACATED CARRIES`, `TARGET COMPETITION`, `BACKFIELD COMPETITION` | Indicate changing team opportunity, reconciled against teammates |
| Market/model | `OUR BUY`, `OUR FADE`, `ADP RISER`, `ADP FALLER`, `BEST-BALL BOOST`, `TIER CLIFF`, `HIGH UNCERTAINTY` | Derived from time-stamped model and market snapshots |
| Draft context | `FILLS NEED`, `STACK`, `YOUR RB'S HANDCUFF`, `BYE CONFLICT`, `MAY NOT RETURN` | Recomputed after every pick for the controlled roster |

A handcuff is not merely every team's RB2. Classify backfields explicitly:

```text
DIRECT HANDCUFF     = expected to inherit most of RB1's role if RB1 is absent
HIGH-VALUE HANDCUFF = direct handcuff with a strong offense/role and major
                      contingent-value increase
STANDALONE RB2      = already has material weekly touches plus contingent value
COMMITTEE BACK      = absence of one teammate would still leave a split workload
```

Store `handcuff_to_player_id`, expected base opportunity, expected opportunity
if the starter is inactive, and confidence. The `YOUR RB'S HANDCUFF` badge is
personalized to the user's roster. Recommendations should not automatically
push a handcuff: compare the diversification cost, round, roster size, and
contingent upside.

Historical leader indicators should emphasize opportunity more than noisy
outcomes:

| Recommended indicator | Display rule |
|---|---|
| `TEAM TARGET LEADER 2025` | Most targets on his prior team; also show targets/game and target share |
| `NFL TOP-10 TARGETS 2025` | Reserve overall badge for top 10 or at least the 90th percentile |
| `TEAM TARGET-SHARE LEADER` | Prefer share over raw targets when team passing volume was unusual |
| `ROUTE PARTICIPATION 90%+` | Qualified games only; useful for identifying full-time roles |
| `RED-ZONE TARGET LEADER` | Show team and NFL percentile rather than touchdowns alone |
| `AIR-YARD LEADER` | Pair with catch rate and target depth to identify unrealized usage |
| `TEAM RUSH LEADER` | Show carries/game and rush share, not only total carries |
| `GOAL-LINE CARRY LEADER` | More predictive role evidence than rushing touchdowns alone |
| `NFL TOP-10 RUSH TDS 2025` | Historical fact only; pair with goal-line work and a TD-regression flag |
| `MISSED-TACKLES LEADER` or `EXPLOSIVE RUNNER` | Require a minimum touch threshold and show percentile |

Top targets overall and rushing-touchdown leaders are useful, but must not be
treated equally. Targets and routes describe repeatable opportunity. Touchdowns
are valuable results but regress strongly, so a `12 RUSH TD` badge should be
accompanied by goal-line attempts, expected touchdowns, and `TD REGRESSION RISK`
when the scoring rate was unsustainably high. Use per-game or share-based
leaders for players who missed time, and always print the season on historical
badges.

Additional model indicators worth including:

- `BREAKOUT CANDIDATE`: young player with increasing participation, strong
  efficiency, and a plausible opportunity gain;
- `BOUNCE-BACK`: prior underlying usage remained strong while results were
  depressed by injury or unstable efficiency;
- `EFFICIENCY REGRESSION +` / `EFFICIENCY REGRESSION -`: expected movement toward
  a stable position/role baseline;
- `TOUCHDOWN REGRESSION +` / `TOUCHDOWN REGRESSION -`: actual touchdowns versus
  expected touchdowns and scoring opportunities;
- `SMALL SAMPLE`: role or efficiency conclusion rests on too few routes/touches;
- `ROLE NOT SECURE`: camp battle, returning starter, unclear committee, or weak
  contract/draft-capital support;
- `OFFENSE UPGRADE` / `OFFENSE DOWNGRADE`: quarterback, line, play caller, pace,
  and implied scoring environment moved materially;
- `PLAYOFF SCHEDULE +` / `PLAYOFF SCHEDULE -`: optional and low-weight because
  preseason schedule-strength estimates are uncertain;
- `DATA STALE` or `LOW CONFIDENCE`: prevents a polished badge from overstating
  weak or old evidence.

Materialized indicators should be reproducible per feature/model snapshot:

```sql
CREATE TABLE ff_player_indicators (
    id BIGSERIAL PRIMARY KEY,
    ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id),
    player_id BIGINT NOT NULL REFERENCES ff_players(id),
    indicator_code TEXT NOT NULL,
    indicator_class TEXT NOT NULL,
    label TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    league_rank INTEGER,
    percentile DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    season INTEGER,
    related_player_id BIGINT REFERENCES ff_players(id),
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ranking_set_id, player_id, indicator_code),
    CHECK (indicator_class IN ('fact','role','risk','model'))
);
```

##### Proving that the model outperforms

Outperformance must be measured, not asserted. Create time-stamped preseason
snapshots and use walk-forward backtests that only include information available
as of each historical draft date. Compare against:

1. FantasyPros ECR;
2. FantasyPros projections;
3. market ADP;
4. a simple prior-year points-per-game baseline;
5. the application's independent model and final recommendation rank.

Evaluate projection MAE/RMSE, rank correlation, top-N hit rate, realized VOR,
tier accuracy, interval calibration, and draft-decision regret. Report results
overall and separately for QB/RB/WR/TE, rookies, players changing teams, players
affected by coaching changes, and players with substantial missed time.

Do not tune and grade on the same season. Use rolling train/validation seasons,
freeze model versions before evaluation, retain losing experiments, and require
a meaningful sample before promoting a feature. The objective is consistent,
explainable improvement over FantasyPros in the decisions users actually face,
not winning a cherry-picked ranking comparison.

#### Refresh and quota policy

All scheduled calls run through one ingestion job and write immutable source
snapshots. Browser page loads and individual picks make zero FantasyPros calls.

Draft-season default cadence (July through the start of the regular season):

| Dataset | Cadence |
|---|---|
| Player directory | Daily, plus an incremental `update=YYYY-MM-DD` refresh |
| Draft ECR for STD/HALF/PPR | Every 6 hours |
| ADP for STD/HALF/PPR | Every 6 hours |
| Preseason projections | Every 6 hours |
| Injuries and news | Every 4 hours; every 2 hours during the final 72 hours before a configured draft |
| Expert directory | Weekly |

Outside draft season, refresh rankings/projections daily and injuries/news only
when the product needs them. Add randomized jitter, exponential backoff for
429/5xx responses, and honor `Retry-After`. Enforce both one request at a time
and a configurable daily budget (`FANTASYPROS_DAILY_CALL_BUDGET`, conservative
default 80) because exact limits depend on the issued plan. Stop non-critical
calls before exhausting the budget; never use rotating proxies to evade API
limits.

Store and display:

```text
last_attempt_at, last_success_at, source_last_updated_at,
http_status, rows_received, rows_matched, rows_unmatched,
request_count_today, stale_reason, response_hash
```

If a refresh fails, retain the last successful snapshot, mark it stale, and let
manual drafting continue. Alert when ECR or projections are older than 12 hours
during draft season, and show a blocking warning (with an override) when no
successful draft snapshot exists.

#### Licensing and attribution

The page footer and exports must say `Rankings and projections powered by
FantasyPros` with a link to FantasyPros. Current public terms require
attribution for published analysis based on the data, restrict ordinary access
to personal/non-commercial use, prohibit use of player images, and require a
commercial agreement for paid, organizational, revenue-generating,
redistributed, or high-volume use. Confirm the account's license before making
this Fantasy Football section public or commercial.

## 12. Database Schema

Add matching definitions to Python schema management, Drizzle, and the web
self-provisioning path.

### `ff_players`

```sql
CREATE TABLE ff_players (
    id BIGSERIAL PRIMARY KEY,
    season INTEGER NOT NULL,
    canonical_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    position TEXT NOT NULL,
    nfl_team_id INTEGER REFERENCES nfl_teams(team_id),
    fantasypros_player_id INTEGER,
    sleeper_player_id TEXT,
    gsis_id TEXT,
    espn_id TEXT,
    yahoo_id TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    bye_week INTEGER,
    injury_status TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (position IN ('QB','RB','WR','TE','K','DST'))
);
```

Use partial unique indexes for nullable external IDs.

### `ff_source_snapshots`

Every FantasyPros response is represented by immutable fetch metadata. Store
the normalized rows in ranking/player tables and keep raw payloads in bounded
object storage or JSONB according to retention policy.

```sql
CREATE TABLE ff_source_snapshots (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    dataset TEXT NOT NULL,
    season INTEGER NOT NULL,
    scoring TEXT,
    ranking_type TEXT,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_updated_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    response_hash TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    matched_count INTEGER NOT NULL DEFAULT 0,
    unmatched_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error_summary TEXT,
    UNIQUE (source, dataset, response_hash),
    CHECK (status IN ('success','partial','failed'))
);
```

### `ff_ranking_sets` and `ff_player_rankings`

```sql
CREATE TABLE ff_ranking_sets (
    id BIGSERIAL PRIMARY KEY,
    season INTEGER NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    source_snapshot_id BIGINT REFERENCES ff_source_snapshots(id),
    source_date DATE,
    scoring_profile JSONB NOT NULL,
    is_baseline BOOLEAN NOT NULL DEFAULT FALSE,
    import_summary JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ff_player_rankings (
    id BIGSERIAL PRIMARY KEY,
    ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id),
    player_id BIGINT NOT NULL REFERENCES ff_players(id),
    overall_rank INTEGER,
    position_rank INTEGER,
    tier INTEGER,
    adp DOUBLE PRECISION,
    projected_points DOUBLE PRECISION,
    projection_low DOUBLE PRECISION,
    projection_high DOUBLE PRECISION,
    projected_stats JSONB,
    source_row JSONB,
    notes TEXT,
    UNIQUE (ranking_set_id, player_id)
);
```

### `ff_draft_sessions`

```sql
CREATE TABLE ff_draft_sessions (
    id UUID PRIMARY KEY,
    owner_key TEXT,
    name TEXT NOT NULL,
    season INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'setup',
    draft_type TEXT NOT NULL DEFAULT 'snake',
    team_count INTEGER NOT NULL,
    controlled_slot INTEGER NOT NULL,
    round_count INTEGER NOT NULL,
    roster_config JSONB NOT NULL,
    scoring_config JSONB NOT NULL,
    recommendation_config JSONB NOT NULL,
    ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id),
    sleeper_draft_id TEXT,
    current_pick INTEGER NOT NULL DEFAULT 1,
    revision INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status IN ('setup','ready','active','paused','completed','abandoned')),
    CHECK (draft_type = 'snake'),
    CHECK (team_count BETWEEN 2 AND 20),
    CHECK (controlled_slot BETWEEN 1 AND team_count)
);
```

### Teams and materialized slots

```sql
CREATE TABLE ff_draft_teams (
    id BIGSERIAL PRIMARY KEY,
    draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id),
    slot INTEGER NOT NULL,
    name TEXT NOT NULL,
    is_controlled BOOLEAN NOT NULL DEFAULT FALSE,
    external_roster_id TEXT,
    UNIQUE (draft_id, slot)
);

CREATE TABLE ff_draft_slots (
    id BIGSERIAL PRIMARY KEY,
    draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id),
    overall_pick INTEGER NOT NULL,
    round INTEGER NOT NULL,
    pick_in_round INTEGER NOT NULL,
    draft_team_id BIGINT NOT NULL REFERENCES ff_draft_teams(id),
    UNIQUE (draft_id, overall_pick)
);
```

### Append-only draft events

```sql
CREATE TABLE ff_draft_events (
    id BIGSERIAL PRIMARY KEY,
    draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id),
    event_type TEXT NOT NULL,
    overall_pick INTEGER,
    player_id BIGINT REFERENCES ff_players(id),
    draft_team_id BIGINT REFERENCES ff_draft_teams(id),
    source TEXT NOT NULL,
    external_pick_id TEXT,
    reverses_event_id BIGINT REFERENCES ff_draft_events(id),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (event_type IN ('pick_made','pick_reversed','draft_started',
                          'draft_paused','draft_resumed','draft_completed',
                          'settings_changed')),
    CHECK (source IN ('manual','sleeper','system'))
);
```

The event ledger is canonical. Corrections append reversal/replacement events;
they never delete history. Add uniqueness for external picks and prevent two
active selections of the same player in a draft.

### Player preferences

```sql
CREATE TABLE ff_draft_player_preferences (
    draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id),
    player_id BIGINT NOT NULL REFERENCES ff_players(id),
    preference TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (draft_id, player_id),
    CHECK (preference IN ('target','fade','exclude'))
);
```

## 13. Application Architecture

Suggested files:

```text
web/src/app/fantasy-football/page.tsx
web/src/app/fantasy-football/draft/new/page.tsx
web/src/app/fantasy-football/draft/[draftId]/page.tsx
web/src/app/fantasy-football/draft/[draftId]/draft-room-client.tsx
web/src/app/fantasy-football/rankings/page.tsx
web/src/app/fantasy-football/actions.ts
web/src/lib/fantasy-football/draft-order.ts
web/src/lib/fantasy-football/roster.ts
web/src/lib/fantasy-football/scoring.ts
web/src/lib/fantasy-football/recommendations.ts
web/src/lib/fantasy-football/csv-import.ts
web/src/db/queries-fantasy-football.ts
ingest/ff_players.py
ingest/ff_rankings.py
tests/test_ff_draft_engine.py
ingest/ff_fantasypros.py
tests/test_ff_fantasypros.py
.github/workflows/fantasy_football_refresh.yml
```

Server actions:

```text
createDraft(settings)
startDraft(draftId, expectedRevision)
recordPick(draftId, overallPick, playerId, expectedRevision)
undoPick(draftId, eventId, expectedRevision)
pauseDraft(draftId, expectedRevision)
resumeDraft(draftId, expectedRevision)
completeDraft(draftId, expectedRevision)
importRankingCsv(formData)
connectSleeperDraft(draftId, sleeperDraftId)
syncSleeperPicks(draftId)
setPlayerPreference(draftId, playerId, preference)
```

Every mutation uses a transaction and optimistic concurrency through
`expectedRevision`. A stale client receives the latest state and a retry prompt
instead of overwriting a pick.

## 14. Sleeper Synchronization

1. User provides a Sleeper draft or league ID.
2. Fetch metadata and picks.
3. Show detected team count, format, roster, and order.
4. Require confirmation before applying settings.
5. Map Sleeper player IDs directly to `ff_players`.
6. Append unseen picks using stable external identity.

Rules:

- read-only and idempotent;
- never delete local history because a response is incomplete;
- flag manual/external conflicts for user resolution;
- pause polling after three consecutive failures;
- keep manual entry active during failure;
- show last successful sync time and current error.

## 15. UI and Accessibility

Desktop layout:

```text
┌──────────────────────────────────────────────────────────────┐
│ Draft status | On clock | Picks until yours | Sync health   │
├───────────────────────────────────────────┬──────────────────┤
│ Draft board                               │ Your roster      │
├───────────────────────────────────────────┼──────────────────┤
│ Available players                         │ Recommendations  │
└───────────────────────────────────────────┴──────────────────┘
```

- tablet: board above tabbed Available/Recommended/Roster panels;
- mobile: current pick and recommendations first, board in another tab;
- sticky search and position filters;
- no hover-only information;
- minimum 44px interactive targets;
- semantic grid labels and keyboard-complete workflow;
- ARIA live announcements for picks and clock changes;
- color is not the only status indicator;
- reduced-motion support.

## 16. Failure Handling

- No rankings: block start, not draft creation.
- Sleeper unavailable: warn and continue manually.
- Duplicate player: reject and refresh state.
- Stale revision: return current revision and conflicting pick.
- Unknown player: create a manual-resolution item, not an anonymous player.
- CSV failure: preserve the previous ranking set.
- Recommendation failure: keep pick entry operational.
- Database failure: do not advance the pick unless the event commits.

## 17. Security and Privacy

- Sleeper IDs are external identifiers, not credentials.
- Never request platform passwords or session cookies.
- Validate draft ownership before mutations once auth exists.
- Limit CSV size and row count and parse server-side.
- Escape imported notes/source labels.
- Rate-limit sync actions.
- Do not send strategy or preferences to analytics by default.

## 18. Testing

Unit tests:

- snake order across supported team counts;
- controlled-team next-pick calculation;
- FLEX/SUPERFLEX eligibility;
- replacement level and custom scoring;
- recommendation components;
- CSV normalization and ambiguity;
- Sleeper idempotency;
- undo/reversal projection;
- duplicate prevention.

Integration/UI tests:

- create, start, draft, undo, resume, and complete;
- optimistic concurrency conflict;
- browser refresh restoration;
- Sleeper failure fallback;
- ranking import to active session;
- round-turn pick ordering;
- keyboard drafting and mobile flow;
- completed exports match active event state.

## 19. Observability

Track sessions created/started/completed, pick latency, manual versus synced
picks, sync health, CSV match rate, recommendation errors, correction rate, and
ranking/config versions used by completed drafts.

## 20. Implementation Plan

### Phase 1A — Foundations

- schemas and migrations;
- player ingestion and identity crosswalk;
- FantasyPros players, ECR, ADP, projections, injury/news ingestion, immutable
  source snapshots, data-health reporting, and scheduled refresh;
- CSV ranking import;
- settings and snake-order engine;
- unit tests.

### Phase 1B — Manual draft room

- setup wizard and board;
- manual picks and append-only events;
- undo/correction;
- persistence, resume, and roster eligibility.

### Phase 1C — Intelligence

- prior-season play-by-play, participation, games-played, and team-opportunity
  feature pipeline;
- rookie translation, transaction/free-agency, and coaching-context features;
- independent opportunity, efficiency, availability, and uncertainty models;
- leakage-safe model snapshots, baseline comparisons, and walk-forward
  backtesting;
- factual, role, risk, model, and roster-context indicator system with evidence
  drawers and contextual badge prioritization;
- custom-scoring projections;
- replacement levels and VOR;
- tiers, scarcity, ADP value, roster need, and urgency;
- explainable recommendations;
- target/fade/exclude.

### Phase 1D — Sleeper

- metadata import;
- read-only pick polling;
- identity matching;
- conflict handling and health.

### Phase 1E — Completion

- exports and printable board;
- responsive/accessibility pass;
- telemetry;
- live database smoke draft.

## 21. Acceptance Criteria

Phase 1 is complete when:

1. A user can finish a configurable 8–14 team snake draft.
2. Standard, half-PPR, PPR, and custom scoring produce reproducible values.
3. Every snake turn is correct and persists after reload.
4. Picks and corrections preserve an audit trail.
5. Eligibility rules prevent duplicate or impossible selections.
6. Recommendations update after every pick and explain their components.
7. CSV import never silently mismatches players.
8. Sleeper import follows picks without credentials and supports manual fallback.
9. Completed board and controlled roster export successfully.
10. Unit, integration, UI, and live database smoke tests pass.
11. FantasyPros ingestion produces separate, attributable ECR, ADP, projection,
    and injury snapshots without exposing the API key.
12. A failed or stale FantasyPros refresh never interrupts pick entry, and an
    active draft remains pinned to its chosen ranking snapshot.
13. Every material Our Rank versus FantasyPros delta exposes its largest
    evidence-based drivers and uncertainty.
14. A leakage-safe historical report compares the model with FantasyPros ECR,
    FantasyPros projections, market ADP, and a prior-year baseline by player
    segment.
15. Role badges distinguish NFL team role from fantasy positional rank, and
    every handcuff/leader/model badge exposes its definition, season, evidence,
    and confidence.

## 22. Follow-On Roadmap

1. Auction drafts.
2. Keeper, dynasty, rookie-only, and traded-pick support.
3. Weekly projections and start/sit decisions.
4. Waivers and FAAB allocation.
5. Trade analysis.
6. In-season injury/news alerts and roster-impact analysis.
7. Best-ball portfolio exposure and stacking.
8. Historical draft-performance calibration.
9. ESPN/Yahoo imports where supported and maintainable.
