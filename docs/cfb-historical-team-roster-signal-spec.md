# College Football Historical, Team, and Roster Signals — Specification

**Status:** Implemented; historical hydration remains gated by the generated coverage audit
**Date:** 2026-09-03
**Product:** CFB Line Terminal
**Primary objective:** Add reproducible historical context and point-in-time team signals to the CFB dashboard without presenting descriptive trends as predictive betting edges.

## 1. Executive Summary

The terminal should answer questions such as:

- How often do home teams win outright or cover when favored by 14.5 points?
- How does the selected team perform in comparable spread ranges?
- Is that team result still notable after accounting for its conference, opponent strength, coaching regime, and small sample size?
- As the current season develops, do efficiency, roster continuity, injuries, and depth-chart changes materially change the comparison?
- Has a proposed signal demonstrated out-of-sample value and prospective closing-line value (CLV), or is it only descriptive context?

The design has three evidence layers:

1. **Historical cohort baseline** — national and conference-level outcomes for comparable games.
2. **Team/regime context** — team, coach, and coordinator results, adjusted toward a broader prior when samples are small.
3. **Point-in-time current-season context** — season-to-date performance and roster information that was actually knowable before the game.

Early in a season, the system should rely mostly on multi-year, coaching-regime, roster-continuity, and market priors. Current-season features gain weight only as games accumulate. All displayed evidence must include sample size, uncertainty, provenance, and validation status.

## 2. Scope

### 2.1 In scope

- Historical FBS game results and pregame betting lines.
- Straight-up (SU), against-the-spread (ATS), and totals outcomes.
- Exact-line and spread-bucket cohort analysis.
- Home/away, neutral-site, favorite/underdog, conference, season phase, and postseason filters.
- Team-specific, coaching-regime, and coordinator-regime context.
- Point-in-time season-to-date team performance features.
- Point-in-time roster continuity, quarterback, offensive-line, transfer, talent, injury, and depth-chart features where sourced reliably.
- Hypothesis registration, historical backtests, holdouts, prospective shadow tracking, and promotion gates.
- Dashboard panels that distinguish descriptive context from validated signals.

### 2.2 Out of scope for the first release

- Automated wager placement or prescriptive bet sizing.
- Claims that historical ATS percentages alone constitute an edge.
- Reconstructing historical intraday line movement from a source that only supplies one reference line.
- Retrospective use of end-of-season ratings, final rosters, or later injury knowledge in earlier games.
- Player-level performance projection unless a separate model and data-quality review approves it.
- Live/in-game wagering signals.

## 3. Product Principles

### 3.1 Descriptive is not predictive

“Home favorites of 14–16.5 covered 52%” is historical context. It becomes a candidate predictive signal only after a preregistered definition, walk-forward evaluation, holdout test, and prospective tracking.

The UI must not label a descriptive split as an `EDGE`, `PLAY`, or recommendation. Approved labels are:

- `HISTORICAL CONTEXT`
- `TEAM CONTEXT`
- `CURRENT-SEASON CONTEXT`
- `CANDIDATE — BACKTEST ONLY`
- `SHADOW — PROSPECTIVE TEST`
- `VALIDATED SIGNAL`
- `RETIRED`

### 3.2 Point-in-time correctness

Every feature used for a game must have an `available_at` or `as_of_at` timestamp no later than the signal snapshot time. A current roster, final-season rating, or corrected injury report cannot be silently applied to an earlier game.

### 3.3 Preserve source truth

- Retain provider-specific lines instead of overwriting them with a consensus.
- Retain raw source identifiers and payload hashes.
- Distinguish historical reference lines, verified openers, observed snapshots, and frozen closes.
- Do not infer a close from the last database row unless the existing close-freeze rules qualify it.

### 3.4 Small samples require shrinkage

Team-specific ATS records will often be too small to interpret directly. The product must show both the raw result and a partially pooled estimate, with the broader cohort as the prior.

### 3.5 First-week uncertainty is a feature

In Weeks 0–2, current-season samples provide little information. The UI should explicitly show that most weight remains on prior seasons, coaching regime, roster continuity, and market information. The current-season weight should rise gradually and visibly.

## 4. Questions and Canonical Definitions

### 4.1 Example: home team favored by 14.5

Canonical population:

- FBS-versus-FBS games.
- Non-neutral site.
- Home team is the favorite.
- Pregame canonical spread is exactly `home_spread = -14.5`.
- Regulation plus overtime final score is available.

Outcomes:

```text
home_margin = home_score - away_score
home_win    = home_margin > 0
home_cover  = home_margin + home_spread > 0
push        = home_margin + home_spread = 0
```

At a half-point spread, a push is impossible. The terminal reports:

- Games (`n`).
- SU wins and losses; SU win percentage.
- ATS wins, losses, and pushes; ATS cover percentage excluding pushes.
- Wilson 95% confidence interval for binary win/cover rates.
- Seasons and date range covered.
- Line source and line-time classification.
- ROI only when the actual historical price is present. An assumed `-110` may be shown as a clearly labeled scenario, never as primary realized ROI.

### 4.2 Exact line versus useful cohort

An exact `-14.5` query is supported, but decision context should also show a preregistered bucket such as `-14.0 through -16.5`. Exact-line results can be sparse and unstable.

Default spread buckets:

```text
Favorite 0.5–2.5
Favorite 3.0–6.5
Favorite 7.0–10.0
Favorite 10.5–13.5
Favorite 14.0–16.5
Favorite 17.0–20.5
Favorite 21.0–27.5
Favorite 28.0+
```

Key football numbers must not be split casually. Bucket definitions are versioned and frozen before results are evaluated.

### 4.3 Canonical line selection

Historical feeds may contain multiple books but no trustworthy timestamped close. The system must preserve every provider quote and compute a canonical reference using a documented rule:

1. Use a verified provider close when the source explicitly identifies it.
2. Otherwise use a configured provider priority when the same provider has consistent coverage.
3. Otherwise use the median provider spread for the selected snapshot class.
4. For an even number of books, use the arithmetic median only for cohort classification; never imply that it was an offered wager.

The UI must label cases 2–4 `HISTORICAL REFERENCE`, not `CLOSE`.

Historical reference lines are suitable for spread-versus-result studies. They are not sufficient to backtest intraday movement, steam, stale-book, or open-to-close hypotheses.

## 5. Evidence Hierarchy

For the selected game, the dashboard backs off through the following hierarchy:

```text
Team × exact spread × venue × current staff regime
  ↓ insufficient sample
Team × spread bucket × venue × current staff regime
  ↓ insufficient sample
Team × spread bucket × venue, all recent regimes
  ↓ insufficient sample
Conference/archetype × spread bucket × venue
  ↓ insufficient sample
National FBS cohort × spread bucket × venue
```

Every result states the level actually used. The system must never present a broad fallback as though it were an exact team match.

### 5.1 Default recency windows

- National baseline: most recent 10 completed seasons, with a coverage report.
- Team context: most recent 5 completed seasons plus current season.
- Coaching regime: games under the current head coach, split again when coordinator or scheme changes are material.
- Current season: games completed before the selected game only.

These defaults are configurable. Postseason, bowls, conference championships, neutral sites, and FBS-versus-FCS games are separate cohorts by default.

## 6. Team-Specific Modeling

### 6.1 Raw and shrunk estimates

For a team-specific rate, display the raw rate but rank and model with a partially pooled estimate. A simple initial implementation is:

```text
shrunk_rate = (team_n × team_rate + prior_strength × cohort_rate)
              / (team_n + prior_strength)
```

Recommended default `prior_strength = 20` comparable games. A beta-binomial hierarchical model may replace this formula later, but it must preserve the same UI concepts: raw rate, adjusted rate, prior, sample size, and interval.

Example:

```text
Kennesaw State as a home favorite of 14–16.5
Raw ATS:       2–0 (100%), n=2
Shrunk ATS:    53.4%
Prior cohort:  51.0%, prior n=20
Reliability:   Very low
```

### 6.2 Reliability bands

Default descriptive reliability labels:

- `VERY LOW`: fewer than 10 team observations.
- `LOW`: 10–24.
- `MODERATE`: 25–49.
- `HIGH`: 50 or more.

These labels describe sampling reliability, not predictive validity. A high-sample trend can still have no out-of-sample value.

### 6.3 Regime awareness

Team identity is not assumed to be stable across years. Store and expose:

- Head coach start/end dates or season/week boundaries.
- Offensive and defensive coordinator regimes where available.
- Major scheme changes.
- Conference membership changes.
- FBS transition or reclassification status.

The default team card prioritizes the current head-coach regime, then shows the broader recent-team sample as secondary context.

### 6.4 Opponent and schedule adjustment

Raw team records can reflect schedule composition. Candidate team features should therefore include:

- Opponent-adjusted offensive and defensive efficiency.
- Opponent rating at the time of the game.
- Rest days and short-week flags.
- Travel distance and time-zone shift when reliable.
- Conference/nonconference and rivalry flags.
- Home-field and neutral-site status.
- Ranked-versus-unranked only as descriptive metadata unless independently validated.

## 7. Current-Season Signal Accumulation

### 7.1 Point-in-time team features

Create one immutable feature row per team/game using only games and information available before kickoff:

- Games played and effective sample size.
- Points, yards, and plays per drive.
- Success rate and explosiveness.
- EPA/PPA per play where the licensed source permits storage.
- Early-down success and passing-down efficiency.
- Rush/pass efficiency and allowed efficiency.
- Havoc, sack, pressure, and turnover rates.
- Pace and seconds per play.
- Red-zone scoring and touchdown rates.
- Special-teams efficiency.
- Penalty rate.
- Opponent-adjusted offense, defense, and overall rating.
- Turnover luck/regression indicators.
- Market performance: opener-to-current movement and prior-game closing error, only from verified timestamped quotes.

Raw box-score accumulation should not be the primary signal when opponent-adjusted measures are available.

### 7.2 Week-dependent blending

Use an explicit prior/current-season blend. Initial default:

```text
current_weight = effective_games / (effective_games + 4)
blended_feature = current_weight × current_season_feature
                  + (1 - current_weight) × preseason_prior
```

Illustrative weights:

| Completed games | Current-season weight | Prior weight |
|---:|---:|---:|
| 0 | 0% | 100% |
| 1 | 20% | 80% |
| 2 | 33% | 67% |
| 4 | 50% | 50% |
| 8 | 67% | 33% |

`effective_games` may be lower than games played when opponents are FCS, garbage-time contamination is high, or data completeness is poor. Weighting rules are versioned and backtested; they are not tuned after viewing a single season’s result.

### 7.3 Preseason prior

The preseason prior may combine:

- Previous-season opponent-adjusted rating, regressed toward conference/FBS mean.
- Returning production.
- Quarterback experience and continuity.
- Offensive-line returning starts/snaps.
- Returning defensive production by unit.
- Transfer portal additions and losses.
- Recruiting/talent composite.
- Head coach and coordinator continuity.
- Multi-year program rating.
- Market-implied preseason strength where licensing permits.

Each component must carry source, season, snapshot time, and missingness. Missing roster data should widen uncertainty, not silently become zero.

## 8. Roster and Availability Signals

### 8.1 Roster snapshots

Roster data must be stored as snapshots rather than a mutable current roster. Each snapshot includes:

- Team and season.
- Player source ID and normalized name.
- Position and position group.
- Class/year and experience.
- Previous team for transfers.
- Recruiting/talent grade when licensed.
- Depth-chart role and status.
- Injury/availability designation with source confidence.
- `source_updated_at`, `available_at`, and `captured_at`.

A historical game may only join to the latest snapshot available before that game’s signal time.

### 8.2 Derived roster features

Initial team-level roster features:

- Returning production percentage, offense and defense.
- Returning starts/snaps by position group.
- Starting quarterback returning flag.
- Quarterback career starts and current-system starts.
- Offensive-line returning starts and projected starter continuity.
- Skill-position returning usage/production.
- Defensive front-seven and secondary continuity.
- Incoming/outgoing transfer counts and weighted production.
- Two-deep availability percentage.
- Starter absence count and weighted impact.
- Late scratch/change flags.
- Roster talent composite and opponent talent differential.

Injury and depth-chart data have variable reliability. The UI displays provenance and confidence, and the model must be able to run with these values missing.

### 8.3 No hindsight roster reconstruction

The following are prohibited:

- Applying a final depth chart to Week 1.
- Marking a player unavailable in an earlier game based on a later report.
- Crediting a transfer with later-season production in a preseason feature.
- Recomputing a stored game signal using a corrected roster without retaining the original snapshot and model version.

## 9. Candidate Hypotheses

Each hypothesis is entered in the registry before its outcome window is evaluated. Examples:

| ID | Candidate hypothesis | Primary outcome | Required controls |
|---|---|---|---|
| CFB-H001 | Non-neutral home favorites of 14–16.5 cover above the market break-even rate | ATS | Season, closing/reference classification, FBS-only |
| CFB-H002 | Teams with high OL and QB continuity outperform the first-half spread in Weeks 0–3 | 1H ATS | Opponent talent, favorite size, coach continuity |
| CFB-H003 | Large favorites with low returning defensive production allow more points than market expectation | Opponent team-total error | Pace, opponent offense, garbage time |
| CFB-H004 | A verified cross-book spread move without a total move predicts positive CLV | CLV | Timestamp alignment, book count, stale quotes |
| CFB-H005 | Teams in Year 1 of a new offensive coordinator are overvalued when returning QB continuity is low | ATS/CLV | Talent, transfers, opponent, week |

The examples are research candidates, not claims.

## 10. Hypothesis Registry and Validation

### 10.1 Required preregistration fields

- Stable hypothesis ID and version.
- Plain-language claim.
- Primary outcome and exact formula.
- Population and exclusion rules.
- Feature definitions and availability timestamps.
- Exact-line or bucket boundaries.
- Minimum sample sizes.
- Training, validation, and untouched holdout seasons.
- Statistical test and confidence interval.
- Multiple-testing family.
- Expected direction and minimum meaningful effect.
- Promotion and retirement rules.
- Registration timestamp and author.

### 10.2 Walk-forward evaluation

Use expanding-window evaluation, never random game splits:

```text
Train 2016–2019 → test 2020
Train 2016–2020 → test 2021
Train 2016–2021 → test 2022
...
Train through 2024 → untouched test 2025
```

Dates may shift based on audited data coverage. COVID-affected seasons should be separately flagged and tested with and without inclusion.

### 10.3 Multiple testing

Related slices belong to a declared hypothesis family. Store raw p-values and false-discovery-rate-adjusted q-values where inference is used. The dashboard must not elevate the best-looking result from dozens of unregistered filters.

### 10.4 Promotion lifecycle

```text
PROPOSED
  → PREREGISTERED
  → BACKTESTED
  → HOLDOUT PASSED
  → PROSPECTIVE SHADOW
  → VALIDATED SIGNAL
  → RETIRED (if degradation or invalidation occurs)
```

Default promotion requirements:

- Definition frozen before holdout evaluation.
- Positive effect in a majority of walk-forward folds.
- Untouched holdout meets the registered effect and uncertainty thresholds.
- No critical leakage or data-quality finding.
- At least 100 prospective qualified observations for broad signals, or a longer explicitly approved horizon for sparse team/regime signals.
- Positive prospective CLV for market-timing hypotheses.
- Performance remains after realistic prices, pushes, and missing quotes.

Only `VALIDATED SIGNAL` may affect alert priority or a composite rating. Even then, the terminal remains a research tool and states the signal’s measured uncertainty.

## 11. Data Model

Names are proposed and should follow the repository’s final migration conventions.

### 11.1 `cfb_historical_game_lines`

One row per historical game, provider, market, and source snapshot/classification.

```text
id                       BIGSERIAL PK
game_id                  FK → cfb_matchups.id
provider                 TEXT NOT NULL
market_type              TEXT NOT NULL  -- spread/total/moneyline/1h/etc.
home_value               DOUBLE PRECISION
away_value               DOUBLE PRECISION
home_price               INTEGER NULL
away_price               INTEGER NULL
line_designation         TEXT NOT NULL  -- verified_open/verified_close/reference
source_event_id          TEXT NULL
source_updated_at        TIMESTAMPTZ NULL
available_at             TIMESTAMPTZ NULL
captured_at              TIMESTAMPTZ NOT NULL
raw_payload_hash         TEXT NULL
is_canonical_reference   BOOLEAN DEFAULT FALSE
```

Do not merge this table with `game_odds_history`. That existing table is the prospective exact-book observation ledger; the historical table has different provenance and often weaker timing semantics.

### 11.2 `cfb_team_game_features`

One row per team/game/model version, frozen before kickoff.

```text
id                       BIGSERIAL PK
game_id                  FK → cfb_matchups.id
team_id                  FK → cfb_teams.id
opponent_team_id         FK → cfb_teams.id
feature_version          TEXT NOT NULL
as_of_at                 TIMESTAMPTZ NOT NULL
available_at             TIMESTAMPTZ NOT NULL
games_played             INTEGER NOT NULL
effective_games          DOUBLE PRECISION NOT NULL
features_json            JSONB NOT NULL
source_completeness      DOUBLE PRECISION
created_at               TIMESTAMPTZ NOT NULL
UNIQUE(game_id, team_id, feature_version)
```

Use typed columns for stable, frequently queried features after the exploratory schema settles. Keep `features_json` for versioned research features.

### 11.3 `cfb_roster_snapshots` and `cfb_roster_players`

```text
cfb_roster_snapshots:
  id, team_id, season, source, source_updated_at, available_at,
  captured_at, payload_hash, confidence, is_complete

cfb_roster_players:
  snapshot_id, source_player_id, normalized_name, position,
  position_group, class_year, previous_team_id, depth_role,
  availability_status, availability_confidence, attributes_json
```

### 11.4 `cfb_staff_regimes`

```text
id, team_id, role, person_name, start_season, start_week,
end_season, end_week, scheme_label, source, available_at
```

Roles initially include `HEAD_COACH`, `OFFENSIVE_COORDINATOR`, and `DEFENSIVE_COORDINATOR`.

### 11.5 `cfb_hypotheses`

```text
id, version, name, claim, status, outcome_definition_json,
population_filter_json, feature_definition_json, bucket_definition_json,
min_sample_json, split_plan_json, test_plan_json, promotion_rules_json,
multiple_test_family, registered_at, frozen_at, retired_at, notes
```

### 11.6 `cfb_hypothesis_results`

```text
id, hypothesis_id, hypothesis_version, evaluation_type,
train_start, train_end, test_start, test_end, n, wins, losses, pushes,
effect, standard_error, ci_low, ci_high, p_value, q_value,
roi, avg_clv, calibration_json, data_version, code_version,
evaluated_at, result_payload_hash
```

### 11.7 `cfb_game_signal_snapshots`

Immutable record of what the terminal knew and displayed at a given time.

```text
id, game_id, team_id, hypothesis_id, hypothesis_version,
signal_status, signal_value, confidence, evidence_level,
inputs_json, model_version, captured_at, qualified_for_tracking
```

## 12. Data Sources and Provenance

### 12.1 Initial source plan

- Historical games and betting fields: [CollegeFootballData games API](https://apinext.collegefootballdata.com/api/games).
- Ratings candidates and metadata: [CollegeFootballData ratings API](https://api.collegefootballdata.com/api/ratings).
- API authentication and access patterns: [CollegeFootballData getting started](https://api.collegefootballdata.com/getting-started).
- Coverage/call-budget planning: [CollegeFootballData API tiers](https://collegefootballdata.com/api-tiers?placement=hero&source=homepage).

Roster, depth-chart, injury, transfer, and returning-production endpoints must pass a separate source-availability and licensing audit before implementation. The schema does not assume that one provider reliably supplies all of them.

### 12.2 Ratings leakage warning

CFBD describes public historical CORE ratings as retrospective and available beginning in 2016; the published historical value is not necessarily what the model would have reported at that point in the season. See [CORE methodology](https://api.collegefootballdata.com/core-ratings). Therefore:

- Retrospective ratings cannot be used as point-in-time predictive inputs unless reconstructed from data available through the relevant week.
- A rating with a trustworthy `throughWeek` snapshot may be used only after its public availability time.
- Retrospective ratings may be used for descriptive opponent-strength normalization if clearly labeled and kept out of predictive validation.

### 12.3 Backfill target

Begin with the 2016–2025 completed seasons because this aligns with the documented public CORE era, then expand earlier only after a coverage audit. The audit must report by season:

- Total games.
- FBS-versus-FBS games.
- Final-score completeness.
- Spread, total, moneyline, and price completeness.
- Provider count and continuity.
- Neutral-site and postseason classification completeness.
- Team-ID mapping failures.
- Duplicate or rescheduled games.

No backtest begins until the audit is stored and reviewed.

## 13. Computation and Query Contract

### 13.1 Cohort result

Every cohort API response returns:

```json
{
  "definitionVersion": "spread-buckets-v1",
  "asOf": "2026-09-03T13:06:00Z",
  "population": "FBS vs FBS, non-neutral, home favorite 14.0–16.5",
  "lineDesignation": "historical_reference",
  "seasons": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
  "n": 0,
  "su": { "wins": 0, "losses": 0, "rate": null, "ci95": [null, null] },
  "ats": { "wins": 0, "losses": 0, "pushes": 0, "rate": null, "ci95": [null, null] },
  "priceCoveragePct": 0,
  "roi": null,
  "provenance": []
}
```

Numbers above are schema examples, not findings.

### 13.2 Team context result

Return exact and fallback matches together:

- Team/regime exact line.
- Team/regime spread bucket.
- Team across recent regimes.
- Conference/archetype bucket.
- National cohort.

Each row includes `n`, raw rate, shrunk rate, prior rate, interval, reliability, regime, and line designation.

### 13.3 Reproducibility

Every output records:

- Data snapshot/version.
- Query/filter definition version.
- Code commit/version.
- Model version.
- Evaluation timestamp.
- Raw input payload hashes where practical.

## 14. Dashboard Design

### 14.1 New tab: `HISTORY`

Add a `HISTORY` tab beside Spread/Total/Moneyline for the selected game.

Sections:

1. **Historical cohort** — exact line and default bucket, SU/ATS results, sample size, interval, source classification.
2. **Team profile** — raw and shrunk team/regime performance with hierarchy fallback.
3. **Season-to-date** — opponent-adjusted team indicators and week-dependent prior/current weights.
4. **Roster continuity** — QB, OL, returning production, transfers, availability, and data confidence.
5. **Comparable matchups** — a small, auditable list of games meeting the selected definition.
6. **Evidence quality** — missingness, source freshness, sample reliability, retrospective-feature warnings.
7. **Signal status** — registry state and prospective results for related hypotheses.

### 14.2 Example card

```text
HISTORICAL CONTEXT — NOT A VALIDATED EDGE

Home favorites 14.0–16.5
SU:  [rate]  [W-L]       95% CI [low, high]
ATS: [rate]  [W-L-P]     95% CI [low, high]
Sample: n=[n], 2016–2025
Line: Historical reference; [provider coverage]

TEAM CONTEXT — CURRENT COACH REGIME
[Team] raw ATS: [rate], n=[n]
Shrunk ATS: [rate] toward national cohort [rate]
Reliability: [band]

CURRENT SEASON
[games] completed · [weight]% current season / [weight]% prior
Roster snapshot: [timestamp] · confidence [band]
```

### 14.3 Market Watch integration

The left rail may show one compact context marker, for example `HIST n=[sample]`, but historical rates must not replace observed line movement. Only validated signals can change alert ranking, and their influence must be separately identifiable.

## 15. API and Service Boundaries

Proposed server functions:

```text
getCfbHistoricalCohort(gameId, market, definitionVersion)
getCfbTeamHistoricalContext(gameId, teamId, definitionVersion)
getCfbCurrentSeasonProfile(gameId, teamId, featureVersion)
getCfbRosterContinuity(gameId, teamId, snapshotPolicy)
getCfbComparableGames(gameId, definitionVersion, limit)
getCfbRelatedHypotheses(gameId)
```

Research jobs/commands:

```text
python -m ingest.cfb_history --season 2016 --audit-only
python -m ingest.cfb_history --start-season 2016 --end-season 2025
python -m ingest.cfb_rosters --season 2026 --prospective
python -m model.cfb_team_features --through-date YYYY-MM-DD
python -m research.cfb_hypotheses evaluate CFB-H001 --walk-forward
python -m research.cfb_hypotheses snapshot-qualified --date YYYY-MM-DD
```

Names are illustrative; implementation should reuse established repository patterns.

## 16. Performance and Caching

- Precompute common national/conference spread buckets in a materialized view or aggregate table.
- Cache selected-game history responses by game, definition version, data version, and feature version.
- Team and comparable-game queries should use indexed normalized fields, not JSON filters for stable features.
- Historical ingestion must respect API quotas, cache raw responses, and resume idempotently.
- Dashboard history failures must degrade independently; they must not interrupt live odds capture or current market display.

Recommended indexes:

```text
cfb_matchups(season, week, kickoff_at)
cfb_matchups(home_team_id, season)
cfb_matchups(away_team_id, season)
cfb_historical_game_lines(game_id, market_type, provider)
cfb_historical_game_lines(line_designation, is_canonical_reference)
cfb_team_game_features(team_id, as_of_at, feature_version)
cfb_roster_snapshots(team_id, season, available_at)
cfb_game_signal_snapshots(game_id, captured_at)
cfb_hypothesis_results(hypothesis_id, hypothesis_version, evaluation_type)
```

## 17. Data-Quality and Leakage Gates

A season or feature is blocked from modeling when any applicable gate fails:

- Game/result completeness below 98% for the target population.
- Canonical team mapping failures exceed 0.5%.
- Spread availability is unreported or materially inconsistent by season.
- Neutral-site classification is missing for games included in venue hypotheses.
- Feature `available_at` is after the signal snapshot.
- Historical rating is retrospective but treated as contemporaneous.
- Current roster state is joined to a historical game without a valid snapshot.
- Duplicate games, reschedules, or postseason classifications are unresolved.
- Provider mix changes materially without a sensitivity analysis.

Blocked data may remain visible in a coverage report but cannot enter a validated result.

## 18. Testing Requirements

### 18.1 Unit tests

- Favorite/underdog orientation for home and away favorites.
- ATS win/loss/push formulas, including half-point lines.
- Exact-line and bucket boundaries.
- Wilson intervals and zero-sample behavior.
- Shrinkage formula and fallback hierarchy.
- Coach-regime boundary selection.
- `available_at <= captured_at < kickoff_at` enforcement.
- Week-dependent blending and missing-feature handling.
- Provider median/reference selection.

### 18.2 Integration tests

- Historical ingestion is idempotent.
- Provider rows remain distinct.
- A rescheduled/doubleheader-like duplicate source event does not overwrite another game.
- Selected game returns national, team, season, and roster panels independently.
- A roster snapshot posted after kickoff is excluded.
- A historical panel failure does not stop live odds capture.
- Signal snapshots are immutable and reproduce the displayed card.

### 18.3 Statistical regression tests

- Known fixture dataset produces fixed SU/ATS counts.
- Walk-forward splits never train on a later date than their test row.
- Changing holdout data cannot alter the frozen hypothesis definition.
- Prospective results include only signals emitted before kickoff.
- Multiple-testing family and q-value calculation remain stable.

## 19. Acceptance Criteria

The first release is complete when:

1. A stored coverage audit exists for every targeted historical season.
2. At least 2016–2025 FBS-versus-FBS games with final scores and available historical spreads are backfilled idempotently.
3. The terminal can answer the exact `home favorite -14.5` question and the registered `14.0–16.5` cohort question with counts, intervals, seasons, and provenance.
4. The selected team card shows raw and shrunk team results and states the regime/fallback used.
5. Current-season features are frozen point-in-time and display their prior/current weighting.
6. Roster features use dated snapshots and tolerate missing data.
7. Descriptive panels are visually and semantically separated from validated signals.
8. No historical reference line is labeled as a verified close.
9. No retrospective rating or current roster leaks into an earlier game.
10. Hypotheses have immutable definitions and walk-forward/holdout result records.
11. Prospective signal snapshots are written before kickoff and later joined to outcomes and CLV.
12. Live odds collection continues normally if historical or roster services fail.

## 20. Delivery Plan

### Phase 0 — Source and coverage audit

- Confirm historical games/lines shape, provider semantics, quotas, and licensing.
- Audit 2016–2025 coverage before building result claims.
- Evaluate roster, injury, depth-chart, transfer, returning-production, and staff sources.

### Phase 1 — Historical foundation

- Add historical-line and provenance schema.
- Backfill games/results/lines idempotently.
- Implement ATS/SU cohort engine and canonical definitions.
- Add fixed-fixture tests.

### Phase 2 — Team and regime context

- Add staff regimes.
- Implement team hierarchy, recency windows, partial pooling, and reliability labels.
- Add comparable-games query.

### Phase 3 — Point-in-time season and roster features

- Add team feature and roster snapshot schemas.
- Build preseason priors and week-dependent blending.
- Add strict availability/leakage checks.

### Phase 4 — Research registry

- Add hypothesis and result tables.
- Implement preregistered walk-forward and untouched holdout evaluation.
- Add multiple-testing controls and reproducible result artifacts.

### Phase 5 — Dashboard

- Add `HISTORY` tab and evidence-quality panel.
- Show exact and bucket cohorts, team shrinkage, current-season weight, and roster confidence.
- Keep all results descriptive unless promoted.

### Phase 6 — Prospective validation

- Emit immutable pre-kickoff signal snapshots.
- Track outcome, price, and CLV.
- Promote or retire signals under frozen rules only after the required prospective sample.

## 21. Decision Log

| Decision | Rationale |
|---|---|
| Separate historical lines from the live exact-book ledger | Historical sources often lack equivalent timestamp semantics. |
| Show raw and shrunk team rates | Preserves transparency while limiting small-sample overinterpretation. |
| Treat coaching changes as regime boundaries | Multi-year team identity is not stationary. |
| Blend current season with a preseason prior | Prevents Week 1–2 noise from dominating. |
| Snapshot rosters and availability | Prevents hindsight leakage. |
| Require walk-forward and prospective validation | Historical fit alone does not establish a usable signal. |
| Allow only validated signals to affect alerts | Keeps research context separate from operational market monitoring. |

## 22. Open Questions Before Implementation

- Which provider and plan provide legally usable point-in-time roster, depth-chart, injury, and returning-production history?
- Does the historical betting source identify opener/close semantics or only a final reference line for each provider?
- Should the first model target full-game ATS, first-half ATS, totals error, or CLV as its single primary outcome?
- Which staff changes qualify as a new regime automatically versus manual review?
- Which team feature source provides stable historical week-by-week availability and acceptable licensing?
- Should postseason and bowl opt-out eras be modeled as separate regimes?
- What minimum prospective duration is required for sparse team-specific hypotheses when 100 observations is unrealistic?

Until these questions are resolved, the historical dashboard may ship as descriptive context, but no candidate signal should be promoted.
