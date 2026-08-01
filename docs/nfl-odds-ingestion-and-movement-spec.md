# NFL Odds Ingestion and Line-Movement Monitoring

Status: Implemented
Date: 2026-08-01
Owner: NBA DFS v2
Target surface: `/nfl`

## 1. Summary

Build the NFL schedule, odds-capture, movement-alert, score-settlement, and
health-monitoring pipeline required to make the dedicated `/nfl` page operate
with the same auditability guarantees as the MLB Vegas page.

The completed system must:

1. Maintain a durable NFL matchup table with stable event identity, kickoff
   time, teams, status, and final scores.
2. Capture pregame moneylines, spreads, totals, and exact per-book prices into
   the existing append-only `game_odds_history` ledger.
3. Poll often enough to provide a 30-minute freshness target before kickoff.
4. Never write in-play prices into the pregame closing-line history.
5. Detect and freeze first-breach sharp-market alerts.
6. Grade alerts against the last eligible pre-kickoff capture and final score.
7. Populate `/nfl` with current games, movement, freshness, alerts, and a
   settled audit ledger.
8. Fail visibly when scheduled work exits successfully without writing the
   required fresh captures.

This specification covers regular-season and preseason game lines. NFL DFS
salaries, projections, player props, injuries, depth charts, and an independent
NFL win model are out of scope.

## 2. Current State

The repository currently has (implementation checkpoint 2026-08-01):

- A dedicated `/nfl` page and NFL sport-navigation entry.
- An NFL board query that reads `game_odds_history WHERE sport = 'nfl'`.
- Shared append-only odds and alert tables:
  - `game_odds_history`
  - `line_alerts`
  - `alert_grades`
- Generic moneyline alert logic for Pinnacle divergence, synchronized steam,
  slow walking movement, and DraftKings value.
- `nfl_teams` and `nfl_matchups` schemas in Python, Drizzle, and the web
  self-provisioning path.
- An idempotent 32-team seed and exact provider-name identity map.
- NFL event, moneyline/spread/total, score, freshness, and kickoff-guard
  ingestion in `ingest/nfl_schedule.py`.
- Separate regular-season and preseason provider feeds, persisted on the same
  matchup/odds ledger with `season_type = 'regular'` or `'preseason'`.
- NFL registration in the shared moneyline movement/alert engine, including
  two-way moneyline ties grading as void.
- NFL spread/total steam and walking alerts with proposition-aware line CLV,
  push-safe outcome grading, and append-only grade history.
- Matchup-aware health diagnostics, automatic page refresh, movement
  sparklines, and settled open-to-close history on `/nfl`.
- Daily refresh and 30-minute capture workflow definitions.
- MLB workflow patterns for capture retries, freshness enforcement,
  notifications, closing-line grading, and score settlement.

The repository does not yet have:

- A paid historical NFL odds backfill command.

Live validation completed 2026-08-01 against the 2026-09-09 slate using the
guarded smoke command. It seeded 32 teams, upserted one matchup, captured one
complete pregame snapshot with DraftKings and Pinnacle moneyline/spread/total
data, and returned a passing health verdict with no post-kickoff contamination.

The `/nfl` page now reads scheduled matchups and eligible pre-kickoff captures;
it remains empty until the workflows run against an in-season slate.

## 3. Source and API Policy

Use The Odds API v4 for the first production version.

- Sport keys:
  - `americanfootball_nfl` -> `season_type = 'regular'`
  - `americanfootball_nfl_preseason` -> `season_type = 'preseason'`
- Events endpoint: schedule identity, teams, and kickoff time. The endpoint is
  quota-free according to the provider documentation.
- Odds endpoint: `h2h,spreads,totals`, American odds, regions `us,eu,us_ex` so
  captures include DraftKings, Pinnacle, and Polymarket when offered.
- Scores endpoint: live/upcoming games plus completed games from the prior
  three days using `daysFrom=3`.
- Historical odds endpoint: optional paid backfill only; never run from the
  normal refresh workflow.

The ingestion command queries both quota-free event feeds, then requests the
paid odds/scores endpoint only for a season type with eligible persisted games.
This prevents a preseason refresh from consuming a regular-season odds request,
or vice versa. Preseason records use the same movement, alert, settlement, and
health logic as regular-season records and are labeled in the `/nfl` board.

Provider reference:
[The Odds API v4 documentation](https://the-odds-api.com/liveapi/guides/v4/).

`ODDS_API_KEY` remains the only required external API secret. Notification
secrets remain optional:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DISCORD_WEBHOOK_URL`

### Source limitations

- The events endpoint is an upcoming-event feed, not a complete historical
  NFL schedule archive. Persist every observed event immediately.
- The scores endpoint only provides recently completed games. A daily score
  reconciliation is mandatory so results are not lost after the three-day
  window.
- Provider event ID is the canonical external identity. Team/date matching is
  a fallback only and must never silently merge two events.
- If a future authoritative NFL schedule source is added, it may enrich
  season/week/status fields but must not change the canonical internal matchup
  ID for an already-mapped provider event.

## 4. Data Flow

```text
The Odds API /events
    -> nfl_teams identity resolution
    -> nfl_matchups upsert by event_id

The Odds API /odds
    -> pregame and identity guards
    -> consensus moneyline/spread/total
    -> nfl_matchups current-line update
    -> game_odds_history append-only capture (sport='nfl', full books JSON)
    -> alert scan and notification

The Odds API /scores
    -> nfl_matchups status + final scores
    -> alert closing-line and outcome settlement
    -> append-only alert_grades

nfl_matchups + game_odds_history + line_alerts
    -> /nfl live board, history, freshness, and audit metrics
```

## 5. Schema

Add the tables to both `db/schema.py` and `web/src/db/schema.ts`. Add idempotent
DDL to the relevant web schema-ensure path if the application is expected to
self-provision them.

### 5.1 `nfl_teams`

```sql
CREATE TABLE IF NOT EXISTS nfl_teams (
    team_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    abbreviation TEXT NOT NULL UNIQUE,
    odds_api_name TEXT NOT NULL UNIQUE,
    city TEXT,
    conference TEXT,
    division TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    logo_url TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Seed all 32 active teams in `ingest/nfl_teams.py`. Store aliases separately in
code or in an optional `nfl_team_aliases` table. Unknown teams are a hard
ingestion error for a selected NFL event; do not create anonymous teams.

### 5.2 `nfl_matchups`

```sql
CREATE TABLE IF NOT EXISTS nfl_matchups (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    season INTEGER,
    season_type TEXT,
    week INTEGER,
    game_date DATE NOT NULL,
    commence_time TIMESTAMPTZ NOT NULL,
    home_team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
    away_team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
    game_status TEXT,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    home_score INTEGER,
    away_score INTEGER,
    vegas_total DOUBLE PRECISION,
    home_ml INTEGER,
    away_ml INTEGER,
    home_spread DOUBLE PRECISION,
    vegas_prob_home DOUBLE PRECISION,
    home_implied DOUBLE PRECISION,
    away_implied DOUBLE PRECISION,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score_fetched_at TIMESTAMPTZ,
    final_at TIMESTAMPTZ,
    CHECK (home_team_id <> away_team_id)
);

CREATE INDEX IF NOT EXISTS idx_nfl_matchups_date
    ON nfl_matchups(game_date, commence_time);
CREATE INDEX IF NOT EXISTS idx_nfl_matchups_upcoming
    ON nfl_matchups(commence_time) WHERE completed = FALSE;
```

`game_date` is the kickoff date in `America/New_York`, not the UTC date.

Event rescheduling updates `game_date` and `commence_time` on the same row.
`event_id` is immutable after insertion. A changed provider event ID must be
reviewed and explicitly reconciled; it must not be guessed from date alone.

### 5.3 Existing odds ledger

Do not create an NFL-specific odds-history table. Continue using
`game_odds_history` with `sport='nfl'`.

Every NFL capture must include:

- `matchup_id = nfl_matchups.id`
- provider `event_id`
- Eastern `game_date`
- home and away team IDs/names
- bookmaker count
- consensus home/away moneyline
- consensus home spread
- consensus game total
- vig-free consensus home win probability
- home/away implied point totals
- full per-book `books` JSON
- unrounded `vegas_total_raw`
- UTC `captured_at` and deterministic `capture_key`

Per-book JSON must preserve both sides and both prices:

```json
{
  "draftkings": {
    "last_update": "2026-09-13T16:22:00Z",
    "ml_home": -145,
    "ml_away": 125,
    "spread_home": -3.0,
    "spread_home_price": -110,
    "spread_away": 3.0,
    "spread_away_price": -110,
    "total_line": 47.5,
    "over": -108,
    "under": -112
  }
}
```

The existing `books` and `vegas_total_raw` migrations remain authoritative.
No in-place update to an existing history row is allowed.

## 6. Ingestion Modules

### 6.1 `ingest/nfl_teams.py`

Responsibilities:

- Seed and idempotently update all 32 NFL teams.
- Export normalized provider-name-to-team-ID mappings.
- Normalize punctuation and whitespace only; do not use fuzzy matching during
  automatic ingestion.
- Raise a clear error listing unmapped provider names.

### 6.2 `ingest/nfl_schedule.py`

Public functions:

```python
fetch_events(db, api_key, game_date=None) -> int
fetch_odds(db, api_key, game_date=None) -> int
fetch_scores(db, api_key, days_from=3) -> int
verify_fresh_upcoming_odds(db, game_date, max_age_minutes=35) -> bool
```

CLI:

```text
python -m ingest.nfl_schedule
python -m ingest.nfl_schedule --date YYYY-MM-DD
python -m ingest.nfl_schedule --date YYYY-MM-DD --require-fresh-upcoming-odds
python -m ingest.nfl_schedule --scores-only --days-from 3
```

#### Event ingestion

1. Query both `/v4/sports/americanfootball_nfl/events` and
   `/v4/sports/americanfootball_nfl_preseason/events`.
2. Parse `commence_time` as timezone-aware UTC.
3. Derive `game_date` in `America/New_York`.
4. Resolve both teams by exact provider name.
5. When `--date` is present, retain only events whose Eastern date matches.
6. Upsert by `event_id`, preserving the feed mapping as `season_type`.
7. Update kickoff/team assignment when the provider changes an upcoming event.
8. Never clear a final score when refreshing schedule metadata.

#### Odds ingestion

Request:

```text
GET /v4/sports/{americanfootball_nfl|americanfootball_nfl_preseason}/odds
regions=us,eu,us_ex
markets=h2h,spreads,totals
oddsFormat=american
dateFormat=iso
```

Before processing odds, call `fetch_events` so every event has a matchup row.
Only request a sport key's paid endpoint when the database contains eligible
upcoming matchups of that season type in the selected date/horizon.

For each event:

1. Resolve by exact `event_id`.
2. Reject events with missing matchup identity or mismatched home/away teams.
3. Reject a capture if provider kickoff is at or before `captured_at`.
4. Also reject it if stored `nfl_matchups.commence_time` is at or before
   `captured_at`. This second guard protects against a provider moving the
   event time after kickoff.
5. Reject implausible/malformed two-way markets rather than partially writing
   corrupted lines.
6. Average moneylines in implied-probability space, never American-odds space.
7. Compute the consensus spread and total from numeric lines. Preserve each
   book's exact line and price in `books`.
8. Calculate vig-free home probability from the paired consensus moneylines.
9. Calculate football implied team totals from total and home spread:

```text
home_implied = (vegas_total - home_spread) / 2
away_implied = (vegas_total + home_spread) / 2
```

10. Update the current fields on `nfl_matchups`.
11. Append one `game_odds_history` row per event/capture.

Use one UTC timestamp, truncated to seconds, as the capture timestamp for the
whole API response. The capture key must be deterministic and idempotent for
retries of the same captured response.

#### Score ingestion

1. Query `/scores?daysFrom=3&dateFormat=iso` at least daily during the season.
2. Resolve scores by provider event ID only.
3. Write integer home/away scores when `completed=true`.
4. Set `completed`, `game_status`, `score_fetched_at`, and `final_at`.
5. A later provider correction may update a final score. If it does, reopen
   the affected NFL alerts for regrading while preserving prior rows in
   `alert_grades`.
6. Missing or non-final scores do not settle outcome bets.

#### Freshness verification

Mirror MLB's silent-no-write protection:

- Upcoming, non-cancelled games inside the monitoring window must have an NFL
  history capture no older than 35 minutes.
- If all selected games have started or no games are scheduled, verification
  succeeds without requiring a capture.
- If any qualifying game is missing a fresh capture, return false and make the
  CLI exit non-zero.
- Log upcoming count, fresh count, missing matchup IDs, and latest capture.

### 6.3 `ingest/refresh_nfl_vegas.py`

Daily orchestration stages:

1. Seed/verify teams.
2. Refresh upcoming events.
3. Capture today's NFL odds when games are present.
4. Refresh scores for the prior three days.
5. Scan new alerts.
6. Settle eligible alerts.
7. Run health checks and return non-zero if required stages fail.

Each stage logs success/failure independently, matching
`ingest/refresh_mlb_vegas.py`. Do not swallow a failed required stage and exit
zero.

## 7. Movement and Alert Engine

### 7.1 Register NFL

Update both Python and TypeScript mappings:

- `model/line_movement.py`
  - `_MATCHUP_TBL['nfl'] = 'nfl_matchups'`
- `model/line_alerts.py`
  - add `nfl` to `_ALERT_SPORTS`
  - `_SCORE_COLS['nfl'] = ('home_score', 'away_score')`
- `web/src/db/queries.ts`
  - `LINE_MOVEMENT_MATCHUP_TABLE.nfl = 'nfl_matchups'`
  - `HISTORY_OUTCOME.nfl` uses NFL home/away scores
  - widen movement function sport unions to include `nfl`

Replace the temporary ledger-only `getNflVegasBoard()` behavior with a join to
`nfl_matchups` so all displayed captures are restricted to
`captured_at <= commence_time`.

### 7.2 Moneyline parity

The shared moneyline alert set includes:

- `pinnacle_divergence`
- `pinnacle_polymarket_delta`
- `steam`
- `walking`
- `dk_value`

Use the existing initial thresholds until NFL-specific backtest sample sizes
justify changes. Do not tune thresholds on ungraded anecdotal results.

NFL tie settlement differs from the existing binary implementation. For
moneyline alerts:

- flagged side wins: `won`
- flagged side loses: `lost`
- tied final score on a two-way market: `void`

CLV remains the primary early audit metric.

`pinnacle_polymarket_delta` fires when Pinnacle's vig-free probability is at
least 2 percentage points higher than Polymarket on a side. Polymarket is
excluded from the retail consensus used by `pinnacle_divergence`. The NFL and
MLB boards display both vig-free home probabilities and signed `Pin - Poly`
percentage-point delta even when the alert threshold is not crossed.

### 7.3 NFL spread and total movement

Moneyline-only movement is insufficient for an NFL page. Add market-aware
movement signals after basic parity is stable.

Required immutable alert types:

- `spread_steam`: at least three books move the same team by at least 0.5
  points during one capture interval.
- `spread_walking`: consensus spread moves at least 1.0 point from open toward
  one team.
- `total_steam`: at least three books move the total at least 0.5 points in the
  same direction during one interval.
- `total_walking`: consensus total moves at least 1.0 point from open.

Thresholds are initial operational defaults, not claims of profitability.
Freeze these fields in `details_json` at first breach:

- `market`
- `selection`
- `trigger_line`
- `trigger_price` when available
- `open_line`
- `current_line`
- `delta`
- `books_moved`
- exact capture key and grading version

Spread and total alerts require proposition-aware grading:

- Home spread selection wins when `home_score + trigger_line > away_score`.
- Away spread selection wins when `away_score - trigger_line > home_score` if
  `trigger_line` is stored from the home perspective.
- Over wins when combined score exceeds `trigger_line`.
- Under wins when combined score is below `trigger_line`.
- Equality is `void`.

Line CLV must use a documented selection-side sign convention. Do not compare
prices or lines from different propositions as if they were the same. Preserve
`comparison_status` and append every regrade to `alert_grades`.

NFL key numbers (3 and 7) should be displayed and retained in analytics, but
must not change alert thresholds in v1 without a backtest.

## 8. Scheduled Workflows

### 8.1 `.github/workflows/refresh_nfl_vegas.yml`

Run daily during the NFL season and remain safe out of season.

- Morning ET: events + scores + current odds + scan + settle + health.
- Manual dispatch inputs:
  - `date` (optional Eastern game date)
  - `scores_days_from` (default 3, maximum 3)

Out of season or no scheduled games is a successful no-op, not an error.

### 8.2 `.github/workflows/capture_nfl_odds.yml`

Run every 30 minutes. Because GitHub schedule delivery may be delayed, the job
must use the same 35-minute freshness gate and retry behavior as MLB.

Execution policy:

1. Call the quota-free events endpoint first.
2. Skip the paid odds request when there are no NFL games within the configured
   monitoring horizon.
3. Default monitoring horizon: 48 hours before kickoff through kickoff.
4. Stop capturing each game at kickoff.
5. Retry a failed or stale capture three times with 15/30/45-second backoff.
6. After a successful capture, run `model.line_alerts --sport nfl`.

Use a dedicated concurrency group:

```yaml
concurrency:
  group: capture-nfl-odds
  cancel-in-progress: false
```

Do not add NFL to an MLB-only command by branching internally. Shared workflow
steps are acceptable, but each sport must retain independent failure reporting.

### 8.3 Quota logging

Log the provider response headers on every paid request:

- `x-requests-remaining`
- `x-requests-used`
- `x-requests-last`

Never log the API key or full request URL containing the key.

## 9. `/nfl` Page Requirements

Retain the dedicated route. Upgrade it from ledger-only rendering to matchup-
aware rendering.

### Header and status

- Selected Eastern game date.
- Current Eastern time.
- Latest eligible NFL capture time and age.
- Target freshness: 30 minutes.
- Automatic refresh every two minutes while the tab is visible.
- Warning when any upcoming game lacks a fresh capture.

### Summary cards

- Scheduled games.
- Games with at least two eligible captures.
- Notable moneyline moves.
- Sharp-signal games.

### Movement board

Each game displays:

- kickoff time
- away at home
- opening and current vig-free win probabilities
- probability movement and sparkline
- opening/current home spread and delta
- opening/current total and delta
- current moneylines
- Pinnacle/steam/walking/DK-value badges
- capture count and latest capture age

All movement queries must exclude captures after kickoff.

### History and audit

- Paginated open-to-close movement history.
- Final score and moved-side result.
- Alert audit by alert type:
  - tracked count
  - average CLV
  - beat-close rate
  - W-L-P
  - win rate
  - DraftKings units where applicable
- Empty states distinguish:
  - no games scheduled
  - games scheduled but never captured
  - one capture only
  - stale capture
  - pipeline error

The page must not claim an independent model edge until an NFL prediction
model exists and has prospective calibration evidence.

## 10. Health and Failure Policy

Add `getNflPipelineHealth()` or an equivalent query returning explicit issues:

- upcoming matchup with no odds capture
- latest capture older than 35 minutes
- capture written after kickoff
- event with unknown team identity
- duplicate event ID or one event mapped to multiple matchup IDs
- matchup final for more than 24 hours without scores
- unsettled alert on a scored game
- odds row with fewer than two usable moneyline books
- odds row missing exact `books` payload

Severity:

- `error`: identity conflict, post-kickoff write, missing fresh capture for an
  upcoming monitored game, or completed game lacking scores after 24 hours.
- `warning`: one capture only, missing Pinnacle/DraftKings, reduced book count,
  or pending settlement within the normal score lag.

Required stage errors make scheduled workflows fail. Optional notification
delivery errors are warnings and never roll back ledger writes.

## 11. Testing

### Unit tests

Add:

- `tests/test_nfl_teams.py`
- `tests/test_nfl_schedule.py`
- `tests/test_nfl_odds.py`
- `tests/test_nfl_scores.py`
- `tests/test_nfl_line_alerts.py`
- `tests/test_nfl_data_health.py`

Required cases:

- Eastern date conversion for Sunday night and Monday night games.
- Exact team mapping for all 32 teams.
- Event upsert is idempotent.
- Rescheduled event moves the existing row rather than duplicating it.
- Unknown team rejects the event.
- Consensus American odds are averaged in implied-probability space.
- Per-book spreads/totals retain both sides and prices.
- Provider and stored-kickoff in-play guards both reject live prices.
- Retry of one capture does not duplicate the history row.
- Freshness check fails on silent zero-write.
- Moneyline tie grades void.
- Spread and total pushes grade void.
- Score correction produces a new current `alert_grades` row without deleting
  prior grade history.

### TypeScript tests/checks

- NFL is accepted by shared movement/history query types.
- `/nfl` renders scheduled, uncaptured, single-capture, fresh, stale, and final
  fixtures correctly.
- Date navigation preserves `/nfl`.
- Switching away from NFL returns to a supported page for the selected sport.

### Integration fixture

Create a deterministic two-game fixture containing:

- at least four bookmakers including DraftKings and Pinnacle
- three pre-kickoff captures
- one rejected post-kickoff capture
- one spread move through 3
- one total move
- one tied final and one normal final

The fixture must prove the full path:

```text
events -> matchups -> captures -> alerts -> scores -> settlement -> page query
```

No integration test may call the live provider API.

## 12. Rollout Plan

### Phase 1: Matchup identity and scores

- Add team/matchup schema and seeds.
- Implement events and scores ingestion.
- Verify final-score persistence for a fixture and one live preseason slate.

Exit gate: scheduled games and final scores persist idempotently by event ID.

### Phase 2: Pregame odds ledger

- Implement odds parsing, exact books JSON, current matchup lines, double
  in-play guard, freshness verification, and capture workflow.

Exit gate: every monitored upcoming game has fresh pregame captures and no
capture exists after stored kickoff.

### Phase 3: Moneyline alerts and grading

- Register NFL in shared movement/alert mappings.
- Add tie-safe settlement and notifications.
- Populate open-to-close history and audit tables.

Exit gate: fixture alerts freeze once, grade CLV from the last pre-kickoff
capture, and settle correct W-L-P outcomes.

### Phase 4: NFL spread/total movement

- Add market-aware movement calculations, alerts, proposition-safe grading,
  and page columns/sparklines.

Exit gate: spread/total fixture alerts and pushes grade correctly, and every
grade retains reproducible trigger/close context.

### Phase 5: Production hardening

- Add health queries, retries, quota telemetry, workflow annotations, and
  operational documentation.
- Observe at least one full game week before changing alert thresholds.

Exit gate: no silent zero-write, no in-play contamination, and no eligible
alert remains unsettled after scores are available.

## 13. Acceptance Criteria

The feature is complete when all of the following are true:

1. `/nfl` shows all selected-date NFL events with correct Eastern kickoff
   times and stable team identity.
2. Every monitored upcoming event receives an append-only odds capture within
   the 35-minute enforcement window or the workflow fails visibly.
3. Captures include full DraftKings/Pinnacle book context when those books are
   returned by the provider.
4. No capture after kickoff is eligible for the page, alerts, or closing line.
5. Opening/current moneyline, spread, and total movements are computed from
   pregame captures only.
6. Shared game-line alerts scan successfully with `--sport nfl`.
7. First-breach alerts are immutable and duplicate scans do not add duplicates.
8. Moneyline ties and spread/total pushes settle as void.
9. Final scores settle outcomes and append a reproducible grade record.
10. The page shows capture freshness, pipeline problems, alert audit metrics,
    and historical movement without presenting a nonexistent NFL model edge.
11. Unit, integration, TypeScript, lint, and schema-idempotency checks pass.
12. Existing NBA, MLB, soccer, and tennis pipelines continue unchanged.

## 14. Out of Scope

- NFL DFS player pool, salaries, projections, or optimizer.
- Player props and prop settlement.
- Injury reports, depth charts, weather, or stadium effects.
- Independent win/spread/total prediction models.
- Automated wagering.
- Historical odds backfill as part of normal deployment.
- Alert-threshold optimization before prospective NFL audit data exists.

## 15. Expected File Changes

New:

- `ingest/nfl_teams.py`
- `ingest/nfl_schedule.py`
- `ingest/refresh_nfl_vegas.py`
- `.github/workflows/refresh_nfl_vegas.yml`
- `.github/workflows/capture_nfl_odds.yml`
- NFL test files listed above

Modified:

- `db/schema.py`
- `db/queries.py`
- `web/src/db/schema.ts`
- `web/src/db/ensure-schema.ts`
- `web/src/db/queries.ts`
- `model/line_movement.py`
- `model/line_alerts.py`
- `web/src/app/nfl/page.tsx`
- `web/src/app/nfl/nfl-vegas-client.tsx`
- project operations documentation and required-secret documentation
