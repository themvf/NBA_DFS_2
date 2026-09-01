# College Football Line Terminal — Live Data Specification

**Status:** Phase 1–3 implementation specification; alerts, weather, and persistent paper settlement remain gated  
**Date:** 2026-09-01  
**Route:** `/cfb`  
**Product posture:** Market-observation and paper-trading terminal; not an automated betting system

## 1. Executive decision

The CFB page now reads canonical games and exact sportsbook captures from the database. The implementation is a point-in-time market ledger:

```text
CFBD schedule ─┐
               ├─> canonical game ─> exact sportsbook quotes ─> history/charts/alerts
Odds API ──────┘             │
                             ├─> weather snapshots
                             ├─> sourced availability/news events
                             └─> immutable paper positions and closing-line results
```

The first production release must answer four questions reliably:

1. What games exist, and when do they start?
2. What did each sportsbook quote at a specific time?
3. What changed, and how fresh is that observation?
4. If a user records a paper position, what exact quote did they record and did it beat the closing line?

It must **not** claim that a movement is predictive, caused by a news item, or a betting edge until prospective results demonstrate that claim.

## 2. Initial specification (v0)

### 2.1 Providers

| Concern | Primary source | Notes |
|---|---|---|
| Schedule, teams, venues, status, final scores | CollegeFootballData.com (CFBD) | Canonical game identity. Query FBS games by season/week. |
| Moneyline, spread, total, and book prices | The Odds API | Use sport key `americanfootball_ncaaf`; request only `h2h,spreads,totals`. |
| Forecast weather | Open-Meteo | Query stadium latitude/longitude; store the forecast as observed at capture time. |
| Injuries/availability | Official school/conference reports or optional licensed SportsDataIO feed | Absence of a report is not proof that a player is healthy. |
| General news | Source links and metadata only in v1 | Do not scrape or republish unlicensed article bodies. |

### 2.2 Basic flow

1. Import the current season's teams and venues from CFBD.
2. Import the current week plus the next two weeks of games.
3. Map each Odds API event to one canonical CFBD game.
4. Poll pregame odds and append a capture; never overwrite a prior capture.
5. Preserve every returned book quote inside the capture row's `books` JSONB.
6. Refresh weather and approved availability/news events.
7. Derive current values, movement, and alerts from stored history.
8. Render `/cfb` from server data rather than the static `GAMES` constant.
9. Freeze the last eligible pre-kickoff quote as the close.
10. Import final scores and grade paper positions and alert outcomes.

### 2.3 v0 definition of done

The page is live when it displays real scheduled games, exact sportsbook quotes, capture timestamps, history charts, and explicit fresh/stale/partial states. A successful HTTP response alone does not make the page live.

---

## 3. Explanation for a junior developer

### 3.1 Think of it as a security master plus a tape

A trading terminal does not treat the price as a field on a team. It keeps two different things:

- The **security master** says what the thing is. Here, that is a football game: stable game ID, teams, venue, and kickoff.
- The **tape** says what people quoted over time. Here, that is every sportsbook's line and price at every capture.

If Oregon is playing Ohio State, there should be one canonical game row even if three providers spell the schools differently. That row may have thousands of quote rows over the week.

### 3.2 A line and a price are different numbers

`Oregon -3.5 (-110)` contains both:

- `-3.5`: the spread, expressed from Oregon's side.
- `-110`: the price paid for that proposition.

Moving from `-3.5 (-110)` to `-3.5 (-125)` is real market movement even though the spread did not change. Moving to `-4 (-110)` is also movement, but it changes the proposition. Store both numbers every time.

For a full-game spread capture, a normalized row looks like:

```json
{
  "gameId": 812,
  "book": "draftkings",
  "market": "spread",
  "period": "full_game",
  "side": "home",
  "line": -3.5,
  "americanPrice": -110,
  "providerUpdatedAt": "2026-09-05T15:28:12Z",
  "capturedAt": "2026-09-05T15:30:03Z",
  "isLive": false
}
```

The provider timestamp tells us when the book last changed the quote. `capturedAt` tells us when our system observed it. They are not interchangeable.

### 3.3 Never edit history

Suppose the spread goes `-2.5`, `-3`, `-3.5`. Updating one database field three times loses the path. Instead, insert three captures. The UI can calculate:

- opener: first eligible observation;
- current: latest eligible observation;
- close: last eligible observation before kickoff;
- change: current minus opener;
- velocity: change divided by elapsed time.

That is why an append-only ledger matters.

### 3.4 Normalize at the boundary

Provider payloads are external input. Convert them into our vocabulary once:

- all timestamps become UTC;
- the stored game date is derived in `America/New_York` for navigation;
- spreads are stored from the named side's perspective;
- totals use `over` or `under` sides;
- moneylines have no `line`, only a price;
- market period must be `full_game` in v1;
- team mapping must use an alias table, not a fuzzy database write.

If an event cannot be mapped safely, quarantine it and show a health error. Do not guess.

### 3.5 Derived data is not source data

The provider quote is source data. Consensus, steam, stale-price gaps, and movement scores are derived data. Keep them separate so we can change an algorithm without rewriting history.

For example, do not arithmetically average `+150` and `-170`. Convert each price to implied probability, remove paired-market vig when possible, aggregate probabilities, then convert the result back for display. For spread and total lines, use a median or modal line across eligible books and retain the paired prices.

### 3.6 “News caused the move” is usually unknowable

If an injury item appears at 2:05 PM and the total moves at 2:09 PM, we know the events are close in time. We do not automatically know causation. The product should say “associated within four minutes,” not “injury moved the line,” unless a human verifies the causal claim.

### 3.7 The happy-path implementation

```text
scheduled job
  -> fetch CFBD games
  -> upsert canonical game by CFBD game ID
  -> fetch free Odds API event list
  -> map provider event ID to canonical game
  -> check that game is upcoming and quota reserve is safe
  -> fetch paid odds
  -> validate both teams and kickoff
  -> write one game_odds_history capture
  -> write one game_odds_history row whose books JSONB preserves exact quotes
  -> compute alerts from the stored rows
  -> run health checks
  -> page reads the database (never calls providers from the browser)
```

### 3.8 The unhappy paths matter just as much

- Provider returns no games: distinguish an off-week from an outage.
- Kickoff changes: move the canonical game; do not create a duplicate.
- A team alias is unknown: quarantine the event.
- Odds arrive after kickoff: reject them from pregame history.
- One book omits a moneyline: show a partial ladder; do not invent one.
- Quota is exhausted: keep the last stored view and mark it stale.
- Weather fetch fails: odds remain live; weather gets its own stale badge.
- A game is canceled: stop polling and void eligible paper positions according to recorded rules.

---

## 4. Refined production specification (v1)

The refined design changes v0 in five important ways:

1. CFBD owns canonical game identity; odds-provider IDs are mappings, not primary keys.
2. Existing `game_odds_history` remains the capture ledger. Its `books` JSONB already preserves exact full-game lines and prices, so v1 adds no second quote table.
3. Freshness is computed separately for schedule, odds, weather, and news.
4. Alerts are observational and immutable at first breach.
5. Rollout is gated by quota, data integrity, and one full shadow week before public “LIVE” labeling.

### 4.1 Scope

Included in v1:

- FBS full-game pregame moneylines, spreads, and totals;
- exact-book ladder and consensus view;
- open/current/close history and charts;
- sourced weather and availability/news events;
- movement and data-quality alerts;
- a session paper blotter initially, followed by server-persisted positions and CLV only after the live quote path passes its shadow gate;
- final-score settlement and an audit view.

Out of scope:

- automatic wager placement;
- in-play betting;
- player props and derivative periods;
- FCS-only board coverage;
- predictive “edge” labels;
- automated claims that news caused line movement;
- unlicensed article-body ingestion;
- alert notifications to external channels until alert precision is observed.

### 4.2 Source-of-truth rules

| Field | Authority | Fallback | Conflict rule |
|---|---|---|---|
| Canonical game ID | CFBD | None | An unmapped odds event is quarantined. |
| Teams/conferences/classification | CFBD | Seeded aliases | Never fuzzy-write a team ID. |
| Kickoff/status/venue | CFBD | Odds API commence time for diagnostic display only | CFBD remains canonical; a material mismatch raises health. |
| Book quote | Odds API payload | None | Preserve exact book value and timestamps. |
| Consensus quote | Our deterministic calculation | None | Rebuildable from the exact quotes in `game_odds_history.books`. |
| Final score | CFBD | Odds API scores | Corrections append an audit event and regrade. |
| Weather | Open-Meteo snapshot | None | Never present old weather as current. |
| Availability | Official report or licensed provider | Human-admin entry | Every item requires provenance. |

### 4.3 Data model

#### `cfb_teams`

```text
id PK
cfbd_team_id UNIQUE NOT NULL
school NOT NULL
abbreviation
conference
classification
color
alt_color
logo_url
active BOOLEAN DEFAULT true
updated_at
```

#### `cfb_team_aliases`

```text
id PK
provider NOT NULL
alias NOT NULL
team_id FK cfb_teams NOT NULL
UNIQUE(provider, alias)
```

Alias writes are reviewed. Runtime ingestion may read this table but may not create mappings automatically.

#### `cfb_venues`

```text
id PK
cfbd_venue_id UNIQUE
name NOT NULL
city
state
latitude
longitude
timezone
roof_type
updated_at
```

#### `cfb_matchups`

```text
id PK
cfbd_game_id UNIQUE NOT NULL
season NOT NULL
week NOT NULL
season_type NOT NULL
game_date_eastern NOT NULL
commence_time_utc
start_time_tbd BOOLEAN DEFAULT false
home_team_id FK cfb_teams NOT NULL
away_team_id FK cfb_teams NOT NULL
venue_id FK cfb_venues
neutral_site BOOLEAN DEFAULT false
conference_game BOOLEAN DEFAULT false
status NOT NULL
home_score
away_score
completed_at
current_home_ml
current_away_ml
current_home_spread
current_total
latest_odds_capture_at
created_at
updated_at
```

The `current_*` columns are a convenience cache only. History remains authoritative.

#### `cfb_provider_event_map`

```text
id PK
provider NOT NULL
provider_event_id NOT NULL
matchup_id FK cfb_matchups NOT NULL
provider_home_name
provider_away_name
provider_commence_time
mapped_by ENUM(exact_alias, reviewed)
created_at
last_seen_at
UNIQUE(provider, provider_event_id)
```

One provider event may map to only one matchup. Rescheduling updates the existing matchup.

#### Existing `game_odds_history`

Use `sport = 'cfb'`. Each row represents one accepted game capture. The existing
`UNIQUE(sport, matchup_id, capture_key)` constraint gives one row per game per
capture—roughly 60 rows for a 60-game board, rather than one row per quote. The
`books` JSONB is the exact full-game quote source and already carries, per book:

```text
ml_home, ml_away
spread_home, spread_home_price
spread_away, spread_away_price
total_line, over, under
last_update, title
```

The scalar `home_spread`, `vegas_total`, and moneyline columns are compatibility
caches. The terminal derives displayed spread/total consensus from `books` at
read time. This preserves the line/price distinction and permits the consensus
algorithm to change without rewriting history.

Validation:

- both home and away moneyline prices are kept when present;
- both spread sides require a numeric line and their own price;
- totals require a line and retain separate over/under prices;
- a pregame writer rejects provider-live events;
- rows captured at or after canonical kickoff are excluded from opener/current/close calculations.

Store a redacted raw-payload checksum and provider request ID in pipeline telemetry. Full raw payload retention is optional and must have a deletion policy.

#### `cfb_weather_snapshots`

```text
id BIGSERIAL PK
matchup_id FK cfb_matchups NOT NULL
forecast_for TIMESTAMPTZ NOT NULL
temperature_f
precip_probability
precip_inches
wind_speed_mph
wind_gust_mph
wind_direction_degrees
provider_model
provider_run_time
captured_at TIMESTAMPTZ NOT NULL
UNIQUE(matchup_id, forecast_for, provider_run_time, captured_at)
```

#### `cfb_information_events`

```text
id BIGSERIAL PK
event_type ENUM(injury, availability, suspension, weather, coaching, other)
headline NOT NULL
summary
source_name NOT NULL
source_url NOT NULL
source_tier ENUM(official, licensed, editorial, manual) NOT NULL
published_at
first_seen_at NOT NULL
effective_at
team_id FK cfb_teams
matchup_id FK cfb_matchups
player_name
status
human_verified BOOLEAN DEFAULT false
content_checksum
UNIQUE(source_url, content_checksum)
```

Only metadata and a short original summary are stored. The UI links to the source.

#### `cfb_paper_positions`

```text
id UUID PK
owner_key_hash NOT NULL
matchup_id FK cfb_matchups NOT NULL
entry_history_id FK game_odds_history NOT NULL
entry_book_key NOT NULL
market_key NOT NULL
side NOT NULL
entry_line
entry_price NOT NULL
stake_units DOUBLE PRECISION NOT NULL
recorded_at TIMESTAMPTZ NOT NULL
status ENUM(open, won, lost, pushed, void) NOT NULL
closing_history_id FK game_odds_history
closing_book_key
clv_line
clv_probability
pnl_units
settled_at
settlement_reason
```

The entry quote is immutable. A paper position cannot be backdated, and the server rejects a quote that was stale beyond the configured entry window when recorded.

#### `data_pipeline_runs`

Use or extend a shared run ledger with provider, sport, stage, start/end, request count, quota headers, rows read/written/quarantined, error code, and status. A zero-write can be success only when the free event/schedule discovery proves there were no eligible games.

### 4.4 Identity and time rules

- Store instants as UTC `TIMESTAMPTZ`.
- Render user-facing dates and kickoff times in `America/New_York` by default.
- Use the CFBD game ID as stable identity.
- Map Odds API events using exact team aliases plus a kickoff tolerance of six hours; require both teams.
- A same-team pairing with multiple candidates is ambiguous and must be quarantined.
- `start_time_tbd = true` blocks final close selection until a real kickoff exists.
- On a reschedule, update the existing canonical row and retain an audit record of the old kickoff.
- Never use the page request time as the market observation time.

### 4.5 Odds ingestion

Implement:

```text
ingest/cfb_teams.py
ingest/cfb_schedule.py
ingest/cfb_odds.py
ingest/cfb_scores.py
ingest/cfb_weather.py
ingest/cfb_information.py
ingest/refresh_cfb_terminal.py
```

`cfb_odds.py` requirements:

1. Call the quota-free events endpoint before any paid request.
2. Request `americanfootball_ncaaf` and only `h2h,spreads,totals`.
3. Use an approved list of no more than ten named US books when coverage testing proves it sufficient; otherwise explicitly budget regions.
4. Skip when no mapped upcoming FBS game is inside the monitoring horizon.
5. Reject provider-live events and any capture whose observation is at/after canonical kickoff.
6. Write one accepted capture row per game, with every valid exact-book quote in `books`.
7. Update the matchup's current-value cache only after the transaction commits.
8. Log `x-requests-remaining`, `x-requests-used`, and `x-requests-last` without logging the API key.
9. On `OUT_OF_USAGE_CREDITS`, do not retry; mark the feed stale and preserve the last good view.
10. Repeated runs with the same capture key must be idempotent.

Consensus rules:

- Eligibility: quote age no older than ten minutes relative to the capture, unless the entire provider response is older and explicitly marked stale.
- Moneyline: remove two-way vig per book, aggregate fair implied probabilities, and display the median fair probability.
- Spread/total line: sort eligible observed lines and select index `(n - 1) // 2`; this is the lower of the two middle values when `n` is even and never invents a quarter-point market.
- Consensus price: compute in implied-probability space using only books quoting the selected consensus line.
- Alert eligibility requires at least four books at the selected line, not merely four books offering the market; store both counts in alert details.
- Preserve every exact quote; consensus never replaces the `books` values.

### 4.6 Capture cadence and quota gate

The shared key has a 20,000-credit monthly plan and other sports already consume most of the scheduled allowance documented in `docs/the-odds-api.md`. Therefore no CFB schedule is enabled until a seven-day provider probe measures coverage and an explicit monthly cost is approved.

The v1 odds collector uses lead-time windows rather than continuous polling. A
15-minute scheduler asks whether *any* mapped game is due, makes one bulk request
when necessary, and lets the accepted rows satisfy every due game. The windows
are deliberately wide enough to survive delayed cron execution:

| Checkpoint | Eligible lead-time window |
|---|---:|
| `open` | 12–72 hours |
| `t_minus_6h` | 5–7 hours |
| `t_minus_90m` | 60–120 minutes |
| `t_minus_15m` | 5–25 minutes |

CFB captures T−48h, T−24h, T−6h, T−90m, T−15m, and T−2m. The one-minute Vercel
dispatcher makes the final window reachable; GitHub Actions performs the paid
bulk fetch. Fulfillment is derived per accepted game from an immutable
`game_odds_history` row inside the checkpoint window, never from HTTP success.
One Saturday response can therefore satisfy many games while retries remain
idempotent. Schedule/status and free event mapping refresh independently.

Quota controls:

- global daily reserve shared with MLB, NFL, NBA, and tennis;
- per-stage maximum spend;
- no paid fetch when all monitored games have started;
- dedupe overlapping Vercel and GitHub workflow windows;
- circuit breaker when remaining credits fall below the configured reserve;
- alert on forecast monthly burn above plan.

The shared event-close worker owns paid checkpoint captures and quota auditing;
the regular CFB refresh job must not make overlapping scheduled paid calls. A
manual `capture_now` dispatch remains an intentional out-of-window spend action.

### 4.7 Movement and alert semantics

Alerts describe observable conditions. Each alert freezes the first qualifying quote IDs and timestamps and is idempotent.

Generic moneyline detectors may be enabled only after CFB is added to every
required sport mapping (`_ALERT_SPORTS`, `_MATCHUP_TBL`, and `_SCORE_COLS`) and a
Pinnacle-class reference is verified in live captures. `clv_report.py` and
`best_price.py` are already sport-parameterized. Spread/total detectors remain
off until the staged `line_alerts` dedupe migration is complete: add the new
constraint beside the legacy constraint, backfill legacy `dedupe_key` as
`alert_type:side`, switch writers, then drop the old constraint in a later
deployment. During the overlap, distinct same-type/same-side alerts will still
be rejected by the legacy constraint and detector health must not be interpreted
as proof of full emission.

| Alert | Initial rule | Guardrails |
|---|---|---|
| `spread_move` | Consensus home spread changes by at least 1.0 point within 30 minutes | Minimum 4 eligible books at both endpoints. |
| `total_move` | Consensus total changes by at least 1.5 points within 30 minutes | Minimum 4 eligible books. |
| `key_cross` | Spread crosses 3, 7, 10, or 14 | UI annotation only until outcomes validate value. |
| `price_pressure` | Same line remains while fair price changes by at least 4 percentage points | Compare identical propositions. |
| `reversal` | At least 1 point one way, then at least 0.75 back within 90 minutes | Freeze both legs. |
| `book_outlier` | One executable quote differs from consensus by at least 1 point or 4 fair-probability points | Quote age ≤5 minutes; same side/period. |
| `reference_break` | Approved reference book moves first and consensus follows within 15 minutes | Call it “sharp” only after the reference-book premise is documented and calibrated. |
| `weather_shift` | Wind/precipitation forecast crosses configured threshold near a simultaneous total move | Label temporal association, not causation. |
| `information_proximity` | Verified item occurs within 30 minutes of a material move | Display both timestamps and source. |
| `stale_feed` | Latest required capture exceeds target freshness | Data-quality alert, never a betting signal. |

Opening line is the first eligible capture. Current line is the newest eligible pregame capture. Closing line is the last eligible capture strictly before canonical kickoff. Alert CLV compares the same market, period, side, and preferably the same book; cross-book consensus CLV is shown separately.

### 4.8 API and server query contract

Add `getCfbTerminal(gameDateEastern)` returning:

```ts
type CfbTerminalPayload = {
  asOf: string;
  status: "live" | "stale" | "partial" | "sample" | "unavailable";
  freshness: {
    schedule: FeedHealth;
    odds: FeedHealth;
    weather: FeedHealth;
    information: FeedHealth;
  };
  quota: { remaining: number | null; capturedAt: string | null };
  games: CfbGameSummary[];
  selectedGame: CfbGameDetail | null;
  alerts: CfbAlert[];
  information: CfbInformationEvent[];
  paperPositions: CfbPaperPosition[];
};
```

All provider calls and secrets remain server-side. The browser reads our database through server components/actions. The page may refresh its query every two minutes while visible, but a browser refresh does not trigger a paid provider request.

### 4.9 `/cfb` presentation requirements

Replace the static `GAMES` constant with server-provided initial data. Retain the current visual language and add:

- top-level `LIVE`, `STALE`, `PARTIAL`, `SAMPLE`, or `UNAVAILABLE` badge;
- “as of” timestamp and target refresh interval;
- independent feed-health tooltip for odds, schedule, weather, and information;
- exact-book ladder with both line and price;
- selectable spread/total/moneyline chart and book/consensus series;
- visible gaps where no observation exists—never interpolate silently;
- opener/current/close markers;
- source and publication timestamp on every information item;
- a clear “temporally associated” label on move/news pairings;
- empty states for no games, scheduled but uncaptured, one capture, stale, and provider failure;
- paper-ticket confirmation that repeats book, side, line, price, quote age, and units.

Do not show `LIVE` merely because the deployment is live. The status refers to the data.

### 4.10 Paper-trading rules

- Use a random browser owner token stored as an HTTP-only cookie; persist only its hash.
- The server action accepts a history row ID, book key, market, side, and stake; it reloads the exact line/price from `books` rather than trusting client-supplied quote values.
- Reject positions after kickoff or against a quote older than five minutes.
- Preserve the exact entry book and quote.
- Grade standard full-game markets using final score including overtime, subject to the recorded market's rules.
- Moneyline ties, spread pushes, total pushes, cancellations, and provider-declared voids settle `void` with zero units.
- Store both same-book CLV and consensus CLV when available.
- No real-money language such as “placed,” “wager confirmed,” or “balance.” Use “recorded paper position.”

### 4.11 Security, provenance, and rights

Required server-only secrets:

```text
ODDS_API_KEY
CFBD_API_KEY
SPORTSDATAIO_API_KEY  # optional, only if licensed
```

- Never send or log API keys.
- Validate all external payloads before writes.
- Escape source text and allow only `http`/`https` source URLs.
- Do not store or display scraped article bodies.
- Record provider, source URL, publication time, first-seen time, and checksum.
- Document the licensed uses of any paid injury/news feed before enabling it.
- Rate-limit paper-position writes and validate ownership on reads.

### 4.12 Observability and failure policy

Health checks must detect:

- upcoming game with no mapped odds event;
- mapped event with team or kickoff conflict;
- latest odds capture beyond its target freshness;
- any pregame-eligible quote captured at/after kickoff;
- incomplete paired market outcomes;
- unknown team alias;
- duplicate provider event mapping;
- schedule fetch success followed by unexplained zero games;
- paid odds request with zero consumed quotes;
- quota reserve breach or projected monthly overrun;
- final game without a score after 24 hours;
- open paper position not settled after a final score;
- information item with no provenance.

Provider failures degrade independently. For example, weather failure must not hide current odds. The page keeps the last good data, marks the affected feed stale, and shows the latest successful timestamp.

### 4.13 Testing

Unit tests:

- all FBS team aliases and ambiguous-name rejection;
- CFBD game upsert and reschedule behavior;
- UTC/Eastern conversion around daylight-saving boundaries;
- exact Odds API market normalization;
- spread-side sign preservation;
- missing moneyline and incomplete book handling;
- vig removal and consensus calculations;
- no invented quarter-point consensus;
- duplicate capture idempotency;
- provider-live and canonical-kickoff guards;
- opener/current/close selection;
- every alert threshold and duplicate scan;
- weather and information provenance;
- paper-position age, kickoff, push, tie, cancellation, and overtime behavior;
- score correction regrading without deleting audit history.

Integration fixture:

Create a deterministic three-game week with:

- six books including DraftKings and one approved reference book;
- four pregame captures and one rejected post-kickoff capture;
- a spread crossing 3;
- a price-only move at the same spread;
- a total reversal;
- a neutral-site game;
- an FBS-vs-FCS game;
- one TBD kickoff later resolved;
- one reschedule;
- one missing moneyline;
- one weather snapshot and one sourced availability event;
- one win, one push, and one cancellation.

The fixture proves:

```text
teams -> schedule -> provider mapping -> captures with exact-book JSONB
      -> charts/alerts -> close -> scores -> paper settlement -> page query
```

No automated test calls a live provider. Live probes are explicit manual or scheduled smoke tests with strict spend caps.

### 4.14 Rollout plan and gates

#### Phase 0 — Coverage and cost probe

- Query free event discovery for two representative CFB weeks.
- Run capped odds samples across the proposed book list.
- Measure FBS coverage, book/market completeness, timestamp quality, and exact credit burn.

**Gate:** At least 95% of scheduled FBS games map exactly; core spread/total coverage meets the product target; monthly spend fits the shared reserve.

#### Phase 1 — Canonical schedule

- Add teams, aliases, venues, matchups, provider mapping, and health telemetry.
- Render real schedules on `/cfb` while keeping market panels labeled sample.

**Gate:** A full week imports idempotently; TBD, neutral-site, FBS/FCS, and rescheduled cases pass.

#### Phase 2 — Shadow odds ledger

- Write `game_odds_history` with exact-book JSONB without changing the public terminal.
- Observe seven consecutive days.

**Gate:** No post-kickoff contamination, no silent zero-writes, acceptable freshness, and approved quota burn.

#### Phase 3 — Live terminal read path

- Replace static games with server queries.
- Enable ladder, consensus, charts, freshness, and partial states.

**Gate:** Production shows exact quotes that reconcile to stored provider fixtures and never reports fresh data when stale.

#### Phase 4 — Information and weather

- Add Open-Meteo snapshots and approved sourced events.
- Add temporal association UI.

**Gate:** Every item has provenance and independent freshness; no causal wording is generated automatically.

#### Phase 5 — Alerts and paper positions

- Enable observational alerts, immutable paper entries, closes, CLV, and settlement.

**Gate:** Fixture settlement is exact and one full live week produces no duplicate/unsettled records.

#### Phase 6 — Edge research

- Evaluate alert families prospectively by sample count, CLV, calibration, and out-of-sample results.
- Promote a signal to an “edge” only through a separately reviewed model specification.

**Gate:** Predeclared thresholds and sample sizes are met. Until then, the UI says “market signal,” not “value” or “edge.”

### 4.15 Acceptance criteria

The live-data project is complete when:

1. `/cfb` contains no production dependency on the static sample `GAMES` array.
2. Every displayed game maps to one canonical CFBD game.
3. Every displayed line can be traced to book, market, side, price, provider time, and capture time.
4. Odds history is append-only and idempotent.
5. No quote at or after kickoff is eligible for pregame current/close or alerts.
6. The UI accurately distinguishes live, stale, partial, sample, and unavailable data.
7. Charts and ladders derive from exact stored book quotes and preserve missing observations.
8. Source outages degrade independently and retain the last good timestamp.
9. Shared Odds API usage remains inside its approved reserve and every paid call records quota telemetry.
10. Information items have provenance and do not imply unverified causation.
11. Paper positions freeze an exact quote and settle reproducibly.
12. A seven-day shadow run and the deterministic integration fixture pass before public `LIVE` labeling.

### 4.16 Expected file changes

New:

```text
ingest/cfb_teams.py
ingest/cfb_schedule.py
ingest/cfb_odds.py
ingest/cfb_scores.py
ingest/cfb_weather.py
ingest/cfb_information.py
ingest/refresh_cfb_terminal.py
tests/test_cfb_teams.py
tests/test_cfb_schedule.py
tests/test_cfb_odds.py
tests/test_cfb_alerts.py
tests/test_cfb_terminal_integration.py
.github/workflows/refresh_cfb_terminal.yml
```

Modified:

```text
db/schema.py
db/queries.py
web/src/db/schema.ts
web/src/db/ensure-schema.ts
web/src/db/queries.ts
model/line_alerts.py
web/src/app/cfb/page.tsx
web/src/app/cfb/cfb-terminal-client.tsx
web/src/app/cfb/cfb-terminal.module.css
docs/the-odds-api.md
```

## 5. Provider references

- The Odds API v4 guide: <https://the-odds-api.com/liveapi/guides/v4/>
- CFBD getting started: <https://api.collegefootballdata.com/getting-started>
- CFBD games API: <https://apinext.collegefootballdata.com/api/games>
- Open-Meteo forecast API: <https://open-meteo.com/en/docs>
- Open-Meteo historical forecasts: <https://open-meteo.com/en/docs/historical-forecast-api>
- SportsDataIO NCAA Football workflow: <https://sportsdata.io/developers/workflow-guide/ncaa-football>

## 6. Final implementation note

The correct first engineering task is Phase 0, not UI wiring. The page already looks live; the remaining risk is whether the underlying observations are complete, affordable, timely, and auditable. Once those facts are measured, the implementation order above lets the current sample terminal become live without changing its successful visual design.
