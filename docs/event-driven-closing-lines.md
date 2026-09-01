# Event-Driven Closing Lines — MLB, Tennis, and CFB

**Status:** Implemented foundation on 2026-08-31
**Scope:** MLB, Tennis, and CFB game markets from The Odds API

## Verified CLV cohort boundary

The first primary cohort is `verified_clv_v1`, produced by methodology `event-close-v1`.

Its immutable start is **2026-08-31 00:00 America/New_York** (`2026-08-31T04:00:00Z`). A close is primary-CLV eligible only when all of these are true:

- `scheduled_start_at >= 2026-08-31T04:00:00Z`;
- methodology is `event-close-v1`;
- quality is A, B, or C;
- the capture precedes the recorded boundary;
- `primary_clv_eligible = TRUE` and `clv_cohort = 'verified_clv_v1'`.

The database view `verified_clv_closes` enforces the rule. Primary reports use that view by default. Stale and legacy observations require an explicit reporting opt-in and must never be pooled with the primary cohort.

`verification_level` distinguishes MLB's `actual_start` boundary from Tennis and CFB `scheduled_boundary` closes. All can enter the capture-quality cohort, but reports must preserve that distinction; Tennis is not represented as verified first serve and CFB is not represented as a verified first snap.

## Decision

Keep the existing pregame snapshots used to detect steam, walking, and sharp-book divergence. Add a separate event-driven layer that targets each event's start and freezes exactly one auditable closing observation.

“Closing line” means the final valid sportsbook observation whose capture time is before the event boundary. It does not mean an official close supplied by The Odds API.

## Capture schedule

MLB and Tennis receive four durable checkpoints:

| Checkpoint | Purpose | Due tolerance |
|---|---|---|
| T−6h | Early movement baseline | Through T−5h30m |
| T−90m | Lineup/session information | Through T−60m |
| T−15m | Reliable close fallback | Through T−5m |
| T−2m | Primary close candidate | Until the scheduled start |

The worker polls frequently but makes no paid request unless a checkpoint is due. Due events are batched by sport/provider key and sent with `eventIds`. MLB events sharing a date are one request; Tennis events sharing a tournament key are one request. The close request uses `h2h,totals,spreads` and an explicit group of at most ten useful books, so it is billed as one bookmaker region rather than three geographic regions.

The ordinary movement collectors continue independently. A snapshot may satisfy both movement analysis and a close checkpoint.

CFB adds T−48h and T−24h to the four checkpoints above. This captures the
early weekly market before the same late sequence (T−6h, T−90m, T−15m, T−2m)
and lets the terminal distinguish early movement from game-day movement.

## Start boundaries

### MLB

The scheduled start comes from MLB Stats API. After the event begins, the worker verifies the boundary from the official live feed in this order:

1. `gameData.datetime.firstPitch`;
2. the first play's `about.startTime`;
3. no freeze yet when the game remains in preview/delay and neither timestamp exists.

The Odds API event time and the internal MLB schedule time must both be future times when a capture is accepted. This prevents a delayed or in-progress feed from being stored as pregame.

### Tennis

The Odds API tournament feed supplies the scheduled start. No currently integrated source provides a sufficiently reliable first-completed-point timestamp, so Tennis closes use `scheduled_provider` as the boundary source. This limitation is visible in the close record and must not be represented as verified first serve. A future point-level adapter can replace this boundary prospectively without rewriting historical closes.

### CFB

CFBD supplies the canonical scheduled kickoff. No play-level kickoff source is
currently integrated, so CFB freezes at `scheduled_cfbd` and records the
limitation in verification evidence. This is a valid scheduled-boundary close,
not a claim that the first snap occurred at that exact instant.

## Immutable close and quality

Once inserted, `(sport, matchup_id)` is never overwritten. The frozen row points to the exact append-only `game_odds_history` record and stores the boundary used, capture lead, verification evidence, and quality:

| Quality | Final capture lead |
|---|---|
| A | 0–5 minutes |
| B | >5–15 minutes |
| C | >15–30 minutes |
| stale | More than 30 minutes |

An observation at or after the boundary is never eligible. Events with no valid observation remain missing rather than manufacturing a close.

## Cost and quota controls

- The worker uses `eventIds` to avoid buying an entire sport slate when only a few events are due.
- `bookmakers` replaces `regions`; up to ten selected books are one billing group.
- Every paid request records `x-requests-last`, `x-requests-used`, and `x-requests-remaining` in `odds_api_usage`.
- Closing captures stop at `ODDS_CLOSE_DAILY_CREDIT_CAP` (default 240 credits/day) and preserve `ODDS_CLOSE_MIN_REMAINING` (default 250 monthly credits). Both are deployment-configurable.
- The T−2m request is skipped when a valid observation already exists inside its checkpoint window.
- Historical Odds API repair is not automatic. It costs ten times the normal multiplier and requires a separate, explicit repair command and budget.

## Operational cadence

`capture_event_closes.yml` runs the worker. Vercel supplies the reliable frequent clock through `/api/cron/event-closing-lines`; GitHub's native schedule is only a fallback. The workflow is intentionally separate from the broad MLB movement collector so frequent polling cannot multiply full-slate calls.

## Measurement and health

Daily health reporting should include:

- eligible events, frozen closes, and missing closes;
- A/B/C/stale distribution by sport;
- median and p90 capture lead;
- due checkpoints that became missed or failed;
- API credits consumed by sport and purpose;
- Tennis and CFB rows using scheduled rather than verified-live boundaries.

CLV reports join to `verified_clv_closes.history_id` by default. Legacy “last observed” and non-primary frozen CLV remain available only through an explicit inclusion flag, are labeled separately, and are never mixed silently into the verified cohort.

## Acceptance criteria

- Existing movement snapshots and detectors continue to operate.
- No paid request occurs when no checkpoint is due.
- Multiple due events are sent in one batched request where the provider permits it.
- Captures at or after the event boundary cannot become a close.
- A frozen close cannot be replaced by a later run.
- Every close has a quality grade and explicit boundary source.
- MLB live events require official first-pitch/first-play verification before freezing.
- Tennis scheduled-boundary limitations are queryable and disclosed.
- API usage headers are persisted for cost accounting.
