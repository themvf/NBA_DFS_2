# NFL Event-Relative Odds Capture Cadence

**Version:** `nfl-capture-v1`  
**Effective:** 2026-09-02  
**Status:** Prospective market-data collection; no betting-edge claim

## Objective

Collect enough pregame NFL spread, total, and moneyline observations to study steam, walking, reversals, key-number movement, price-only movement, and closing-line value without continuous paid polling.

The system separates three facts:

1. A scheduler run occurred.
2. A paid provider request occurred because at least one event was due.
3. A valid pregame history row was accepted for a particular game and checkpoint.

Only the third fact satisfies a checkpoint.

## Frozen cadence

Calendar-day slots use `America/New_York`, including daylight-saving transitions. Event-relative slots use the stored UTC kickoff.

| Period | Targets | Acceptance window |
|---|---|---|
| Three calendar days before kickoff | 00:00, 06:00, 12:00, 18:00 ET | Target through target +20 minutes |
| Two calendar days before kickoff | 00:00, 06:00, 12:00, 18:00 ET | Target through target +20 minutes |
| One calendar day before kickoff | Every 3 hours, 00:00 through 21:00 ET | Target through target +20 minutes |
| Kickoff calendar day | Hourly from 00:00 ET through the last whole-hour target at least 60 minutes before kickoff | Target through target +20 minutes |
| Final hour | T−30 minutes | T−30 through T−20 |
| Final hour | T−15 minutes | T−15 through T−5 |
| Closing candidate | T−5 minutes | T−5 through scheduled kickoff |
| Closing boundary | Scheduled kickoff | Freeze the latest eligible pre-kickoff row; do not make or accept an in-play capture |

Example: a Sunday 1:00 PM ET kickoff receives 4 D−3 slots, 4 D−2 slots, 8 D−1 slots, 13 game-day hourly slots (midnight through noon), and 3 final-hour slots: 32 checkpoint observations when every window is satisfied.

## Execution model

- GitHub Actions invokes the worker every five minutes. A run with no due checkpoint exits without an Odds API odds request.
- Each run refreshes the provider's free NFL event list before seeding jobs, so future games are mapped in time for D−3 collection. Event discovery does not count as a market capture.
- If the provider changes a kickoff, `nfl_matchups.commence_time` is updated, checkpoints for the former kickoff are retained as `missed: superseded by kickoff reschedule`, and a complete new cadence is seeded from the revised time. Superseded jobs cannot trigger paid calls.
- The former half-hour `capture_nfl_odds.yml` schedule is disabled to prevent duplicate paid captures; that workflow remains available as a manual recovery path.
- Durable rows in `odds_capture_checkpoints` make retries idempotent. Existing accepted `game_odds_history` rows reconcile checkpoints before a new request is considered.
- Interrupted attempts remain retryable inside their original window. Provider failures record their reason and quota headers when available; an event-discovery outage does not prevent already-mapped games from capturing or freezing a close.
- All due NFL games of the same provider season type are submitted in one targeted bulk odds call. Regular season and preseason are separate provider sport keys and therefore separate calls when both are due.
- The request uses no more than ten named books and three markets (`h2h,spreads,totals`), normally three Odds API credits per provider call.
- Every provider call is written to `odds_api_usage`, including requested event IDs, season type, markets, books, response status, and quota headers.
- Every accepted event is appended to `game_odds_history`; the latest-cache fields on `nfl_matchups` are derived convenience values.
- Provider event ID, team identity, stored kickoff, provider kickoff, and pregame timing must all pass before a row is accepted.

## Closing-line contract

At scheduled commencement the worker freezes the latest eligible history row whose capture timestamp and bookmaker update timestamp are both before the boundary. The frozen record stores the history ID, scheduled boundary, capture lead, quality grade, methodology version, and verification limitation.

NFL v1 uses a scheduled boundary because no official play-level kickoff timestamp is integrated. It is labeled `scheduled_nfl`, not represented as an actual-start close. Close quality remains:

- A: at most 5 minutes before the boundary
- B: more than 5 and at most 15 minutes
- C: more than 15 and at most 30 minutes
- stale: more than 30 minutes

Only A/B/C rows enter the verified CLV cohort. A missing closing candidate may fall back to T−15 or T−30 with the resulting lower quality; it is never silently presented as an exact close.

## Audit and backtest requirements

- Detector outputs must point to immutable source history rows.
- Settlement must use `verified_clv_closes`, official final scores, and the frozen entry quote.
- Backtests must isolate detector and capture-methodology versions and use only prospectively generated signals for promotion decisions.
- Report capture completion by checkpoint, missed windows, quota deferrals, book support, average line CLV, beat-close rate, outcomes, and one-unit P&L.
- Multiple observations from one game are correlated. Confidence intervals must cluster by game or game date.
- Any cadence or threshold change creates a new version; historical rows are not rewritten.
- Apparent ROI is descriptive until a time-ordered holdout and a second untouched confirmation window both support positive CLV after execution-cost sensitivity.

## Operational limits

GitHub scheduled jobs can start late. Twenty-minute standard windows and durable reconciliation tolerate ordinary delay; the final five-minute window remains best-effort. The system exposes missed checkpoints rather than fabricating observations.

The daily credit cap and monthly reserve can defer due work. Deferrals are stored as failures with an explicit quota reason and remain auditable. Before adapting this cadence to CFB, measure NFL capture completion, provider cost, and the marginal signal value of each time bucket.
