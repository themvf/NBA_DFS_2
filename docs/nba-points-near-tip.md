# NBA game-specific near-tip points collection

Added October 28, 2026 after the owner explicitly requested immediate closing-line collection. This authorizes the additional bounded Odds API requests below; it does not authorize website deployment, model changes, historical paid backfills or unrelated captures.

## Evidence definition

The configured Odds API endpoint returns current quotes, not certified sportsbook closing records. Capture **T-5 minutes** (window T-5 to T-2) and **T-1 minute** (window T-1 to T-5 seconds) separately for every game. Preserve the exact capture and provider update timestamps, per-book prices and lines, raw responses and any missing/failing attempts. These are `near_tip_not_verified_close` observations. Never label them actual or verified closing lines. The latest successful archived quote automatically enters the existing daily per-game benchmark; morning and contest-lock benchmarks stay separate and unchanged.

Match each provider event to exactly one free ESPN scoreboard game using both canonical teams and the Eastern date. Require an official scheduled/pregame status. Use the earlier official scheduled start or market start; reject a response arriving at/after that cutoff, changed game identities, a payload whose own start has passed, or future provider timestamps. Keep rejected raw responses for audit, outside the eligible history. Missing provider update times remain visible in daily benchmarks.

## Budget and failure handling

Request only `player_points`, six configured books: estimated one account credit per attempt. At most two attempts per event/day (24 credits for October 28's twelve games), with an explicit 40-credit daily reservation ceiling. This is provider credit usage, not a dollar estimate. Existing morning captures are separate and do not count toward this worker's cap. Every call is reserved to a local durable state file BEFORE dispatch; restarts, timeouts, empty responses and HTTP failures do not trigger repeated paid requests. Account usage is also appended to `odds_api_usage` with purpose `nba_points_near_tip`. This deliberately favors bounded spend over automatic retries. Empty, failed, rejected and missed captures remain explicit; budget exhaustion blocks remaining attempts. Global subscription balance is still shared with other collectors.

No new database schema is required. Accepted lines append to `prop_odds_history` using existing ingestion. Original research forecasts are never updated. OS process locking prevents concurrent local instances; a restart reads the existing per-day ledger. Do not delete/reset that ledger or run copies using different state roots.

## Run and operations

```
C:\Python313\python.exe -m ingest.nba_points_near_tip --state-dir "C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\nba-near-tip-lines"
C:\Python313\python.exe -m ingest.nba_points_near_tip --state-dir "C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\nba-near-tip-lines" --apply --watch --max-credits 40
```

Without `--apply`, only free schedule/event discovery runs and a plan is written; no paid calls or database writes. `--watch` polls every five minutes when far from tip, and every twenty seconds inside ten minutes. Serial requests and network latency can miss a narrow window; that is recorded rather than backdated. The worker exits shortly after the last official scheduled start, with a 5 AM Eastern hard stop. A local Windows Task Scheduler task can start it daily and at user logon; it is independent of the 11:35 research automation. This requires the PC to be on, awake, online and signed in. A local task is not a deployed cloud collector. Wake-to-run can wake a sleeping PC at the daily trigger, but does not guarantee it stays awake during the evening. Inspect `state.json`, `latest-plan.json` and `last-error.json` under each Eastern date. A running process and successful dry run establish readiness, not that tonight's near-tip observations have already been captured.

Provider documentation: https://the-odds-api.com/liveapi/guides/v4/ and https://the-odds-api.com/sports-odds-data/update-intervals.html . Source update cadence limits the freshness of a quote even if our capture occurs seconds before tip.
