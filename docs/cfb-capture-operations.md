# CFB movement capture — September 3, 2026

## Prospective measurements

The shared `capture_event_closes.yml` worker owns paid captures. It polls every
five minutes; Vercel can also dispatch the same serialized workflow. Due games
are batched into one CFB request for spreads, totals, and moneylines. The CFB
terminal refresher does not make duplicate paid calls unless explicitly forced.

`cfb-dense-v1` adds hourly targets from T-12h through T-7h and 15-minute targets
through the last six hours. Existing T-48h, T-24h, T-6h, T-90m, T-15m, and T-2m
checkpoints remain; a T-5m closing candidate is added. Overlapping windows can
share one accepted observation. These are targets, not guaranteed delivery:
GitHub scheduling delays, provider outages, missing mappings, and quota guards
can still cause misses. No synthetic or retrospective snapshots are inserted.

Repository variables explicitly configure the upgraded allowance:

- `ODDS_CLOSE_DAILY_CREDIT_CAP=2000`: all sports in this capture worker, ET day.
- `ODDS_CLOSE_MIN_REMAINING=5000`: stop paid captures before consuming the reserve.

Code defaults remain conservative (240 / 250) for unconfigured environments.
The observed account allowance was 100,000 credits (97,784 remaining). This
worker's daily ceiling is not the account-wide spending cap: other odds/props
jobs also consume the account. `odds_api_usage` records response usage headers,
CFB cadence version, configured daily cap, and event IDs. Checkpoints record
accepted history IDs or failure reasons. Review actual cadence and quota after
the next full slate; raising the cap alone cannot solve a delayed scheduler.

## Results and audit

Full/manual refreshes synchronize official CFBD school, alternate-name, and
mascot aliases by CFBD team ID. Cross-team name collisions are excluded and
existing reviewed mappings are not overwritten. Accents and punctuation are
normalized; fuzzy team matching is not allowed. Provider events must still match
both teams and the canonical kickoff window before any odds are accepted.
Three remaining provider variants have explicit, source-reviewed mappings in
code: [Citadel Bulldogs](https://citadelsports.com/),
[Nicholls State Colonels](https://geauxcolonels.com/), and
[Southeastern Louisiana Lions](https://lionsports.net/sports/2008/11/7/GEN_1107082533.aspx).
Conflicts fail closed rather than replacing another team's identity.

Full canonical schedule refreshes run every six hours. Recent game weeks refresh
hourly, keyed to the scheduled workflow expression rather than the runner's
actual start minute. Each response is written in one transaction rather than
opening separate connections for each row. Final scores are required for spread/total result grading.
Final W/L/push and price-based hypothetical units settle independently of CLV.
Missing verified closes remain null, not zero or the latest available line;
later verified closes may enrich the existing settlement with a new audit grade.

## At-a-glance board

Every game shows time-scaled home-spread and total consensus sparklines, with
dashed segments for observation gaps over 30 minutes. Each chart is independently
scaled; changing book coverage can change consensus. One observation is a dot,
not evidence of no movement. Missing history and stale captures are labeled.

Steam, walk, and reversal badges use persisted detector records, never chart
shape inference. They show market, side, and observation time, with filters for
each movement family. All signals for the displayed game's IDs are loaded, so a
global recent-alert limit cannot silently hide old or busy-slate signals. These
are research observations, not established predictive edges or live bet advice.

## Verification

### 2026 historical pilot (missing games only)

`python -m ingest.cfb_historical_replay` prints a dry-run credit estimate.
`--execute` downloads the plan with a default cumulative 7,500-credit cap.
Only 2026 games already started, with known kickoff and no live history, are
eligible. 2025 is deliberately excluded. Sampling is every five minutes during
the final six hours, two reference snapshots at 24/48 hours, and a request one
second before kickoff (the provider returns its latest available earlier snapshot).
This does not reconstruct the full path from true market open.

Raw timestamped responses are cached in `cfb_historical_archive`; replay outputs
are stored in `cfb_historical_replays`. Neither table changes live captures,
first-breach alerts, immutable closing records, or dashboard badges. Historical
origin is explicit, and reconstructed trigger timestamps are not detection times.
Event matching requires both canonical team identities and kickoff within five
minutes; ambiguous identity or invalid quote evidence aborts processing.

Replay uses the live first-breach rules on successive prefixes. W/L/push and
hypothetical units use the actual recorded selection line/price and completed
game scores. CLV is signed line-point improvement against the same execution
book, not vig-adjusted price CLV: it requires a snapshot within ten minutes of
kickoff and a book update within fifteen minutes. Missing evidence yields null.
The closing observation is a near-close proxy, not a guaranteed exact final tick.
Signals from the same game overlap; they are not independent bets or validation
of an edge. Archived quote transitions remain available for repeated-movement
analysis even though the graded signal cohort retains first-breach semantics.

No automatic retry occurs after a transport failure because charges may be
uncertain. Responses are cached as they arrive and quota usage is reported to
the shared audit table. Concurrent invocations of this one-off importer should
not be run; use one process and resume from its cache.
Failed/interrupted attempts reserve 30 credits conservatively against the cap.
Duplicate event rows may be collapsed only when identities and normalized quote
values agree; the older bookmaker timestamp is retained. Conflicting evidence
still aborts. Pilot findings are in `cfb-2026-historical-pilot-results.md`.

### Every observed quote movement

CFB grading first reconciles `cfb_quote_movements` from the append-only pregame
history. This separate ledger records every changed field at every book for
spread lines/prices, totals/over/under prices, and home/away moneylines. There is
no minimum move, time-window cutoff, or first-breach deduplication: repeated moves
and slow/full retracements remain separate observations. Availability changes
are explicitly `appeared`/`disappeared`, not inferred directional price moves.
The first snapshot is the baseline; timestamp-only updates are not price moves.

Each row links both source snapshots. Their capture timestamps bound when we
observed the change; book update timestamps remain available in source JSON.
Processing time is stored separately. Replay fills transitions from already saved
history without claiming retroactive prospective signals or inventing missing
quotes. Every run reconciles the ledger exactly against the source transitions
and fails on unsupported/mismatched rows. Repeated runs cannot duplicate rows.
Run independently with `python -m ingest.cfb_movements`.

This adds no paid API requests and does not change polling cadence. Changes
entirely between polls, unsupported markets, and in-play changes are not covered.
Existing chart snapshots and first-breach signal badges are unchanged; the new
ledger is the research data foundation, not a new dashboard movement feed.

The shared worker now runs `python -m ingest.cfb_capture_audit` after CFB
grading. The audit uses a read-only consistent database snapshot and fails on
identity, boundary, duplicate, quote, checkpoint, close, trigger, or settlement
integrity errors. It separately reports missing windows and replays stored
history to identify missing/late first-breach detections; it never backdates a
prospective alert. Passing integrity is not a claim of complete coverage.

Capture and football grading CLIs reuse a database connection while retaining
individual transaction commits. A later detector failure cannot roll back saved
captures. CFB kickoff changes supersede obsolete pending checkpoint windows.

Run Python tests for CFB market signals, schedule, and event closing lines;
`node --import tsx scripts/test-cfb-movement.ts` from `web`; TypeScript checking;
and the production build. After publication, dispatch the shared capture worker
and CFB refresher, then inspect workflow conclusions, usage headers, actual new
history timestamps, checkpoint statuses, and final-score settlement counts.
