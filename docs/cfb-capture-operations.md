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

Run Python tests for CFB market signals, schedule, and event closing lines;
`node --import tsx scripts/test-cfb-movement.ts` from `web`; TypeScript checking;
and the production build. After publication, dispatch the shared capture worker
and CFB refresher, then inspect workflow conclusions, usage headers, actual new
history timestamps, checkpoint statuses, and final-score settlement counts.
