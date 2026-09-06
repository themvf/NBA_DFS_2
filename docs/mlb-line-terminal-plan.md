# MLB line terminal conversion

Date: 2026-09-06. Status: isolated release branch prepared from origin/main at 75aec19; production not deployed. Production verification was read-only; no paid capture, signal insertion or settlement was executed against production during this work.

## Delivered implementation

- `/vegas?sport=mlb` now uses the MLB terminal. The former model/combined-signal screen remains at `/vegas/mlb-diagnostics?date=YYYY-MM-DD`.
- `queries-mlb-terminal.ts` reads canonical selected-date games, pregame history, verified closes and a 90-day signal audit. It retains completed games and zero/one-capture games. Mixed sportsbook/exchange captures are retained and their sportsbook quotes selected explicitly; dropping every capture containing Polymarket would discard valid MLB history.
- `mlb_terminal_quotes.py` adds away run-line prices, outcome lines, market timestamps and titles without changing legacy quote keys or scalar calculation formulas.
- `mlb_terminal_signals.py` adds prospective total/run-line steam, walking and reversal observations for both line changes and same-line price changes, plus moneyline reversal. Existing moneyline steam, walking, sharp divergence and DK-value detectors remain in place. Entries are first-breach-only and freeze book, line, price, support and capture evidence. New scans run after each saved MLB capture, including checkpoint calls; failures do not roll back odds. The regular MLB alert job retries the scan and settles new observations after existing prop work.
- Routine movement capture now uses a session lock, all-upcoming-game freshness check, quota reserve and actual usage audit. It selects ten provider keys, including Pinnacle **and Polymarket**, preserving the existing sharp/exchange signal inputs. Full-refresh and closing collector invocation patterns remain intact; checkpoint jobs do not share the new routine lock, so cross-worker timing still merits monitoring. Routine bridge/fallback calls share the lock; recent complete checkpoint captures can satisfy the routine freshness check.
- The terminal lists only sportsbook quotes. Capture-policy metadata separates new routine-book coverage from prior signal cohorts. No other sport's schedules, schema or UI were changed.
- No schema migration or notification channel is introduced. A separate MLB results-only workflow targets every 30 minutes, including late West Coast finishes; it calls free MLB scores and stored-quote settlement only. Existing broad refresh and closing-line APIs continue using their current signatures with optional additive metadata.

## Signal grading delivered

**Win/loss** grades the frozen selection at its actual entry handicap/total and price once the recorded status is `Final` or `Game Over`. **Push** means the final result exactly matches the entry line. The existing ledger stores it as `outcome='void'`, with `grading_json.settlement_reason='push'`; the new UI distinguishes it from a true void. **Void** covers postponed/cancelled/rescheduled games and a tied moneyline game. **Pending** covers unfinished, suspended, missing-score and shortened games requiring review. Historical settled entries are not rewritten.

Win rate is `W / (W + L)`; pushes and voids are excluded. Units use one unit at each frozen executable price, with pushes returning zero profit. Missing prices are excluded and the priced sample count is shown. This is observed research performance, not proof of an edge.

Verified CLV is a separate measure. Moneyline and same-line run-line/total price CLV use fair-probability percentage points at the frozen execution book; total-line CLV uses total movement in the chosen over/under direction, and handicap CLV uses the selected side’s entry handicap minus its closing handicap. These units are never averaged together. Missing, stale, pre-trigger or nonmatching closes are unavailable. Newly settled entries without a verified close are revisited so a late close can fill in CLV. Legacy game-line CLV appears only when its recorded grading metadata identifies the verified cohort; Polymarket comparisons are not passed off as verified sportsbook CLV.

Frozen `mlb-terminal-v1` detector specification: at least three matched retail books, fresh book timestamps within 35 minutes, total/handicap movement at least 0.5 runs, and same-line run-line/total probability movement at least 1.5pp. Steam requires adjacent observations at most 40 minutes apart. Walking requires at least three observations spanning 40 minutes to six hours and a monotone path. Reversal requires an initial move and retrace of the threshold. Moneyline steam/walking are not duplicated. Executable entries use the project's existing six-book allowlist. No model rating or recommendation cap changes.

## Validation and release boundary

Read-only database checks on September 6 returned 15 selected-date games. September 5 returned all 15 games, 373 captures and 15 verified closes, while truthfully displaying three missed T−15m checkpoints. This demonstrates that close availability and checkpoint success are different measurements. Validation passed: 160 focused Python tests (including CFB, NFL and tennis regression checks), five TypeScript assertion scripts, TypeScript checking and targeted ESLint. Browser checks covered market/side/book switching, historical quotes and 390px mobile layout in an isolated temporary preview, keeping the shared workspace's Next build directory untouched.

Deploying is a separate release step: this working tree contains pre-existing CFB, tennis and shared closing-line changes from other work. Do not publish or broadly commit the entire working tree to ship this feature. Integrate the narrow MLB changes with their existing shared-file prerequisites and run the listed checks on that release tree. New signal cohorts start accruing only when that code is running in the capture jobs; no historical signals are fabricated.

The remainder records the original design rationale and proposed later work. A higher-frequency cadence, a warm close worker, cross-worker checkpoint/routine lock unification and longitudinal monitoring remain future operational work, not changes silently enabled by this release.

## Recommendation

Convert `/vegas?sport=mlb` into a market terminal using CFB's dark, compact three-column layout and tennis's emphasis on a selected matchup. Reuse MLB's existing odds ledger, alerts, schedule identity, and verified closes. NFL is outside this work's scope.

The two reference pages are not identical: CFB uses a dark monospace terminal; tennis uses an editorial layout with serif headings and featured matches. Use CFB for the terminal structure, with tennis's clear separation of market observations and model research. Do not transplant tennis's headline or a football-specific spread interpretation.

## What already exists

| Area | Repository evidence | Implication |
|---|---|---|
| MLB screen | `web/src/app/vegas/mlb-vegas-client.tsx` | Movement table, sparklines, model comparisons, date navigation, alert audit and detector health already exist. Current white cards and gradient header differ from CFB. |
| Data loading | `web/src/app/vegas/mlb-vegas-content.tsx` | Seven existing data fetches; preserve their useful data while moving expensive historical audit fetches out of the frequent board refresh. |
| Exact quotes | `ingest/mlb_schedule.py::fetch_odds` | Appends bookmaker JSON to `game_odds_history`; moneyline and totals exist. Run-line storage currently preserves home line and `spread_price`, but lacks away line/price. |
| Identity and pregame integrity | `ingest/mlb_odds_policy.py` | Exact teams, provider identity, nearest start time, doubleheader ambiguity rejection, American-price validation and pregame guards already exist. |
| Movement data | `web/src/db/queries.ts::getLineMovement` and `getMlbLineMovement` | Current trail contains home probability only. Query requires future commence time and at least two captures, with a 40-game cap. It cannot supply a full historical selected-date terminal or a totals/run-line chart. |
| Signals | `model/line_alerts.py`, `web/src/lib/mlb-movement-*.ts` | Existing MLB steam, walking, sharp-book divergence and other observations. Client movement-shape badges and immutable detector alerts are distinct kinds of evidence. |
| Closing lines | `ingest/event_closing_lines.py`, `verified_clv_closes` | MLB already has durable T−6h, T−90m, T−15m and T−2m checkpoints, boundary verification and quality-graded closes. |
| CFB reference | `web/src/app/cfb/cfb-terminal-client.tsx`, `cfb-terminal.module.css` | Game watchlist, market selector, book table, chart, capture health, signal tape and research audit. |

These are source-code findings, not confirmation that the current production deployment, secrets, database coverage or scheduler executions are healthy. Several relevant files already have uncommitted work from other tasks.

## Proposed screen

- Header: MLB Line Terminal, Eastern date controls, team search, last successful observation and refresh state. Keep links to the existing separate props pages.
- Left: today's game watchlist, first-pitch time, doubleheader game identity, current market and open-to-current change. Filters for upcoming, started/final, all, and games with recorded signals.
- Center: selected matchup; **Moneyline / Run line / Total** tabs; home/away or over/under selection; opening, latest and verified close; time-scaled chart; exact-book quotes and source timestamps.
- Right: capture/checkpoint health, starter and lineup status where sourced, weather timestamp, and immutable signal tape. Context is not labeled as the cause of a move without timestamped evidence.
- Below: research audit and historical results, preserving model diagnostics and the existing model suppression rules in a secondary panel.
- Mobile: stack watchlist, selected game and health; keep market buttons and table scrolling accessible. No hover-only quote details.

Represent no quote, one capture, stale quote, missing market and unavailable close explicitly. One capture is useful as an opening observation but cannot demonstrate movement. A completed game shows its frozen pregame trail; it does not disappear or acquire a misleading live-freshness warning.

UI refresh should read stored data every 60 seconds while visible. It must not trigger paid odds ingestion. Distinguish page refresh time, capture time and bookmaker update time.

## Market and query contract

Add an MLB-specific query module, `web/src/db/queries-mlb-terminal.ts`, rather than changing the shared movement query used by other sports. Select canonical games by the requested Eastern game date, then left-join their pregame history. Retain games with zero or one observation and completed games. Bound the requested date and chart payload; avoid the shared query's top-40 movement ranking.

Return for each game: canonical matchup ID, MLB gamePk, provider event ID, scheduled start, status, teams, available starter context, opening/current bookmaker maps and timestamps, market trails, capture counts, and verified close reference/quality/lead/boundary. Join the verified-close view for a verified close; `closeProb` in the existing live movement type means latest observation, not a verified close.

Moneyline: calculate fair probability only from both sides at the same book and observation. Keep the plotted side fixed as prices cross even money. For movement comparisons use a stable eligible bookmaker intersection; expose support counts so book entry/exit cannot masquerade as movement. A consensus probability is not an executable sportsbook price.

Run line: show both the handicap and its price. A move from −1.5 at +120 to −1.5 at +100 is meaningful price movement even though the handicap is unchanged. Never compare prices at different handicaps as a same-line edge.

Total: show run total and over/under prices. Show 8.5 → 9.0 separately from over 8.5 −110 → −125. Only pair over and under for de-vigging when their lines match; retain each outcome's actual point when available.

Extend MLB quote parsing additively with `spread_home_price`, `spread_away`, `spread_away_price`, title and market-level update timestamps. Preserve legacy `spread_price` for existing consumers. Old missing away prices remain null; do not infer them. The CFB parser in `ingest/game_odds_market.py` offers a reference, but preserve MLB's Athletics aliases and stronger identity checks when adapting it. Do not silently rewrite historical JSON or existing scalar model inputs.

## Current configured cadence

| Job | Actual local configuration | Notes |
|---|---|---|
| MLB board refresh | 120 seconds while visible | Database read, not an odds purchase. |
| MLB movement bridge | `7,37 14-23,0-3 * * *` in `web/vercel.json` | 28 scheduled dispatch opportunities per UTC day; 10 AM–midnight Eastern during DST. Older comments still say 15 minutes. Fixed UTC means a different local window in winter. |
| Movement fallback | `12 14,20,2 * * *` | Three daily GitHub opportunities; separate from bridge dispatches. |
| MLB full refresh | 13:10, 17:10, 22:10 UTC | Schedule/odds/model pipeline; morning also runs historical catch-up. |
| Event-close bridge | Every minute | Due-work check before GitHub dispatch; worker supports MLB checkpoints. Runner startup/queue time can still miss a two-minute window. |
| CFB collector | Every 15 minutes | Free event mapping and due checkpoints; regular paid capture has a repository-variable gate defaulting false. |
| Tennis sportsbook | Every 3h during configured 2026 US Open window; otherwise 6h | Separate from closing checkpoints and settlement. Current UI's static six-hour wording is incomplete. |

The close-worker source defaults to 240 daily credits, while its older document says 120. Treat configuration and observed usage as authoritative; do not copy that document's number into the MLB UI.

## Proposed MLB capture policy

First release: retain the existing 30-minute broad movement cadence and the existing four closing checkpoints. Coordinate the writers and improve quote coverage before increasing frequency. Do not copy tennis's sparse cadence for MLB steam detection or CFB's 48-hour window into every baseball game.

1. Keep schedule/context refresh independent of odds purchases. Use MLB Stats API for schedule, starters, scores and first-play verification, retaining the current canonical gamePk mapping.
2. Before a movement purchase, check eligible upcoming MLB games, recent accepted captures, scheduler slot ownership and quota. Both bridge and fallback must use the same MLB database claim/lease; workflow concurrency alone does not deduplicate different workflows.
3. Batch eligible events into one `baseball_mlb` request. Persist actual quota headers for broad movement requests as well as checkpoint purchases.
4. Let an accepted capture satisfy both an eligible close checkpoint and a movement observation. A late checkpoint remains missed, rather than being relabeled successful. Rescheduled games invalidate the old scheduled-start checkpoint identity.
5. Prioritize final pregame checkpoints over routine broad refreshes if the MLB allocation is running low. Respect the shared account reserve without changing NFL's cadence or budget configuration.
6. Continue scanning existing MLB detectors after accepted captures. Any future market-specific detector or cadence-dependent threshold change gets a prospective version and separate audit; do not silently relabel historical alerts.

Potential later cadence: 60 minutes far from first pitch, 30 minutes within six hours, and 15 minutes within 90 minutes, plus close checkpoints. This is a separate measured rollout, not part of the initial recommendation. Evaluate actual gaps and quota first. A GitHub-dispatched T−2m job is best effort; improving close precision requires measuring queue/startup delay and, if necessary, a warm worker with the same durable lease.

## API calls and estimated cost

Use the existing endpoint:

```text
GET https://api.the-odds-api.com/v4/sports/baseball_mlb/odds
  ?markets=h2h,spreads,totals
  &bookmakers=<validated MLB bookmaker keys, at most 10>
  &eventIds=<eligible provider event IDs>
  &oddsFormat=american
  &dateFormat=iso
```

Keep the API key server-side. Choose the book list from a coverage probe, including Pinnacle when available and useful retail books. Validate provider keys; the existing collectors do not all use identical keys. Preserve sportsbook and exchange lanes separately. Do not restart the paused player-prop collector as part of this conversion.

The provider documents billing by markets × region-equivalent groups, with up to ten explicit bookmakers counting as one region. Three markets therefore cost approximately **3 credits per populated request**, compared with the broad MLB writer's current three-region request at **9**. `eventIds` filters events; it does not itself lower the per-request market/region multiplier. Source: [The Odds API v4 documentation](https://the-odds-api.com/liveapi/guides/v4/index.html).

At all 28 current bridge opportunities: 252 → 84 credits/day, or 7,560 → 2,520 over 30 days, for the bridge's broad captures alone. These are arithmetic scenarios, not measured consumption. They exclude full refreshes, fallback runs, checkpoint requests, retries and other sports; no-upcoming-game guards can reduce actual spend. Reducing the book universe changes the consensus population, so record the capture-policy version and compare coverage before switching.

No need for a new paid data provider to build the game-line terminal. Preserve source capture time and outcome prices, and report actual `x-requests-last`, `x-requests-used`, `x-requests-remaining` rather than presenting estimates as account usage.

## Implementation order and file boundaries

1. **MLB data contract:** new `queries-mlb-terminal.ts`, MLB-specific types/transforms and regression tests for date history and three markets. Add quote fields narrowly in `ingest/mlb_schedule.py` after reconciling its existing uncommitted changes.
2. **MLB terminal:** new `web/src/app/vegas/mlb-terminal-client.tsx` and `mlb-terminal.module.css`; wire through `mlb-vegas-content.tsx`. Use CFB styling as a reference without editing its in-progress files. Keep existing model helpers and props routes.
3. **Capture coordination:** MLB-specific policy/lease helper, explicit-book support for broad captures and usage accounting. Reconcile existing MLB and shared close work rather than installing another independent scheduler.
4. **Verification:** fixture-backed UI and query tests, focused existing MLB regressions, then a read-only production coverage/usage audit. After release, measure seven days of actual capture gaps, checkpoint coverage, book availability and credits.

Do not edit NFL files or its branches in shared modules. Shared files already under active work (`db/queries.py`, `db/schema.py`, web DB files, `model/line_alerts.py`, `ingest/event_closing_lines.py`, `web/vercel.json`) require narrow integration against their latest contents, not a broad refactor. This plan does not require changing the shared detector engine for initial UI delivery.

## Acceptance checks

- Selected-date games remain visible after first pitch and across Eastern midnight; doubleheaders never share trails.
- Zero/one capture, missing book, invalid price, missing market, provider outage and stale quote states are honest.
- Moneyline crosses even money correctly; matched-book support is used for movement; total/run-line point changes remain distinct from price changes.
- Legacy home spread prices remain readable; missing historical away prices are not invented.
- Verified close remains immutable, carries its boundary and quality, and excludes post-start data. Existing model suppression and research limits remain intact.
- Page reload/navigation causes no paid API calls. Two overlapping MLB jobs cannot purchase the same claimed routine slot. No due work causes no purchase.
- Failed HTTP calls and retries preserve usage evidence when supplied, never expose credentials, and do not mark failed captures complete.
- Targeted checks: MLB exact-book quotes, odds policy/freshness, refresh pipeline, movement snapshot/signals, line-alert model context; new three-market adapter and history-date tests; TypeScript/lint; desktop/mobile visual checks with deterministic fixtures.
- NFL files and behavior remain untouched.

## Isolated release verification

Release branch: `codex/mlb-line-terminal-release`, based on `75aec19`. Closing-line schema and optional fetch arguments are already committed upstream. Only MLB files and four MLB-only additions to `model/line_alerts.py` are included; newer NFL, CFB and tennis code is preserved. The results-only worker uses `initialize_schema=False` to avoid recurring global schema DDL.

Release checks: 144 focused Python tests passed, TypeScript checking, targeted ESLint and all five MLB TypeScript scripts passed. The additional tennis walking suite has 17 passes and two existing failures (`KeyError: market` in its mock observation rows); both failures were reproduced with the unchanged upstream `model/line_alerts.py`. No tennis fix is included. Earlier desktop/mobile verification is described above.
