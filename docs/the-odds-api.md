# The Odds API — cost model, budget, and every consumer

Single source of truth for how this project spends its Odds API quota, what
each call actually costs, and which spends have been proven worthless and
removed. Written 2026-08-24, after the shared key hit its ceiling and took
MLB, NFL and tennis ingestion down simultaneously.

**Standing rule: a call that returns nothing we consume is a bug, not a cost
of doing business.** Every reduction recorded here was measured against
stored data (`game_odds_history.books`, `player_prop_history`,
`mlb_matchups`) before it was made. Do not reverse one on a symmetry
argument — re-measure first.

---

## 0. The quota is 100,000/month, not 20,000 — MEASURED 2026-09-02

Every budget figure written below on 2026-08-24 assumes a **20,000/month**
plan. That is wrong now. Read live from the provider's own response
headers during the NFL prop probe (GitHub Actions run 33628093963):

```
x-requests-used=749   x-requests-remaining=99251     (sum = 100,000)
```

Recorded as a measurement, not a plan change anyone announced — the
account may have been upgraded, so **confirm the tier before relying on
the headroom**. Two things follow either way:

- The percentages in section 2 are off by 5x. The ~395/day post-reduction
  burn is **~12% of a 100,000 key**, not ~59%.
- The 2026-08-24 cadence cuts were made against the wrong denominator.
  That does **not** automatically justify reversing them: several were
  removed for producing nothing we consume (`us_ex` for NFL and tennis at
  0% Polymarket coverage), and the standing rule at the top of this file
  applies — a call that returns nothing is a bug regardless of how much
  quota is spare. Reversals still need fresh measurement, not just room.

The cost-per-call model in section 1 is unaffected and was re-confirmed by
the same probe: 7 markets x 10 books billed exactly 7 credits/event.

---

## 1. Pricing model

| Endpoint | Cost |
|---|---|
| `/v4/sports` (discovery) | **free** |
| `/v4/sports/{key}/events` | **free** |
| `/v4/sports/{key}/odds` | `markets × regions` |
| `/v4/sports/{key}/events/{id}/odds` with `regions=` | `markets × regions` |
| `/v4/sports/{key}/events/{id}/odds` with `bookmakers=` (≤10 books) | `markets × 1` |
| `/v4/sports/{key}/scores` with `daysFrom` | 2 |
| Historical (`/v4/historical/...`) | **10× multiplier** |

Two consequences worth internalising:

- **`bookmakers=` is dramatically cheaper than `regions=`** for per-event
  calls: 10 books cost the same as 1. The 11th book doubles the bill.
  `ingest/mlb_prop_odds.py` is built around this and asserts `<= 10`.
- **Adding a region multiplies the entire call**, so an unproductive region
  is not a rounding error — it is a fixed percentage of every request.

### Quota exhaustion looks like an auth error

At zero credits the API returns **`401 Unauthorized`**, not `429`:

```json
{"message":"Usage quota has been reached.","error_code":"OUT_OF_USAGE_CREDITS"}
```

Verified 2026-08-24: a 401 in this state reports `x-requests-last: 0` — a
rejected call costs nothing. Retrying is futile but not expensive.

The dangerous part is that `/v4/sports` and `/events` are **free and keep
working**, so discovery, tournament lists and event lists all look healthy
while every priced call fails. That is exactly how the 2026-08-24 outage was
first misdiagnosed as "the provider has no tennis coverage."

**Every response carries `x-requests-remaining` and `x-requests-used`.**
Read them. Ignoring them is how the key silently reached 20,000/20,000.

---

## 2. Budget

Plan: **100,000 credits/month ≈ 3,333/day sustainable.**

**Corrected 2026-09-01.** This section previously said 20,000/month, which was
true when the 2026-08-24 emergency cuts were made and is no longer true — the
live response headers now read `x-requests-remaining: 99,800` against
`x-requests-used: 186`, i.e. a 100,000 key. Read the headers rather than this
line: the plan can change under us in either direction, and the 2026-08-24
outage happened because nobody was reading them.

**This does not reopen the 2026-08-24 removals.** Every one of those was made
on a *measured* basis — `us_ex` returned Polymarket on 0 of 1,685 NFL captures,
`batter_runs_scored` never matched Pinnacle's line, and so on. Those spends
were removed because they bought nothing, not because the budget was tight.
More headroom is not a reason to restore a consumer that was measured to be
useless; it is only a reason not to refuse a *new* consumer on cost grounds
alone.

| Date | Scheduled burn | Status |
|---|---|---|
| Before 2026-08-24 | ~955/day (~28,650/30d) | Exhausted the then-20,000 plan mid-cycle |
| After the 2026-08-24 work | ~395/day (~11,850/30d) | ~12% of the current plan |
| Plus NFL survivor (2026-09-01) | +~1.7/day (6 credits, 2x/week) | negligible |

---

## 3. Every consumer

### Scheduled

| Consumer | Module | Params | Credits/call | Cadence | Credits/day |
|---|---|---|---|---|---|
| MLB game lines (history) | `ingest.mlb_schedule` | 3 regions × 3 markets | 9 | Vercel 30-min (28/day) + GH 6-hourly (3/day) | ~279 |
| MLB game lines (vegas refresh) | `refresh_mlb_vegas` | 3 × 3 | 9 | 3×/day, **deduped** | ~9 |
| Tennis | `ingest.tennis_schedule` | 3 regions × 3 markets | 9/tournament | 3h in US Open window, else 6h | ~72 |
| NFL game lines | `ingest.nfl_schedule` | 2 regions × 3 markets × 2 keys | 12 | 1×/day | 12 |
| NFL scores | `ingest.nfl_schedule` | `daysFrom=3` × 2 keys | 4 | 1×/day | 4 |
| CFB game lines | `ingest.event_closing_lines` | 10 named books × 3 markets | expected 3/bulk call | T−48h, T−24h, T−6h, T−90m, T−15m, T−2m/game cluster | quota guarded |
| MLB props | `ingest.mlb_prop_odds` | 4 markets × 1 (10 books) | 4/event | **PAUSED 2026-08-24** | 0 |
| Soccer | `refresh_soccer.yml` | — | — | **disabled 2026-08-01** | 0 |

### On-demand (user-triggered — no cadence control applies)

| Consumer | Params | Cost |
|---|---|---|
| MLB "Fetch Player Props" | 8 markets × 1 region | 8/event (~80/click on a 10-game slate) |
| NBA "Fetch Player Props" | 5 markets × 1 region | 5/event |
| `auditMlbPropCoverage` / `auditNbaPropCoverage` | 8 / 5 markets | per caller-supplied `gameKeys` |
| NBA game lines | 1 region × 3 markets | 3 (manual only — no workflow) |

Both prop buttons are deduped (10-minute window via
`odds_api_prop_fetch_log`) and report live quota in their result message.

### Free — do not "optimise" these, they cost nothing

Tennis tournament discovery (`/v4/sports`); tennis settlement (15-minute
cadence — tennisexplorer / tennis-data.co.uk / TheSportsDB); MLB scores
(`statsapi.mlb.com`); **all Polymarket capture**
(`ingest/polymarket_tennis.py`, via the Gamma API); every detector and
report that reads stored rows (`model.line_alerts`,
`model.mlb_prop_program`).

---

## 4. Decisions on record

### `us_ex` (Polymarket) is a per-sport decision, not a blanket one

Measured over 30 days against `game_odds_history.books`:

| Sport | Polymarket present | Decision |
|---|---|---|
| MLB | **5,364 / 6,000 (89.4%)** | **KEEP** — its Pin/Poly detector fires |
| NFL | **0 / 1,685 (0%)** | removed 2026-08-24 |
| Tennis | **0 / 2,838 (0%)** | removed 2026-08-24 |

Tennis is the clearest case: all 577 Polymarket tennis rows came from the
**free** Gamma ingest, not the paid region. We were buying a feed we already
get for nothing — and not even receiving it.

This also explains two long-standing **DEAD** detectors:
`tennis/pinnacle_polymarket_delta` and `nfl/pinnacle_polymarket_delta`, 0
alerts ever. Not code bugs — the data never arrived on those paths.

`tests/test_nfl_schedule.py` asserts this asymmetry deliberately. Re-adding
`us_ex` requires fresh evidence from the books JSONB, not a symmetry
argument.

### Never buy odds for a slate with nothing left to price

`ingest.mlb_schedule.fetch_odds()` fired unconditionally. On off-days and
across the ~5-month MLB offseason that is ~279 credits/day for an empty
list — roughly **40,000 credits over a Nov–Mar offseason, two full months of
plan.** It now checks `mlb_matchups` (populated first by the free MLB Stats
API) and skips when no **upcoming** game remains.

"Upcoming", not "scheduled": the parser already skips started games to keep
closing lines frozen, matching `verify_fresh_upcoming_odds()`, which also
passes at zero upcoming — so the guard cannot trip the capture workflow's
retry loop.

It logs the empty-slate and all-games-started cases separately, because zero
scheduled rows can also mean `fetch_schedule` failed upstream, and a spend
guard must never disguise an outage as an off-day.

### Do not buy the same snapshot twice

`capture_odds_history` runs every 30 minutes; two of `refresh_mlb_vegas`'s
three slots (17:10, 22:10 UTC) land ~3 minutes after one. That stage now
skips when a capture for the date is under 20 minutes old — comfortably
inside `verify_fresh_upcoming_odds()`'s 35-minute bar, so a skip can never
leave that check failing. It fails open: no recent capture means it fetches
exactly as before.

### Cadence is a budget lever, and it has a real cost

`capture_odds_history` is the largest single consumer. Cut 2026-08-24:

| | before | after |
|---|---|---|
| Vercel cron | 15 min (56/day) | 30 min (28/day) |
| GitHub fallback | hourly (14/day) | 6-hourly (3/day) |
| | 630/day | 279/day |

`cancel-in-progress: false` means the two schedules **queue rather than
dedupe**, so the fallback was never free. It was thinned, not deleted — it
is the only thing preventing a broken Vercel bridge from silently capturing
nothing at all.

**The cost is real:** MLB line-movement resolution is now 30-minute, so fast
pre-game steam between captures is less likely to be observed. The
15-minute cadence existed because GitHub's sub-hourly scheduler is
unreliable; that reasoning is unchanged. This is a budget concession to
revisit if headroom returns.

### The MLB prop button is NOT overspending — a corrected claim

It was flagged as ~50% waste for pulling 8 markets while the scheduled
pipeline funds only 4. **That was wrong**, and the correction matters:

- `ingest/mlb_prop_odds.py`'s 4 markets are the ones with a **same-line
  Pinnacle anchor**, required by the `dk_prop_value` / `prop_line_gap`
  detectors. That is a *detector* constraint.
- The button's 8 markets feed **DFS projections** — a different consumer.
  All 8 map to real DK scoring stats, and all 8 are populated in
  `player_prop_history` (4.6k–4.7k batter rows; 350–365 pitcher rows each).

Different purposes, both legitimate. Cutting the button to 4 would break MLB
DFS projections to save credits on a path that was **dormant anyway** — last
used 2026-05-22.

---

## 5. Open items

- **CFB uses the shared event-close worker.** The regular CFB workflow refreshes
  canonical CFBD games and free provider-event mappings. The one-minute Vercel
  dispatcher starts a paid worker only when a durable checkpoint is due. CFB
  checkpoints are T−48h, T−24h, T−6h, T−90m, T−15m, and T−2m; one bulk call
  satisfies every due mapped game in that run. Request success alone does not
  mark fulfillment, and the daily cap plus monthly reserve remain hard guards.
- **NFL regular season needs a cadence.** Currently 1×/day (16 credits) for
  weekly games, with preseason and regular both fetched daily. The right
  cadence is undecided; the season opens 2026-09-09.
- **The retry loop can pay 3× for one date.** `capture_odds_history` retries
  on a *DB freshness assertion*, not on API failure, so a persistent
  non-API condition can spend 9 → 18 → 27 credits across three successful
  calls. Fixing it means giving `mlb_schedule` distinct exit codes for "API
  refused" vs "freshness assertion failed".
- **No global spend telemetry.** `x-requests-remaining` is surfaced on the
  DFS prop buttons only. A persisted per-run record would make the next
  overrun visible before it happens rather than after.

---

## 6. Checklist before adding any new Odds API call

1. Is the data already available free? (Polymarket → Gamma; MLB scores →
   statsapi; tennis results → tennisexplorer.)
2. Use `bookmakers=` instead of `regions=` if it is per-event and ≤10 books
   suffice.
3. Justify every region and market — each multiplies the whole call.
4. Guard against firing when there is nothing to price (off-day, offseason,
   all games started).
5. Confirm it does not duplicate a call another job just made.
6. Log the quota headers.
7. Compute credits/day and add the row to the table in §3.
