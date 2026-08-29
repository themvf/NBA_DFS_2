# NBA DFS v2 — Project Instructions

## Data Sources

### NBA Stats (`nba_api`)
- Use `LeagueGameLog` endpoint for player stats (not `BoxScoreTraditionalV2`) — more reliable per-game log format
- Use `LeagueDashTeamStats` with `measure_type_detailed_defense="Advanced"` for pace/OffRtg/DefRtg
- stats.nba.com is flaky from CI/shared IPs — all API calls must use `_call_with_retry()` with exponential backoff

### DraftKings API
- Individual resource lookups work without auth:
  - `GET /contests/v1/contests/{contestId}` → resolves to draftGroupId
  - `GET /draftgroups/v1/draftgroups/{draftGroupId}/draftables` → full player pool
- **Listing endpoints are auth-gated.** Every combination of `sport`, `sportId`, `gameTypeId`, and date range returns `400 SPO117` or `422 DRA107` without a DK session cookie. Do not attempt to build auto-discovery of draft groups without DK account auth.
- **The contest_id workflow_dispatch input is intentionally manual.** DK has 5–10+ NBA classic contests per slate (different entry fees, field sizes, max-entry rules). Which contest to target is a strategic decision — it should not be automated away.
- To find the contest_id: open any DK NBA Classic contest → the URL contains `/draft/contest/{contestId}`

### LineStar API
- Requires a `.DOTNETNUKE` session cookie stored as `DNN_COOKIE` in GitHub Secrets
- Cookie expires every 24–48 hours or on logout — must be manually refreshed in GitHub Secrets
- `sport=5` for NBA (was `sport=4` for CBB)
- `site=1` for DraftKings
- LineStar is a **soft dependency** — slate loads should proceed without it if the cookie is expired

### Vegas Odds (The Odds API)
- `sport_key = "basketball_nba"`
- Stored as `ODDS_API_KEY` in GitHub Secrets and `.env`
- **One key, one 20,000/month quota, shared by every sport.** MLB, NFL,
  tennis and the DFS prop buttons all draw on it, so an overspend in one
  sport takes the others down with it. That happened 2026-08-24: the key hit
  20,000/20,000 and every paid `/odds` call started returning **401
  Unauthorized** (not 429) across all three sports at once, while the free
  `/v4/sports` and `/events` endpoints kept answering and masked it.
- **Read [`docs/the-odds-api.md`](docs/the-odds-api.md) before adding,
  re-enabling or re-scheduling ANY Odds API call.** It is the single source
  of truth for the cost model (`markets x regions`; `bookmakers=` is
  `markets x 1`), the per-consumer credits/day table, the measured reasons
  specific spends were removed, and a pre-flight checklist. Several removals
  recorded there are deliberately asymmetric between sports and are backed
  by measurement, not preference -- re-measure before reversing one.

### NFL regular-season odds cadence — UNDECIDED (opens 2026-09-09)

`refresh_nfl_vegas` currently runs **1x/day** (16 credits: 12 odds + 4
scores) against a schedule where games are **weekly**, and it fetches the
preseason and regular-season sport keys on every run regardless of which is
actually in season. That was adequate for preseason; it is not obviously
right for a real season, in either direction:

- Daily is likely too COARSE near kickoff. NFL lines move most in the last
  hours before a Sunday 13:00 ET slate, and one 13:20 UTC pass per day
  cannot see that. If NFL is to have a CLV/line-movement story like MLB, it
  needs a burst around game windows, not a flat daily poll.
- Daily is too FINE mid-week. Tuesday and Wednesday carry almost no
  information for a Sunday game, so those passes are near-pure spend.

The obvious shape is game-proximity-weighted (sparse Mon-Thu, denser
Sat-Sun), not a single uniform interval -- but the cadence should be chosen
against a stated purpose (bet-rating freshness vs line-movement resolution),
and costed against [`docs/the-odds-api.md`](docs/the-odds-api.md) first.

Cheap and independent of that decision: **only fetch the sport key that is
in season.** Fetching preseason in November, or regular-season odds in
August, is spend for an empty list -- the same class of waste the MLB
no-upcoming-games guard now prevents.

### Polymarket (Gamma + Data APIs)

Free, unauthenticated, **unmetered** — unlike the Odds API, nothing here
competes for quota. Two endpoints matter: `gamma-api.polymarket.com` for
markets and resolved outcomes, `data-api.polymarket.com` for the per-wallet
fill tape (`proxyWallet`). `clob.polymarket.com/trades` requires auth and is
NOT the endpoint to use.

Live use is one thing only: `ingest/polymarket_tennis.py` captures tennis
match prices into `game_odds_history`, feeding the Pin/Poly delta detector.

**Wallet tracking: v1 closed 2026-08-26, v2 reopened it 2026-08-27, and the
v2 positive result was WITHDRAWN 2026-08-28 after review.** v1's metric
measured trading style, not skill. v2 re-asked the question with
closing-line value and appeared to find an MLB selection gap of +0.0084 —
but an independent statistical review found the CLV metric was **weighting a
price move by dollars instead of by shares**, which drops the 1/p converting
dollars to shares and destroys the antisymmetry that makes CLV zero-sum.
Two perfectly offsetting share positions scored +0.0400 instead of 0.0000,
handing free CLV to whoever's dollars sat on favourites — a persistent
style, so it survived the walk-forward looking like skill. Four further
defects (gates leaking the holdout, market-clustered intervals blind to the
per-wallet effect they test, leave-one-out on the level not the gap, and a
powerless favourite-longshot check) were each independently capable of
producing the same result. **Do not quote the +0.0084 figure.** All six are
fixed and both sports are being re-measured. Read
[`docs/polymarket-wallet-tracker.md`](docs/polymarket-wallet-tracker.md)
before proposing wallet tracking again — it records the method, the five
ranking bugs found in sequence, the negative result and why it is a real
answer rather than an inconclusive one, plus the four bars any revival must
clear first. It also flags an earlier uncommitted attempt whose two recorded
conclusions ("blocked without auth", "no MLB games in August") are both
wrong and will otherwise cause the work to be redone.

## Projection Model

### How `ourProj` is computed

Data flows through three stages every time a slate is loaded:

```
stats.nba.com  ──→  10-game rolling averages (refreshed weekly via refresh_stats.bat)
nba_matchups   ──→  vegasTotal, homeMl, awayMl (from The Odds API, game-level)
nba_team_stats ──→  pace, offRtg, defRtg per team
Odds API       ──→  propPts, propReb, propAst per player (fetched via "Fetch Player Props" button)
LineStar       ──→  linestarProj (display only), projOwnPct (feeds leverage — NOT blended into ourProj)
DK API         ──→  avgFptsDk (field projection baseline for leverage)
```

**Stage 1 — Environment factors**
```
paceFactor    = avg(teamPace, oppPace) / LEAGUE_AVG_PACE
totalFactor   = teamImpliedTotal(vegasTotal, homeMl, awayMl, isHome) / LEAGUE_AVG_TEAM_TOTAL
combinedEnv   = paceFactor × 0.4 + totalFactor × 0.6
defFactor     = oppDefRtg / LEAGUE_AVG_DEF_RTG
oppOffFactor  = oppOffRtg / LEAGUE_AVG_OFF_RTG   ← opponent shot volume proxy
usageFactor   = clamp(playerUsage / LEAGUE_AVG_USAGE, 0.5, 2.0)
adjustedEnv   = 1 + (combinedEnv − 1) × usageFactor
```

**Stage 2 — Per-stat projections**
Props (pts/reb/ast) are used directly when available — they already bake in matchup,
pace, and injury context. Rolling-average formula is the fallback.
```
pts = propPts  ?? (ppg  × defFactor)
reb = propReb  ?? (rpg  × adjustedEnv × oppOffFactor^0.4)   ← more opp FGA = more misses to rebound
ast = propAst  ?? (apg  × defFactor × (1 + (combinedEnv−1) × 0.5))
stl = spg  × adjustedEnv × (1/oppOffFactor)^0.5   ← better opp offense = fewer turnovers = fewer steals
blk = bpg  × adjustedEnv × oppOffFactor^0.3        ← more opp shots = more block chances
tov = tovpg × adjustedEnv
3pm = threefgmPg                                    ← no adjustment (shot selection, not pace)
dd  = ddRate × adjustedEnv
```

**Stage 3 — DK fantasy points**
```
ourProj = pts×1 + reb×1.25 + ast×1.5 + stl×2 + blk×2 − tov×0.5 + 3pm×0.5 + dd×1.5
```
Players with < 10 avg minutes get `ourProj = null` and are excluded from optimization.

### How `ourLeverage` is computed

LineStar is **not blended** into `ourProj`. It provides `projOwnPct` which feeds leverage:
```
edge        = ourProj − fieldProj
              (fieldProj priority: avgFptsDk → linestarProj → null)
              positive = we like this player MORE than the field does

ourLeverage = edge × (1 − projOwn%)^0.7 × ceilingBonus
ceilingBonus = 1 + spg×0.05 + bpg×0.04
```
Negative leverage = we are below-field on this player → correct GPP fade.
The optimizer filters `leverage > 0` for GPP mode.

### How `ourOwnPct` is computed

Our own ownership estimate (independent of LineStar):
```
score    = ourProj / √(salary / $1K)
ourOwnPct = score / poolTotal × 800%   (800 = 8 roster slots × 100%)
```

### DK Scoring Reference
```
PTS × 1.0 | REB × 1.25 | AST × 1.5 | STL × 2.0 | BLK × 2.0 | TOV × −0.5
3PM × 0.5 (bonus) | DD × 1.5 (bonus)
```

### Key Design Decisions
- **Implied team total** (not raw O/U ÷ 2): derive each team's expected points from moneylines using `computeTeamImpliedTotal()`. A -180 home favorite in a 230 O/U game gets ~118 implied, not 115.
- **Usage rate** scales the pace/environment benefit. Stars (30%+ usage) capture more extra possessions in high-pace games. Capped at 0.5×–2.0×.
- **Props replace formula for pts/reb/ast** when available. Market lines already embed defFactor, paceFactor, and injury status — applying additional adjustments on top would double-count.
- **No blowout curve**: NBA teams rarely blow out enough to affect starter minutes.
- **`avg_minutes` directly from the API** — do not derive from min_pct × 48.
- **Assists** get partial pace adjustment (50% of `combined_env`) in addition to defensive factor.
- **DD rate** scaled by `adjusted_env` — more possessions = more double-double chances.
- **LineStar delta** (`ourProj − linestarProj`) is the primary edge signal for GPP. Do not blend LineStar into `ourProj` — the disagreement IS the edge.

### League Average Constants (2025-26)
```
LEAGUE_AVG_PACE       = 100.0   # actual: 100.18 (58 games)
LEAGUE_AVG_DEF_RTG    = 114.5   # actual: 114.57 (was 112.0 — stale, caused systematic over-projection)
LEAGUE_AVG_OFF_RTG    = 114.5   # actual: 114.62; used for reb/stl/blk opponent adjustment
LEAGUE_AVG_TOTAL      = 230.0   # actual: 229.88 (was 228.0)
LEAGUE_AVG_TEAM_TOTAL = 115.0   # actual: 114.94 (was 114.0)
LEAGUE_AVG_USAGE      = 20.0
```

### Player Stats Source
- Use `LeagueDashPlayerStats?LastNGames=10&PerMode=PerGame` (not `LeagueGameLog`) for rolling averages.
  Returns one pre-aggregated row per player. Much faster than LeagueGameLog (one row per player-game).
  Provides real `USG_PCT` and `DD2` (double-double count).
- stats.nba.com blocks Vercel/cloud IPs — run `refresh_stats.bat` locally (weekly).
  The Odds API has no IP restrictions — player props work from Vercel directly.

## Data Refresh Workflow

```
Weekly  : refresh_stats.bat          → team pace/ratings + player rolling stats → Neon
Daily   : python -m ingest.nba_schedule  → today's schedule + game-level odds → Neon
Daily   : "Fetch Player Props" button    → pts/reb/ast prop lines → dk_players → recompute ourProj
Each slate: "Load Slate" button          → reads all of the above from Neon → dk_players upserted
```

## GitHub Actions

The `daily_stats.yml` workflow was removed (2026-03-28) because stats.nba.com blocks
GitHub shared runner IPs (ReadTimeout on every attempt).

Replacement:
- Stats refresh: run `refresh_stats.bat` locally (no IP block from home network)
- Slate load: "Load Slate" button in the web UI
- Props: "Fetch Player Props" button (Odds API works from Vercel — no IP block)

### `load_slate.yml` — manual `workflow_dispatch` (still active if needed)
- Requires: `contest_id` (from DK contest URL)
- Optional: `date_override` (YYYY-MM-DD), `season`
- Uses `DNN_COOKIE` secret for LineStar — if missing/expired, LineStar projections will be NULL but the slate still loads

### `capture_odds_history.yml` scheduling — Vercel Cron bridge (2026-08-04)

**Problem found:** the MLB "Today's movement board" (`/vegas?sport=mlb`) showed every
game STALE with empty fixed-time snapshot columns. Root cause verified via GitHub's
own run-history API: `capture_odds_history.yml`'s native `schedule:` trigger was
configured for every ~15 minutes, but GitHub was actually firing it every 60–95
minutes during active hours and with 3–13 hour gaps overnight — a documented GitHub
Actions platform limitation (sub-hourly `schedule:` triggers get silently
delayed/dropped under load), not a bug in the workflow itself (28/30 sampled runs
succeeded when they did fire).

**Fix:** Vercel Cron (Pro plan — fires within the configured minute, unlike GitHub's
best-effort scheduler) now owns the real cadence. `web/src/app/api/cron/mlb-odds-capture`
is a thin route that Vercel invokes on a `7,37 14-23,0-3 * * *` schedule
(`web/vercel.json`) and does nothing but fire a `workflow_dispatch` REST call against
`capture_odds_history.yml` — **no capture/business logic moved**; the single-writer
Python odds pipeline (`ingest.mlb_schedule`, `model.line_alerts`) still runs entirely
inside GitHub Actions, untouched. GitHub's own `schedule:` trigger is a sparse
fallback (`12 14,20,2 * * *`) in case the Vercel→GitHub bridge itself breaks.

**Cadence reduced 2026-08-24 — quota, not a change of heart about timing.** The
shared `ODDS_API_KEY` hit its 20,000/month ceiling (`remaining=0 used=20000`), at
which point The Odds API returns **401 Unauthorized** — not 429 — on every paid
`/odds` call. That took MLB, NFL *and* tennis ingestion down simultaneously while
the free `/v4/sports` and `/events` endpoints kept answering, which masked the
outage and made it look like missing provider coverage. This capture step is the
single largest consumer in the project (3 regions × 3 markets = **9 credits/run**),
so it absorbed the cut:

| | before | after | credits/day |
|---|---|---|---|
| Vercel cron | every 15 min (56/day) | every 30 min (28/day) | 504 → 252 |
| GitHub fallback | hourly (14/day) | 6-hourly (3/day) | 126 → 27 |
| | | | **630 → 279** |

Note `cancel-in-progress: false` means the Vercel dispatches and the GitHub
fallback **queue** rather than dedupe, so both schedules genuinely ran — the
fallback was not free. It was thinned rather than deleted because it is the only
thing preventing a broken Vercel bridge from silently capturing nothing.

The cost of the reduction is real and should be stated: line-movement resolution
on the MLB board halves (30-minute granularity instead of 15), so fast pre-game
steam between captures is now less likely to be observed. The 15-minute cadence
was originally chosen precisely because GitHub's own sub-hourly scheduler was
unreliable; that reasoning is unchanged, and this is a budget concession to be
revisited if quota headroom is restored.

Deliberately NOT ported to a Vercel Python Function: Vercel's Root Directory (`web/`)
cannot access files outside itself (`..` is blocked), so a real port would mean either
changing the Vercel project's Root Directory to the repo root (risky, dashboard-only,
untested against the live Next.js deployment) or duplicating `ingest`/`model`/`db`
logic into `web/` (violates the single-writer odds policy and this project's
zero-duplication discipline). The dispatch-bridge approach avoids both.

**Required Vercel project env vars** (Production + Preview) — not yet set as of this
writing:
- `CRON_SECRET` — random 16+ char string; Vercel sends it as `Authorization: Bearer
  <value>` on every cron invocation, checked by the route to reject non-Vercel callers.
- `GITHUB_DISPATCH_TOKEN` — a GitHub fine-grained PAT scoped to `themvf/NBA_DFS_2`
  only, with **Actions: Read and write** repo permission (nothing else). Used solely
  to POST to `/repos/themvf/NBA_DFS_2/actions/workflows/capture_odds_history.yml/dispatches`.

**Cost:** $0 marginal — the account is already on Vercel Pro (required for sub-daily
cron; Hobby caps at once/day with ±59min imprecision). The dispatch route itself is a
single outbound HTTPS call (~1-2s), well under a cent/month even at ~90 invocations/day,
fully absorbed by the $20/month usage credit already included with the Pro seat.

**Known residual issue, not fixed here:** `db/database.py`'s `_ensure_schema()` runs
schema DDL (with a 4-attempt/lock-timeout retry) on every single script invocation
across every Python workflow in this repo; it occasionally hard-fails a job
(`psycopg2.errors.LockNotAvailable`) when contending with another concurrent writer —
observed once in the sampled runs above, after that run's actual odds capture had
already committed. Low priority (doesn't touch the capture step itself, rare), but a
real fix (schema init as a separate one-time migration rather than per-invocation)
touches every Python entrypoint in the repo and deserves its own change, not a
drive-by edit inside this fix.

## NBA Lineup Structure (DraftKings)
```
PG / SG / SF / PF / C / G / F / UTIL  (8 players, $50,000 salary cap)
```
- G slot: PG or SG eligible
- F slot: SF or PF eligible
- UTIL: any position

---

## Postmortem & Model Calibration — Implementation Plan

### Current State (as of 2026-03-25)

What exists:
- `ingest/dk_results.py` — manual CLI script; parses DK results/standings CSV → updates `actual_fpts` + `actual_own_pct` in `dk_players`, rolls up to `dk_lineups.actual_fpts`, prints terminal report
- Web UI — single-slate MAE/bias panel, biggest misses table, cross-slate strategy leaderboard (cash rate, avg FPTS)

Gaps identified:
1. No cross-slate projection accuracy trend (is the model improving slate-over-slate?)
2. No position-level accuracy breakdown (PG vs C vs F — where is the model wrong?)
3. No salary-tier accuracy (are $5k plays or $9k plays more miscalibrated?)
4. No leverage calibration (do high-leverage players actually outperform?)
5. Ownership correlation computed per-slate in terminal but never persisted
6. `cashThreshold = 300` hardcoded — varies by contest type and is likely wrong
7. Results ingestion is fully manual — no web upload, no GitHub Action
8. LineStar is a hard dependency — expired DNN_COOKIE kills the entire slate load

---

### Phase 1 — `/analytics` Route (Cross-Slate Calibration) ✅ Done

**Goal:** Surface model calibration trends across all historical slates in the web UI.

**New file:** `web/src/app/analytics/page.tsx` (Server Component)
**New file:** `web/src/app/analytics/analytics-client.tsx` (Client Component)
**New file:** `web/src/app/analytics/analytics-content.tsx` (async data-fetching wrapper)
**Modified:** `web/src/db/queries.ts` — 6 query functions added

**Implemented sections in `/analytics`:**
1. Accuracy Trend — line chart, MAE over time (our model vs LineStar), ownership correlation
2. Position Breakdown — MAE + bias per position, sorted worst→best
3. Salary Tier — MAE + bias per $1k salary bucket
4. Leverage Calibration — Q1→Q4 avg_proj vs avg_actual vs avg_beat
5. Ownership vs Team Total — ownership sensitivity by team implied run total (MLB) / point total (NBA)
6. **Projection Source Breakdown** — per-slate MAE/bias comparison for live (v2), our (v1), LineStar — last 20 slates, excludes DNPs (`getProjectionSourceBreakdown(sport)`)
7. **MLB Batting Order Calibration** — avg proj vs actual vs delta vs ownership by batting slot #1–9, excludes SP/RP (`getMlbBattingOrderCalibration()`)

Both #6 and #7 were added 2026-04-11.

#### Queries to add in `queries.ts`:

**`getCrossSlateAccuracy()`** — per-slate accuracy trend, ordered chronologically
```sql
SELECT
  ds.slate_date,
  COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS n,
  AVG(ABS(dp.our_proj - dp.actual_fpts))
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS our_mae,
  AVG(dp.our_proj - dp.actual_fpts)
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS our_bias,
  AVG(ABS(dp.linestar_proj - dp.actual_fpts))
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS ls_mae,
  AVG(dp.linestar_proj - dp.actual_fpts)
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.linestar_proj IS NOT NULL) AS ls_bias,
  CORR(dp.proj_own_pct, dp.actual_own_pct)
    FILTER (WHERE dp.actual_own_pct IS NOT NULL AND dp.proj_own_pct IS NOT NULL) AS own_corr
FROM dk_players dp
JOIN dk_slates ds ON ds.id = dp.slate_id
GROUP BY ds.slate_date
HAVING COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL) > 0
ORDER BY ds.slate_date ASC
```

**`getPositionAccuracy()`** — MAE/bias/n grouped by primary position (all slates)
```sql
SELECT
  CASE
    WHEN dp.eligible_positions LIKE '%PG%' THEN 'PG'
    WHEN dp.eligible_positions LIKE '%SG%' THEN 'SG'
    WHEN dp.eligible_positions LIKE '%SF%' THEN 'SF'
    WHEN dp.eligible_positions LIKE '%PF%' THEN 'PF'
    WHEN dp.eligible_positions LIKE '%C%'  THEN 'C'
    ELSE 'UTIL'
  END AS position,
  COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS n,
  AVG(ABS(dp.our_proj - dp.actual_fpts))
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS mae,
  AVG(dp.our_proj - dp.actual_fpts)
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS bias
FROM dk_players dp
GROUP BY 1
ORDER BY mae DESC NULLS LAST
```
Note: Uses CASE priority — a PG/SG player is binned as PG. This matches how the optimizer's position assignment works.

**`getSalaryTierAccuracy()`** — MAE/bias/n grouped by $1k salary buckets (all slates)
```sql
SELECT
  CASE
    WHEN dp.salary < 5000  THEN 'Under $5k'
    WHEN dp.salary < 6000  THEN '$5k–$6k'
    WHEN dp.salary < 7000  THEN '$6k–$7k'
    WHEN dp.salary < 8000  THEN '$7k–$8k'
    WHEN dp.salary < 9000  THEN '$8k–$9k'
    ELSE '$9k+'
  END AS salary_tier,
  MIN(dp.salary) AS tier_min,
  COUNT(*) FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS n,
  AVG(ABS(dp.our_proj - dp.actual_fpts))
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS mae,
  AVG(dp.our_proj - dp.actual_fpts)
    FILTER (WHERE dp.actual_fpts IS NOT NULL AND dp.our_proj IS NOT NULL) AS bias
FROM dk_players dp
GROUP BY 1
ORDER BY tier_min ASC NULLS LAST
```

**`getLeverageCalibration()`** — do high-leverage players actually outperform?
```sql
SELECT
  NTILE(4) OVER (ORDER BY dp.our_leverage ASC NULLS LAST) AS leverage_quartile,
  AVG(dp.our_leverage)  AS avg_leverage,
  AVG(dp.our_proj)      AS avg_proj,
  AVG(dp.actual_fpts)   AS avg_actual,
  AVG(dp.actual_fpts - dp.our_proj) AS avg_beat,
  COUNT(*)              AS n
FROM dk_players dp
WHERE dp.our_leverage IS NOT NULL AND dp.actual_fpts IS NOT NULL
GROUP BY 1
ORDER BY 1
```
Q4 (highest leverage) should show the largest positive `avg_beat` if the leverage model is working.

#### UI layout for `/analytics`:

Four sections, all using the queries above:
1. **Accuracy Trend** — line chart: MAE over time (our model vs LineStar), ownership correlation over time
2. **Position Breakdown** — horizontal bar chart or table: MAE + bias per position, sorted worst→best
3. **Salary Tier** — table: MAE + bias per salary bucket, shows where the model is most miscalibrated
4. **Leverage Calibration** — 4-row table: Q1 (lowest leverage) → Q4 (highest), showing avg_proj vs avg_actual vs avg_beat

Add link to `/analytics` in the root layout nav.

---

### Phase 2 — LineStar Soft Dependency Fix

**Goal:** A stale/expired `DNN_COOKIE` should degrade gracefully (NULL ownership fields) rather than crashing the entire slate load.

**Modified:** `ingest/linestar_fetch.py`

Wrap `fetch_linestar_for_draft_group()` in a top-level try/except. On any `requests.HTTPError` with 401/403 status, log a warning and return `{}`. The caller (`dk_slate.py`) already handles an empty `linestar_map` correctly — ownership and LineStar proj will simply be NULL.

```python
def fetch_linestar_for_draft_group(dk_draft_group_id, dnn_cookie=None):
    try:
        # ... existing implementation ...
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        if status in (401, 403):
            logger.warning(
                "LineStar auth failed (HTTP %s) — DNN_COOKIE likely expired. "
                "Continuing without LineStar projections.", status
            )
            return {}
        raise
```

**Modified:** `ingest/dk_slate.py` — no code change needed; empty `linestar_map` already causes `linestar_proj=NULL` + `proj_own_pct=NULL` on all players.

**Modified:** `.github/workflows/load_slate.yml` — no change needed; the Python layer handles it.

---

### Phase 3 — Results Upload via Web UI

**Goal:** Allow results ingestion from the `/dfs` page without running a local Python script.

**Modified:** `web/src/app/dfs/actions.ts` — add `uploadResults(formData)` server action
- Accepts a DK results CSV or standings CSV file
- Parses in TypeScript (reuse the CSV parsing pattern from `processDkSlate`)
- Updates `actual_fpts` + `actual_own_pct` via Drizzle for the most recent slate
- Fuzzy-matches by name using the existing `levenshtein()` helper
- Rolls up lineup actuals: for each `dk_lineups` row, SUM `actual_fpts` of its players
- Returns match rate + updated count

**Modified:** `web/src/app/dfs/dfs-client.tsx` — add "Upload Results" section
- File input (results CSV or standings CSV)
- "Upload & Analyze" button → calls `uploadResults` server action
- Shows match rate + updated count feedback

This eliminates the need to run `python -m ingest.dk_results` locally after each slate.

---

### Phase 4 — Cash Line Calibration

**Goal:** Make the cash threshold meaningful per-contest instead of a hardcoded 300.

**Modified:** `web/src/db/schema.ts` + Python `db/schema.py` — add `cash_line DOUBLE PRECISION` to `dk_slates`

When loading a slate, the user can optionally input the cash line for the contest (visible on DK's contest page). Default remains `NULL` (falls back to the 300 constant in `getDkStrategySummary`).

**Modified:** `getDkStrategySummary()` in `queries.ts` — use `COALESCE(ds.cash_line, 300)` as the threshold per slate instead of a fixed parameter.

**Modified:** `web/src/app/dfs/dfs-client.tsx` — add a "Cash Line" input field in the Load Slate panel (optional, sent alongside `contest_id`).

---

### Implementation Order

| Priority | Phase | Impact | Effort | Status |
|----------|-------|--------|--------|--------|
| 1 | Phase 2 — LineStar soft dependency | Prevents daily slate failures | Low | Planned |
| 2 | Phase 3 — Results web upload | Removes manual step after every slate | Medium | Planned |
| 3 | Phase 1 — `/analytics` route | Core model calibration visibility | High | ✅ Done |
| 4 | Phase 4 — Cash line calibration | Accuracy of strategy leaderboard | Low | Planned |

---

## Model Improvement Roadmap

### Priority Implementation Order

| Priority | Feature | Phase | Status |
|----------|---------|-------|--------|
| 1 | EWMA rolling stats (α=0.25) | Phase 1 — Better Signal | ✅ Done |
| 2 | Rest/travel features (B2B penalty) | Phase 1 — Better Signal | ✅ Done |
| 3 | Monte Carlo ceiling/floor/boom | Phase 2 — Distributions | ✅ Done |
| 4 | Position-specific prop weighting | Phase 1 — Better Signal | Planned |
| 5 | HMM regime detection | Phase 2 — Distributions | Planned |
| 6 | KL-divergence ownership gap | Phase 3 — Mispricings | Planned |
| 7 | Behavioral bias inventory | Phase 3 — Mispricings | Planned |
| 8 | Kelly Criterion lineup sizing | Phase 4 — Game Theory | Planned |
| 9 | Network graph stacking | Phase 4 — Game Theory | Planned |
| 10 | GPP vs Cash objective functions | Phase 4 — Game Theory | Planned |
| 11 | Bayesian prior updating | Phase 5 — Adaptive | Planned |
| 12 | Calibration feedback loop | Phase 5 — Adaptive | Planned |

### Phase 1 — Better Signal
1. **EWMA rolling stats** — Replace flat N-game average with exponential weighted moving average (α=0.25). Most recent game gets ~2.5× the weight of a game 5 days back. Implemented in `ingest/nba_stats.py` — stored ppg/rpg/etc. are now EWMA-smoothed.
2. **Rest/travel features** — B2B 2nd night −5%, 3-in-4 nights −3%, 4+ days rest +2%. Applied as scalar multiplier on final FPTS in `model/dfs_projections.py`. Rest days computed from nba_matchups history in `ingest/dk_slate.py`.
3. **Position-specific prop weighting** — Centers rely more on reb props (less on pts). Guards rely more on ast props. Currently props fully replace the formula; partial blending weighted by position would improve accuracy for hybrid roles.

### Phase 2 — Probability Distributions
4. **Monte Carlo ceiling/floor** — 1000 simulations per player sampling from N(ourProj, ftpsStd). Stores P10 (floor), P90 (ceiling), boom_rate = P(FPTS ≥ 50) in `dk_players`. `fpts_std` computed from per-game FPTS variance and stored in `nba_player_stats`. Implemented in `model/dfs_projections.py`.
5. **HMM regime detection** — Hidden Markov Model with 3 states (hot streak / average / slump). Regime probabilities inform whether to trust or discount the EWMA, particularly for players recently injured or returning from rest.

### Phase 3 — Market Mispricings
6. **KL-divergence ownership gap** — Measure information difference between our projection distribution and the field's implied ownership distribution. Large KL-divergence = market mispricing = GPP opportunity beyond simple edge × (1 − own).
7. **Behavioral economics bias inventory** — The field systematically over-owns: stars coming off big games (recency bias), players in nationally televised games (availability heuristic), players with round salary numbers. Inventory and systematically fade these biases.

### Phase 4 — Game Theory
8. **Kelly Criterion lineup sizing** — Derive optimal number of lineups per contest from edge and variance. f* = edge / variance. Prevents both over-exposure (too many lineups on same players) and under-exposure (leaving +EV plays on the table).
9. **Network graph stacking** — Model player correlations as a directed graph (pass chains, usage trees). Stacks that maximize correlated upside given low collective ownership outperform naive same-team stacks.
10. **Contest-type objective functions** — GPP: maximize variance-adjusted ceiling (P90 score). Cash: maximize floor (P10 score). Separate optimizer modes instead of the current `leverage > 0` filter for GPP.

### Phase 5 — Adaptive Learning
11. **Bayesian prior updating** — Start with population priors per position (e.g., PG averages), update toward player EWMA as sample grows. Shrinks aggressive projections for small sample sizes (< 5 games) toward the mean.
12. **Calibration feedback loop** — After each slate, compute MAE/bias per position/salary tier and store as correction deltas. Auto-apply to next slate's projections. PGs overvalued by 2 FPTS → subtract 2 from next slate's PG projections automatically.

---

## MLB Expansion Plan

### Architecture Decision

Parallel MLB tables alongside existing NBA tables. Add `sport TEXT DEFAULT 'nba'` to `dk_slates`.
`dk_players` and `dk_lineups` are sport-agnostic — no changes needed to those tables.

### New Tables

**`mlb_teams`** — 30 MLB teams
```
team_id SERIAL PK | name | abbreviation | dk_abbrev | ballpark | city | division | mlb_id | logo_url
```

**`mlb_park_factors`** — updated annually
```
id | team_id FK | season | runs_factor | hr_factor
(Coors ≈ 1.15 runs, Petco ≈ 0.88 — affects batter and pitcher projections)
```

**`mlb_matchups`** — same shape as nba_matchups plus pitchers and weather
```
id | game_date | game_id (MLB gamePk, UNIQUE) | home_team_id | away_team_id
home_sp_id | away_sp_id                     ← confirmed starter FK to mlb_pitcher_stats
vegas_total | home_ml | away_ml | vegas_prob_home
home_implied | away_implied               ← team-specific run totals from moneylines
ballpark | weather_temp | wind_speed | wind_direction
```

**`mlb_batter_stats`** — rolling 15-game EWMA (same α=0.25 as NBA)
```
player_id | season | team_id | name | batting_order
pa_pg | avg | obp | slg | iso | babip | wrc_plus | k_pct | bb_pct
hr_pg | singles_pg | doubles_pg | triples_pg | rbi_pg | runs_pg | sb_pg | hbp_pg
wrc_plus_vs_l | wrc_plus_vs_r   ← L/R splits
avg_fpts_pg | fpts_std
UNIQUE (player_id, season)
```

**`mlb_pitcher_stats`** — season + rolling
```
player_id | season | team_id | name | hand (R/L)
ip_pg | era | fip | xfip | k_per_9 | bb_per_9 | hr_per_9
k_pct | bb_pct | hr_fb_pct | whip
avg_fpts_pg | fpts_std | win_pct | qs_pct
UNIQUE (player_id, season)
```

**`mlb_team_stats`** — offensive + bullpen environment
```
team_id | season | team_wrc_plus | team_k_pct | team_bb_pct | team_iso | team_ops
bullpen_era | bullpen_fip | staff_k_pct | staff_bb_pct
UNIQUE (team_id, season)
```

### Data Sources

| Source | What | Notes |
|---|---|---|
| MLB Stats API (`statsapi.mlb.com`) | Schedule, teams, rosters, results | Free, no auth, no IP blocks |
| pybaseball | FanGraphs batting/pitching (wRC+, xFIP, ISO) | Cache aggressively — FanGraphs rate-limits |
| The Odds API | Game O/U, moneylines | `sport_key = "baseball_mlb"` |
| DraftKings API | Player pool, salaries | Already integrated, sport-agnostic |
| LineStar | Projected ownership, injury status | MLB sport ID unknown — discover empirically |

**MLB Stats API key endpoints:**
```
GET /api/v1/schedule?sportId=1&date=YYYY-MM-DD     # daily game schedule
GET /api/v1/teams?sportId=1                         # all 30 teams + mlb_id
GET /api/v1/sports/1/players?season=2025            # full active roster
GET /api/v1/schedule/{gamePk}/boxscore              # post-game results
```
No API key required. No IP restrictions.

### DraftKings MLB Scoring

**Batters:**
```
1B: +3.0 | 2B: +5.0 | 3B: +8.0 | HR: +10.0
RBI: +2.0 | R: +2.0 | BB: +2.0 | HBP: +2.0 | SB: +5.0
```

**Starting Pitchers:**
```
IP:  +2.25 (per inning)    K:    +2.0
W:   +4.0                   ER:   -2.0
H:   -0.6                   BB:   -0.6
HBP: -0.6
CG:  +2.5   CGSO: +2.5   NH: +5.0   (too rare to model — omit)
```

**Classic lineup slots** (10 players, $50,000 cap):
```
SP  SP  C  1B  2B  3B  SS  OF  OF  OF  UTIL
UTIL = any hitter (C/1B/2B/3B/SS/OF). Pitchers cannot fill UTIL.
```

**Showdown slates** (single-game, 6 players — out of scope for Phase 1):
```
CPT × 1 (1.5× scoring, 1.5× salary)
FLEX × 5
```
Set `contest_format = 'showdown'` on `dk_slates` — optimizer will detect and refuse to process until Phase 2 optimizer adds Showdown mode.

### Projection Model Design

Two entirely separate models — batters and pitchers share no logic.

**Batter projection:**
```
1. Base: per-game rates (hr_pg, singles_pg, doubles_pg, rbi_pg, runs_pg, bb_pg, sb_pg, hbp_pg)
2. Matchup: use wrc_plus_vs_l or wrc_plus_vs_r based on opposing SP hand
3. Pitcher quality: opponent xFIP / LEAGUE_AVG_XFIP scales ER/hit rates
4. Park: multiply HR by hr_factor, all scoring by runs_factor
5. Game environment: team_implied / LEAGUE_AVG_TEAM_TOTAL (same formula as NBA)
6. Batting order PA weight:
     1-2: ×1.08    3-4: ×1.05    5-6: ×1.00    7-9: ×0.93
7. DK FPTS = 1B×3 + 2B×5 + 3B×8 + HR×10 + RBI×2 + R×2 + BB×2 + HBP×2 + SB×5
```

**Pitcher projection:**
```
1. Base: ip_pg, k_per_9, era, xfip
2. Opposing lineup: team_wrc_plus / 100 scales ER rate; team_k_pct scales K count
3. Park: runs_factor inversely scales ER
4. Win probability: from moneyline using existing _ml_to_prob()
5. DK FPTS = ip×2.25 + k×2 + win_prob×4 - er×2 - h×0.6 - bb×0.6
```

**Monte Carlo:** Same `compute_monte_carlo()` function — fpts_std powers it identically.
Change boom_threshold per sport: batters = 35 FPTS, pitchers = 50 FPTS.

**League average constants (2025 MLB):**
```python
MLB_LEAGUE_AVG_TEAM_TOTAL = 4.5   # runs per game per team
MLB_LEAGUE_AVG_XFIP       = 4.20  # league average xFIP
```

### Stacking — Core MLB Strategy

MLB DFS without stacking is playing at a severe disadvantage. **4-5 correlated batters
from the same consecutive batting order positions** dramatically increases ceiling variance.

**Stack types:**
- **Primary stack:** 4–5 batters from Team A (consecutive batting order preferred — 1-2-3-4 more correlated than 1-3-6-8)
- **Bring-back:** 1–2 batters from Team B (the opponent). Captures game-script correlation.
- **Mini-stack:** 2–3 batters from a second game
- **Game stack:** 5+ batters across both teams in a single high-total game

**Critical anti-correlation rule:** Do NOT stack SP's opposing batters.
If your lineup contains SP from Team A, do NOT also have batters from Team B facing him.
Your pitcher is trying to suppress the same batters you'd be stacking.

**Optimizer additions for MLB:**
```
min_team_batters: int = 4        # at least 4 hitters from one team
max_team_batters: int = 6        # cap to prevent 8-man stacks
bring_back: bool = True          # require 1+ from opposing team
no_sp_stack: bool = True         # block pitcher-opponent batter combos
```

**MLB optimizer features implemented (as of 2026-04-11):**

- **HR Correlation stacking** (`hrCorrelation`, `hrCorrelationThreshold` in `MlbOptimizerSettings`): When a batter's `hr_prob_1plus` exceeds the threshold (default 0.12), the preceding batter (order − 1) gets +5 score and the batter two spots ahead (order − 2) gets +2. Wraps around: batter #1's predecessor is #9. Computed via `computeHrBonusMap()` in `mlb-optimizer.ts`. Bonus map is JSON-serialized as `Record<number, number>` in `MlbPreparedOptimizerRun` for incremental job persistence.

- **Pitcher Ceiling Boost** (`pitcherCeilingBoost`, `pitcherCeilingCount`): The top-N pitchers by ceiling score (K rate, outs, ER, opponent team total, projection, value) receive a search bonus during pitcher pair enumeration. Computed via `getMlbPitcherCeilingBadges()` and passed to `enumeratePitcherPairs()`.

- **GPP Blowup Candidates panel** (`dfs-client.tsx`): Client-side computed signal for low-salary batters with high GPP upside. Score = `(teamTotal / 4.5) × ceiling × value / 10`. Displayed above the player pool table for MLB slates. Excludes SP/RP and OUT players.

### Issues to Anticipate

| Issue | Mitigation |
|---|---|
| **Lineup confirmation latency** — batting order not posted until 3–4h pre-game | Store `batting_order = NULL` until confirmed; surface warning in UI for unconfirmed players |
| **Starter scratches** — SP can change within 1h of first pitch | Store "probable" vs "confirmed" status; add confirmation flag to `mlb_matchups` |
| **DK stat attribute ID differs from NBA 279** | Discover empirically on first test slate — inspect `draftStatAttributes` array |
| **LineStar MLB sport ID unknown** | Discover empirically — inspect network requests on a LineStar MLB slate |
| **Name matching — accents** | Normalize with `unicodedata.normalize('NFKD', ...)` before fuzzy matching (Acuña → Acuna) |
| **DK team abbreviation overrides** | MLB has more non-standard DK abbrevs than NBA — build `MLB_DK_ABBREV_OVERRIDES` map |
| **Park factors seasonality** | Update `mlb_park_factors` annually; Coors changes year-to-year based on humidor |
| **Doubleheaders** | Two distinct gamePks — both appear in slate; UNIQUE on game_id handles it. **Implemented for real 2026-07-07** after a rescheduled STL/MIL makeup game crashed every pipeline run: `mlb_matchups` row identity is now **game_id-first** (the old `UNIQUE(game_date, home, away)` slot constraint was dropped — it couldn't represent a split DH at all). `upsert_mlb_matchup` resolves by gamePk (a reschedule MOVES the row to its makeup date), adopts a single game_id-less orphan in the slot (odds-ingest rows carry no gamePk), else inserts. Odds + prop events resolve to the row with the NEAREST commence_time when a home team has two games. `fetch_scores` stamps final statuses too, clearing a stale 'Postponed' once the makeup completes |
| **pybaseball rate limiting** | Add sleep between calls; cache to local CSV before writing to Neon |
| **Season format** | MLB uses `"2025"` not `"2025-26"` — new `MLB_SEASON` constant |
| **DH slot** | All 30 teams use DH since 2022 — include DH in position list; DH maps to UTIL |
| **Showdown slates** | Detect via contest_format; block optimizer with clear error until Showdown mode built |
| **Weather** | Wind blowing out at Wrigley materially impacts run environment — add weather API or manual input |

### Phase Plan

| Phase | Scope | New/Modified Files |
|---|---|---|
| **P1 — Schema** | New MLB tables, `sport` col on dk_slates | `db/schema.py`, `web/src/db/schema.ts`, `db/queries.py` |
| **P2 — Teams + Schedule** | 30 MLB teams, schedule ingestion, Odds API | `ingest/mlb_teams.py`, `ingest/mlb_schedule.py` |
| **P3 — Stats Ingestion** | pybaseball batters + pitchers, EWMA | `ingest/mlb_stats.py` |
| **P4 — Slate Pipeline** | DK API reuse, MLB abbrev overrides, matchup linking | `ingest/mlb_slate.py`, minor changes to `ingest/dk_api.py` |
| **P5 — Projection Model** | Batter model, pitcher model, park factors | `model/mlb_projections.py` |
| **P6 — Web Actions** | MLB slate load, MLB-specific columns in queries | `web/src/app/dfs/actions.ts`, `web/src/db/queries.ts` |
| **P7 — Frontend** | Sport switcher, pitcher rows, stacking view | `web/src/app/dfs/page.tsx`, new components |
| **P8 — Optimizer** | MLB lineup slots, stacking + bring-back constraints | `web/src/app/dfs/actions.ts` optimizer section |

### Reuse Map

**Zero changes needed:**
- `dk_api.py` — DK API is sport-agnostic (no sport parameter)
- `compute_monte_carlo()` — works for any FPTS distribution
- `compute_leverage()` — works for any sport
- `compute_team_implied_total()` — same moneyline math
- `_ml_to_prob()` — same formula
- `_levenshtein()` — same fuzzy matching
- LineStar auth/cookie flow — only sport ID parameter differs

**Parameterize (small changes):**
- `config.py` — add `MlbApiConfig`, `sport_key = "baseball_mlb"` for Odds
- `linestar_fetch.py` — make `_SPORT` a parameter (not hardcoded `5`)
- `dk_api.py` — make `_POS_ORDER` a parameter; stat attribute ID per sport

**New files (parallel to NBA equivalents):**
- `ingest/mlb_teams.py` → analogous to `ingest/nba_teams.py`
- `ingest/mlb_stats.py` → analogous to `ingest/nba_stats.py` (uses pybaseball)
- `ingest/mlb_schedule.py` → analogous to `ingest/nba_schedule.py` (uses MLB Stats API)
- `ingest/mlb_slate.py` → analogous to `ingest/dk_slate.py`
- `model/mlb_projections.py` → analogous to `model/dfs_projections.py`

---

## Soccer / World Cup 2026 Expansion Plan

### Strategic Context

Target slate: **FIFA World Cup 2026** (`sport_key = "soccer_fifa_world_cup"` on The Odds API).
The tournament runs Jun–Jul 2026 across US/Canada/Mexico — DK runs heavy contest volume
during a World Cup, primarily **Showdown (Captain Mode)** single-match slates and some
multi-match **Classic** slates on busy group-stage days.

### Key Architectural Insight — Props-First, No Stats API Required (V1)

Unlike NBA (`nba_api`) and MLB (pybaseball/MLB Stats API), **soccer has no free first-party
player-stats API.** But we don't strictly need one for V1, because **The Odds API — already
integrated in this repo — exposes soccer player prop markets** that the NBA/MLB models do not have:

| Odds API market key | DK stat it feeds |
|---|---|
| `player_goal_scorer_anytime` | P(goal) → expected goals contribution |
| `player_shots_on_target` (O/U) | shots on target |
| `player_shots` (O/U) | total shots |
| `player_assists` (O/U) | assists |
| `player_to_receive_card` | card / negative points |

This is the same **"props replace the formula"** pattern already documented for NBA pts/reb/ast
and MLB. The market lines already embed matchup, form, and (post-lineup-release) starting status.
**So the projection model is built directly from props — a separate stats API is optional, not required.**

**Constraints on World Cup props (verified Jun 2026):**
- **US books DO post World Cup player props** — FanDuel and DraftKings both carry anytime
  goalscorer, shots on target, and assists for 2026 World Cup matches (confirmed: FanDuel had
  Pulisic +210 ATGS / +390 assist / +290 2+ SOT for USA vs Paraguay, Jun 2026). FanDuel/DK are
  in The Odds API **`us`** region, so use **`regions=us,uk,eu`** to maximize book coverage and
  get consensus lines. ⚠️ One thing still to verify in P0: a book offering a market on its own
  site does not guarantee The Odds API *surfaces* it in the feed — test one event-odds call.
- Player props are a **premium / "additional" market** on The Odds API and may require a paid
  plan tier. Confirm the `ODDS_API_KEY` plan returns player-prop markets before building (P0).
- Props are only available **per-event** via `/v4/sports/soccer_fifa_world_cup/events/{id}/odds`
  (the same event-odds pattern as `nba_schedule.py` / `actions.ts` already use), not on the
  bulk `/odds` endpoint.
- **No historical box-score stats** from The Odds API → calibration (`actual_fpts`) must come
  from a free supplement: **StatsBomb open data** (free, complete World Cup event data,
  `github.com/statsbomb/open-data`) or **football-data.org** (free tier, results only).

### DK Soccer Scoring & Roster — VERIFY EMPIRICALLY BEFORE CODING

⚠️ DK soccer scoring and roster structure must be confirmed on a live DK soccer contest page
before implementation — do not trust the values below blindly. As of last public DK rules,
**Classic** soccer scoring (approx):

```
Goal              +10      Shot On Goal      +1
Assist            +6       Created Chance    +1.5  (key pass)
Tackle Won        +1?      Pass Interception/Clearance/Block  (small +)
Goalkeeper Save   +1       Goal Allowed (GK/D)  −1 per
Penalty Save      +5       Win bonus (GK/D)  +
Card (Yellow)     −2       Red Card          −5
Clean Sheet (GK/D)  +     Cross  (small +)
```

**Classic roster** (8 players, $50,000 cap — confirm slot names on DK):
```
GK  D  D  M  M  F  F  UTIL     (UTIL = any non-GK outfielder)
```
**Showdown / Captain Mode** (6 players): `CPT ×1 (1.5× pts, 1.5× salary)  +  5× FLEX` —
the dominant World Cup contest format. Reuse the MLB-plan note: detect via
`contest_format = 'showdown'` on `dk_slates`; the optimizer must refuse until Showdown mode exists.

> The bulk of available data (anytime-goalscorer, shots, assists) maps cleanly to Classic batter-
> style scoring. **Goalkeeper and clean-sheet scoring is the hard part** — The Odds API has no
> save/clean-sheet props. GK projections will rely on team-defense odds (clean-sheet markets,
> match totals) rather than player props.

### Schema (parallel tables, mirrors MLB approach)

`sport TEXT` on `dk_slates` already exists. `dk_players` / `dk_lineups` are sport-agnostic.

- **`soccer_teams`** — 48 World Cup nations: `team_id | name | abbreviation | dk_abbrev | fifa_code | group | logo_url`
- **`soccer_matchups`** — `id | game_date | game_id (Odds API event id, UNIQUE) | home_team_id | away_team_id | vegas_total | home_ml | draw_ml | away_ml | home_implied | away_implied | clean_sheet_home_prob | clean_sheet_away_prob`
  (note the **3-way moneyline** — soccer has draws; implied-total math must handle draw probability, unlike NBA/MLB)
- **`soccer_player_props`** — per-slate prop snapshot: `player_id | game_id | name | position | team_id | goal_anytime_prob | shots_line | shots_on_target_line | assists_line | card_prob`
- **`soccer_player_stats`** (optional, calibration only) — from StatsBomb: `player_id | tournament | minutes | goals | assists | shots | sot | avg_fpts_pg | fpts_std`

### Projection Model — `model/soccer_projections.py`

Per-position, props-driven:
```
Outfielders (F/M/D):
  exp_goals   = goal_anytime_prob × 1.15      (anytime→expected uplift for multi-goal tail)
  exp_assists = assists_line       (or prob-implied)
  exp_sot     = shots_on_target_line
  exp_shots   = shots_line
  card_pen    = card_prob × (−2)
  FPTS = exp_goals×10 + exp_assists×6 + exp_sot×1 + created_chance×1.5 + card_pen
         + clean_sheet_prob×CS_bonus×(position is D)
Goalkeepers:
  FPTS driven by clean_sheet_prob (from match total / clean-sheet odds) + expected_saves
         (derived from opponent implied goals × shot volume) − expected_goals_allowed
```
Monte Carlo: reuse `compute_monte_carlo()`; `fpts_std` from StatsBomb history or a position-default.
**Boom threshold ~ 18 FPTS** (a single goal ≈ 10–16 pts; soccer FPTS distributions are
extremely low-mean / heavy-tail — leverage & ceiling matter more than in NBA).

### Stacking Strategy (soccer-specific)

- **Team correlation:** goals are rare and bunched — stack 2–3 attackers (F + attacking M)
  from a heavy favorite in a high-total match. Goal + assist often share the same two players.
- **Game stack** for high-total matches; **bring-back** one attacker from the opponent.
- **GK ↔ own-defender correlation:** clean sheet rewards GK *and* defenders simultaneously →
  GK + 1–2 D from the same team is a natural correlated block (the inverse of MLB's no-SP-stack rule).
- **Anti-correlation:** do not pair your GK/D with opposing attackers.

### Reuse Map

**Zero changes:** `dk_api.py` (sport-agnostic), `compute_monte_carlo()`, `compute_leverage()`,
`_ml_to_prob()`, `_levenshtein()`, the event-odds fetch pattern in `nba_schedule.py` / `actions.ts`.
**Parameterize:** `config.py` add `sport_key="soccer_fifa_world_cup"` + `regions="uk,eu"` + props markets list;
implied-total math must add **3-way (draw) handling**. **Name matching:** normalize accents with
`unicodedata.normalize('NFKD', ...)` — essential for international names (Mbappé, Güler, Şahin).

### Phase Plan

| Phase | Scope | New/Modified Files |
|---|---|---|
| **P0 — Verify** | Confirm DK soccer scoring + roster on a live contest; confirm `ODDS_API_KEY` plan surfaces World Cup player props via the feed (`regions=us,uk,eu`, FanDuel/DK books) | none (manual) |
| **P1 — Schema** | `soccer_teams`, `soccer_matchups`, `soccer_player_props`, optional `soccer_player_stats` | `db/schema.py`, `web/src/db/schema.ts` |
| **P2 — Teams + Schedule** | 48 nations, World Cup fixtures + 3-way odds via Odds API events endpoint | `ingest/soccer_teams.py`, `ingest/soccer_schedule.py` |
| **P3 — Props Ingestion** | Fetch player props per event (`regions=uk,eu`), upsert `soccer_player_props` | `ingest/soccer_props.py` |
| **P4 — Slate Pipeline** | DK API reuse, soccer abbrev overrides, match linking, Showdown detection | `ingest/soccer_slate.py` |
| **P5 — Projection Model** | Outfielder + GK models, Monte Carlo | `model/soccer_projections.py` |
| **P6 — Web Actions + Frontend** | Sport switcher → Soccer, props fetch button, GK/clean-sheet columns, stacking view | `web/src/app/dfs/actions.ts`, `dfs-client.tsx` |
| **P7 — Optimizer** | Classic GK/D/M/F slots + correlation stacking; Showdown CPT mode (shared w/ MLB showdown work) | `actions.ts` optimizer section |
| **P8 — Calibration** | StatsBomb `actual_fpts` backfill → feed `/analytics` | `ingest/soccer_results.py` |

### Open Questions / Risks

| Risk | Mitigation |
|---|---|
| **Odds API feed may not surface FanDuel/DK World Cup player props** | P0 — test one event-odds call with `regions=us,uk,eu&markets=player_goal_scorer_anytime,player_shots_on_target` before building anything |
| **Lineup confirmation latency** — World Cup XIs released ~1h pre-match | Mark props `confirmed=false` until starter status known; surface UI warning (same pattern as MLB batting order) |
| **GK / clean-sheet has no player props** | Derive from team clean-sheet & match-total markets, not player props |
| **DK soccer scoring drift** | Re-verify on each new DK contest — DK has changed soccer rules between tournaments |
| **Sparse contest calendar** | World Cup is ~6 weeks; treat as a seasonal module, not always-on |
| **Showdown is the dominant format** | Showdown optimizer mode is a hard prerequisite — coordinate with MLB Showdown work |

---

## Soccer Prediction Model (our own number vs the market)

Goal chosen 2026-06-13: **beat the market** — produce an independent game prediction
and surface over/under + win-probability edges where we disagree with Vegas. This is
the soccer analog of NBA `model/game_predictions.py` / `model/nba_game_total_model.py`,
which predict the Vegas miss from team-efficiency features and write `our_game_total_pred`.

### Why soccer needs a different model than NBA/MLB

1. **Low-scoring, discrete, with draws** → a linear "total points" Ridge is the wrong
   shape. Use a **bivariate Poisson goal model** that produces the full score matrix,
   which yields the O/U number AND 3-way win/draw/away probabilities together.
2. **Cold-start** — zero completed soccer games in the DB and no team-ratings table at
   tournament start. The strength signal must come from external history first, then
   update in-tournament as `soccer_matchups.home_score/away_score` fill in.

### Model

```
λ_home = exp(μ + home_adv + attack[home] − defense[away])
λ_away = exp(μ +           attack[away] − defense[home])      (home_adv=0 at neutral sites)
```
Score matrix P(i,j) from (λ_home, λ_away) + small Dixon-Coles low-score correlation →
```
our_total_pred = λ_home + λ_away          ← our O/U (direct NBA analog)
our_home_xg / our_away_xg                 ← our implied goals per side
our_prob_home / our_prob_draw / our_prob_away  ← our 3-way, vs market's vig-removed probs
our_prob_over_2_5, btts_prob              ← reused later for DFS ceiling / environment
```
**Market anchor:** shrink ratings toward the opening line so value comes from
*disagreement*, not from fighting a sharp market — same philosophy as the LineStar delta.

### Strength ratings (the "team stats" layer soccer lacks)

- **Elo** from historical international results — recency- and margin-weighted, +home_adv,
  tournament-importance K. Soccer analog of off/def rating.
- **Attack/defense coefficients** via Poisson regression on the same history with
  time-decay (Dixon-Coles). Fit with sklearn `PoissonRegressor` over a sparse team-dummy
  design matrix (2 rows per match: each side's goals). Ratings computed over ALL teams in
  history (opponents matter) but **stored only for teams in `soccer_teams`**.

### Data source (free, no auth — solves cold start)

- **`martj42/international_results`** (GitHub raw `results.csv`): every men's international
  1872→present (date, teams, scores, tournament, neutral). Cache to `data/` then train.
- In-tournament: a results fetch fills WC scores → ratings re-trained as games complete.

### Schema additions

- **`soccer_team_ratings`** — `team_id | elo | attack | defense | matches | rating_date` (P1).
- **`soccer_matchups`** new cols (P2) — `our_total_pred, our_home_xg, our_away_xg,
  our_prob_home, our_prob_draw, our_prob_away`. Mirrors `our_game_total_pred` on nba_matchups.

### New files (parallel to NBA)

- `ingest/soccer_results_history.py` → downloads/caches martj42 history (analog of stats refresh)
- `model/soccer_ratings.py` → Elo + Poisson attack/defense fit → writes `soccer_team_ratings`
- `model/soccer_predictions.py` → bivariate Poisson → writes `our_*` to soccer_matchups,
  called after `ingest.soccer_schedule` (analog of `game_predictions.predict_and_write`)

### Phases

| Phase | Scope | Status |
|---|---|---|
| **P1** | History ingest + Elo/attack-defense ratings + `soccer_team_ratings` | ✅ Done (2026-06-13) |
| **P2** | Bivariate Poisson `soccer_predictions.py` → write `our_*` columns | ✅ Done (2026-06-13) |
| **P3** | Frontend "Our vs Vegas" deltas + edge flags on the soccer Vegas view | ✅ Done (2026-06-13) |
| **P4** | In-tournament rating updates from completed scores; backtest our vs market vs actual | Planned |

Automation: `refresh_soccer.yml` (every 3h) fetches odds + writes predictions. Ratings manual/weekly.
Global params (`mu`, `home_adv`) persisted to `soccer_model_params` (DB) so CI predictions are self-sufficient.

---

## Soccer Betting Recommendations + Backtest Framework

Goal (2026-06-13): rate individual bets **1–5 stars**, keep an auditable running ledger,
and **backtest whether 4–5★ bets actually win at the rate we claim**. Traceability and
testability are first-class: every recommendation is reproducible (model version + frozen
input snapshot) and every outcome is settle-able.

### Bet types & market availability (verified against the live key)

| Bet type | Market source | Our model | Settlement |
|---|---|---|---|
| **Moneyline (3-way)** | h2h home/draw/away (consensus, prob-space averaged) | bivariate-Poisson 3-way, market-anchored (w=0.35) | from the **90-minute** score (`reg_home_score`/`reg_away_score`, `settle_game_bets`) — never the ET-inclusive final |
| **Total (O/U)** | totals over/under prices at the consensus line | Poisson(`our_total_pred`) on total goals, anchored (w=0.40) | from the 90-minute score; push on integer line = void |
| **First goal scorer** | `player_first_goal_scorer` (+ `player_goal_scorer_anytime` as a model input), per event, `regions=us,uk,eu` | Poisson superposition (below) | goal-events feed (TheSportsDB best-effort) + manual CLI |
| **Outright winner** | `soccer_fifa_world_cup_winner` / `outrights` (48 teams, DK + 4 books) | Monte Carlo tournament sim from Elo | champion from final score |
| **Group winner** | **No API market** — model-only | Same Monte Carlo (P finish 1st in group) | final group standings from scores |

Moneyline + totals are rated for **every game** by `model/soccer_game_bets.py` (`gameline-v1`),
with `event_commence = kickoff` so the ledger locks the rating pre-kickoff. Correctness
rules learned here: **(1)** average odds in probability space, never arithmetic American
(`prob_to_american`); **(2)** efficient single-game markets get a `longshot_odds_cap` (cap stars
on decimal odds ≥ 11/≥ 21) so a tiny model edge on a big price can't manufacture a fake 5★;
**(3)** never ingest scores or odds for a match that is (or may be) in progress — TheSportsDB
publishes `intHomeScore` live and the Odds API serves in-play prices after kickoff, so an
unguarded mid-match refresh froze Belgium 0-2 Senegal (true final: 3-2 aet) and settled all
56 bets on the game against a scoreline that never happened (2026-07-01). Guards: backfill
skips live-status / <3h-past-kickoff events; schedule ingest skips started events; predictions
skip started events; `fetch_scores` corrections also clear `winner_team_id` + reg scores and
reopen only ML/total/DNB (locked);
**(4)** knockout game bets settle on the **90-minute** score (`derive_regulation_scores`
rebuilds it from the `soccer_match_goals` timeline; TheSportsDB caps stoppage goals at the
period boundary, so minute ≤ 90 = regulation). ML/totals graded on an ET-inclusive final are
wrong at every sportsbook — Belgium 3-2 aet grades as ML Draw, total 4.

### Models

**First scorer (`firstscorer-v2`):** Poisson superposition with favorite-longshot-aware
de-vigging. v1 deflated favorites (Haaland ~13% vs true ~23%) because it built shares from the
RAW anytime market — books vig longshots harder, inflating the denominator. v2 uses the **power
method**:
```
anytime de-vig: solve exponent k_a so Σ_p −ln(1 − p_anytime^k_a) = our_total_pred (Λ).
                This removes vig AND anchors total to our match model at once.
                share_p = λ_p / Λ ;  our_model_first = share_p · (1 − e^(−Λ)).
first-scorer market de-vig: solve k_f so Σ p^k_f = 1 over players + "no goalscorer".
blend:  our_prob = 0.5 · our_model_first + 0.5 · market_fair.
```
Reference for edge = the power-de-vigged first-scorer market; EV uses the best offered price.
Glitch lines (implied > 0.45 — no player is >35% to score first) are dropped before de-vigging,
or one stale −10000 line corrupts the mutually-exclusive normalization. First-scorer offered odds
carry ~300–500% combined overround, so the model correctly rates ~all 1★ (avoid); the v2 value is
**calibration**, not finding bets. Re-rating clears UNLOCKED pending rows first (no orphans);
locked closing lines + settled rows are preserved.

**Futures (`futures-v1`):** Monte Carlo. Simulate group round-robins with the bivariate
Poisson match model (Elo-driven λ), rank, advance, then a strength-seeded knockout to a
champion; repeat N sims. Yields P(win tournament) and P(win group). The bracket pairing is a
documented simplification — championship probability is dominated by team strength, so it is
defensible; refine pairing when the full bracket is known. Group composition is **derived from
the loaded group-stage fixtures** (`soccer_groups`); group-winner bets activate only once a
clean 4-team group is available (no fabricated groups).

### Star rubric (deterministic → testable; shared engine, constants in `model/soccer_bet_rating.py`)

`rate_market()` / `rate_no_market()` are a single shared implementation, not a
per-sport reimplementation — soccer, MLB (`model/mlb_game_bets.py`), and tennis
(`model/tennis_bets.py`) all call the same two functions.

```
decimal_odds = full gross payout per 1 unit (incl. stake)
EV   = our_prob · decimal_odds − 1                      (only when a market exists)
edge = our_prob − reference_prob   (reference = vig-free market prob, else 1/num_options)

Market-based:                          No-market (e.g. group winner):
  5★: EV ≥ .20 and edge ≥ .04           5★: our_prob ≥ .45 and edge ≥ .15
  4★: EV ≥ .10 and edge ≥ .025          4★: our_prob ≥ .32 and edge ≥ .08
  3★: EV ≥ .03                          3★: edge ≥ .03
  2★: EV ≥ −.03                         2★: edge ≥ −.03
  1★: EV < −.03                         1★: edge < −.03

Guards applied after tiering, in order:
  1. Longshot floor: our_prob < .02 → cap at 3★ (tail calibration noise).
  2. Longshot odds cap (single-game markets only, `longshot_odds_cap=True`):
     decimal odds ≥ 21 → cap 2★; ≥ 11 → cap 3★ (stops a tiny edge on a huge
     price manufacturing a fake 5★). Not applied to futures — longshots are
     the legitimate value play there.
  3. Per-market hard cap (`max_stars=`), set only after a walk-forward backtest
     showed no edge — never a priori, always reversible per the pre-
     registration discipline elsewhere in this file:
```
| Sport | Market | Cap | Set | Why |
|---|---|---|---|---|
| Soccer | totals | 2★ | 2026-06-28 | no walk-forward edge |
| Soccer | moneyline / DNB | 2★ | 2026-07-01 | −36% ROI over 221 bets, all 3 gameline versions |
| Soccer | first-scorer | 2★ | 2026-07-01 | ≥2★ tier went 1/47 |
| MLB | moneyline / totals | 2★ | 2026-07-02 | holdout eval: ML logloss .6772 vs market .6717; totals MAE 3.36 vs 3.31 (see MLB Underdog-Value spec for the pending re-test) |
| Tennis | moneyline | self-capping | 2026-07-01 | `our_prob` set = market prob directly → edge≈0, no explicit `max_stars` constant needed |

Only **futures** (soccer outright/group winner) currently reach 4-5★ live — the
one component with demonstrated out-of-sample skill (Elo + Monte Carlo,
group-winner Brier .036 vs .188). Any 3★+ row on a capped market in the ledger
predates that cap — legacy audit history, never a live recommendation.

### Star rating improvement ideas — discussion only, not built (2026-07-05)

The rubric above is deterministic but blunt: it treats every market as
uniform, treats every edge estimate as equally reliable, and treats
calibration as a read-only dashboard rather than a control system. Three
improvements were discussed and are recorded here for later spec work — **no
code has been written, and none of this is scheduled**:

1. **Segment-aware caps (do first).** A whole-market cap (`max_stars=2`) is
   safe but crude — it can't express "this market is capped except this
   validated slice." The MLB Underdog-Value spec already showed why this
   matters: the whole moneyline market shows no broad edge, but the
   underdog side specifically showed a real win-rate gap the current
   all-or-nothing cap can't act on without reopening the entire market. A
   segment-aware cap (favorite/underdog, home/away, odds range, league)
   would let a validated slice surface without exposing the rest of the
   market to noise — generalizing the MLB spec's P5 idea ("raise the cap
   for this specific tier only") directly into the rubric. **Guardrail:
   every segment needs its own pre-registered minimum-sample floor before
   it can carry a higher cap** — segmentation without a sample floor is
   exactly how a false discovery gets manufactured from a slice, the same
   failure mode the MLB spec's team-concentration check exists to catch.

2. **Sample-size / confidence-aware edge (do second).** `edge = our_prob −
   ref_prob` is currently a pure point estimate — a mature, thousands-of-
   games Elo rating and a five-game prop line produce identically-treated
   edges even though one is far more trustworthy. A confidence-discounted
   edge (raw edge × a confidence factor from sample size/variance) would
   shrink thin estimates toward zero before they can cross a 4-5★
   threshold — the Bayesian-prior-updating Model Improvement Roadmap item,
   applied to the star gate instead of only the projection layer. This
   becomes more important, not less, once #1 ships: segmentation by
   construction shrinks the sample behind each slice, which is exactly
   where a thin, noisy edge is most likely to masquerade as a real one.

3. **Calibration as an enforcement layer, not a dynamic threshold engine (do
   third, only after #1-#2 exist).** The `/vegas` panels already compute
   realized-vs-expected win rate and CLV per star tier, but it's purely
   descriptive — the fixed EV cutoffs (.20/.10/.03) never respond to it.
   Letting a market/segment's own calibration history auto-**downgrade** it
   (N consecutive windows of a 4-5★ tier underperforming expected win rate
   or CLV → auto-cap to 3★ until revalidated) is safe. Letting the system
   auto-**uncap** or move thresholds upward on its own is not — that would
   be reacting to short-term variance in exactly the way this file's
   pre-registration discipline exists to prevent. Auto-cap-on-drift only;
   uncapping or raising a threshold still requires a fresh, separately
   pre-registered study, same as every other cap change recorded in this
   file.

**Sequencing matters**: #1 and #2 come before #3. A calibration-enforcement
layer reacting to segments that were never sample-checked or confidence-
discounted would let the enforcement layer chase noise instead of signal —
the same class of mistake this file's specs have already caught repeatedly
(soccer totals mirage, MLB odds-repair contamination, tennis multiple-
comparisons risk). Status: discussion only — no implementation phase, spec,
or kill criterion exists yet for any of the three.

**Honest assessment of expected impact (2026-07-05):** none of the three
ideas above are edge-discovery mechanisms — they are a precision/governance
layer on top of whatever edge already exists, and this project's own
settled-ledger history says that's very little (every tested game-line
market — soccer ML/totals/first-scorer, MLB ML/totals, tennis ML — has
independently confirmed no edge).

- **Segment-aware caps**: nothing to promote today. The only candidate
  segment in the pipeline (MLB underdog moneyline) is still INCONCLUSIVE
  (n=125, needs ≥200). Realistic near-term deliverable: zero to one narrow
  rule, not a general reopening of any capped market.
- **Confidence-discounted edge**: makes the system MORE conservative, not
  more profitable — it suppresses thin-sample false positives, so the
  expected effect is FEWER 4-5★ ratings on early-season/small-sample
  inputs, not more winning bets. Deliverable is a trust/quality signal
  (a visible confidence measure per rating) and a tighter calibration
  curve, not new picks.
- **Calibration enforcement**: purely defensive — catches decay in weeks
  instead of months (the failure mode that already happened twice: the
  soccer totals mirage and the MLB odds-averaging bug). Deliverable is an
  audit trail of automatic downgrades, not new profit.

**Bottom line**: expected volume of new actionable bets from this work is
low — plausibly zero to one narrow rule near-term. The Edge-Finding
Roadmap already identified that soft/illiquid markets (MLB props,
first-scorer) are where real signal has actually shown up (8 alerts vs. 0
on game lines) — this rubric work is worth doing for rigor and to prevent
repeat failures, but is not the highest-leverage path to finding new edge.

### Traceability / accountability design

- **`soccer_bets`** — the running ledger. One row per (bet_type, scope, selection, model_version),
  upserted each run. Carries market odds/prob, our_prob, edge, EV, stars, `inputs_json` (frozen
  model inputs), `status` (pending/won/lost/void), settlement. **Rows lock at kickoff**
  (`event_commence`) so the backtest uses the closing recommendation we actually committed to —
  no post-hoc edits.
- **`soccer_bet_snapshots`** — append-only audit trail: every refresh writes each selection's
  (stars, our_prob, market_prob, edge, ev, capture_key, captured_at). Full lineage of how a
  recommendation evolved.
- **`model_version`** stamped on every row; bump it when a model changes so old and new
  recommendations never silently mix in the backtest.

### Backtest (the headline metric)

Calibration by star tier on **settled** bets:
```
for each star tier: n, expected_win_rate = avg(our_prob), realized_win_rate = wins/n,
                    ROI = Σpayout / Σstake (market bets), Brier score
```
The 4–5★ rows are the focus: realized win rate should ≥ expected. First-scorer supplies the
volume (≈20 selections × 60+ games) for statistically meaningful calibration; futures are few
but high-value. Surfaced on `/vegas?sport=soccer` (Bets + Backtest panels) and re-runnable.

### Files

- `db/schema.py` — `soccer_bets`, `soccer_bet_snapshots`, `soccer_groups`
- `model/soccer_bet_rating.py` — vig removal, EV, star rubric, ledger upsert + snapshot + locking
- `ingest/soccer_props.py` — fetch first-scorer + anytime player markets per event
- `model/soccer_first_scorer.py` — superposition model → rate → ledger
- `model/soccer_futures.py` — Monte Carlo sim + outright market → rate → ledger; derives `soccer_groups`
- `ingest/soccer_results.py` — Odds API `/scores` → settle match/group/outright; first-scorer settle (TheSportsDB + manual)
- `web` — `getSoccerBets`, `getSoccerBetBacktest`; Bets + Backtest panels on the soccer Vegas view
- `refresh_soccer.yml` — add rating + settlement steps

### Known limitations (documented for accountability)

- **First-scorer settlement** has no guaranteed free feed; TheSportsDB is best-effort, manual CLI
  is the fallback. Until settled, those bets stay `pending` and are excluded from the backtest.
- **Group winner** has no market line → EV is null; stars come from edge over the 1/4 baseline.
- **Futures sample is tiny** (1 champion, 12 group winners) → low-power calibration; first-scorer
  carries the statistical weight.
- **Knockout bracket pairing** is simplified (strength-seeded), not the exact FIFA bracket.

---

## Edge-Finding Roadmap (2026-07-02)

Three sports of settled-ledger evidence (soccer −36% ROI on ML/DNB value tiers; tennis
walk-forward failure; MLB models losing to market on their own holdout evals) established
that we cannot out-predict closing lines with public stats. All game markets are capped at
2★. The path to a real edge is **structure, not prediction**: weak benchmarks, slow prices,
information latency, and disciplined measurement. Priorities:

| Priority | Initiative | Why | Status |
|---|---|---|---|
| **P1** | **CLV harness** (`model/clv_report.py`) + **per-book capture** (`game_odds_history.books` JSONB, Pinnacle via eu region for MLB) + **line movement** (`model/line_movement.py` CLI and Line Movement panels on the MLB, soccer, and tennis vegas views; soccer + tennis write the same per-book trail — 3h/6h cadence — and tennis gained bet snapshots so all three ledgers are CLV-measurable) | Closing Line Value converges ~10× faster than ROI (every bet scores, win or lose). For each bet: entry = first snapshot, close = last pre-kickoff snapshot; did the market move toward our number? Slices by sport/market/stars/model_version. The instrument every other idea is measured with. | ✅ Done (2026-07-02) |
| **P2** | Pre-registered studies: (a) MLB underdog anomaly (65 stored 5★ bets span the whole season, not 4 days as first thought; 29% were stale pre-repair EV; honest n=125 shows a real win-rate gap but ROI/split-half/min-sample don't clear the bar — full result in "MLB Underdog-Value Investigation" below); (b) opener-vs-closer (does our disagreement with the 13:10 open predict movement by close?) | Hypothesis + eval rules written BEFORE looking at data — the discipline that caught the soccer-totals mirage. Walk-forward only, post-odds-fix data only. | (a) INCONCLUSIVE, cap stays, revisit at n≥200 (2026-07-05); (b) Planned |
| **P3** | Soft markets: **MLB props live** (`ingest/mlb_prop_odds.py` — pitcher K + batter TB per-book 3×/day; `dk_prop_value` EV≥3% same-line + `prop_line_gap` ≥1.0 detectors into the alert ledger with ROI @ DK; settled from free MLB boxscores). First scan: 8 alerts vs 0 on game lines — DK's prop board is where it goes stale. WC anytime-scorer added same day: Pinnacle posts no WC player props, so the anchor is the overround-NORMALIZED market median (raw medians flagged 24% of the board — book-margin artifact; normalized flags ~4%); settles from the goal timeline, 90-minute rule, DNP-as-loss conservative bias documented. NBA props at season start. **WC anytime-scorer RETIRED 2026-08-13** — confirmed loser, see "Soccer Anytime-Scorer Detector — Retired". | ✅ MLB (2026-07-02); WC retired |
| **P2b** | **Sharp line alerts** (`model/line_alerts.py` + `line_alerts` table + Alerts panel on all three vegas views) | Pinnacle-divergence (≥2pp) and multi-book steam (≥3 books, ≥1.5pp) detectors run after every capture. Each alert is an IMMUTABLE ledger row frozen at trigger, then audited: clv_pp (did the market close toward the flagged side) + outcome (soccer graded on the 90' score). Telegram push if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID secrets are set — the ledger never depends on delivery. An alert type with no positive CLV is noise; the backtest panel says so. | ✅ Done (2026-07-02) |
| **P4** | Information latency (lineups, weather, injuries) — event-driven capture | Only if P1 shows we're directionally right but late. 30-min cron cadence cannot exploit minute-scale news. Don't build speculatively. | Gated on P1 |
| **P5** | Execution: best-price line shopping across books in every ledger | Worth 1–3%/bet with zero predictive skill; first-scorer already uses best offered price. Makes every edge study realistic. | Planned |

**Discipline rules (non-negotiable):**
- No fourth gameline revision; no new models from public season stats vs closing lines.
- Every hypothesis pre-registered (metric, slice, sample size) before data is examined.
- Walk-forward or CLV only — in-sample win rates are how the totals mirage happened.
- MLB CLV uses probability movement only for pre-2026-07-02 snapshots (entry odds in old
  snapshots carry the arithmetic-averaging corruption; bets table was repaired, snapshots kept
  as-recorded).
- Star caps are reversible per market the moment a slice demonstrates walk-forward edge.

---

## Detector Health Check — dead-detector auto-flagging (2026-08-17)

**Why this exists.** Manually auditing tennis's win-rate table surfaced that
`scan_tennis_totals` had run every cycle since it shipped 2026-07-02 and
produced **zero alerts, ever** — 0 of 2,891 DraftKings captures for tennis
have ever carried a `total_line` (DK only quotes tennis moneyline; totals/
spreads there come from Pinnacle + ~25 other books, never DK). The detector's
own guard (`if dk.get("total_line") is None: continue`) skipped silently,
forever. A "ran fine, found nothing today" cycle and a "ran fine, is
structurally incapable of ever finding anything" cycle are indistinguishable
in logs — same exit code, same "0 new alerts" line — which is exactly the
same failure class as several other bugs in this project's history (the
`AZ`/`ARI` team-abbreviation gap, `_selection_prices` hardcoded to
DraftKings, the DeepSeek workflows missing `httpx` and failing every
scheduled run): an untested assumption from one context (DK covers this
market) silently carried into a new one (tennis) that never actually held.

**What it does.** `check_detector_health()` in `model/line_alerts.py`
classifies every registered `(sport, alert_type)` as:
- `too_new` — deployed < 14 days ago, not enough time to judge yet.
- `no_opportunity` — the sport has had zero eligible game captures in the
  last 14 days (e.g. soccer between World Cups) — a dormant sport is never
  mistaken for a broken detector.
- `dead` — deployed ≥ 14 days ago, the sport HAS had games in the last 14
  days, and the detector has produced **zero alerts, ever**. This is
  precisely the `scan_tennis_totals` shape.
- `active` — has fired at least once.

`DETECTOR_REGISTRY` pins `deployed_at` to the date the CODE shipped (read
from git history), not the date of the first alert — using first-alert-date
as a proxy would systematically make every detector look newer than it is
and under-flag real bugs; a detector that took 12 days to find its first
legitimate signal is healthy, one that never has isn't, and only a code-ship
date tells them apart.

**First real run found 4 dead detectors**, not just the one that motivated
this: `tennis/dk_prop_value`, `tennis/prop_line_gap` (the known
`scan_tennis_totals` bug, 46 days), plus **`tennis/pinnacle_polymarket_delta`
and `nfl/pinnacle_polymarket_delta`** (both 0 alerts ever, 16-17 days
deployed) — Polymarket has never appeared in a single tennis capture (0 of
3,192) or fired for NFL either; only MLB's Pin/Poly delta has ever produced
alerts (n=7). **Cause established 2026-08-24 and fixed:** the paid `us_ex`
region returns Polymarket for MLB (89.4% of captures) but never for tennis
(0/2,838) or NFL (0/1,685), so those two detectors were starved of the input
they exist to compare. `us_ex` was dropped from both — see
[`docs/the-odds-api.md`](docs/the-odds-api.md). Tennis still gets Polymarket,
free, via `ingest/polymarket_tennis.py`. Correctly did NOT flag soccer's detectors, which are legitimately
quiet post-World-Cup (`no_opportunity`), proving the eligibility gate works.

**Delivery: web UI only**, per explicit user choice — a `Detector Health`
panel (`web/src/app/vegas/detector-health-panel.tsx`) on all four
vegas-family pages (`/vegas?sport=tennis`, `/vegas?sport=soccer`,
`/vegas?sport=mlb`, `/nfl`), reading `getDetectorHealth(sport)` in
`web/src/db/queries.ts`. Telegram push was considered and declined —
`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` aren't configured as GitHub secrets,
so a push channel would ship silently non-functional. `report()`'s CLI
output (`python -m model.line_alerts --report`) also prints a Detector
Health section first, above the existing CLV/win-rate tables.

**Maintenance note:** `DETECTOR_REGISTRY` is duplicated between
`model/line_alerts.py` (Python, source of truth for `report()`) and
`web/src/db/queries.ts` (TypeScript, source of truth for the web panel) —
same dates, same thresholds, kept in sync by hand, no shared source of truth
across that language boundary. Adding a new detector requires updating both.

**Cross-sport status page — `/vegas/detectors` (2026-08-17).** The four
per-sport panels solved "is this specific detector dead" but not "is
anything dead, anywhere" — that still required visiting all four Vegas
pages in rotation. `getAllDetectorHealth()` (queries.ts) computes health for
every registered `(sport, alert_type)` in one pass (one query pair per
sport, not per detector) and `/vegas/detectors` renders it as a single
scannable status grid, dead detectors surfaced first under "Needs
attention," grouped by sport below that. Deliberately **three colors, not a
literal red/green binary** — red (dead), green (active), gray (too_new /
no_opportunity) — collapsing the two gray cases into either red or green
would throw away the exact distinction `check_detector_health()` exists to
make (a young detector or a dormant sport is not the same as a broken one).
The per-sport panels stay; this page is the "check this first" entry point,
not a replacement — the per-sport panels sit next to the actual alerts/
backtest for that sport, which is real context this page doesn't have.
Linked from the nav (`sport-nav.tsx`, no `sports` filter so it's visible
regardless of the active sport tab). Verified live: 4 dead / 19 active / 5
not-yet-judged, matching the CLI report exactly.

---

## MLB Health Gates Starved the Prop Pipeline — Found + Fixed (2026-08-24)

**Symptom:** the MLB prop board showed no live plays. The old prop page hid
this — it renders every alert it fetches with no notion of whether the game has
started, so a wall of finished-game rows read as a populated board. The
redesigned board filters on `commence_time` and therefore reported the truth:
nothing was live, because almost nothing was being captured.

**Root cause #1 — the bullpen miscount (fixed).** `model/mlb_data_health.py`'s `bullpen_snapshots` gate
counted `COUNT(b.id)` over a plain `LEFT JOIN mlb_bullpen_snapshots` and
demanded **exact equality** with `games * 2`. But that table is append-only with
`UNIQUE(matchup_id, team_id, raw_checksum)` — a team-game legitimately gains a
new row every time the underlying relief data revises. One extra append made it
`31/30` and hard-failed.

**Why that killed props.** `refresh_mlb_vegas` exits non-zero on any health
failure, and the next steps carry no `if:`, so they inherit `success()`:

```
6  Refresh MLB Vegas schedule, scores, and odds   FAILURE
7  Capture MLB player-prop odds                   SKIPPED
8  Scan + settle sharp line alerts                SKIPPED
9  MLB prop-value program status                  SKIPPED
```

**19 of the last 30 scheduled runs** failed, going back to 2026-08-13. Broken
out by cron slot, the pattern is diagnostic — and shows the bullpen miscount is
only part of it:

```
cron slot      pass  fail
  13:10 UTC      8     2     first run of the day: writes the initial 30, passes
  17:10 UTC      3     7     an extra append has landed -> 31/30 -> fails
  22:10 UTC      0    10     never once succeeded
```

So the registered 3x/day cadence has in practice been **~1.1 captures/day, at
9:10am ET** — the thinnest hour for player-prop coverage, since books post most
prop markets closer to first pitch. The two captures that would see a real prop
board are the two being skipped.

**The fix** counts coverage, not rows:
`COUNT(DISTINCT (b.matchup_id, b.team_id)) FILTER (WHERE b.id IS NOT NULL)`.
This is not a loosened gate — it makes the check measure what its own message
always claimed ("team-game bullpen snapshots"), and it brings the check in line
with its two siblings: the schedule and weather gates already collapse to one
row per game via `LEFT JOIN LATERAL ... LIMIT 1`. Bullpen was the only one of
the three that counted raw rows. Validity is untouched: `empty_quality` and
`post_start_snapshots` still scan **every** row, so a bad appended revision
cannot hide behind a covered team-game.

The `FILTER` is load-bearing. Without it Postgres counts `ROW(NULL, NULL)` from
the unmatched side of the LEFT JOIN as a distinct value and reports 1 for a date
with no snapshots at all. Verified against a real PostgreSQL 16 instance, not
only the mocked unit tests: reproduced production's exact `31/30`, confirmed the
fix yields `30/30`, confirmed a genuinely uncovered team-game still fails, and
confirmed the zero-snapshot date returns 0 rather than 1.

**Root cause #2 — post-start provenance (FIXED, separately). The 22:10 slot was
a second, independent bug; the bullpen fix alone did not restore it.**
Stated plainly because the first version of this note implied otherwise. That
run fails three gates, not one:

```
bullpen_snapshots     FAIL  36/30 team-game bullpen snapshots   <- root cause #1
schedule_provenance   FAIL  4 latest revisions are post-start   <- root cause #2
weather_provenance    FAIL  4 latest forecasts are post-start   <- root cause #2
```

The cause is structural. At 22:36 UTC it is 6:36pm ET and games are underway —
the same log records "2 in-play games skipped — closing lines frozen". The
refresh re-captures schedule and weather for **every** game on the date,
including in-progress ones, so the latest revision/forecast for those games is
legitimately post-start. Both gates use `LEFT JOIN LATERAL ... LIMIT 1` to take
the LATEST row, correctly judge it unusable for a pregame decision, and then
fail the whole run — which kills prop capture for the games that have **not**
started yet. Same class as the bullpen bug (a gate blocking unrelated downstream
work), different mechanism: not a miscount, but failing the day's health over
data that only ever concerned games already gone.

**The fix** judges each game on the latest revision/forecast available **before
its own commence time**: both `LEFT JOIN LATERAL`s now carry
`AND <capture>.available_at < m.commence_time`, so a post-start row cannot be
selected at all. A post-start row existing is not a defect; a decision built on
one would be, and the query can no longer build one. The real failure — no
pregame capture exists for a game — moves to `schedule_revisions` /
`weather_forecasts`, which now read `revisions == starts` rather than
`== games`.

Denominator is `starts`, not `games`, deliberately: a game with no commence time
cannot be judged pregame at all and is already owned by `schedule_starts`.
Counting it in the provenance gates too would report one defect as three. The
now-impossible post-start clause was dropped from `invalid_forecasts`; the
completeness checks (`source_status`, `provider_issued_at`, `valid_at`,
`source`/`raw_json`) are untouched and still enforced on the pregame row.

Verified against a real PostgreSQL 16 instance, again rather than only the
mocked tests: reproduced production's exact "4 latest revisions are post-start"
under the OLD query, confirmed the new query returns 15/15 with 0 invalid on the
same fixture, confirmed a game whose ONLY capture is post-start still fails
15/16, confirmed a pregame revision missing `raw_json` still fails, and confirmed
a NULL-commence game fails `schedule_starts` alone without double-reporting.

**Cadence: do NOT increase it (decided 2026-08-24).** The question came up
naturally — three captures a day looks thin. Reasons it is the wrong lever now:

1. **The registered cadence has never actually run.** Nominal 3x/day, effective
   ~1.1x/day. Tuning a knob that has never been set is guessing; restore 3x/day
   and measure that first.
2. **It would add a third regime to an already-contaminated cohort.**
   `mlb-prop-program-v1` was registered 2026-08-15; the outage runs from at
   least 2026-08-13. So **every enrolled observation was accrued at ~1x/day,
   morning-only, and none has ever seen an evening prop board.** The program's
   own analysis treats capture cadence as a covariate and forbids a
   mixed-cadence median. Whether the pre-fix observations may pool with post-fix
   ones is an open question that belongs to the program, not to a bug fix —
   under this file's standing rule, a change of this kind is a NEW program
   version with its own registration, never a silent continuation.
3. **Credits.** 4 markets x 10 books x ~15 events = 60/run -> 180/day ->
   ~5,400/30d against a ~20,000/month shared key that game-line capture also
   draws on. 6x/day is ~10,800/30d — over half the quota — to chase a signal
   this file already prices at $700–$3,700/year.
4. **Cadence may not even be the binding constraint.** The ledger records
   observed quote persistence (`dk_survival_min` + interval bounds). If a
   flagged DK quote survives hours, 3x/day is sufficient; if it vanishes in
   minutes, more cadence finds alerts that were never capturable — the
   "stale != available" caveat recorded elsewhere in this file. That data
   answers the question empirically once the pipeline actually runs.

**Higher-value than any cadence increase: fixing the 22:10 slot** — done above.
It is the capture closest to first pitch, when prop boards are fullest, and it
had produced nothing at all. Going 2 -> 3 working captures a day by repairing the
post-start gates beats going 3 -> 6 nominal captures that fail the same way. Once
three real captures a day have actually run for a while, re-ask the cadence
question against observed quote persistence — not before.

**Standing rules from this:**
- **Never gate on `COUNT(rows)` from an append-only table.** Count the entities
  the check claims to be about. Every provenance table in this repo
  (`mlb_bullpen_snapshots`, `mlb_weather_forecast_snapshots`,
  `mlb_schedule_revisions`, `mlb_prediction_runs`, `line_alerts`) grows by
  design; an equality gate over one is a time bomb that fires the first time the
  data legitimately revises.
- **A health gate that blocks unrelated downstream work is a second bug.** Prop
  capture does not read bullpen data, yet a bullpen gate silently disabled it
  for weeks. Decoupling steps 7-8 from the game-line refresh is a real remaining
  question and is deliberately NOT changed here — running the prop scan on a
  slate whose odds failed to refresh has its own risk, and that is a judgment
  call, not a bug fix. Note this bug appeared **twice** in one incident, via two
  independent gates (bullpen count, post-start provenance), which is the
  argument for decoupling rather than for fixing gates one at a time.
- **Fixing the failure you found is not the same as fixing the outage.** The
  bullpen miscount was real, but it accounted for the 17:10 slot only; the 22:10
  slot was dead for an entirely different reason and stayed dead until root
  cause #2 was fixed separately. Before declaring a scheduled job restored,
  check a failing run **from each slot** — the slots fail at different times of
  day and therefore under different data conditions.
- **A pregame gate must ask "before THIS event", not "latest row".** Both bugs
  here reduce to a query answering a subtly different question than the check
  claimed. Any gate over a table that keeps accumulating rows through the day
  needs an explicit per-entity time bound, or it will eventually judge a pregame
  decision on data that postdates the event.
- **A UI that hides staleness hides outages.** The failure was weeks old and
  invisible because the original prop page could not distinguish a live alert
  from a finished one. The empty board was the redesign working.

---

## Pre-Registered Studies — line_alerts attribution engine (2026-07-03)

The alert ledger is now a market-disagreement attribution engine (instrumentation
solved: independent closes, path-aware movement, magnitude preservation, interval-
censored persistence, explicit proposition comparability via `comparison_status`,
classifier version stamp `grading_version`). The remaining risk is **interpretation
drift** — a rich attribution engine becomes a story generator if hypotheses are
formed after seeing the data. These comparisons are pre-registered: metric, slice,
and minimum sample fixed BEFORE the data is examined. No conclusion is drawn, and no
star cap is changed, until the stated sample is reached, walk-forward.

### Study 1 — fate of DIVERGENCE_PERSISTED (SAME_PROPOSITION only)
- **Population**: settled `dk_value`/`dk_prop_value` alerts with `comparison_status =
  SAME_PROPOSITION` and `convergence = DIVERGENCE_PERSISTED` (gap neither closed nor
  widened past ε; DK and reference both held).
- **Metrics**: frozen-price ROI @ DK; calibration (Brier vs price-implied prob);
  realized win rate vs price-implied; mean reference CLV; mean execution CLV.
- **Pre-specified outcomes**: (a) zero CLV + zero excess return → noise (winners are
  variance, e.g. Sullivan); (b) zero CLV + positive calibrated return → the reference
  market systematically misses something; (c) return concentrated in a specific
  sport/market → localized counterparty weakness, not a general edge.
- **Minimum sample**: ≥ 30 settled, and treated cautiously if correlated (same slate).
  No verdict before then; the report tags every under-floor row `descriptive-only`.

### Study 2 — identification: which trigger-time features predict a real AND capturable discrepancy?
- **Candidate predictors (fixed now, before slicing)**: initial same-line gap (pp);
  claimed EV; book × market type; number of reference books; reference-market
  agreement; time-to-event; capture cadence; observed quote-persistence interval;
  convergence path; odds range; price-disagreement vs line-disagreement origin.
- **Targets**: (i) reference CLV (real), (ii) frozen-price ROI among persisted quotes
  (capturable). Reported as the three-verdict monotonicity table (signal /
  tradability / decay) — the three are NOT required to agree; each combination has a
  distinct pre-specified meaning (real-but-fleeting, execution-lost, false-discrepancy…).
- **Discipline**: walk-forward only; interval-censored survival stratified by cadence
  (never a mixed-cadence median); ratio nulled below `min_gap_for_ratio_pp`; absolute
  pp movement always outranks the closure ratio.

### Non-negotiables (inherited from the gameline/totals mirages)
Convergence is a PATH classification, never a quality verdict. `REFERENCE_CONVERGED_
TO_EXECUTION` is *evidence* DK may have led price discovery, not proof. Regrading under
a later `grading_version` is legitimate; erasing which rule produced the original label
is not. A memorable winner never promotes a signal — only repeated excess performance
relative to the frozen odds does.


---

## Plain-Language Guide — the alert attribution engine (what & why)

Analogy: BettingPros says *"this bet is a bargain."* This system is the strict
detective that asks *"was it really a bargain, was it still available, and who
changed their mind afterward?"* Everything below is implemented in
`model/line_alerts.py` (grading) + `db/schema.py` (`line_alerts`, `alert_grades`)
and surfaced in `python -m model.line_alerts --report`.

1. **Freezes the original bet.** On trigger, one immutable `line_alerts` row saves
   DK's offered price, Pinnacle's fair value, claimed edge, exact time, eventual
   result, and frozen-price profit. First-breach only — re-scans never rewrite it.
   (Photograph the price tag before the store changes it.)

2. **Saves Pinnacle's close separately.** `pin_close_prob` (reference close) is
   stored as its own quantity, distinct from DK's close (`dk_close_decimal`) — so
   "DK corrected", "Pinnacle moved toward DK", and "neither moved" are different
   stories, not one blurred CLV number.

3. **Watches who moved toward whom.** `convergence` classifies the full alert→close
   price path: `EXECUTION_CONVERGED_TO_REFERENCE` (DK→Pinnacle),
   `REFERENCE_CONVERGED_TO_EXECUTION` (Pinnacle→DK), `BOTH_MOVED_TOWARD_BET`,
   `BOTH_MOVED_AGAINST_BET`, `DIVERGENCE_PERSISTED`. A PATH label (who moved), NOT
   a quality verdict. `REFERENCE_CONVERGED_TO_EXECUTION` is *evidence* DK may have
   led price discovery, not proof.

4. **Measures how much the disagreement changed.** `grading_json` stores
   `gap_initial_pp`, `gap_final_pp`, `gap_abs_closure_pp`, `gap_max_closure_pp`,
   `d_dk_pp`, `d_pin_pp`, `n_captures`, and the **gap-closure ratio**
   (`(|initial|−|final|)/|initial|`). Label says who moved; ratio says how much
   disagreement disappeared.

5. **Suppresses misleading ratios on tiny gaps.** Below `min_gap_for_ratio_pp`
   (1.0pp, separate from the 0.5pp categorical `movement_epsilon_pp`) the ratio is
   NULLed (`gap_closure_ratio_suppressed=true`) so 0.10→0.02pp doesn't read as 80%
   closure. Absolute pp movement is always recorded and outranks the ratio.

6. **Checks same-proposition before comparing.** `comparison_status` ∈
   {`SAME_PROPOSITION`, `DIFFERENT_LINE`, `NO_REFERENCE`, `RULE_MISMATCH`,
   `INSUFFICIENT_CAPTURE`}. Only `SAME_PROPOSITION` (same player/stat/line/side/
   settlement) can receive convergence — Over 1.5 and Over 2.5 are not the same
   bet. Prevents apples-to-oranges (the Herrera fix, made structural).

7. **Admits when it didn't observe enough.** A single pre-game snapshot can't show
   movement → `INSUFFICIENT_CAPTURE`, convergence NULL. The system says "I don't
   know" instead of inventing a story (the Sullivan reclassification).

8. **Measures whether DK's price survived** — "observed quote persistence"
   (`dk_survival_min` + interval bounds), NOT verified availability. We poll; we do
   not press the bet button. Honest, weaker, measurable.

9. **Stores a survival RANGE, not a fake exact minute.** Interval-censored:
   `survival_lower_min` / `survival_upper_min` / `last_same_at` /
   `first_changed_at` in `grading_json`. The true change lies between two checks;
   never summarized with a cadence-mixing median.

10. **Three independent verdicts** in the tier table: **signal** (do larger claimed
    edges produce better reference CLV?), **tradability** (frozen-price ROI among
    surviving quotes), **decay** (do the biggest edges disappear faster?). They are
    NOT required to agree — each combination has a distinct pre-specified meaning.

11. **Reports small samples honestly.** Below `_MIN_SETTLED_FOR_CI` (30 independent
    bets) the report shows raw W-L-P + profit + a `descriptive-only` tag, never a
    rate implying a conclusion. Correlated same-slate props carry less information
    than the count suggests.

12. **Explicit persisted-to-close denominator.** Reported as `numerator/
    denominator` where the denominator = alerts with an OBSERVABLE comparable close
    (`close_status = OBSERVED`). `MARKET_CHANGED` (line moved) and
    `CLOSE_UNAVAILABLE` are reported separately, never flattered into the numerator
    or silently dropped.

13. **Append-only grade history** (`alert_grades`): every grade event stored
    permanently; a later method ADDS a grade rather than erasing the old one.
    Answers: what did the system conclude then, what does the new method conclude,
    did the change come from new data or new rules. (Keep every answer-key version.)

14. **Every grade stamped with its rule version** (`grading_version =
    "convergence_v2"`) so old/new grades compare fairly.

15. **Regrading is idempotent** — `_append_grade_history` only writes a new row when
    (version, comparison_status, convergence, outcome) meaningfully changed; a
    re-run doesn't duplicate.

16. **NULL carries information.** A blank convergence field shows its reason
    (different line / no reference / rule mismatch / too few observations) — `NULL`
    means "this conclusion is not permitted here", not "the software failed".

17. **Research questions pre-registered** (see the section above): metrics, slices,
    and minimum samples fixed BEFORE the data is examined, so a detailed database
    doesn't become a machine for producing convincing stories from noise.

**The shift:** the old system asked *"did this bet win?"* The new one asks, in
order — are the two prices comparable? did we observe enough? which book moved? by
how much? was the original price still visible? did these bets pay over enough
trials? does the evidence justify trusting the signal? It is an audit trail that
separates **what was observed**, **what can be inferred**, and **what cannot yet be
known**.

---

## Tennis Recency/Fatigue Model — Spec (2026-07-04)

### Why this is a legitimate new test, not a fourth revision

P3 (`model/tennis_model.py`) proved the market is sharp using exactly
`FEATURE_COLS = ["market_fav_prob", "elo_diff", "grass_elo_diff"]` — a flat,
full-history Elo (`ingest/tennis_history.py`, K=32, no time decay, one match =
one equal-weight update since 2011) blended with the market. That result
stands; tennis moneyline stays ≤2★ on those three features.

What was **never** fit, and is sitting unused in sources we already ingest:

| Field | Source | Currently | Status |
|---|---|---|---|
| `winner_rank`/`loser_rank`, `_rank_points`, `_seed` | TML-Database (ATP) | Not parsed by `tennis_history.py` at all | New |
| `WRank`/`LRank`, `WPts`/`LPts` | tennis-data.co.uk (WTA) | Parsed into the training corpus (`ingest/tennis_training.py:192-201`, comment: "carried for future features") but **never added to `FEATURE_COLS`** | Captured, unused |
| `minutes` (match duration) | TML-Database (ATP only) | Not parsed | New |
| `round`, `best_of` | Both | Not parsed | New |
| Recency weighting (EWMA / time-decay) on Elo itself | — | Doesn't exist — Elo is flat | New |

So this spec covers two genuinely untested axes: (1) recency-weighted rating
instead of flat Elo, (2) rank/points/fatigue features that were captured but
never used. Confirming this before writing the spec — not assuming it — is
the point: don't re-run a test already run.

### Motivating observation (context, not proof)

The Wimbledon favorite/dog drill-down (`/vegas/wimbledon`) found ATP favorites
realizing 63.0% (n=27) against 70.6% implied vs. WTA favorites realizing 95.5%
(n=22) against 75.5% implied — a ~28pp gap in over/under-performance between
tours. n is small and this is **not** evidence of anything yet; it's the
reason to test fatigue/round-depth features specifically for ATP (best-of-5 —
more games, more cumulative fatigue across a 2-week draw) rather than a reason
to believe the result.

### Hypotheses (pre-registered — fixed before any data is touched)

**H1 — Recency-weighted rating.** An EWMA-style Elo (recent matches weighted
more than career-long ones) produces better-calibrated probabilities than the
flat Elo specifically in matches where recency-weighted and flat ratings
diverge (a "form gap" subset) — not across the whole population, since P3
already shows no edge there.
- Falsifiable prediction: in the top-quartile-by-|form_gap| subset, walk-forward
  CLV and realized-vs-implied win rate for the recency-weighted signal beat the
  flat-Elo signal in the same subset.
- Kill criterion: if the form-gap subset shows no improvement over flat Elo,
  stop — do not try a third Elo variant. This joins `tennis-moneyline-no-edge`.

**H2 — Rank/points/fatigue features.** Adding `rank_diff`, `rank_points_diff`,
`round_number`, and (ATP only) `cumulative_minutes_this_tournament` to
`FEATURE_COLS` improves walk-forward logloss vs. the P3 three-feature model,
and/or a fatigue feature specifically predicts the ATP favorite-loss gap
(positive coefficient on `cumulative_minutes_this_tournament` × `best_of=5`
interaction, walk-forward significant).
- Falsifiable prediction: refit with the expanded feature set on the existing
  66k-match corpus (`ingest/tennis_training.py`), walk-forward split by
  tournament (never by random row — see Non-negotiables), logloss must beat
  the P3 baseline by more than noise (bootstrap CI, not a point estimate).
- Kill criterion: if the expanded model doesn't beat P3's logloss out of
  sample, stop. Do not keep adding features to chase significance.

H1 and H2 are separate, independently falsifiable tests — a positive H2 result
does not retroactively justify skipping H1's kill criterion, and vice versa.

### Data build-out

1. **Extend `ingest/tennis_history.py`'s ATP parser** to carry `winner_rank`,
   `loser_rank`, `winner_rank_points`, `loser_rank_points`, `minutes`, `round`,
   `best_of` through `_run_elo`'s match dicts (currently discarded — only
   `wkey/lkey/is_grass/serve` survive). WTA equivalent from tennis-data.co.uk's
   `WRank`/`LRank`/`WPts`/`LPts`/`Round`/`Best of` columns (already fetched by
   `tennis_training.py`, just needs the same fields plumbed to the Elo/history
   layer, not only the training corpus).
2. **Recency-weighted Elo variant**: a second engine alongside `_run_elo`
   (don't replace it — flat Elo is still the calibration baseline), e.g.
   `_run_elo_ewma(matches, half_life_days)`. Tennis players log far fewer
   matches/year than NBA teams log games (≈60-90 tour matches/year for a top
   player vs. 82 NBA games), so the decay constant must be tuned in **days**,
   not match count, unlike the NBA's per-game α=0.25 — grid search half-life
   candidates (e.g. 90/180/365 days) rather than assuming NBA's constant
   transfers.
3. **Layoff/uncertainty signal**: `days_since_last_match` per player at
   prediction time — purely derived from existing match dates, no new source.
   Widen predictive uncertainty (or regress the rating toward a population
   prior) past some layoff threshold (e.g. 60+ days) rather than trusting a
   stale rating at face value.
4. **Fatigue signal (ATP-specific)**: cumulative `minutes` played by each
   player already in the current tournament, updated match-by-match within a
   draw. WTA lacks `minutes` in tennis-data.co.uk — either accept ATP-only for
   this specific feature or find a supplementary WTA duration source before
   claiming a cross-tour fatigue comparison.
5. **Surface-transition signal**: days since each player's last grass-court
   match (or count of grass matches in the last 60 days) — TML has `surface`
   per match already; tennis-data.co.uk has a `Surface` column too. Directly
   relevant to Wimbledon specifically (most of the tour arrives off clay/hard).
6. Backfill all of the above into the existing training corpus
   (`ingest/tennis_training.py`) as additional columns — reuse its point-in-time
   replay discipline (features computed from state strictly *before* the match
   being labeled, never leaking the outcome) rather than building a parallel
   corpus.

### Model architecture

- Stay inside the existing pattern: `LogisticRegression` on standardized
  features (`model/tennis_model.py`'s `_fit`), market-anchored blend (shrink
  raw model probability toward the market by weight `w`, grid-searched
  walk-forward — never let the fitted model fully replace the market line).
- `FEATURE_COLS` candidates to test (added incrementally, each justified by its
  own walk-forward logloss delta, not all six dumped in at once): `elo_diff`,
  `grass_elo_diff` (existing) + `rank_diff`, `rank_points_diff` (log-scaled —
  rank is heavily right-skewed), `recency_elo_diff`, `form_gap` (=
  `recency_elo_diff − elo_diff`, the H1 test variable), `layoff_diff`,
  `cumulative_minutes_diff` (ATP only, `best_of=5` matches only).
- Fit ATP and WTA **separately** given the observed tour asymmetry — a pooled
  fit could average away a real tour-specific effect (or manufacture a fake
  pooled one). Report both tours' walk-forward metrics independently, same as
  the favorite/dog breakdown already does.

### Evaluation

- **Walk-forward split by tournament, not by row.** Matches within a
  tournament are correlated (same field, same conditions); a random row split
  leaks tournament-level information across train/test.
- **Minimum sample before any conclusion**: pre-register ≥200 matches in the
  H1 form-gap top-quartile subset (use the 66k-match historical corpus for
  this, not live Wimbledon bets alone — current live sample is ~49 total
  favorite bets, nowhere near sufficient on its own).
- **Metrics**: logloss delta vs. the P3 baseline (bootstrap CI, not a point
  estimate), Brier score, and — once/if deployed live — CLV via
  `model/clv_report.py` sliced by `form_gap` quartile and by tour.
- **Report both tours separately, always.** A combined ATP+WTA number would
  hide exactly the asymmetry that motivated this spec.

### Phases

| Phase | Scope | Gate to proceed | Status |
|---|---|---|---|
| P1 | Extend `ingest/tennis_training.py` (not `tennis_history.py` — the training corpus is this spec's actual backtest input, and it already carries rank/points/round/best_of; `tennis_history.py`'s production ratings are a separate, live-ratings concern) to add `round_num`, `rank_diff`, `pts_diff`, `matches_played_fav/dog` (fatigue proxy — no match-duration column exists in this source for either tour), `days_since_last_fav/dog` (layoff), and a new `_RecencyElo` engine (`recency_elo_diff`, `form_gap`) alongside the untouched flat Elo | Data present, leak-free, point-in-time verified | ✅ Done (2026-07-04) |
| P2 | Grid-search the `_RecencyElo` half-life (`model/tennis_recency_calibration.py`, candidates 60/90/180/365/730 days); define and freeze H1's form-gap subset | H1's form-gap subset defined and frozen before any backtest is run | ✅ Done (2026-07-04) |
| P3 | Walk-forward backtest H1 (recency) and H2 (rank/fatigue) on the historical corpus (`model/tennis_recency_fatigue_backtest.py`) — offline only, no live deployment | Both hypotheses' kill criteria evaluated honestly; ≥200-match minimum met before concluding | ✅ Done (2026-07-04) |
| P4 | **Not triggered.** H1 failed cleanly; H2's only pass (WTA) is a marginal, likely-noise result that doesn't meet "a real out-of-sample edge" | Live paper-trading period before any bet from this signal counts above 2★ | Not triggered |
| P5 | Ongoing CLV monitoring via `model/clv_report.py`, sliced by `form_gap` quartile and tour, to confirm live performance tracks the backtest | Continuous — a live/backtest divergence pulls the signal back to ≤2★ | Gated on P4 |

**Honest prior (set before P1 started, 2026-07-04):** the motivating ATP/WTA gap
did not survive a significance check (z=-0.87 ATP, z=2.18 WTA — neither robust
at n=27/22), and tennis moneyline at a Slam is about the least promising market
to find public-data edge in — rank and recent form are exactly what
professional oddsmakers already price first. Expected outcome of P2-P3 is a
third "confirmed no edge, calibration-only" result, same as the original P3
finding above; P1 was still worth doing for its own validation/calibration
value regardless of whether P3 finds anything.

**P2 result (2026-07-04) — a discouraging sign, reported honestly, not hidden.**
Tuning-period-only grid search (matches before 2022-01-01; 2022-01-01+ held
completely untouched for P3): **every recency half-life tested underperformed
flat Elo's own standalone tuning-period logloss** (0.6323). Logloss improved
monotonically as half-life grew (60d: 0.6660 → 90d: 0.6596 → 180d: 0.6492 →
365d: 0.6404 → 730d: 0.6347), i.e. the least-aggressive decay tested came
closest to flat Elo but never beat it — consistent with decay only ever
discarding information in this dataset, not adding a compensating "recent
form" signal strong enough to offset the loss. Chosen half-life = **730 days**
per the pre-registered rule (best of the tested grid), not because it beat
the baseline. Frozen H1 subset: `|form_gap| >= 47.5` Elo points (the tuning
period's 75th percentile at 730d) — **8,150 reserved-period matches qualify**
(3,796 ATP / 4,354 WTA, both tours well past the ≥200 minimum). This result
doesn't kill H1 outright (H1's actual claim is about market disagreement in
the qualifying subset, not standalone rating quality — a different, narrower
test that's P3's job), but it's one more data point supporting the honest
prior above. Script: `model/tennis_recency_calibration.py`. Constants frozen
in `ingest/tennis_training.py`: `_RECENCY_HALF_LIFE_DAYS = 730.0`,
`FORM_GAP_FREEZE_THRESHOLD = 47.5` — do not re-tune either without a new,
separately pre-registered P2 study.

**P3 result (2026-07-04) — both hypotheses graded against their pre-registered
kill criteria on the reserved period (2022-01-01+), never touched before now.**

*H1 (recency Elo, in the frozen `|form_gap| >= 47.5` subset):* **FAIL, both
tours.** ATP n=4,024: bootstrap delta (flat − recency logloss) = +0.0018, 95%
CI `[-0.0049, +0.0088]` — includes zero, no reliable improvement. WTA n=4,615:
delta = **-0.0087**, 95% CI `[-0.0146, -0.0030]` — CI excludes zero, but in the
WRONG direction: recency Elo is *reliably worse* than flat Elo in this subset,
not better. H1 is dead — consistent with P2's own standalone finding.

*H2 (rank/points/round/fatigue features, expanding-window walk-forward, OOS
scored on 2022+ only):* ATP OOS n=9,476 — FAIL (delta +0.0004, CI
`[-0.0002, +0.0011]`, includes zero). WTA OOS n=11,230 — technically PASSES
the mechanical CI-excludes-zero rule (delta +0.0009, CI `[+0.0004, +0.0014]`),
but **do not read this as confirmed edge**: the effect size is tiny (<0.001
nats/match — no established practical significance), and 5 total comparisons
were run across H1+H2+the sub-claim (2 tours × 2 hypotheses + 1 sub-claim);
at nominal 95% CIs, P(≥1 false positive among 5 independent tests) ≈ 22.6%.
One marginal pass out of five tests is exactly the pattern this spec's
multiple-comparisons discipline exists to catch, not celebrate.

*The original motivating sub-claim* (fatigue predicts ATP best-of-5 favorite
losses — the thing that started this whole spec) **found nothing**:
`fatigue_diff` coefficient on ATP best-of-5 matches (n=6,196) = -0.0057, 95%
CI `[-0.1831, +0.0667]` — includes zero by a wide margin, not close to
significant. The Wimbledon favorite/dog gap that motivated this entire spec
remains what it looked like from the start: small-sample noise, now refuted
directly rather than just statistically doubted.

**Verdict: P4 is not triggered.** H1 failed cleanly in both tours. H2's only
technical pass is a marginal, likely-noise result in one tour, occurring
alongside a null result in the other tour AND a null result on the specific
sub-claim that motivated the whole investigation — this does not meet the
bar of "a real out-of-sample edge" the P4 gate requires. Tennis moneyline
remains calibration-only across every feature family tested so far (market
consensus, flat Elo, recency Elo, rank/points, round depth, fatigue-proxy) —
a third confirmed no-edge result, exactly as the honest prior anticipated.
Per the Non-negotiables below: no further recency-Elo variants and no further
feature additions to this feature set without a new, separately pre-
registered study — this one is closed. Script:
`model/tennis_recency_fatigue_backtest.py`.

### Non-negotiables (inherited from every prior gameline/totals mirage)

- Walk-forward or nothing — the corpus already exists; there's no excuse to
  fit and grade on the same period.
- No multiple-comparisons fishing: H1 and H2 are the two registered tests.
  If both fail, tennis moneyline goes back to calibration-only across all
  tested feature families, and the next idea (if any) gets its own spec with
  its own pre-registered kill criterion — not a quiet third attempt bolted
  onto this one.
- A small-sample live result (e.g. this Wimbledon's 49 favorite bets) can
  motivate a hypothesis; it can never confirm one. Confirmation requires the
  historical corpus and the stated minimum sample.
- If deployed, the signal only ever applies within the subset it was proven
  on (the form-gap top quartile) — extending it to the general population
  without re-testing there would repeat the exact mistake the 50/50 blend
  made in P3.

---

## MLB Underdog-Value Investigation — Spec (2026-07-05)

### Why this is worth a real study, not a quiet revival

`model/mlb_game_bets.py` hard-caps MLB moneyline and totals at 2★
(`_GAMELINE_MAX_STARS = 2`) because the fitted models' own holdout evals show
neither beats the market (ML logloss .6772 vs .6717; totals MAE 3.36 vs Vegas
3.31 — see memory `mlb-gameline-caps-odds-bug`). Separately, a real bug was
found and fixed 2026-07-02: `ingest/mlb_schedule.py` was arithmetic-averaging
American odds across books, which manufactures fictional payouts for
mixed-sign near-even-money prices (a −100/+100 average lands in the impossible
(−100,+100) zone). All 2,187 pre-fix moneyline rows were repaired in place.

After that repair, the 4-5★ tiers (the star rating the anchored `mlb-
gameline-v2` formula would have assigned before the 2★ cap silenced further
high-star ratings) showed **+22% ROI over 65 settled 5★ bets**, with realized
underdog win rate **33.8% vs 24.2% market-implied — a ~2σ gap**.

**Correction (2026-07-05, made while running P1 — see that section):** an
earlier draft of this spec claimed those 65 bets span "2026-06-28 to
2026-07-01 — four calendar days." That was wrong, caused by checking the
date range on `mlb_bets.event_commence`, which is mostly `NULL` in this
ledger — the MIN/MAX aggregate silently summarized only the ~6 rows that
happened to have a non-null value, not all 65. Joined to
`mlb_matchups.game_date` instead (populated on every row): **the 65 bets
actually span 2026-03-31 to 2026-07-01 — essentially the whole season so
far**, not four days. The underlying finding was never as thin a slice as
first reported; see P1 below for what this changes and doesn't.

### Hypothesis (pre-registered — fixed before re-scoring any of that data)

**H — MLB underdog value.** The existing `mlb-gameline-v2` anchored rating
formula (market-anchor w=0.5, shared `rate_market` EV/edge rubric, longshot
cap on), applied WITHOUT the 2★ cap, identifies underdog-side moneyline bets
that realize win rates above market-implied probability and produce positive
ROI at real (post-repair) closing odds — across the full 2026 season, not
just the four days it was first noticed in.

- Falsifiable prediction: re-scoring all 2026-season moneyline sides with the
  existing v2 formula (uncapped), the resulting 4-5★ tier shows (a) a
  realized-vs-implied win-rate gap whose 95% CI excludes zero, in the same
  direction as the original four-day finding, (b) positive ROI at actual
  closing odds, **independently in both chronological halves of the season**
  (not just pooled) — a real signal shouldn't need cherry-picked date ranges
  to appear — and (c) that gap is **broadly distributed across teams, not
  carried by 2-3 of them** (see Success Metric 7) — a real market-wide
  mispricing shouldn't be one or two teams' surprising season in disguise.
- Kill criterion: if the full-season CI includes zero, or the effect is
  positive in only one half of the season (concentrated, not distributed),
  or the effect disappears once the single most-represented team is removed
  (team-specific, not market-wide), the +22%/2σ result is ruled a small-
  sample or team-concentration artifact — H is dead, and the 2★ cap stays
  exactly as-is.

No new hyperparameter is being introduced (unlike the tennis spec's `_K`/half-
life), so there is no separate tuning phase — the formula is already fixed
and deployed. That makes this a **large-sample re-verification**, not a
model-search exercise: the only honest move is to test it against far more
data than discovered it, not to keep the original 65 as "confirmation."

### Data

**Corrected while building P1 (2026-07-05) — the first draft of this section
was wrong, and the mistake is worth keeping on record:** it proposed
re-deriving `our_prob`/ratings from `mlb_matchups` (`home_ml`/`away_ml`,
`vegas_prob_home`, `our_prob_home`) back to season start. That doesn't work:
`mlb_matchups.our_prob_home`/`vegas_prob_home` are **not a preserved
historical record** — later pipeline runs overwrite them even for games that
already happened, so querying that table for an old date returns whatever
the model says about that game TODAY, not what it said at rating time. That
would have silently broken the "point-in-time, leak-free" requirement this
whole file's discipline depends on.

The correct, actually-used source: **`mlb_bets` itself**, which already
freezes `(our_prob, market_prob, market_decimal, edge, ev)` at the moment
each `mlb-gameline-v2` bet locks (`model/mlb_game_bets.py`). No re-derivation
of `our_prob` or the anchor happens anywhere in P1 — every input is read
directly off the existing settled ledger, joined to `mlb_matchups.game_date`
for reliable dating (not `mlb_bets.event_commence`, mostly `NULL` — see
above) and to `mlb_teams` for the team-concentration check.

Star tiers ARE recomputed (not read from the stored `stars` column), for two
independent reasons, both real: (1) the 2★ cap (deployed 2026-07-02) clamps
`stars` at INSERT time for every bet rated after that date, while
`ev`/`edge` are stored uncapped regardless (`rate_market` computes all three
together; only `stars` gets clamped afterward) — so calling `rate_market`
again on the frozen inputs recovers the natural tier exactly. (2) **19 of
the 65 stored 5★ bets (29%) turned out to be inflated by the same odds bug
this project already fixed once** — see P1 below.

### Success metrics (fixed now, before re-scoring)

1. **Realized-vs-implied win rate gap**, with a **Wilson or bootstrap 95% CI**
   on the gap — not a point estimate. This is the primary metric; the
   original finding is a point estimate (33.8% vs 24.2%) with no CI reported,
   which is itself part of why it isn't yet evidence.
2. **ROI at real closing odds**, full season, with a bootstrap CI on the ROI
   itself (a mean can look good while individual-bet variance makes the CI
   span negative territory).
3. **Split-half stability**: the season divided at its midpoint
   (chronological, not random — matches this project's "walk-forward, never
   by random row" rule everywhere else). Both halves must independently show
   the effect (CI excludes zero, same sign) — a real edge shouldn't vanish or
   flip sign in either half.
4. **Brier score / calibration by probability decile** across the whole
   uncapped 4-5★ tier — confirms the gap is a genuine calibration miss
   (claimed probability systematically wrong) and not an artifact of a few
   large-odds wins dominating the ROI average.
5. **CLV, once/if this reaches live tracking**: MLB per-book capture
   (Pinnacle via the `eu` region) and `model/clv_report.py --sport mlb`
   already exist (Edge-Finding Roadmap P1) — if H survives the offline
   re-score, the next real test is whether the market moves TOWARD our
   number pre-close on flagged sides, which is evidence the mispricing is
   real rather than closing-price noise. This is a P4-equivalent gate, not
   part of the offline pass/fail.
6. **Minimum sample**: pre-registered ≥200 qualifying (uncapped 4-5★)
   bet-sides across the full season before any conclusion is drawn — the
   original n=65 does not clear this on its own regardless of outcome.
7. **Team concentration** (added 2026-07-05, before any backtest ran — same
   pre-registration discipline as everything else here). `subject_team_id`
   is already recorded per bet (`model/mlb_game_bets.py`), so this is cheap:
   report the qualifying-bet count AND realized-vs-implied gap **per team**,
   then (a) no single team may account for more than **25%** of the
   qualifying bet-sides, and (b) the full-season CI must still exclude zero
   with the single most-represented team **removed** (leave-one-team-out).
   A real market-wide underdog mispricing should be broadly distributed
   across many teams; if 2-3 teams are carrying the whole effect, that's a
   specific team overperforming this season, not a general pricing edge —
   a different, much narrower (and separately-nameable) finding that this
   spec's broad "bet all qualifying underdogs" hypothesis does NOT cover.

### Phases

| Phase | Scope | Gate to proceed | Status |
|---|---|---|---|
| P1 | Recompute the natural (uncapped) star tier for every settled `mlb-gameline-v2` moneyline bet, from the frozen ledger inputs (`model/mlb_underdog_value_backtest.py`) | Recomputation reproduces the 65 stored-5★ bets exactly, OR every mismatch is independently explained (see result below) | ✅ Done (2026-07-05) |
| P2 | Compute all seven success metrics on the qualifying tier; split-half stability; per-team breakdown + leave-one-team-out check | Metrics computed honestly, both halves and the team breakdown reported separately, before any verdict is stated | ✅ Done (2026-07-05) |
| P3 | **Verdict.** PASS only if the kill criterion is cleared in full (CI excludes zero, both halves, ≥200 n) | If FAIL: 2★ cap stays. If INCONCLUSIVE (sample too small): cap stays, re-run later. If PASS: proceed to P4 | **INCONCLUSIVE — see result below. Cap stays; not a kill, not a pass.** |
| P4 | **Only if P3 passes**: shadow-track the signal live (rate uncapped in a research-only column/table, never surfacing above 2★ in the real ledger) for a full slate cycle, measuring real-time CLV via the existing `model/clv_report.py` infrastructure | Live shadow CLV must be positive and consistent with the offline backtest before the real ledger's cap is touched | Not triggered — P3 inconclusive |
| P5 | **Only if P4 confirms live**: raise the cap for this specific tier only (not moneyline broadly) in `model/mlb_game_bets.py`, with the exact qualifying criteria documented | Ongoing CLV monitoring, same as the tennis spec's P5 — a live/backtest divergence pulls it back to 2★ | Not triggered |

### P1+P2 result (2026-07-05)

Two data-quality issues were found and fixed *while building the backtest*,
before any metric was computed — both are corrected above, restated here
because they change what the result means:

1. **The "four calendar days" claim was wrong** (see Context and Data above).
   The 65 originally-flagged 5★ bets actually span 2026-03-31 to 2026-07-01
   — essentially the whole season, not a cherry-picked week.
2. **19 of those 65 bets (29%) were stale, not real.** Recomputing
   `rate_market` on the frozen inputs reproduced 46/65 exactly; the other 19
   all carry `inputs_json.odds_repaired = true` from the 2026-07-02 odds-bug
   fix — their `market_decimal` was corrected in place, but `stars`/`ev` were
   never retroactively recomputed from the corrected odds, so they were
   still displaying 5★ ratings computed from the OLD, inflated, pre-repair
   EV. Verified the recomputation logic itself is sound independently,
   against 39 untouched (never-repaired) rows across all star tiers: 38
   matched exactly, and the one "mismatch" was a genuinely-natural-5★ bet
   correctly shown as capped-to-2★ post-2026-07-02 — exactly the kind of
   row this investigation exists to surface, not a bug.

**With the honest, corrected inputs, the natural 4-5★ tier across the whole
season so far is n=125** (not 65 — the correct tier is broader once the cap
is lifted from every dated bet, not just the ones that happened to still
show 5★ despite the stale-EV bug):

- **Win-rate gap**: realized 40.8% vs implied 30.9%, gap **+9.9pp, 95% CI
  [+1.8pp, +17.8pp] — excludes zero.** This is the one genuinely encouraging
  number.
- **ROI**: +31.2%, 95% CI **[-3.1%, +66.0%] — includes zero.** Not
  significant — the win-rate edge doesn't yet translate to a provable profit
  once bet-to-bet odds variance is accounted for.
- **Split-half stability: FAILS.** First half (n=95): gap +9.2pp, CI
  [-0.0pp, +18.8pp]. Second half (n=30): gap +12.0pp, CI [-4.1pp, +28.6pp].
  Both point estimates are positive and roughly consistent with each other —
  this is NOT a sign-flip or a one-half-only pattern — but neither half
  *individually* clears the CI-excludes-zero bar, mostly because splitting
  n=125 into 95/30 leaves the second half underpowered. Per the
  pre-registered rule this still counts as a fail, not a partial pass.
- **Calibration by decile: mixed, not monotonic.** Most deciles show a
  positive gap, but two (of ten) show a meaningful negative gap — not the
  clean "biggest gap in the longest-odds decile" pattern a genuine
  systematic underdog mispricing would produce most cleanly.
- **Team concentration: PASSES.** 30 different teams represented; the
  largest (COL) is 8.8% of the tier, well under the 25% bar. Leave-one-out
  (excluding COL) still shows the gap excluding zero (+11.4pp, CI [+2.3pp,
  +19.6pp]). The pattern is not one team's story.
- **Minimum sample: FAILS.** n=125 < the pre-registered 200.

**Verdict: INCONCLUSIVE, not PASS, not FAIL.** This is a real third outcome
the original kill criterion didn't name, and it's the honest one: the signal
survives the check it was most likely to fail (team concentration) and shows
a statistically real win-rate gap, but ROI isn't significant, split-half
stability isn't met, and the sample is still below the pre-registered floor.
Per this file's standing rule ("no conclusion is drawn... until the stated
sample is reached"), **the 2★ cap stays exactly as-is.** This is not ruled a
dead hypothesis the way tennis's H1 was (which was reliably wrong-signed) —
it is ruled *not yet decidable*. Re-run `model/mlb_underdog_value_backtest.py`
once the season has produced enough further natural-4-5★ bets to clear
n≥200, and grade again against the same frozen bars — do not lower the bar
to manufacture a pass.

**Rerun target: ~2026-08-30.** Computed from the actual accumulation rate,
not guessed: 125 qualifying bets over the 95 days from 2026-03-31 to
2026-07-04 is ~1.32 qualifying bets/day; reaching the n≥200 floor needs 75
more, i.e. ~57 more days from 2026-07-04. This is a data-driven estimate, not
a promise — the real qualifying rate can shift (roster changes, a hot/cold
streak in which teams draw underdog-side bets, schedule density). Re-run the
script itself at that point (or earlier, harmlessly — it just reports
"FAILS the pre-registered minimum" again if n hasn't caught up) rather than
trusting this projection blindly; the regular season runs into early
October, so there's real room before the sample window closes for the year.

### Non-negotiables (same discipline as every prior spec in this file)

- The original 65-bet, four-day observation is what motivated this
  investigation — it is not allowed to also be the confirmation. The full
  season (a superset that includes those 65, but is ~20-40x larger) is the
  actual test population.
- Split-half stability is not optional. A pattern that only appears in one
  half of the season is exactly what a four-day fluke inflated to season
  scale would look like.
- Team concentration is not optional either. A pattern carried by 2-3 teams
  is a team story, not a market-pricing story — report the per-team
  breakdown regardless of how the pooled numbers look, not only if the
  pooled result looks too good to be true.
- No stacking exceptions: if H fails, the answer is "2★ cap confirmed
  correct," not "try a different anchor weight" or "try a different star
  threshold" as an immediate follow-up — that would be the same kind of
  quiet multiple-comparisons drift the tennis and soccer specs were written
  to prevent. A different formula variant is a new, separately pre-registered
  study.
- P4's live shadow-tracking step exists specifically so this is never
  "backtested well, shipped straight to the ledger" — every other edge-
  finding effort in this project (CLV harness, line alerts, tennis P4/P5)
  requires a live confirmation step before a backtest result touches
  anything a user-facing star rating depends on.

---

## MLB Moneyline — Point-in-Time Leak Finding + Edge Ideas (2026-07-05)

### The leak

`model/mlb_moneyline_model.py` (`mlb-ml-v1`) and `model/mlb_game_total_model.py`
share `load_game_data()`, which joins `mlb_team_stats` and `mlb_pitcher_stats`
by grabbing the **latest** row per team/pitcher (`ORDER BY season DESC,
fetched_at DESC`). Both tables are `UNIQUE(team_id, season)` /
`UNIQUE(player_id, season)` and refreshed **daily**
(`.github/workflows/refresh_mlb_stats.yml`) via a true overwrite-in-place
upsert (`ON CONFLICT ... DO UPDATE`, confirmed in `db/queries.py`'s
`upsert_mlb_team_stats`/`upsert_mlb_pitcher_stats`) — no history is retained.

`backfill_predictions()`'s "walk-forward" correctly restricts the *logistic
regression's training labels* to `game_date < d`, but every non-market
feature (`sp_xfip_adv`, `wrc_adv`, `iso_adv`, `bullpen_adv`) is pulled from
these single current-state rows for **every** game regardless of date — a
March prediction is partly built from June/July team stats. Same bug class
already caught twice in this project (`mlb_matchups.our_prob_home`,
`mlb_bets.event_commence` — see the MLB Underdog-Value spec above).

**Which direction this cuts:** leakage inflates *apparent* backtest skill.
The model still didn't beat the market even with that unfair advantage
(`mlb_ml_v1_eval.json`: logloss .6772 vs market .6717) — so real, honestly-
computed live skill is almost certainly worse than reported. This doesn't
hide a hidden edge; if anything it makes the existing 2★ cap more clearly
correct, not less. But it means no future feature work should be evaluated
on top of this join until it's point-in-time safe.

### Not-yet-tried feature ideas for MLB moneyline (ordered by plausibility)

1. **Weather** — `weather_temp`/`wind_speed`/`wind_direction` already exist
   as columns on `mlb_matchups` and are already captured, but aren't in
   `FEATURE_COLS` at all. Zero new ingestion required — cheapest test.
2. **Short-term bullpen fatigue** — relief innings thrown in the last 1-3
   days. Current `bullpen_fip` is a season aggregate; day-to-day workload is
   a real, well-known professional angle season stats can't see. Needs new
   ingestion from free MLB boxscore data (already have API access).
3. **Recency-weighted (EWMA) team stats** instead of season-to-date — same
   test class as the tennis Elo work ([[tennis-moneyline-no-edge]]), which
   found recency weighting reliably lost to flat ratings. Honest prior here
   is also low, but MLB team strength streaks harder than a tennis rating,
   so it isn't a pure repeat of that result.
4. **Reverse line movement / public bet-% divergence** — a fundamentally
   different strategy (follow sharp money instead of out-predicting it).
   Needs a new data source not currently ingested (Action Network-style
   bet-split feeds); the existing Pinnacle-vs-DK line-alerts infrastructure
   is adjacent but isn't quite this.
5. **Umpire assignment** — real, documented effect, but mostly a totals
   lever (strike-zone size → run environment), weaker/noisier for
   moneyline specifically. Needs new ingestion (crew assignments aren't in
   the schema).

Sequencing: fix the leak first (undermines confidence in every downstream
test), then weather alone (near-zero cost), then bullpen fatigue if
inconclusive — one pre-registered feature at a time, not a bundle, per the
same multiple-comparisons discipline used everywhere else in this file.

### Fix (implemented 2026-07-05)

Added `mlb_team_stats_history` / `mlb_pitcher_stats_history` (`db/schema.py`,
append-only, `UNIQUE(team_id/player_id, season, snapshot_date)`) populated
daily in `ingest/mlb_stats.py` alongside the existing current-state upserts
(via new `db/queries.py` helpers `insert_mlb_team_stats_snapshot`/
`insert_mlb_pitcher_stats_snapshot`) — `mlb_team_stats`/`mlb_pitcher_stats`
are untouched and still serve DFS projections and slate loads, which
correctly want "current," not point-in-time, stats. `load_game_data()`
(`model/mlb_game_total_model.py`, shared by the moneyline and totals models)
now joins each side via a `LATERAL` "latest snapshot at or before
`game_date`" lookup instead of the global-latest row.

**Honest consequence, not a bug (corrected after testing against live data,
2026-07-05):** no snapshot exists for any date before this shipped, so the
LATERAL join correctly returns NULL for `home_sp_xfip`/`home_wrc`/`home_iso`/
`home_bullpen_fip` (and the away side) on every historical game. Row COUNT
is unaffected — `build_ml_features()`/`build_features()` already `.fillna()`
each side with a league-average constant *before* computing the home/away
differential, so a game with two NULL sides still produces a defined
`sp_xfip_adv`/`wrc_adv`/etc. of **exactly 0** (informationless, not missing).
Verified live: 1,309 rows still load, 1,305 pass the full-feature-non-null
filter — same as before the fix. What actually changes is **information
content, not availability**: `evaluate()`/`backfill_predictions()` will
train and score on rows where every non-market feature is a neutral 0 until
daily snapshots accumulate enough real point-in-time history (weeks), so any
walk-forward result run before that depth exists is really testing
`market_home_prob` alone, not the strength-differential features — an
important thing to check before trusting a near-term eval as a real test of
those features. This is still the correct trade for removing the leak; it's
gentler than originally estimated, not harsher. Historical `our_prob_home`
values already written before this fix are unaffected and remain what they
were; they should be read as "computed under the old, leaky methodology,"
not deleted or restated.

---

## NFL `total_walking` Fade — Pre-Registered Study (registered 2026-08-15)

**Registered BEFORE Week 1 (2026-09-09) so the regular season is a clean test.**
Sealed while the only evidence was 14 preseason observations. Implemented in
`model/nfl_walking_fade_study.py`.

### Discovery sample — FROZEN, and it cannot confirm anything

Preseason, 2026-08-06 → 2026-08-15. When `total_walking` fired and the flagged
side was followed, the consensus total then moved **back against it**:

```
n=14 across 12 distinct games   (top game 14% of alerts — not concentrated)
mean −0.85 points   game-clustered 95% CI [−1.114, −0.531]   excludes zero
moved toward the flagged side: 1 of 14   (one-sided binomial p = 0.0009)
sides mixed: 5 over / 9 under   settled record of flagged side 2W-5L
```

Confound checked and rejected: book coverage is stable open→close (median 20 →
18.5 books quoting a total; only 1 of 18 games grew), so this is not consensus
composition drift.

**Three reasons this is a hunch, not a finding:**
1. **Preseason is a different regime.** Starters rest; totals move on
   playing-time news that has no regular-season analogue.
2. **n=14.** A fortnight.
3. **It is 1 of 8 detectors examined.** p=0.0009 × 8 ≈ 0.007 survives, but the
   result was *found*, not predicted — and this project has been burned by
   exactly that (the 9.5-line totals mirage; soccer ATGS's six memorable
   winners inside a −64% ledger).

**A mechanical alternative explanation that n=14 cannot rule out:** the
detector is *defined* as "the line has wandered furthest from open." Selecting
the extreme of a noisy series and then observing regression toward the mean is
arithmetic, not inefficiency. On an efficient random-walk line the expected
future move conditional on a large past move is zero — so measured reversion
does suggest overshoot — but distinguishing overshoot from selection-on-noise
needs the prospective sample.

### Hypothesis (fixed now, before any regular-season data)

**H — `total_walking` flags overshoot.** Taking the side OPPOSITE the flagged
one, at trigger, obtains a better number than the closing line.

- **Falsifiable prediction:** on regular-season games only, mean game-clustered
  CLV on the **fade** side is positive and its 95% CI excludes zero.
- **Kill criterion:** CI includes zero at n≥100 ⇒ **dead**. No re-slicing to
  over-only/under-only, no threshold re-tuning, no migration to
  `spread_walking`. A variant is a new study with its own registration.

### Population (frozen)

- `sport='nfl'`, `alert_type='total_walking'`, **`season_type='regular'`**,
  commence ≥ **2026-09-09**. Preseason is excluded entirely — it is the
  discovery set and a different regime.
- Requires ≥2 pre-commence captures (trigger + a later close) with a consensus
  total at both.

### Metrics

- **Primary:** fade-side CLV in **points** = (trigger consensus total − closing
  consensus total), signed so positive = the line moved toward the fade side.
  Game-clustered bootstrap CI.
- **Conjunctive gate:** ROI at the frozen executable price (available from
  2026-08-15 — see the NFL execution-price fix), CI lower bound > −5pp.
  A better *number* that still loses money is not an edge.
- **Reported always, gating nothing:** distinct games, DEFF, execution-book
  distribution, over/under split, per-month breakdown.

### Floors (conjunctive, frozen)

`n ≥ 100` **and** `≥ 40 distinct games`. Expected accrual ≈ 0.88 alerts/game in
preseason → ~240 over 272 regular-season games, so the floor should be reached
around mid-season. **No verdict before both floors**, and no interim peeking
that could stop the study early for efficacy — futility only.

### Non-negotiables

- The detector is **not changed** by this study. It keeps flagging the same
  side; the study measures the opposite side. A passing result licenses a
  *live shadow* period, never an immediate flip.
- The 14 preseason observations are discovery only. They are never pooled into
  the confirmation sample, not even as a "combined" secondary.
- MDE and floors do not move. If accrual is slower than projected, the verdict
  waits for 2027 rather than the bar dropping.

### Honest prior

Expected outcome is **no edge**. This system is 0-for-4 across sports on
out-guessing closing lines, and the mechanical selection-on-noise explanation
above is at least as plausible as overshoot. Even if real, ~240 alerts/season
at $100/bet and ~0.85 points of CLV is a few hundred dollars — worth measuring
because it is now nearly free, not worth restructuring around.

---

## MLB Prop Detectors — CLV Split + Multi-Book Rebuild (2026-08-15)

### The measurement that split them

Pooled MLB prop ROI (+3.46%, n=558, CI includes zero) was hiding two detectors
pointing in OPPOSITE directions. Same-book same-line CLV — entry price vs
DraftKings' own last pre-commence close on the identical proposition, 512
comparable settled alerts, date-clustered bootstrap — separated them in one pass:

| detector | n | mean CLV | 95% CI | verdict |
|---|---:|---:|---|---|
| `prop_line_gap` | 439 | **−0.13%** | [−0.25%, −0.04%] | **negative, excludes zero** |
| `dk_prop_value` | 73 | **+1.29%** | [+0.43%, +2.32%] | **positive, excludes zero** |

CLV resolved at n=439/73 what ROI could not: per-bet ROI SD is ~89–114pp, so an
ROI verdict needs ~1,000–2,500 settled bets per cell. **Grade prop detectors on
same-book same-line CLV first, ROI second, and never pool detectors.**

### `prop_line_gap` — DEMOTED to control, not retired

Its trigger is a LINE disagreement, which says the two books differ but not
WHICH is stale — so direction is a coin flip, and the ledger confirms it. It
keeps scanning (detectors run on already-captured data and cost no credits) and
its trigger is **frozen at v1** so it stays comparable to those 439
observations. It renders as a grey `CONTROL` chip with "DO NOT BET". Reviving it
as actionable requires a new, separately pre-registered study.

### Same-line anchor census (16 markets × 15 events, 223 credits)

Both detectors need a **same-line** Pinnacle quote. Only **4 markets** clear it:

```
pitcher_strikeouts 100%   batter_total_bases 100%
pitcher_outs        93%   pitcher_hits_allowed 80%
```

Three distinct failure modes, all worth recording:
- **Pinnacle posts 0%** — `pitcher_earned_runs` (was 98% in July; 98→51→1→0
  across recent weeks), `pitcher_walks`, `pitcher_record_a_win`, and every
  batter market except total_bases.
- **`batter_runs_scored`: Pinnacle 87%, same-line 0%** — it quotes a different
  number than DK every time. An anchor that never matches the proposition is
  not an anchor. The Herrera rule, biting at census level.
- **`batter_home_runs`: DK posts 0%** (Pinnacle 100%). Briefly added on the
  strength of espnbet/fliff/pinnacle coverage — wrong, the detector executes at
  DK. **Market availability is always relative to the books you asked for.**

### `dk_prop_value` v2 — multi-book best price

v1 checked DraftKings only. v2 checks all six executable books against the same
Pinnacle fair value and fires **ONE alert per proposition at the best price**.

One per proposition, not one per book, on two grounds: economically you place
the bet once; statistically six books quoting one player is ~one observation,
and counting six would inflate n and shrink every CI (the clustering trap).

Stamped `detector_version = "prop-value-v2-multibook"`. **The trigger changed,
so v1's 73 observations CANNOT pool with v2 — v2 starts at n=0.** Canonical keys
are `exec_book`/`exec_odds`/`exec_decimal`/`books_qualifying`; legacy
`dk_odds`/`dk_decimal` are still written (holding the executed book's price) so
settlement/grading/ROI queries keep working, and the UI reports the distinct
execution-book count rather than claiming "@ DK".

**Two bugs found while wiring it, both silent-failure class:**
1. `_selection_prices` hardcoded DraftKings, so a FanDuel alert would have had
   its CLV graded against DraftKings' close — a price we never had. Now follows
   `exec_book`; legacy rows fall back to DK.
2. The same function carried a **hardcoded 6-market list**, so every market
   added after it was written would silently fail to grade. Now generic across
   any over/under prop — which mattered immediately at 5 → 16 markets.

**v2 was wrong and was superseded the same day, after 10 rows.** Its "5× volume
at the same 3% bar" was **max-of-N selection bias**, not signal: taking the best
EV across six books while holding the threshold fixed selects whichever quote
is most erroneous in our favour, and quote error shares a tail with genuine
value. Measured — every executable book's median EV is **≈ −6.5%**, i.e.
exactly the two-way hold (6.1–7.2%), so a +3% alert is a ~9.5pp tail outlier.
**Raising the threshold does not fix it**: at 6.0% the best-of-6 rate was still
15× the DraftKings-only rate at 3.0%, because the selection is over *books*,
not over the threshold.

A "model disagreement" gate (require the executing book's own de-vigged fair to
differ from Pinnacle's) was written, measured, and **rejected as circular** —
it rejected nothing on live data. Proportional de-vig forces a book's two sides
to sum to 1, so "posted price is generous" and "own fair differs from Pinnacle"
are the same statement; at 6–8% hold an EV ≥ 3% already implies 1.7–6.9pp of
apparent disagreement by construction. Separating margin placement from genuine
model disagreement needs an **asymmetric (Shin/power) de-vig** — not attempted.
**Do not re-add the symmetric version as a filter.**

### `dk_prop_value` v3 — selection and execution separated (shipped)

- **Selection: DraftKings alone**, exactly as v1. One book, no max-of-N, no
  selection bias — and it is the trigger whose CLV is validated.
- **Execution: best same-line price across the executable books**, applied
  *after* selection, so it cannot influence which propositions are flagged.
  Pure D1-style line-shopping gain (+1.2–1.9%/bet measured), carried as
  `exec_book` / `exec_gain_pct`.
- **CLV continuity preserved**: `dk_decimal` still holds DraftKings' price and
  `clv_book` names it, so entry-vs-close stays DK-against-DK and v3 remains
  **poolable with v1's n=73**. `_selection_prices` resolves
  `clv_book → exec_book → draftkings`, covering all three generations.

**v1's CLV is robust, and smaller than the headline.** Dropping the top
observations: top-3 → +0.87% [+0.38, +1.45]; top-5 → +0.62% [+0.26, +1.07];
**top-10 → +0.27% [+0.08, +0.54], still excluding zero at n=63.** It degrades
gracefully and never collapses — a real, small, broadly-distributed effect.
Plan against **0.3–0.6%**, not 1.29%.

`books_qualifying = 1` on 10/10 v2 alerts is **not a finding** — at ~7% hold,
two books clearing +3% on the same side would require near-zero margin, so 1 is
the arithmetically expected result. If anything it corroborates single-book
pricing quirks rather than market-wide staleness.

Two open cautions, not yet resolved:
- **Execution-book concentration.** If v2's CLV comes back positive but is 70%
  one soft counterparty, that is a single-book finding, not a market edge. It
  needs a pre-registered concentration gate analogous to the MLB Underdog
  spec's team-concentration check, plus leave-one-book-out robustness.
- **Stale ≠ available.** A slow-updating book looks mispriced constantly; those
  prices may be suspended, limited, or gone on click. Observed quote
  persistence is polled, never verified.

### Pre-registered measurement program — `mlb-prop-program-v1` (2026-08-15)

Registered **before the v3 cohort had a single observation**.
`model/mlb_prop_program.py` implements it; `tests/test_mlb_prop_program.py`
makes every constant tamper-evident.

**The question, narrowed honestly:** *does `dk_prop_value` produce capturable
CLV at all?* NOT "which market is most exploitable" — projected season sample
is ~180–220 effective observations TOTAL, which split four ways is 45–90 per
cell, far below any defensible floor. So the four anchored markets are **ONE
POOLED CELL and the test family is exactly 1.**

| | |
|---|---|
| Markets | 4 anchored (the census set). Exploratory tranche **killed** — the 223-credit census answered its question better than a ~7,200/month program would |
| Cost | 180 credits/day, ~5,400/30d (was 13,500 proposed) |
| Test family | **1** — no multiplicity correction needed; count reported anyway so silent expansion is visible |
| Primary metric | Mean reference CLV, date-clustered bootstrap |
| Floors (conjunctive) | `n_eff ≥ (2.80 × SD ÷ 1.0)²` = **134** at SD 4.12pp · **≥25 distinct dates** · **≥30 settled** |
| Day-14 gate | **2026-09-01**, blinded — nuisance parameters only |
| Verdict | **2026-10-04**, computed once, frozen. No efficacy stopping; futility only |

**MDE is 1.0pp, not the 0.5pp first drafted.** At $100/bet, +0.5pp CLV ≈ +1%
ROI ≈ $650/yr, which does not exceed the cost of running the program. *An
effect not worth acting on is not worth powering for.* Set on economics
confirmed after the first design; it does not move again.

**≥25 distinct dates cannot be bought with volume.** A date-clustered bootstrap
resamples clusters, so 10 dates gives a meaningless CI at any n.

**Execution-book concentration gates (C1–C4).** Uniform share across 6 books is
16.7%, so the MLB-underdog spec's 25% team bar would be nearly non-binding. One
book's pricing quirk is also more dangerous than one team's lucky season — it is
a systematic artifact the book removes by limiting you.
- **C1** >40% one book ⇒ headline reads `SINGLE-BOOK FINDING`. May not be called
  a market edge, a detector edge, or "exploitable."
- **C2** leave-one-book-out must keep CI excluding zero **and** ≥50% of the
  point estimate.
- **C3** at ~70% concentration LOBO is underpowered *by construction* ⇒
  `SINGLE-BOOK — UNVERIFIABLE`. Blocks market-finding status; may promote only
  as a **counterparty finding**, assumed to **decay, not compound**.
- **C4** no per-book verdicts, ever. Book distribution is a covariate, never a
  hypothesis family.

**The control arm is load-bearing, not a museum piece.** `prop_line_gap` keeps
its v1 trigger frozen and costs zero credits. Both arms share pipeline,
grading, latency and voids, so the **difference** cancels systematic error we
haven't thought of — an accidental placebo arm. Every verdict reports it. If it
drifts from its −0.13pp baseline that is an **instrument alarm** (cadence,
close definition, reference drift), not a finding. *First run reads −0.14pp —
the instrument checks out.*

**`books_qualifying`** is recorded with a fixed interpretation and **creates no
stratum**: >90% at 1 strengthens the single-book classification. A
consensus-confirmed subgroup becomes a separately registered study only if it
independently reaches its own floor — never carved out after the fact.

**Program-level kill, declared before the data:** if the live cell fails its
verdict at floor — CLV CI includes or sits below zero — that is the **fifth
confirmed negative and edge-finding terminates.** Declaring that in advance is
what makes running it worth doing.

Guaranteed 2026 deliverables regardless of how the gate resolves: SD̂, ρ̂,
per-market coverage, alert rates, persistence distributions, and the quarterly
census — i.e. the inputs to a correctly-powered 2027 cohort.

### Economics, stated plainly

At **$100/bet** and ~650 bets/year, the CLV-implied edge range gives roughly
**$700–$3,700/year**. The instrumentation to *prove* it plausibly costs more
effort than the edge returns. Any decision to keep building this should be made
against that number, not against the +15.2% ROI point estimate (n=73, CI
through zero) that it is tempting to quote instead.

---

## Soccer Anytime-Scorer Detector — Retired (2026-08-13)

`scan_props_soccer` (`prop_outlier`, WC `player_goal_scorer_anytime`) is the
**first detector retired under the standing "an alert type with no positive CLV
is noise" rule.** It is a CONFIRMED negative, not an unproven one.

Settled record at frozen DK prices (the book actually bettable here):

```
n=101   6 won (5.9%)   median DK decimal 8.00 (12.5% implied)
-65.0u  ROI -64.4%     date-clustered 95% CI [-85.6%, -44.7%]
```

The CI lies **entirely below zero**. The signal ran backwards: when DK priced a
player longer than the overround-normalized median book, DK was right. This is
not a threshold-tuning problem — the normalization fix documented in the P3
roadmap row already solved the book-margin artifact, and what remained still
lost.

**The documented DNP-as-loss conservative bias does not rescue it.** Books void
ATGS when a player never takes the field and we have no lineup feed, so some
losses should be voids. But breakeven requires **65 of 101 flagged players
(64%) to have been no-shows**, which is not credible for players priced at a
median 12.5% to score. At a generous 20-25% DNP rate the detector still runs
-52% to -56%.

**Retirement mechanics (the pattern for future retirements):**
- The scan function is inert — it logs and returns 0. Its body is preserved as
  `_scan_props_soccer_retired_impl` (uncalled) so the logic stays auditable.
- `settle_props_soccer` **still runs**, so anything already open finishes
  grading.
- The 101 ledger rows are **not deleted**. The ledger is append-only; they are
  audit history and the evidence for this decision.
- Reviving it requires a new, separately pre-registered study — never a
  parameter change to the retired function.

**Contrast, so this is not over-generalized:** MLB props pooled over the same
period are n=558, ROI **+3.46%**, CI [-3.98%, +11.19%] — includes zero, so
**unproven, not confirmed either way**, and they stay live and accumulating.
Retiring soccer ATGS is not a verdict on props generally; it is a verdict on
one detector whose CI cleared the bar in the wrong direction.

---

## MLB Totals — Mean-vs-Median Side-Selection Bug — Found + Fixed (2026-08-12)

### The finding

Reviewing why the MLB ledger loses, the totals side-selection rule was found to
commit a category error that accounts for essentially the entire loss. It is not
a modelling judgement call and it is not "no edge" — it is arithmetic.

**The totals model predicts the conditional MEAN; books set the line at the
MEDIAN.** `model/mlb_game_total_model.py` fits `y = actual_total − vegas_total`
with Ridge (squared-error loss ⇒ conditional mean). League-wide across 1,757
completed 2026 games:

```
mean(actual − vegas_total)   = +0.511
median(actual − vegas_total) =  0.000     ← the book is dead-on
mean total 8.98  vs  median total 8.00    ← right skew, from blowouts
league Over hit rate 46.5%   (breakeven at −110 = 52.4%)
```

So the model correctly learns ≈ +0.5, which is the *mean*, and is then compared
to a *median*-set line. Three layers each pushed the same way:

1. `mlb_game_total_model.py:452` — squared-error fit ⇒ predicts the mean.
2. `mlb_game_bets.py` — `is_over = lam > line`, a mean-vs-median point
   comparison ⇒ says Over on **74–87% of all games, every single month**.
3. `mlb_game_bets.py` — `from model.soccer_game_bets import _over_under_probs`
   (Poisson, mean ≈ median) converted that structural +0.5 gap into a fake
   ~55–58% P(over). The 2026-07-11 audit already said "do not import the soccer
   Poisson assumption"; it was still imported.

### Measured cost

| Cohort | Real book prices? | Units | ROI |
|---|---|---|---|
| v1 (Mar 29–Jun 26) | no (`book` NULL, totals synthetic −110) | +39.9 | +1.88% |
| v2 (Mar 29–Jul 11) | no | +52.3 | +2.11% |
| v4 (Jul 16–Aug 11) | **yes** (6 books) | **−50.3** | **−7.58%** |

v4 totals Over alone: 227 bets, 42.3% win, **−41.9u of the −50.3u**.

**v1/v2's apparent profit was never real.** All 5,469 pre-v4 bets carry
`book = NULL`; totals were booked at a synthetic −110 that existed at no book,
and the entire v1/v2 moneyline profit came from ~125 longshot rows priced at
9.1% implied but realizing 19.2% — the in-play contamination repaired
2026-07-08. Strip that bucket and v1/v2 moneyline is negative too; favorites
lost outright in both (−62.8u).

### The fix (shipped, `mlb-gameline-v5`)

The side now comes from the model's own **skew-aware empirical predictive
distribution** — `total_distribution()` / `total_probabilities()`, built from
rolling-origin prior-only residuals and already frozen in every prediction
snapshot's `feature_values.total_distribution` since 2026-07-11. It was written
but never wired into the bet path.

- `is_over = p_over > p_under` — **never** `lam > line`. New helper
  `frozen_total_distribution()` reads the snapshot and validates the
  distribution was built at the *same* line (a different line is a different
  proposition).
- `our_prob` is **conditional on no push** (`p_win / (p_win + p_loss)`), so it
  is directly comparable to the two-sided vig-free book quote, which also
  excludes the push. Integer lines carry real push mass (~8%).
- **Fails closed**: no usable distribution ⇒ decline the bet and log. It never
  falls back to a symmetric parametric distribution, because "mean == median"
  is precisely the assumption that caused this.
- The soccer Poisson import and the dead `_record_fixture_legacy` (never called;
  it carried a second copy of the buggy path) are removed.
- Version bumped **v4 → v5** per the standing rule: decision semantics changed,
  so the evidence must not mix with v4's.

### Replay at exact recorded book prices (n=325 settled v4 totals)

| | Over share | Record | Units | ROI |
|---|---|---|---|---|
| OLD (`lam > line` + Poisson) | 69.8% | 144-181 (44.3%) | −46.7 | −14.38% |
| NEW (empirical distribution) | 25.2% | 159-166 (48.9%) | −21.1 | **−6.49%** |

The side flips on 145 of 325 (44.6%). Prices are the real recorded ones — where
the side flips, the bet is graded at the stored `paired_price` for that exact
line and book.

**Read this honestly: the fix removes a structural loss, it does not create an
edge.** −6.49% is still a loss, and this is a *replay* on the same data that
diagnosed the problem, not a validated forward result. Totals MAE is worse than
Vegas in 5 of 6 months (e.g. 3.93 vs 3.75 in July) and moneyline Brier is worse
than market in 5 of 6, so the honest expectation after the fix is
roughly breakeven-minus-vig. **The 2★ cap on both MLB game markets stays.** v5
is the clean prospective cohort that will actually test this; do not read the
+25.6u replay delta as demonstrated edge.

Note the post-fix board now leans Under (~25% Over). That is not an
over-correction: the league Over rate genuinely is 46.5%, so "Under is more
likely" is true in most games. Being on the more-probable side is not the same
as having value — the book prices it accordingly, which is exactly why the
replay still loses.

### Standing rules from this

- **Never derive a bet side from `prediction > line` on a skewed market.**
  Derive it from P(over) vs P(under) under the model's own predictive
  distribution. Covered by `tests/test_mlb_total_side_selection.py`, which
  asserts on the AST that no mean-vs-line comparison exists in the bet path.
- Before trusting any residual-over-market model, check `mean(actual − line)`
  against `median(actual − line)`. If they differ, a point comparison is a
  permanent tilt toward one side.
- Any new sport/market that regresses `actual − line` inherits this bug the
  moment a point comparison picks the side.

---

## MLB Beat-Writer Information-Latency Pilot — Spec (2026-07-05)

### Relation to the standing Edge-Finding Roadmap

The Edge-Finding Roadmap already names this idea (P4 — information latency:
lineups, weather, injuries) and explicitly gates it: "Only if P1 shows we're
directionally right but late... Don't build speculatively." That gate has
not been formally cleared. This pilot is deliberately scoped small enough
(2 teams, one free source, extraction-feasibility first) to be a cheap,
falsifiable probe rather than a violation of that discipline — if Phase 0
fails, the cost is a few hours of scraping/prompting work, not a production
system. Nothing here surfaces to the real bet ledger or star ratings until
Phase 2 clears its own bar, same as every other spec in this file.

### Why this is a different, weaker-by-default hypothesis than beat-writer
### injury reporting sounded like at first

Discussed and rejected first: an LLM "watches YouTube betting shows /
reads betting-site articles and judges if there's an edge." Rejected
because (a) that content is downstream commentary on the same public data
this project already ingests, not new information, and (b) asking an LLM
to freely judge "is there an edge" on unfalsifiable opinion content is the
highest-risk version of this idea — LLMs reliably produce confident,
narrative-consistent conclusions regardless of whether real signal exists,
and there is no way to pre-register or backtest a "vibe check." The
version below avoids both problems: the source is factual reporting (not
opinion), and the LLM's only job is **structured fact extraction**, never
edge-judgment — the actual edge test happens afterward, numerically,
against real market data, exactly like every other backtest in this file.

### Scope (Phase 0/1)

**Teams: Baltimore Orioles only (corrected 2026-07-05, before any code was
written).** The original plan assumed MASN covers both Nationals and
Orioles. Verified live before scraping anything — it doesn't: MASN's
category sitemap has only `orioles`/`masn`/`uncategorized` (no `nationals`
category exists), and none of its 9 listed authors is an active Nationals
beat writer (the "Zuckerman" URL resolves but hasn't posted since 2022;
other Nationals-tagged bylines are stale since 2023). Decision: build and
validate the pipeline against Kubatko's very active, precisely-timestamped
Orioles coverage first; add Nationals later once a viable free source is
found (not MASN).

**Source:** MASN Sports (masnsports.com), Roch Kubatko's Orioles blog only
— free, non-paywalled, robots.txt fully permissive for the domain.
Washington Post and The Athletic were considered and dropped for this
phase regardless of team: both are subscription-gated, and scraping
paywalled content raises real ToS questions this project hasn't needed to
navigate before.

**Channel:** published articles, not X/Twitter (explicit choice — avoids
X's API paywall, at the honest cost of likely lagging real-time posts by
hours on fast-moving news; Phase 0 should surface how large that lag is
rather than assume it away).

### Fixed candidate fact types (pre-registered — no others added mid-pilot)

1. **Starting pitcher confirmed or changed**
2. **Named player injury/IL status change**
3. **Bullpen availability note** (e.g. "closer unavailable after 3 straight
   days," a workload/fatigue signal — the same day-to-day bullpen-fatigue
   idea already on the not-yet-tried list in the MLB Moneyline section
   above, arriving here via a different channel)

Anything else the model notices is discarded, not added as a fourth
category mid-pilot — new fact types require a new, separately pre-
registered extension, same non-negotiable as every other spec in this file.

### Architecture

```
MASN article (masnsports.com)
    → ingest/mlb_beat_articles.py   scrape + parse real publish timestamp
                                     (not scrape time — point-in-time
                                     correctness is the whole point)
    → mlb_beat_articles table       raw text + metadata, append-only
    → model/mlb_beat_extraction.py  DeepSeek structured-extraction call
    → mlb_beat_facts table          one row per extracted fact, or none
    → model/mlb_beat_timing_study.py (Phase 1)
          joins facts to game_odds_history / mlb_matchups,
          measures market movement timing relative to fact publish time
```

**LLM:** DeepSeek API, reusing the conventions already established in
`Agents/deepseek_mcp/server.py` (`DEEPSEEK_API_URL =
"https://api.deepseek.com/chat/completions"`, model `deepseek-chat`,
`DEEPSEEK_API_KEY` from environment/GitHub Secrets — same secret-storage
pattern as `ODDS_API_KEY`/`DNN_COOKIE`). The production ingestion path
calls the API directly from `model/mlb_beat_extraction.py` (not through the
MCP server, which is an interactive dev-tool interface, not a scheduled
pipeline component) and reuses this project's `_call_with_retry()`
exponential-backoff convention for resilience.

**Extraction contract, to control hallucination risk:**
- Structured JSON output only: `{"facts": [{"fact_type", "team", "player_name"
  (nullable), "description", "quote"}]}` — empty `facts: []` is the expected,
  common case (most articles won't contain any of the 3 fact types).
- `quote` is mandatory and must be a **verbatim substring of the source
  article** — this is the grounding check. Any extracted fact whose quote
  doesn't literally appear in the source text is discarded as a likely
  hallucination before it ever reaches `mlb_beat_facts`, no human review
  needed for that specific failure mode.
- Temperature low (0.1-0.2, matching the existing DeepSeek MCP convention)
  since this is extraction, not creative generation.
- `model_version` stamped on every row (e.g. `"beat-extract-deepseek-v1"`),
  same non-negotiable as `soccer_bets`/`mlb_bets`/`tennis_bets` — a future
  prompt or model change bumps the version rather than silently mixing.

### Schema (new tables)

```
mlb_beat_articles
  id | source ('masn_nationals' | 'masn_orioles') | team_id | url (UNIQUE)
  | title | published_at TIMESTAMPTZ | raw_text | scraped_at

mlb_beat_facts
  id | article_id FK | fact_type | team_id | player_name (nullable)
  | description | quote | model_version | extracted_at
```

### Phases

| Phase | Scope | Hypothesis / gate | Status |
|---|---|---|---|
| **P0 — Feasibility** | Scrape MASN, extract against the 3 fixed fact types, hand-label a sample (~30-50 articles) to score extraction precision (grounded, correct fact_type) and recall (didn't miss an obvious one) | No statistical edge claim yet. Gate to proceed: precision ≥ 80% on the hand-labeled sample — below that, the extraction step itself isn't trustworthy enough to build a timing study on top of | **✅ Cleared (2026-07-05).** Precision ~95-97% (v1 and v2). Recall improved via one targeted prompt revision (v2: 26→20 zero-fact articles of 58) but remains imperfect by design acceptance, not by oversight — see build results below |
| **P1 — Timing study** | For every extracted fact, does the market (moneyline/total, or a specific prop if player-specific) move in the implied direction after `published_at`, and with what lag? | Pre-registered minimum sample before any conclusion — realistic expectation-setting up front: 2 teams × MASN-only likely yields on the order of a few qualifying facts per team per week, so reaching even a modest 50-fact sample will likely take multiple months, not weeks. State the actual accumulated n honestly when this phase reports, the same way the MLB Underdog spec reported n=125 against its own floor rather than rounding up | **Stage A built and run (2026-07-05) — mechanical check only, see below. Stage B (directional test) not started.** |
| **P2 — Backtest** | Only if P1 shows real, timely movement: would betting the gap before that movement have been profitable at the odds available then? Walk-forward, bootstrap CI, same discipline as every other backtest in this file | Gated on P1 clearing its bar | Not triggered |
| **P3 — Live shadow / scale** | Only if P2 clears its bar: shadow-track live before anything touches a real star rating, then consider scaling beyond 2 teams / MASN-only | Gated on P2 | Not triggered |

### P0 build results (2026-07-05)

Built `ingest/mlb_beat_articles.py` (scraper) and `model/mlb_beat_extraction.py`
(DeepSeek extraction, `model_version = "beat-extract-deepseek-v1"`).

**Real-site findings, verified before/while building (not assumed):**
- robots.txt is fully permissive for masnsports.com.
- The listing page's own `<link rel="next">` pagination is broken — the
  linked `/page/2/` URL 404s. Worked around via the site's own
  `post-sitemap43.xml` (site-wide, all authors) to backfill history:
  fetch each candidate, keep only ones actually bylined "Roch Kubatko",
  discard the rest.
- Backfill run: 60 sitemap candidates checked → 52 stored (8 filtered out
  as wrong author) + 6 from the initial listing-page run = **58 articles**,
  spanning 2026-05-28 to 2026-07-05 (~5.5 weeks).

**v1 extraction run:** all 58 articles processed, 85 facts stored total
(83 + 2 from an earlier small test batch), 4 ungrounded facts discarded by
the quote-verification check before ever reaching the database (the safety
mechanism doing exactly its job). Breakdown: `injury_status` 63,
`starter_change` 21, `bullpen_availability` 1. 26 of 58 articles yielded
zero facts.

**Full manual review of all 85 v1 facts against source quotes (2026-07-05
— this is one reviewer's careful pass, not the formally independent
hand-label the spec calls for, but thorough and worth recording honestly):**
- **Precision on extracted facts: strong, ~95-97%.** Only one clear
  misclassification found: a Cameron Weston callup tagged `starter_change`
  when the article explicitly says he's joining "to get their bullpen back
  to eight relievers" — a `bullpen_availability`/roster move, not a starter
  change. Two more facts (a Triple-A pitcher's surgery, a Triple-A player's
  shoulder issue) were accurately extracted but concern minor-league
  players with no corresponding MLB betting market — not wrong, just not
  useful for Phase 1.
- **Recall: a real problem, not a nitpick.** Spot-checking the 26 zero-fact
  articles found genuine, unambiguous misses — not subtle inference calls.
  One article contained the explicit sentence *"The Red Sox are starting
  southpaw Connelly Early"* and the pipeline extracted nothing from it at
  all. Another mentioned Adley Rutschman's IL reinstatement in passing
  (inside a story primarily about a different player's DFA) and also went
  unflagged. Starter identification is arguably the single most valuable
  fact type for Phase 1, so this mattered enough not to wave through.

**v2 (2026-07-05, one targeted revision, not a shotgun rewrite):** rather
than declare Phase 0 done on precision alone, revised the system prompt
(`model_version` bumped to `"beat-extract-deepseek-v2"`, v1 preserved for
comparison — never mutated in place) to explicitly instruct full,
sentence-by-sentence reading rather than pattern-matching on
announcement-style phrasing, to treat a fact buried mid-paragraph as
equally valid as a standalone bulletin, to disambiguate bullpen-callup vs.
starter-change transactions, and to exclude minor-league mentions at the
prompt level (not just post-hoc). Re-ran fresh against all 58 articles:

- Zero-fact articles: **26 → 20** (net recall improvement).
- The Connelly Early case: **now caught cleanly**, correctly typed and
  grounded.
- The Rutschman/Huff case: **still missed** even under v2 — a genuine,
  persistent gap (the fact is a subordinate clause inside a sentence
  primarily about a different player's transaction, a harder extraction
  case than a full standalone sentence).
- Spot-checked the 14 new facts v2 found in articles v1 had entirely
  missed — all correctly grounded and classified on inspection, no
  apparent precision cost from loosening the instructions (ungrounded-quote
  discards did rise from 4 to 10, consistent with the model attempting more
  extractions overall — the safety check catching the difference is exactly
  what it's there for).

**Status: one disciplined iteration (measure → one targeted fix → re-measure)
completed, not open-ended prompt chasing.** Precision clears the 80% bar
comfortably on both versions. Recall improved materially but is not
perfect and never will be — that's an accepted, documented limitation
going into P1, not a blocker. `model_version = "beat-extract-deepseek-v2"`
is the one Phase 1 should build on. A fully independent human hand-label
pass (the report already delivered to the user) is still valuable but is
no longer the sole gate, since concrete before/after evidence now exists.

### P1 Stage A build results (2026-07-05)

Built `model/mlb_beat_timing_study.py`. Rather than jump straight to the
full P1 question ("did the market move in the *implied* direction"), which
requires a per-fact expected-direction label (is this injury_status fact
good or bad news for the team?) that hasn't been designed yet, this stage
asks a narrower, already-answerable question first: **is there more market
movement (either direction) around beat-writer-linked games than around a
baseline of all other games in the same window?** Assigning directionality
only after looking at movement data would be exactly the kind of
after-the-fact rationalization this file's pre-registration discipline
exists to prevent — so Stage A deliberately stops short of that, and Stage
B (the real directional test) is not yet designed.

**Mechanism:** for each `beat-extract-deepseek-v2` fact, find the team's
next `mlb_matchups` game (skip if none within 7 days), then find the first
`game_odds_history` snapshot at/after the fact's `published_at` ("entry")
and the last snapshot for that matchup ("close") — same entry/close
convention as `model/clv_report.py`. Compute movement in the fact's own
team's implied win probability (flipping sign for away-team facts so
home/away are comparable) and in the total.

**Run result (mechanical check, not an analysis):** 61 of 111 facts linked
to a measurable window; 28 baseline games in the same span. Avg
|movement| on fact-linked games 15.6pp vs. 13.2pp baseline — directionally
higher but the two samples are far too close and far too small (n=61/28)
to distinguish from noise; no CI is computed here on purpose, since
computing one would imply this is a real test rather than a plumbing
check. Lag from publish to the first observable odds snapshot averaged
2.5-6.4 hours across fact types, consistent with the roughly twice-daily
capture cadence currently in place for MLB odds history — a real
resolution limit worth naming: sub-day lag cannot currently be measured
precisely.

**Status: infrastructure works end-to-end.** No conclusion drawn — the
sample is nowhere near the pre-registered minimum from the P1 phase
definition above, and Stage B's direction-labeling design hasn't started.
Next real steps, not yet begun: (1) design and pre-register the
expected-direction rule per fact_type before looking at any more movement
data, (2) let facts keep accumulating over the season, (3) revisit once
volume clears a real minimum sample.

### Automated collection (2026-07-05)

Actual accumulation rate turned out faster than the spec's original
worry (~11 linkable facts/week observed, not "a few per team per week") —
reaching a real minimum sample (200, matching this file's usual bar) is
~18 weeks away, not indefinite. Given that, and that the marginal cost of
collection is near-zero, `.github/workflows/refresh_mlb_beat_articles.yml`
now runs the scraper + extraction every 3 hours (Kubatko posts throughout
the day, ~4am to ~9pm ET observed, and catching articles close to
publication matters for this pilot's own latency measurement — a once-daily
batch would blur exactly the timing this is trying to measure).
`workflow_dispatch` is also enabled for manual runs. Requires the
`DEEPSEEK_API_KEY` GitHub secret (not yet set as of this writing — `.env`
has it for local runs, but the scheduled workflow needs it added
separately via `gh secret set DEEPSEEK_API_KEY`).

Stage B (the directional rule) and the real sample-size analysis remain
deliberately deferred — collection running does not mean analysis is
running. Revisit once volume is worth the design effort.

### Non-negotiables (same discipline as every prior spec in this file)

- The 3 fact types are fixed now, before any article is read. No new
  category gets added mid-pilot without its own pre-registration.
- The LLM extracts facts; it never judges whether a fact constitutes an
  edge. That determination happens numerically in P1/P2 against real
  market data, not as a model opinion.
- No fact reaches `mlb_beat_facts` without its `quote` being a verbatim,
  checkable substring of the source article.
- P0's 80% precision gate is a hard stop, not a suggestion — a leaky
  extraction step would corrupt every phase built on top of it, the same
  lesson as the MLB odds-averaging bug and the point-in-time leak above.
- Do not scale to more teams or add the X/Twitter channel until P0 and P1
  both report honestly, including if the answer is "inconclusive" or
  "no meaningful movement detected" — those are legitimate, recordable
  outcomes in this file, not failures to hide.

---

## Video Analysis — General YouTube Summarization Feature (2026-07-05)

A separate, sport-agnostic feature (not part of the MLB beat-writer pilot
above, though it grew out of the same discussion): a web page where a user
pastes any YouTube URL and gets a structured, per-team/per-player
breakdown of what the video discusses. Unlike the beat-writer pilot, this
makes no edge claim at all — it's a decision-support/summarization tool,
so it doesn't carry the walk-forward/pre-registration discipline the rest
of this file requires for betting-relevant claims.

### Architecture

Built as a Next.js feature (page + server action), not a Python script —
the live site is Next.js/Vercel, which doesn't shell out to Python, and
this needs to run synchronously from a website button.

```
YouTube URL
    → web/src/lib/youtube-transcript.ts   fetch transcript, no API key
    → web/src/app/video-analysis/actions.ts   DeepSeek structured summary
    → video_analysis table (cached by video_id)
    → web/src/app/video-analysis/{page,video-analysis-client}.tsx
```

**Transcript fetching (`youtube-transcript.ts`):** there is no official
public API for this (Data API v3 caption download requires OAuth as the
video owner). Uses the same technique as the widely-used
`youtube-transcript-api` Python library: impersonate YouTube's internal
"innertube" API as the ANDROID client, which returns caption track URLs
that work without the bot-detection "PO token" the web client's caption
URLs require. Verified against a live video before writing the TypeScript
port (a naive web-client-based attempt failed with an empty response —
the innertube/Android approach was the fix, confirmed by inspecting the
actual `youtube-transcript-api` source rather than guessing). This is
unofficial and could break if YouTube changes the innertube contract —
accepted and documented, not hidden.

**Analysis:** DeepSeek (`video-analysis-deepseek-v1`), sport-agnostic
prompt — explicitly does not assume a single sport, since one video can
span several. Asks for team/player name, type, best-guess sport, a
plain-language summary of what was said, and an approximate timestamp
grounded in the transcript's own timestamps. No verbatim-quote-grounding
requirement like the beat-writer pilot (this is a summarization task, not
a fact-extraction pipeline feeding a later numeric test), but timestamps
are still tied to real transcript positions to keep it checkable.
Transcript capped at 60K characters to stay safely under the model's
context window — long videos get truncated, noted in the response
message rather than silently dropped.

**Caching:** results keyed by `video_id` (UNIQUE) — re-analyzing the same
video returns the cached result instead of re-fetching/re-calling the LLM,
unless the user explicitly clicks "re-analyze."

### Verified (2026-07-05)

Tested against the live dev server via direct HTTP calls (the browser
preview tool itself was unresponsive this session — a tooling issue, not
a code issue, confirmed by testing the exact same server-action code path
directly): page renders (200 OK, `video_analysis` table self-provisions
via `ensureVideoAnalysisTables()` on first request, same pattern as every
other experimental table in `web/src/db/ensure-schema.ts`); full pipeline
works end-to-end against a real video (transcript fetched, DeepSeek
correctly returned zero subjects for a non-sports video rather than
hallucinating fake ones); cache hit confirmed on a second call; both error
paths (unparseable URL, unavailable video) return clean, specific
messages instead of a generic failure.

### Secrets

Requires `DEEPSEEK_API_KEY` as a **Vercel environment variable** — a third
place this key now lives, distinct from the Python `.env` (local dev) and
the GitHub Actions secret (`gh secret set DEEPSEEK_API_KEY`, for the MLB
beat-writer pilot's scheduled workflow). Not yet confirmed set on Vercel
as of this writing.

---

## YouTube Picks Channel Tracking — BettingPros Pilot (2026-07-05)

Grew out of the "confirmations for parlay/prop edges" discussion: rather
than another information-latency test, this pipeline tracks a specific
betting-picks YouTube channel's actual track record over time — a
structured, gradable extension of the general Video Analysis feature
above, following this project's standard ledger-and-settle pattern
(same shape as `mlb_bets`/`soccer_bets`/`tennis_bets`), not a new
methodology.

### Two real technical findings, verified before building on them

1. **New-video detection needs no scraper.** Every YouTube channel has a
   free, public, no-API-key RSS feed: `youtube.com/feeds/videos.xml?
   channel_id=UC...`. Verified live — returns exact video ID, title, and
   publish timestamp for the ~15 most recent uploads, no auth. This is a
   documented, intentionally-public mechanism, unlike the transcript-fetch
   technique, so it's far less likely to break. Resolving a channel's
   `@handle` to its `channel_id` is a one-time lookup: fetch
   `youtube.com/@handle` once and read `"externalId":"UC..."` from the
   page (cross-checked against the `<link rel="canonical">` tag — matched).

2. **Transcript fetching gets IP-blocked at volume — a proxy fixes it.**
   Mid-testing, `youtube_transcript_api` started raising `RequestBlocked`
   from this dev sandbox's IP, including on a video that had worked
   minutes earlier — the same "cloud/datacenter IP" blocking class already
   hit with FanGraphs and stats.nba.com elsewhere in this project. A
   residential proxy (user-provided, already used for a separate project)
   fixed it immediately — verified against the exact video that had just
   been blocked (363 transcript segments fetched successfully through the
   proxy). Wired in via `YOUTUBE_PROXY_URL` (`http://user:pass@host:port`),
   read directly via `os.environ` in the ingest script — same one-off
   secret pattern as `DNN_COOKIE` (no `config.py` dataclass needed for a
   single-source proxy string).

### Architecture

```
BettingPros channel RSS feed (youtube.com/feeds/videos.xml?channel_id=...)
    → ingest/youtube_picks_videos.py   detect new videos, fetch transcript
                                        (proxied via YOUTUBE_PROXY_URL)
    → youtube_pick_videos table        raw transcript + metadata
    → model/youtube_picks_extraction.py  DeepSeek structured pick extraction
    → youtube_picks table              one row per pick, or none
    (settlement against real outcomes — NOT YET BUILT, see below)
```

Channel: **BettingPros** (`@bettingpros`, channel_id
`UC8hVLL1dC1NjEtL1208U--g`) — an extremely active multi-sport picks
channel: a daily flagship "Daily Juice" show (Matt Perrault) plus many
same-day #shorts, covering MLB, World Cup soccer, WNBA, NFL futures, F1,
and novelty markets (e.g. Nathan's Hot Dog Eating Contest).

### Extraction schema (fixed, pre-registered — same discipline as the MLB
### beat-writer pilot)

```
youtube_picks
  sport ('nba'|'mlb'|'nfl'|'nhl'|'wnba'|'soccer'|'tennis'|'f1'|'other')
  bet_type ('moneyline'|'spread'|'total'|'prop'|'futures'|'other')
  subject | opponent (nullable) | selection | odds_american (nullable int)
  game_context (nullable) | confidence_label (nullable, e.g. "best bet")
  quote (mandatory verbatim substring of the transcript — the grounding
    check against hallucination, same as mlb_beat_facts)
  model_version | status ('pending' — settlement not built yet)
```

Transcripts here are auto-generated ASR captions (no punctuation, run-on,
occasional mishearings) — messier than the beat-writer pilot's clean
written prose. The prompt explicitly tells the model to copy quotes
exactly as they appear in the messy transcript rather than "cleaning up"
grammar, so the grounding check still works against the raw text.

### Build results (2026-07-05)

Ran against the channel's 8 most recent videos (3 flagship episodes,
5 shorts). **Extraction quality is strong on inspection**: 23 picks
extracted (1 ungrounded pick correctly discarded by the quote check),
every pick specific and gradable — e.g. "Guardians moneyline at -149",
"Red Sox run line at +104", "Argentina to win the World Cup at 4 to 1"
(correctly converted to `odds_american=400`), WNBA player props (Angel
Reese under 12.5 rebounds), F1 top-N-finish props. `odds_american` is
populated whenever a specific price is stated in the transcript and left
null otherwise — added to the schema mid-build (before any commit, so no
migration needed) specifically so settlement/ROI math doesn't need to
re-parse odds out of free text later.

Not yet independently hand-labeled (same caveat as the beat-writer pilot's
own build-results section) — this is inspection-level confidence, not a
formal precision gate.

### What's NOT built yet: settlement

Extracted picks currently sit at `status='pending'` forever — there is no
mechanism yet to resolve a pick to a real game and grade it won/lost/push.
This is a meaningfully harder problem than the single-sport MLB beat-writer
pilot, because picks span **five-plus sports**, and this project currently
only has real game-result infrastructure for MLB and soccer (World Cup) —
there is no NFL, WNBA, or F1 results pipeline anywhere in this codebase.
Honest scoping for the next phase: settlement should start with MLB and
soccer picks only (where this project already has real outcome data and
existing fuzzy team-name matching, e.g. `_levenshtein()`), and NFL/WNBA/F1
picks will accumulate in the ledger unsettled until (if ever) this project
builds those sports' result pipelines — a known, documented limitation,
not an oversight.

### Automated collection

`.github/workflows/refresh_youtube_picks.yml` runs the scraper +
extraction every 3 hours, mirroring the MLB beat-writer workflow's
cadence (this channel posts multiple times a day). Requires
`YOUTUBE_PROXY_URL` as a GitHub secret — **not yet set** as of this
writing (only in local `.env`); `DATABASE_URL` and `DEEPSEEK_API_KEY` are
already set.

### Non-negotiables (same discipline as every prior spec in this file)

- The extraction schema is fixed now, before settlement is designed. No
  new field categories added mid-pilot without updating this section.
- The LLM extracts picks; it never judges whether a pick is good. Rating
  the channel's actual accuracy is a separate, later, numeric phase
  against real settled outcomes — exactly like every other ledger in this
  project.
- No pick reaches `youtube_picks` without its `quote` being a verbatim,
  checkable substring of the transcript.
- Settlement scope stays honest: MLB/soccer first (real data exists);
  NFL/WNBA/F1 picks are tracked but not graded until this project has
  the outcome data to grade them against — do not fake a settlement for
  a sport with no real result pipeline.

### Web UI (2026-07-05)

Added `/youtube-picks` — a feed of extracted picks (sport/bet-type filters,
search, odds, confidence label, linked source video, expandable source
quote), reading the same `youtube_pick_videos`/`youtube_picks` tables the
Python pipeline writes (Drizzle definitions added to `web/src/db/
schema.ts`, read-only from the web app — same "Python owns the table"
pattern as `mlb_matchups`/`mlb_team_stats`, no `ensureSchema()` needed
since the tables already exist via `db/schema.py`).

**Deliberately does not show a leaderboard, accuracy rate, or "rate the
YouTuber" view** — every pick is still `status='pending'` since
settlement isn't built. A prominent banner says so explicitly rather than
implying more confidence than the data supports. Once MLB/soccer
settlement lands, this same page is where won/loss and accuracy should
surface next — sequencing chosen explicitly (ship visibility now,
extend with real grading later) rather than waiting for settlement to
ship any UI at all.

Verified against live data: 200 OK render, all 23 extracted picks display
correctly (subject/opponent, selection, signed odds, confidence badges,
linked video, expandable quote), sport filter verified narrowing 23→3 for
WNBA exactly as expected from the underlying data.

### Channel management (2026-07-05)

Added `youtube_pick_channels` — the one table in this pilot with genuinely
shared ownership: **written from the web app** (the new "Add Channel" UI
on `/youtube-picks`) and **read by the Python ingest script** (which
channels to scrape each run). Defined in both `db/schema.py` and
`web/src/db/ensure-schema.ts` (`ensureYoutubePickChannelsTable()`) so it
self-provisions regardless of which side runs first — same precedent as
`game_odds_history`/`player_prop_history`.

**Resolution mechanism (web side, TypeScript):** `addYoutubeChannel()`
accepts an `@handle` or a full channel URL, fetches the channel page, and
extracts `"externalId":"UC..."` — the same technique already verified for
`web/src/lib/youtube-transcript.ts` and the Python ingest script, ported a
third time. New channels are picked up by the scheduled scraper on its
next run; adding one does not fetch videos immediately (correctly
communicated in the UI, not left ambiguous).

**Python side:** `ingest/youtube_picks_videos.py`'s `fetch_new_pick_videos()`
already took `channel_id`/`channel_name` as parameters — no change needed
there. Added `fetch_new_videos_for_all_channels()`, which queries
`get_active_youtube_pick_channels()` and loops, plus a one-time
`_seed_default_channel_if_empty()` backfill so existing BettingPros
tracking keeps working unchanged for anyone who already had this running
against the old hardcoded constant. `__main__` now calls the
all-channels function.

**Verified end-to-end through the actual UI** (not just curl): added a
real second channel (`@MrBeast`) via the browser, confirmed "Added
MrBeast." success message, confirmed both channels appeared in the
tracked-channels list, confirmed both rows persisted correctly in
Postgres with the right `channel_id`/`handle`. Removed the test channel
afterward — not something actually meant to be tracked, added purely to
prove the resolve-and-register flow works live.

### Settlement (2026-07-06)

**Scope, fixed before writing any grading logic:** moneyline picks only,
for MLB/soccer/tennis — the three sports with real, already-built
game-result infrastructure and existing fuzzy team-name matching
(`_levenshtein()`/`rapidfuzz`). Spread/total grading needs a structured
numeric line value the extraction schema doesn't capture yet (only
free-text `selection`, e.g. "Braves -1.5 runs") — parsing that reliably
enough to grade real money outcomes is deferred, not faked. Every other
sport (WNBA, NFL, F1, other) and every other bet type is classified
`unsettleable` up front rather than left ambiguously `pending` forever —
the UI can now tell "waiting on a game" apart from "will never be
graded," instead of implying more coverage than exists.

**New file:** `model/youtube_picks_settlement.py` — three phases, run
every time:
1. **classify** — mark out-of-scope picks `unsettleable` (one-time per pick)
2. **resolve** — fuzzy-match subject/opponent to a real game/match, freeze
   `matchup_ref` (`"{sport}:{row_id}:{side}"`) so grading never re-runs
   the fuzzy match
3. **grade** — for resolved picks whose game is now final, compare the
   picked side to the actual winner; status → `won`/`lost`

**Schema additions:** `youtube_picks.result_detail TEXT` (human-readable
final score/winner, e.g. `"Final 7-6"` or `"Final (90') 2-1"`),
`youtube_picks.settled_at TIMESTAMPTZ`. Added to both `db/schema.py` and
the Drizzle `web/src/db/schema.ts` (read-only from the web app — Python
still owns writes to `youtube_picks`, same as extraction).

**New `db/queries.py` functions:** `mark_youtube_picks_unsettleable()`,
`get_resolvable_youtube_picks()`, `set_youtube_pick_matchup_ref()`,
`get_resolved_pending_youtube_picks()`, `settle_youtube_pick()`.

**Bug found and fixed during build:** the first resolution attempt used a
fixed 2-day `BETWEEN` date window (publish day, publish day + 1) to find
the game a pick referred to. This produced false ambiguity — 2 candidate
games — for 4 MLB moneyline picks, because these were back-to-back series
(same two teams playing on consecutive days). The resolver correctly
refused to guess (`len(games) != 1` → `None`, left honestly `pending`),
so this wasn't wrong output, but a real limitation. **Fixed** by trying
the exact publish date first, only falling back to the next day if zero
games match at the exact date (`_find_team_games()`/
`_find_tennis_candidates()`). Verified: all 4 previously-stuck picks
(Phillies/Pirates, Royals/Rays, Braves/Mets, Nationals/Pirates) resolved
and graded correctly on the next run, each manually cross-checked against
real `game_date`/scores in `mlb_matchups`.

**Grading detail per sport:** MLB compares `home_score`/`away_score` from
`mlb_matchups`. Soccer uses `reg_home_score`/`reg_away_score` (the
90-minute regulation score, not an ET-inclusive final — same non-
negotiable established after the Belgium-Senegal incident) from
`soccer_matchups`, and supports a `draw` winner. Tennis compares against
`tennis_matches.winner` (`home`/`away`).

**Verified results (2026-07-06):** first run — 35 picks marked
`unsettleable` (correct sport/bet-type scope), 4 resolved but stuck
pending on the date-window bug above; after the exact-date-first fix,
all 4 resolved and the full graded set was 5 `won` / 3 `lost`, every one
manually cross-checked against real final scores as correct.

**Web UI updated:** the `/youtube-picks` warning banner now reads
"Settlement only covers moneyline picks for MLB, soccer, and tennis...
grade automatically once the game finishes. Spread/total bets and every
other sport... show as unsettleable for now, not silently ignored" —
replacing the earlier "settlement isn't built yet" banner. Status badges
are now color-coded (`STATUS_STYLE` map: won=emerald, lost=rose,
unsettleable=muted, pending=amber) and each settled pick shows its
`resultDetail` (final score) inline. Verified rendered correctly against
live data: 5 won (emerald), 3 lost (rose), 36 unsettleable (muted).

**Automation:** `.github/workflows/refresh_youtube_picks.yml` gained a
`python -m model.youtube_picks_settlement` step after extraction, so
settlement runs every 3 hours alongside scraping/extraction — no separate
schedule needed.

**How to apply:** if asked to grade WNBA/NFL/F1/other sports, this
project has no result-data infrastructure for them at all — say so
explicitly rather than attempting a fake settlement, same documented
boundary as when the pilot was first scoped.

### Settlement — extended to totals + spreads (2026-07-06)

The original "moneyline only, schema change required first for
totals/spreads" scoping (above) was **superseded the same day** at the
user's explicit request ("grade based on all of the available settlement
stats we have. Not just moneyline"). Rather than add a structured numeric
`line` column + re-extract, the free-text `selection` is parsed at
settlement time — a deliberate reversal of the earlier "don't regex-parse
`selection`" note, justified by (a) the user's explicit call, and (b) a
conservative parser that grades ONLY when the market parses unambiguously
and refuses everything else, so nothing is faked.

**Scope now:** `moneyline`, `total`, `spread` for **MLB and soccer**
(both teams' numeric scores available: MLB runs, soccer 90-minute goals);
**tennis stays moneyline-only** (we store a winner, not game counts, so
tennis totals/spreads are intentionally out of scope). Encoded as an
explicit `_ALLOWED_PAIRS` tuple of `(sport, bet_type)` — the classify
query is now `(sport, bet_type) NOT IN %s`, not the old
`sport NOT IN … OR bet_type NOT IN …` (which couldn't express per-sport
bet-type support).

**Parsers (`model/youtube_picks_settlement.py`):**
- `_parse_total` → `(line, 'over'|'under', is_team_total)` from e.g.
  "Over 9.5 runs", "Phillies team total over 4.5 runs". Team totals grade
  on the subject's own score (via the resolved `side`); game totals sum
  both.
- `_parse_spread` → signed line relative to the subject, e.g.
  "Braves -1.5 runs" → -1.5. Grades on `margin + line` (>0 win, =0 push,
  <0 loss). Push verified live (Argentina -2 winning exactly 2-goal →
  `push`).
- Anything that doesn't parse to a line (bare "Over"/"Under" extraction
  glitches, "Both teams to score") is **left `pending` and logged**, never
  guessed — a known, tiny edge (~4 rows) accepted over faking a grade.

**Two honesty guardrails added:**
1. **Partial-game markets excluded** (`_SUBGAME_RE`): "first 5 innings" /
   "F5" / "first five" / "first half" etc. are marked `unsettleable` — we
   only store full-game finals, so an F5 line is ungradable. This also
   **corrected a mis-grade**: the old moneyline-only pass had graded a
   "Royals first 5 innings moneyline" on the full-game final; the
   reclassifier re-checks already-settled won/lost rows too and reset it
   to `unsettleable`.
2. **Self-healing reclaim** (`reopen_in_scope_unsettleable`): the old
   moneyline-only pass had marked ~200 totals/spreads `unsettleable`, and
   resolve only looks at `pending`, so they were stranded. New step
   re-opens `unsettleable` picks whose `(sport, bet_type)` is now in scope
   (excluding partial-game markets) back to `pending`. Makes settlement
   self-healing whenever scope widens, instead of stranding old rows. Runs
   FIRST in `run()`, before classify/subgame/resolve/grade.

**`push` status added** (ties/voids — integer totals landing on the line,
spreads landing exactly on the number). New status string flows through
`settle_youtube_pick` unchanged; web `STATUS_STYLE` gained a sky-blue
`push` pill, and `pending` is now amber.

**Performance:** resolve was loading the full team table *per pick*
(N+1) — this is why the CI settlement step hung 20+ min on the big
backlog. Now each sport's `{team_id: name}` map is loaded once per run
(`_load_teams`).

**Verified results (2026-07-06), across a ~660-pick backlog** (the
pipeline had been silently failing on the missing-`httpx` bug — see
below — so many videos accumulated, then all extracted at once once fixed):
`won 218 / lost 197 / push 6`, `unsettleable 141`, `pending 98` (74
resolved-but-pending = games not yet final). Graded-by-bet-type:
moneyline 268, total 140, spread 13. Multiple totals and spreads
hand-checked against real scores (e.g. "Under 8.5 runs" on 7-4 → total
11 → lost; "Dodgers -1.5" on 15-3 → margin +12 → won; the Argentina -2
push). `0 reopened` on the final run confirms no in-scope pick is wrongly
stranded `unsettleable`.

**New `db/queries.py` functions:** `get_youtube_picks_for_subgame_check`,
`mark_youtube_pick_unsettleable`, `get_unsettleable_in_scope_youtube_picks`,
`reopen_youtube_pick`. `mark_youtube_picks_unsettleable` /
`get_resolvable_youtube_picks` signatures changed from `(sports, bet_types)`
to `(allowed_pairs)`.

**Unrelated bug found while verifying this (2026-07-06):** both scheduled
DeepSeek workflows (`refresh_youtube_picks.yml`,
`refresh_mlb_beat_articles.yml`) had failed on **every** run since they
shipped — `model/youtube_picks_extraction.py` and
`model/mlb_beat_extraction.py` both `import httpx`, which was never in
`requirements.txt`, so CI's `pip install` never installed it
(`ModuleNotFoundError`). Added `httpx>=0.27,<1`. Separately hardened the
scraper: a single video's transcript SSL/connection reset (YouTube
resetting even through the residential proxy) used to crash the whole run
(skipping extraction + settlement); `_fetch_transcript_text` now retries
transient `RequestException` 3× with backoff, and the per-video loop
catches `RequestException` to skip one bad video (it reappears as "new"
next run) instead of aborting.

---

## MLB Totals — 8.0-Line Under Bias — Pre-Registered Study (2026-07-08)

### Origin

The `/vegas` O/U-hit-rate-by-total-tier panel showed the `vegas_total = 8.0`
tier at 37.8% over (93/246 games incl. 36 pushes) — the most under-leaning
of 8 tiers scanned, motivated by eyeballing that table. Per this file's
standing discipline, that observation is a hypothesis source, not evidence
— it is examined here, not confirmed.

### Two artifacts in the raw panel number (found before registering anything)

1. **Pushes inflate the apparent skew.** 37.8% divides overs by ALL games
   including 36 pushes (only integer lines can push — none of the other
   listed tiers except 9.0/10.0 can). **Decided-only, 8.0 is 44.3% over /
   55.7% under** (93/210) — still the most under-leaning tier, but far less
   dramatic than the raw panel number.
2. **"Avg actual > avg line" does not imply under-pricing.** Every tier in
   the panel shows avg actual runs above the line, yet most tiers hit
   ~50/50 or over-lean. This is ordinary right-skew (blowouts pull the MEAN
   up; books set lines near the MEDIAN) — not evidence of systematic total
   mispricing. Do not re-derive "book bias" from avg-error columns again.

### Honest significance check (done before registering, not after)

- z = −1.66 (p ≈ 0.10) vs 50% on the decided-only 210 games — NOT
  significant alone, and this is the most extreme of 8 tiers scanned
  (P(≥1 tier reaches |z|≥1.66 by chance) ≈ 55%). Same multiple-comparisons
  trap as the original soccer-totals mirage.
- Split-half (chronological, decided-only): half 1 = 41.9% over (44-61,
  n=105), half 2 = 46.7% over (49-56, n=105). Same direction both halves
  (mildly encouraging — no sign flip), but neither half individually clears
  significance, and the more recent half drifted toward 50%.
- **−110 breakeven (52.4%) is the WRONG bar.** Integer-total unders are
  routinely juiced past −110 by books (line stays on the integer, price
  shades instead) — `game_odds_history` already captures real per-book
  under prices for every 8.0-line game, so the real test grades at ACTUAL
  captured prices, never an assumed constant.

### Hypothesis (fixed now, before any further data is examined)

**H — MLB 8.0-total unders beat market-implied at real prices.** Games
whose CLOSING consensus total is exactly 8.0 see the under win at a rate
that produces positive ROI at the actual captured closing under price
(`game_odds_history`, not an assumed −110).

- Falsifiable prediction: PROSPECTIVE games only (commence date >
  2026-07-08 — the 246-game discovery sample motivated this, it cannot
  also confirm it, same rule as every other spec in this file), n ≥ 200
  decided (push excluded from n, tracked separately), bootstrap CI on ROI
  at real closing under prices excludes zero.
- Kill criterion: CI includes zero at n=200 → dead, cap stays at whatever
  the standing MLB totals cap is, no re-slicing to 7.5/8.5/9.0 as a
  follow-up (that would be the exact multiple-comparisons drift this
  file's discipline exists to prevent).

### Minimum sample / timeline

**246 games have accumulated at the 8.0 line over the season so far**
(discovery sample, frozen — not part of the test population). New 8.0-line
games arrive at roughly the same rate going forward (~1-2/day in season),
so reaching a fresh, prospective n≥200 is a full-season-scale wait, not a
quick check — same shape as the MLB Underdog-Value study's timeline.
Re-run the query below periodically; do not shortcut the prospective-only
rule by including any of the 246 discovery games in the test count.

### Status

**Registered, not started.** No code changes yet — this section exists so
the hypothesis, slice, and minimum sample are frozen before the next
batch of 8.0-line games is examined. When ready to grade: pull all
`vegas_total = 8.0` games with `commence_time > '2026-07-08'`, join
`game_odds_history` for the closing consensus (or per-book) under price,
compute decided win rate + bootstrap ROI CI. Do not grade early on a
partial sample and call it a trend.

---

## Literature Review & Strategy Confirmation — Development Plan D-series (2026-07-08)

### Origin

Reviewed six documents in `C:\Users\joshb\OneDrive\Documents\Sports
Betting Modeling\` at the user's request to assess whether this project's
modeling approach is correct. Full assessment delivered in-session;
condensed here so the resulting plan is frozen like every other spec.

### Documents reviewed (graded)

| Document | Grade | One-line verdict |
|---|---|---|
| arXiv 2410.21484 — "Systematic Review of ML in Sports Betting" (2024) | B− survey / D as profit guide | Useful challenges/features catalog; uncritically repeats in-sample "99% accuracy" claims and never asks the only question that matters (accuracy vs the CLOSING LINE at real prices) |
| Levine thesis — "Beating Vegas" (Reed, 2019) | C+ | One genuinely novel idea (model the LINE's trajectory → bet timing) buried under fatal flaws: k-fold on time series (leakage), EV estimates >0.2 (screaming miscalibration), Martingale seriously entertained, 2 seasons of data, 9 strategies compared post-hoc |
| Unabated "Intro to Data Science Pt. 3" | B | Beginner content from a sharp source; correct segment logic — props/derivatives, not main lines |
| `sports-betting` PyPI docs | B philosophy / C utility | Package inferior to our infra, but its `market_maximum` (best-price) backtest convention and its "estimate value bets, don't chase accuracy" line are both correct |
| World Tennis Magazine "Risk Modelling" | F | Casino-affiliate SEO filler; zero technical content |
| "How to Improve Sports Betting Odds" blog | D | Ridge-regression team ratings ("60% accuracy", "$20K" anecdote) — a strictly weaker version of ratings we already have and have PROVEN don't beat closing lines |

### Verdict on our methodology

**Confirmed correct — ahead of all six documents on every dimension that
matters**: walk-forward-only validation (vs Levine's leaky k-fold), CLV +
realized-vs-implied at real captured prices (vs accuracy metrics),
probability-space de-vigging + per-book capture (vs assumed flat −110),
pre-registration with frozen kill criteria (vs post-hoc strategy
shopping), and published no-edge verdicts (vs "astronomical returns"
claims). The documents, read critically, corroborate the standing
three-sport conclusion: public-data models do not beat closing lines in
major markets. The strategic question is not method — it is where the
method points.

### Standing DO-NOT-BUILD list (from this review)

1. No new game-outcome model on public stats — not a fourth gameline
   revision, not an NHL/any-sport ratings regression, not neural nets
   chasing survey-paper accuracy numbers. Three settled-ledger verdicts
   is the answer.
2. No Martingale ever (guaranteed ruin). No Kelly sizing until a specific
   market shows a CONFIRMED live edge — Kelly amplifies miscalibration
   into ruin (Levine's own EV>0.2 cap is the live demonstration).
3. No "betting portfolio" layer before there is more than one proven edge
   to diversify across — portfolio math on zero-edge assets optimizes the
   distribution of losses.

### Development plan (D-series) — sequence set by user 2026-07-08

User decision: **D4 first (MLB prop-market expansion), then D1
(best-price grading)**, then D2/D3/D5. Rationale: props are where actual
signal has appeared (8 alerts vs 0 on game lines, P3 finding); expand the
surface where edge exists before improving the accounting.

| Order | ID | What | Why / evidence | Kill criterion | Effort |
|---|---|---|---|---|---|
| 1 | **D4** | **MLB prop-market expansion** ✅ shipped 2026-07-08 (see below): added pitcher_hits_allowed/earned_runs/outs; NBA props at season start still open | Soft markets are where signal showed up (P3: 8 alerts vs 0 game lines); Unabated segment logic | Each new market inherits the standing rule: no positive CLV over a real settled sample → retire that market's detector | Medium |
| 2 | **D1** | **Best-price grading** ✅ shipped 2026-07-08 (see below): `model/best_price.py` overlay, US-retail + any-book tiers, exchanges excluded; surfaced + fixed the MLB in-play rating incident en route | +1–3%/bet with zero predictive skill; `market_maximum` convention; roadmap P5 — confirmed: median uplift +1.2–1.9%/bet, flips no verdicts | None — accounting correctness, not a hypothesis | Small |
| 3 | **D2** | **Execution-timing study** ✅ shipped 2026-07-08 (see below): MLB not yet gradable (n=85 < 100 min, rerun pending); soccer/tennis M3 descriptive-null | Levine's salvageable idea done honestly; zero new data required | Median best-entry vs close price gap < 1% → timing doesn't matter, drop | Small–medium |
| 4 | **D3** | **Opener-vs-closer study** ✅ shipped 2026-07-09 (see below): MLB not yet gradable (n=78 < 150 min each market, rerun ~07-16); soccer descriptive-only | Beating the opener is a weaker benchmark than beating the close; distinct mechanism from D2 (signal content, not entry timing) | MLB ML/totals each independently dead if 95% CI of directional-agreement rate includes 50% AND correlation CI includes 0, at n≥150 each | Medium |
| 5 | **D5** | **Confidence/segment layer** ✅ shipped 2026-07-09 (see below): #1/#2 have no ready data (checked, not assumed); built #3 only — calibration-drift auto-downgrade monitor, dormant (outright_winner 0 windows/tournament unfinished, group_winner 1 window, both need 3 to ever fire) | Prevents the next mirage; prerequisite for ever uncapping | N/A (defensive) | Medium |

**Standing (no new work until dates hit):** MLB underdog re-run at n≥200
(~2026-08-30), 8.0-totals grading at prospective n≥200 (~season end) —
grade against frozen bars, at best captured prices once D1 lands.

### D4 — MLB prop-market expansion — Shipped (2026-07-08)

**Market discovery (empirical, not documentation-assumed):** probed 12
candidate MLB player-prop market keys against 5 real upcoming events,
checking both DraftKings AND Pinnacle presence per market (Pinnacle alone
can't anchor the DK-vs-Pinnacle detector). Result:

| Market | DK | Pinnacle | Verdict |
|---|---|---|---|
| `pitcher_hits_allowed` | 5/5 | 4/5 | ✅ added |
| `pitcher_earned_runs` | 5/5 | 4/5 | ✅ added |
| `pitcher_outs` | 5/5 | 4/5 | ✅ added |
| `batter_home_runs` | **0/5** | 4/5 | ❌ NOT added — DraftKings never posts this market key; Pinnacle-only can't feed the detector regardless of cost |
| `batter_hits`, `batter_rbis`, `batter_runs_scored`, `batter_stolen_bases`, `batter_walks`, `batter_singles`, `batter_doubles`, `batter_triples`, `batter_hits_runs_rbis`, `pitcher_walks`, `pitcher_record_a_win` | mixed | **0/5** | ❌ NOT added — no Pinnacle anchor at all |

**Cost discovery (from the Odds API's own `x-requests-last` response
header, not assumed from docs):** the existing capture used
`regions=us,eu`, priced at `markets × regions`. Switching to
`bookmakers=draftkings,pinnacle` — the exact two books the detector
reads, nothing else — prices at `markets × 1` regardless of book count:
**4 credits/event → 2 credits/event for the original 2 markets (half
price, identical data)**, and the 3-market expansion costs 5 credits/event
total (not 10, which the old `regions` pattern would have required).
Verified directly: 3 real API calls against the same event, reading
`x-requests-last` — 4 → 2 → 5 credits respectively.

**Budget check before deploying (quota is shared across every sport's
odds capture on one key):** `x-requests-used`/`x-requests-remaining`
summed to a round 20,000 (suggesting a 20k/month plan). Presented the
cadence tradeoff to the user via AskUserQuestion rather than deciding
unilaterally — a shared-quota exhaustion would silently degrade every
sport's data capture, not just this feature. **User chose: keep 3×/day
cadence, adopt the `bookmakers` param** (~225 credits/day for props, net
+45/day vs the old 2-market/regions setup — down from a would-be +270/day
if the expansion had naively used `regions`).

**Settlement — boxscore fields verified against real completed games**
(not assumed): the free MLB boxscore's per-player `pitching` stats
dict has direct fields for all 3 new markets — `hits` (hits allowed),
`earnedRuns`, and critically `outs` as a **direct integer** (verified
7.0 IP → outs=21, 5.0 IP → outs=15 on a real game), no need to parse the
`inningsPitched` string. `_mlb_boxscore_stat()` in `model/line_alerts.py`
generalized via a `_PITCHING_STAT_FIELD` market→field map instead of the
old `if market == "pitcher_strikeouts"` special case.

**Changes:**
- `ingest/mlb_prop_odds.py`: `MARKETS` → 5 markets; `REGIONS` replaced
  with `BOOKMAKERS = "draftkings,pinnacle"`.
- `model/line_alerts.py`: `_PROP_MARKET_LABEL`, the Telegram/Discord
  notify label dict, `_mlb_boxscore_stat`'s new `_PITCHING_STAT_FIELD`
  map, and the same-line-price CLV grading market tuple in
  `_grade_alert_prices` all extended to the 3 new markets. `scan_props()`
  itself needed NO changes — it already reads `market`/`player` generically
  from `prop_odds_history` rows.
- `web/src/app/vegas/line-alerts-panel.tsx`: market label switch extended.
- Confirmed unrelated: `web/src/app/dfs/actions.ts`'s
  `MLB_PROP_MARKET_TO_STAT` (a separate DFS-projection prop-coverage audit
  subsystem, not the alert detector) already listed `pitcher_outs` /
  `pitcher_earned_runs` independently — left untouched, no overlap risk.

**Verified end-to-end against real data (2026-07-08):** ran the actual
capture — 336 prop rows written across 5 markets in one real pass (228
batter_total_bases, 27 each of the 4 pitcher markets); 23/27 (85%) of each
new pitcher market's rows carry both DK+Pinnacle, matching the 5-event
probe. Ran the real detector (`scan_props`) against this live data: **16
new alerts fired, including on all 3 new markets** (1 `pitcher_earned_runs`,
3 `pitcher_outs`, 3 `pitcher_strikeouts` `prop_line_gap`s, 1
`pitcher_hits_allowed` `dk_prop_value`) — confirms detector, ledger, and
settlement path all work without any market-specific scan-side code.
Quota after the real production run matched the ~5 credits/event
projection (delta consistent with 15 events × 5 credits ≈ 75/run).

**Standing rule inherited:** each new market's detector is subject to the
same no-positive-CLV-over-a-real-sample retirement rule as every other
signal in this file — this expansion is instrumentation, not a confirmed
edge. Grade via the existing `line_alerts` backtest (`report()` / web
`getLineAlertBacktest`), sliced by `details_json->>'market'`, once enough
settled alerts accumulate on the 3 new markets specifically.

**Expected-value statement (honest):** none of this is a get-rich path.
D1 is guaranteed but small; D2/D3 test the one hypothesis class
(timing/execution) the no-edge verdicts haven't already killed; D4 is
where actual signal has appeared. Realistic ceiling = grind-level edges
of a few percent in soft markets plus execution efficiency. Anything
promising more is selling something.

### MLB In-Play Rating Incident — found + fixed while building D1 (2026-07-08)

D1's spot-checks surfaced ledger corruption before any conclusion could be
drawn from it (the point of rigorous verification): an MLB bet frozen at
**decimal 34.0** ("PHI ML +3300") turned out to be an IN-PLAY price — KC
−10000 / PHI +3300 late in a blowout — written into `mlb_matchups` and
then rated into the ledger as a "closing" recommendation.

**Root causes (two distinct holes, both now guarded):**
1. **Post-commence rating.** The 22:10 UTC `refresh_mlb_vegas` cron takes
   ~65 min (weather geocoding), so `rate_slate` executed ~23:15 — AFTER
   evening first pitches — minting post-commence "recommendations"
   nightly. A recommendation created after the event starts is not a
   recommendation (books void tickets placed post-start). Fix:
   `rate_slate` now only rates fixtures with
   `commence_time IS NULL OR commence_time > NOW()`.
2. **In-play odds leaking into `mlb_matchups`.** `fetch_odds`' in-play
   guard trusts the FEED's `commence_time`, which the Odds API moves on
   rain delays — a delayed in-progress game can reappear with a future
   commence and sail past the guard. Fix: a second guard on OUR
   statsapi-sourced `matchup.commence_time <= now`. Same class of hole in
   `ingest/backfill_mlb_odds.py`: its 20:00 UTC historical snapshot lands
   mid-game for afternoon starts and the historical feed serves the LIVE
   price at that timestamp — now skips games that had started by the
   snapshot time.

**Repair applied to the live DB (2026-07-08, documented not hidden):**
- **58 `mlb_matchups` rows** (spanning 2026-06-26 → 07-06) held odds >10pp
  off their own last clean pre-commence capture — restored from that
  capture (`game_odds_history` consensus). This also cleans the
  moneyline/totals models' training labels (`vegas_prob_home`), which had
  been silently poisoned for those games. Era caveat: for pre-07-02 rows
  the restore source is the as-recorded arithmetic-era consensus (per-book
  data doesn't exist that far back) — still strictly better than in-play
  prices.
- **105 `mlb_bets` rows voided** (55 lost / 50 won — near-symmetric, i.e.
  noise not signal) — every bet with `created_at > event_commence`, with
  `result_detail` documenting the reason and
  `inputs_json.voided_post_commence = true` for audit. The underdog-value
  re-run (n≥200, ~2026-08-30) naturally excludes voids.

**How to apply:** never trust a single upstream timestamp for an in-play
guard — cross-check against our own statsapi-sourced commence. Any future
"rated after commence" bet is a bug, not a feature; the guard makes it
structurally impossible.

### D1 — Best-price grading — Shipped (2026-07-08)

**`model/best_price.py`** — a grading OVERLAY over the immutable ledgers
(never mutates `market_decimal`): for each settled ML/total bet, finds the
best same-proposition price across the captured per-book close
(`game_odds_history.books`, last capture ≤ commence — same close
convention as `model/clv_report.py`) and reports frozen-consensus ROI vs
best-price ROI **on the same covered subset** (uncovered bets — pre-07-02
captures, moved total lines, missing quotes — counted separately, never
folded in).

**Correctness rules learned/enforced while building:**
- **Exchanges excluded everywhere** (matchbook/smarkets/betfair_ex): feed
  odds exclude their 2-5% commission, so they masquerade as free value —
  matchbook "won" 78/175 MLB bets in the naive version.
- **Two tiers reported:** best **US-retail** (DK/FanDuel/BetMGM/Caesars/
  BetRivers/Fanatics/…, the realistic execution set) as the primary, and
  best any-book (excl. exchanges) as the upper bound. Offshore books
  (onexbet etc.) only appear in the upper bound.
- **Same-proposition only:** totals match at the exact captured line
  (`total_line == bet line`) — the Herrera rule applied to pricing.
- Deadlock-resilient: one bulk `DISTINCT ON` close-lookup per sport (the
  per-matchup query loop deadlocked live against the 30-min capture
  cron's transactions) + one retry.

**First results (2026-07-08, post-repair ledger):**
| Sport | n covered | Frozen ROI | US-retail ROI | Any-book ROI | Retail uplift (med) |
|---|---|---|---|---|---|
| MLB | 145 | −3.39%/bet | −3.73%/bet | −2.21%/bet | +1.34%/bet |
| Soccer | 59 | −10.64%/bet | −9.52%/bet | −0.81%/bet | +1.19%/bet |
| Tennis | 142 | −7.83%/bet | −5.37%/bet | −2.61%/bet | +1.94%/bet |

**Honest read:** the ~1-2%/bet median execution uplift matches the
literature's line-shopping estimate — real, free, and worth taking on any
bet actually placed. But it does NOT flip any verdict: every tier stays
net-negative, so best-price grading confirms (not rescues) the standing
no-edge conclusions. MLB's negative MEAN uplift (−0.67%) vs positive
median is itself informative: on a tail of bets the global consensus
(which includes sharper EU books) beats the best US-retail price. The
standing studies (underdog re-run, 8.0-totals) should report both frozen
and best-price ROI when they grade, per their registered notes.

Usage: `python -m model.best_price [--sport mlb|soccer|tennis|all] [--since]`

### D2 — Execution-Timing Study — Pre-Registered (2026-07-08)

**Registered BEFORE any trail price path was examined.** D1 only ever read
the single closing capture per game; the intraday price paths are
unexamined as of this writing. The only data inspection performed before
freezing this spec was STRUCTURAL (trail depth/columns, no prices): MLB has
dense 30-min trails only since ~2026-07-02 (older history rows are single
20:00Z backfill snapshots, and pre-07-02 consensus is arithmetic-era
as-recorded); soccer has 18 games of 3-hourly trails; tennis 76 games of
6-hourly trails.

**Question (Levine's salvageable idea, done honestly):** for the sides our
ledgers flagged, when in the pre-game window did the best price occur, and
how much EV does entry timing control?

**Population (frozen):** moneyline bets in `mlb_bets`/`soccer_bets`/
`tennis_bets` (any status except `void`) whose matchup has **≥ 5
pre-commence consensus captures** carrying a price for the bet's side, with
captures restricted to **≥ 2026-07-02** (the odds-fix + dense-cadence
epoch). Totals are EXCLUDED from the primary analysis — the line drifts
intraday, so a cross-time same-proposition price comparison is unstable;
they may be reported descriptively, clearly labeled.

**Price basis (frozen):** the CONSENSUS American price for the bet's side
at each capture, converted to decimal. Consensus-at-both-timestamps
isolates TIMING; per-book shopping is D1's job and is deliberately kept
out of this study's primary metrics.

**Metrics (frozen):**
- **M1 — oracle premium:** max side-decimal over the trail vs the closing
  side-decimal (`best/close − 1`). Median + IQR per sport. This is an
  UPPER BOUND — a hindsight max, never claimable as achievable EV.
- **M2 — when:** distribution of where the trail max occurs (hours before
  commence + normalized [0,1] trail position). Clustered-at-open vs
  uniform distinguishes structural drift from noise.
- **M3 — actionable fixed rules (no hindsight):** premium vs close of
  betting at: first capture, T-24h, T-12h, T-6h, T-3h (nearest capture
  at/before each horizon; a rule skips a bet when no capture exists in
  its window).
- **Slices:** sport; favorite vs underdog at the close (drift is
  plausibly asymmetric).

**Kill criterion (frozen, per the D-series table):** if MLB's **median M1
oracle premium < 1%**, timing doesn't matter even WITH hindsight → D2 is
dropped, no re-slicing rescue. Soccer/tennis are reported but not
decisive (n far too small; soccer has 18 games total).

**Minimum sample:** ≥ 100 qualifying MLB bets. If the current epoch hasn't
accrued that yet, report the shortfall and wait — no conclusion on less.

**Interpretation guardrails:** only M3's fixed rules are actionable, and a
positive M3 rule is a MEASUREMENT, not a strategy — before it influences
any behavior it must separately survive the standing CLV/walk-forward
discipline (a rule fit on this window confirming itself on this window is
the exact circularity this file exists to prevent). M1 exists to size the
ceiling and trigger the kill criterion, nothing else.

**Status: first run 2026-07-08 (`model/execution_timing.py`) — NO VERDICT
yet, trending toward kill.** Results against the frozen bars:

| Sport | n (qual.) | M1 oracle median | M3 fixed rules (median vs close) |
|---|---|---|---|
| MLB | 85 (< 100 min → no verdict) | **+0.69%** (below the 1% kill line) | first/T-12h/T-6h/T-3h all **+0.00%** |
| Soccer | 51 (not decisive) | +1.44% | first −0.49%; all horizons ~0.00% |
| Tennis | 108 (not decisive) | +0.83% | first **−1.04%**; horizons −0.3%→0.00% |

**Descriptive read (no verdict claimed):** the actionable metric (M3) is a
clean null in all three sports — every fixed no-hindsight entry rule
medians ≈ 0% vs the close with wide symmetric IQRs, i.e. intraday price
movement is variance, not capturable drift. Tennis early entry is
actually NEGATIVE (−1.04% at first capture), consistent with the flagged
sides' known negative CLV. M1's oracle premium exists (+0.7–1.4%) but M3
demonstrates no fixed rule captures it. MLB favorites show larger price
swings (M1 +2.04% vs dogs +0.44%) — descriptive only.

**Rerun when MLB n ≥ 100** (accruing ~15 qualifying bets/day under the
dense capture cadence → ~2026-07-10). Grade the kill criterion then; do
not act on any of the descriptive numbers above before that.

---

## D3 — Opener-vs-Closer Study — Pre-Registered (2026-07-09)

**Question (Edge-Finding Roadmap P2b):** does our model's disagreement
with the market's *opening* line predict which way the market *moves* by
close? This is a different mechanism than D2: D2 tested whether a fixed
**entry time** beats the close (result: null, all three sports). D3 tests
whether **our model's signal** leads price discovery — i.e., is the early
market slow to price in something our model already sees, so it drifts
toward our number by close? A positive result here would not re-open D2
(timing-only entry rules already died); it would be a distinct claim
about information content.

### A mechanical confound found and designed around, before writing any
### analysis code

`mlb_bets`'s bet-facing `our_prob` field is **anchored**:
`market_prob_at_lock + w×(raw_model − market_prob_at_lock)` (`model/
mlb_game_bets.py::_anchor`, w=0.5), and lock happens near game time — i.e.
close to the *close*, not the open. Testing "does the market move toward
our_prob" using that anchored value would be circular: our_prob is
already built from half of the closing price, so of course close is
closer to it than open is. **Fix:** use the **raw, unanchored** model
output instead, which every gameline bet already freezes separately in
`inputs_json` for exactly this kind of reuse — `our_prob_home` (ML) and
`our_total_pred` (totals) on both `model/mlb_game_bets.py` and `model/
soccer_game_bets.py`. Verified present on both files before writing this
spec (not assumed).

### Population

- `{mlb,soccer}_bets` rows: `bet_type IN ('moneyline','total')`,
  `status != 'void'`, one per `(matchup_id, bet_type)` (`DISTINCT ON`,
  latest `id` — same dedup convention as D1/D2).
- Matchup has **≥ 2** pre-commence consensus `game_odds_history` captures
  at/after epoch `2026-07-02` (the odds-fix + dense-cadence epoch already
  used by D1/D2). **Deliberately 2, not D2's 5** — D2 needed a dense trail
  to locate M2's timing and M3's fixed horizons; D3's metrics only need
  the first and last pre-commence capture. This is a genuinely different,
  more minimal data requirement dictated by the metric design, not a
  loosened bar to manufacture a bigger sample — stated here so it reads as
  principled, not as sample-shopping.
- MLB is the primary, kill-criterion-gated sport (best volume, real prop
  history). Soccer reported descriptively only, no kill test (low volume,
  same treatment D2 gave it). **Tennis excluded entirely** — its raw
  model-vs-market disagreement was already the exact subject of
  [[tennis-moneyline-no-edge]] (H1/H2, both closed 2026-07-04); re-running
  the same question in a different wrapper would be exactly the kind of
  quiet re-litigation this file's multiple-comparisons discipline exists
  to block.

### Price/line basis (all values home-referenced or total-line-referenced
### — never bet-selection-referenced, to avoid sign-flip bugs)

- **ML**: `open_home_prob` / `close_home_prob` = raw implied probability
  (`1/decimal`, no vig removal — a deliberate simplicity choice, stated
  not hidden) from `game_odds_history.home_ml` at the first and last
  pre-commence capture. `our_home_prob` = frozen `inputs_json.
  our_prob_home` (already stored in home-team terms on both sports, no
  side-flipping needed).
- **Totals**: `open_line` / `close_line` = `game_odds_history.
  vegas_total_raw` (unrounded consensus) at first/last capture, falling
  back to `vegas_total` if raw is null. `our_total` = frozen `inputs_json.
  our_total_pred` (MLB) / `lambda` (soccer).

### Metrics (fixed now; M1/M2 are the only gating metrics)

```
edge_open = our_value − open_value        (home-prob or total-line units)
movement  = close_value − open_value

M1 — directional agreement rate:
    fraction of bets where sign(movement) == sign(edge_open)
    (ties at edge_open == 0 excluded from the denominator, count reported)
    baseline under no signal = 50%

M2 — correlation(edge_open, movement):
    Pearson r AND Spearman rho (both pre-specified, not picked post hoc)
```

Reported separately per market (ML, totals) — never pooled, they're
different quantities on different scales. Descriptive slices (not
gating): favorite (`close_home_prob >= 0.5`) vs underdog, and soccer overall.

### Kill criterion (evaluated independently per market — a market can die
### while the other survives; no shared/blended verdict)

MLB ML is dead if **both**: (a) the 95% bootstrap CI of M1 includes 50%,
**and** (b) the 95% bootstrap CI of Pearson r includes 0. MLB totals
graded identically and independently against the same bar. Bootstrap CI,
not a point estimate — consistent with every other study in this file.

### Minimum sample

≥ 150 MLB ML bets **and** ≥ 150 MLB totals bets, evaluated separately, no
conclusion before either individually clears its own floor. Chosen before
querying the actual count (only the schema/logic was checked ahead of
time, per this file's standing discipline) — the 2-capture floor (vs D2's
5) is expected to clear D2's own 85-bet MLB ML shortfall, but that
expectation is not itself evidence and the real number is checked only
after this spec is committed.

### Non-negotiables (same discipline as every prior spec in this file)

- Raw (unanchored) model values only — never the bet ledger's anchored
  `our_prob`, for the mechanical-confound reason above.
- Bootstrap CI, walk-forward-safe data only (epoch ≥ 2026-07-02, the same
  odds-fix boundary as D1/D2) — no pre-fix corrupted consensus prices.
- Tennis is closed; do not re-add it to this study without a new,
  separately pre-registered spec.
- If MLB ML and MLB totals disagree (one dies, one doesn't), report both
  independently — do not average them into a single "D3 verdict."
- A positive M1/M2 result is a measurement of correlation, not a trading
  rule — same guardrail as D2's M1 oracle premium: real edge would still
  need a fixed, no-hindsight capture rule and live CLV confirmation before
  it touches any star rating.

**Built + first run (2026-07-09), `model/opener_closer.py`.** One real bug
caught before trusting the output: the query didn't filter `model_version`,
so `mlb_bets`/`soccer_bets` — which hold superseded versions (`mlb-gameline-
v1`; `gameline-v1`/`v2`) alongside the current ones — inflated the
candidate count 4-7x (1229 vs the correct 167 for MLB ML). Fixed by
filtering to the current version only (`mlb-gameline-v2` / `gameline-v3`),
matching this project's own "bump the version, never silently mix" rule.
Final qualifying counts were unaffected by the fix (same underlying raw
value regardless of version tag) — only the reported candidate/exclusion
denominators were wrong before the fix.

```
MLB moneyline : n=78/167  (< 150 min — NO VERDICT)   M1=46.2%  Pearson r=-0.05
MLB total     : n=78/165  (< 150 min — NO VERDICT)   M1=52.6%  Pearson r=+0.30
Soccer ML     : n=18/77   (descriptive only)          M1=38.9%  Pearson r=-0.14
Soccer total  : n=18/77   (descriptive only)          M1=44.4%  Pearson r=+0.26
```

Both MLB markets are short of the pre-registered n≥150 floor — no verdict
either way yet, per the spec's own rule. Descriptive read, not a
conclusion: MLB ML's directional agreement (46.2%) is if anything *below*
the 50% no-signal baseline; MLB totals shows a moderate positive
correlation (+0.30) that would be worth watching, but n=78 is nowhere
near enough to distinguish that from noise (a bootstrap CI wasn't even
computed — the script correctly withholds it below the sample floor,
same discipline as D2). Soccer stays descriptive-only per the spec at any
n.

**Rerun target:** ~11/day accrual observed (78 bets over the 7 days since
the 2026-07-02 epoch) → ~6-7 more days to reach n=150, roughly
**2026-07-16**. Stated as a rough projection, not a promise — D2's own
~15/day estimate didn't hold up in practice (its MLB sample was flat at
n=85 across a full day), so treat this the same way: re-run the script
itself at that point rather than trusting the projection blindly.

---

## D5 — Confidence/Segment Governance Layer — Pre-Registered (2026-07-09)

### Feasibility check (done before writing any spec, per this file's standing
### discipline — check the data before designing the mechanism)

**Idea #1 (segment-aware caps):** the only candidate segment identified
anywhere in this project is the MLB underdog-value moneyline slice — still
INCONCLUSIVE (n=125 < 200), gated to ~2026-08-30. Nothing to promote
today; revisit only if/when that re-run lands a PASS.

**Idea #2 (confidence-discounted edge):** checked the one live market that
would actually benefit — soccer futures, the only tier rated above the
standing 2★ cap anywhere in the ledger — against its underlying signal's
own sample-size column, `soccer_team_ratings.matches`. Real distribution
across all 57 rated teams: min 243, median 663, max 1109 — every team
already has hundreds of matches; there is no thin-vs-thick gradient left
to discount. The idea's own motivating contrast ("a mature, thousands-of-
games Elo rating and a five-game prop line") is a CROSS-market comparison
(soccer Elo vs e.g. an MLB player prop), not something that varies
meaningfully WITHIN soccer team ratings. No supporting signal exists today
to build a confidence discount against, for the one market it could
actually move.

**Conclusion:** neither #1 nor #2 has real, checkable data behind it right
now. Per this file's own prior honest assessment (2026-07-05, "Star rating
improvement ideas"), that was the expected outcome, not a surprise.

### What's actually buildable today: idea #3, scoped narrowly

The only D5 idea with anything to build against right now is calibration-
drift monitoring — and even that has a real constraint: it must monitor an
ALREADY-vetted tier (soccer futures, validated via its own dedicated Monte
Carlo backtest — group-winner Brier .036 vs .188), not a newly-discovered
segment. This does not violate the "#1/#2 before #3" sequencing rule
recorded above — that rule exists to stop #3 from being used to DISCOVER
and validate new fragile segments in place of #1/#2's job. Applying it as
a pure downgrade-monitor on the one tier already validated through its own
dedicated process is a narrower, different claim.

### Mechanism (`model/calibration_guard.py`)

For each (sport, model_version, bet_type) combination currently rated
ABOVE the standing 2★ cap anywhere in the ledger — today: soccer
`futures-v1` `outright_winner`/`group_winner` only; every game-line market
is already capped and this mechanism has nothing to add there — computed
over settled (`won`/`lost`) bets only:

```
window = one distinct model_version run for that bet_type (the only
          natural window boundary available for futures — there is no
          per-slate cadence the way there is for daily game lines; a new
          tournament cycle gets a new model_version per this file's
          standing "bump the version" rule, which IS the window boundary)
per window: realized_win_rate = wins / n        (won/lost only)
            expected_win_rate = avg(our_prob) over the same bets
            brier            = avg((our_prob - outcome)^2)

trigger (auto-downgrade to 3★ pending revalidation — DOWNGRADE ONLY):
    3 consecutive windows where realized_win_rate < expected_win_rate - 5pp
    OR 3 consecutive windows where brier > 2x the validated baseline
       (.036, the group-winner Brier recorded when futures was first cleared)
```

`N_CONSECUTIVE = 3` and the 5pp/2x thresholds are frozen now, before any
second-window data exists to tune them against — deliberately conservative
so a single bad tournament can't fire the trigger (one champion + one
group-stage cycle is not enough evidence to distrust a Brier-.036-
validated model). Per the standing rule elsewhere in this file, this
mechanism can only ever downgrade — it can never auto-uncap or raise a
threshold; only a fresh, separately pre-registered study can do that.

### Minimum sample / honest expectation

World Cup 2026 is the FIRST tournament run through this system, so there
is currently exactly **1 window**. The trigger structurally cannot fire
before at least 3 tournament cycles exist — realistically years away
(World Cups are quadrennial). This is INFRASTRUCTURE laid down now, ready
the moment enough windows exist — the same "build the harness before the
data exists to use it" pattern as the CLV harness (Edge-Finding Roadmap
P1), not a mechanism expected to do anything soon.

### Non-negotiables (same discipline as every prior spec in this file)

- Downgrade-only. Never auto-uncaps, never raises a threshold, never
  applies to a market that hasn't already been separately validated.
- The 3-window / 5pp / 2x constants are frozen now, before there is a
  second window to tune them against — do not adjust them retroactively
  once real drift data exists (that would be exactly the kind of
  after-the-fact rationalization this file's discipline exists to block).
- If a future idea #1/#2 candidate segment ever passes (e.g., the MLB
  underdog re-run), this mechanism extends to it only via its own
  explicit addition to the monitored-segment list — never silently.

### Status

**Built + first run (2026-07-09), `model/calibration_guard.py`.** Real
result, not identical for the two bet types: `outright_winner` shows **0
windows** — all 48 bets are still `pending` (the 2026 World Cup hasn't
concluded; the final hasn't been played yet), correctly reported as
insufficient rather than assumed. `group_winner` shows **1 window**
(`futures-v1`, n=48): realized 25.0% vs expected 24.9%, brier 0.0356 —
both essentially matching the .036 baseline recorded when this tier was
first validated, a good sign the calibration hasn't drifted, though 1
window can't say anything about drift by construction. Both bet types
correctly report NOT TRIGGERED — insufficient windows (0 or 1 < 3) —
rather than fabricating a verdict from an incomplete or single cycle. No
star rating changed. Re-run once the tournament concludes (for
`outright_winner`'s first window) and again once a second global
tournament's futures ledger exists (for either to ever reach 3).

---

## MLB Vegas Game-Line Model Audit — SWOT & Remediation Plan (2026-07-11)

### Scope and standing decision

This audit covers **MLB game moneylines and full-game totals only**. It
does not cover DFS, player projections, or player-prop models. It reviews
the complete game-line chain: odds ingestion, feature availability,
moneyline/total models, prediction persistence, bet-ledger identity,
settlement, CLV, and backtest reporting.

**Standing decision:** the architecture is thoughtful, but the current
MLB game-line backtest is **not decision-grade and does not demonstrate a
bettable edge**.

- **Moneyline:** informational / no-bet. Keep the 2-star cap.
- **Totals:** shadow/research only. Keep the 2-star cap and remove the
  `>= 1 run` **ACTIONABLE** UI treatment until a clean prospective test
  passes the gates below.
- **Underdog segment:** hypothesis-generating only. It is not confirmed by
  a prospective sample, and the current audit script fails its own sanity
  gate after later operational cleanup.
- No segment may be uncapped from retrospective ROI, a mutable
  `mlb_matchups` query, or a post-hoc edge slice.

This section supersedes the older positive interpretation of the
`>= 1 run` total tier. Earlier results remain in this file as an audit
trail, but they are not current evidence of actionability.

### Verified current-state snapshot

Read-only checks against the live database plus fresh local evaluations
on 2026-07-10 produced the following:

| Check | Verified result | Interpretation |
|---|---:|---|
| `mlb_team_stats_history` | 0 rows | Point-in-time team strength is not operationally populated |
| `mlb_pitcher_stats_history` | 0 rows | Point-in-time starter strength is not operationally populated |
| Fresh totals holdout | n=271; our MAE 3.97 vs Vegas 3.91; side accuracy 48.1% | No current market-relative edge |
| Fresh moneyline holdout | n=270; our Brier .2458 vs market .2454 | Slightly worse than market; all non-market coefficients were 0 |
| `abs(our_total_pred - line) >= 1`, legacy dates | 264-208, 55.9% | Dominates the old positive UI claim; contaminated by older timing/data semantics |
| Same total tier after the 2026-07-05 PIT fix | 15-17, 46.9% (n=32) | Too small to kill the idea, far too small to call actionable |
| v2 duplicate decisions | 59 ML game-markets; 122 total game-markets | One matchup can contribute multiple/opposing ledger rows |
| Odds-history away-team mismatch | 106 rows | Observed event-to-matchup identity failures, including next-series opponents |
| Current matchup rows with impossible American prices | 200 | A writer can still reintroduce the pre-07-02 odds bug |
| v2 bets with `event_commence IS NULL` | 2,038 / 2,649 | Most historical rows cannot prove a pregame lock time |
| Settled v2 ML rows created >=7 days after game | 977 / 1,187 | Most underdog-study observations are retrospective backfills |

The checked-in `mlb_total_v1_eval.json` and `mlb_ml_v1_eval.json` are
therefore historical artifacts, not reproducible current baselines. They
do not record a data cutoff, row population, feature-coverage counts,
dependency versions, git SHA, or odds/prediction snapshot IDs.

### SWOT

#### Strengths

1. **Correct market-aware prior.** Predicting `actual_total - vegas_total`
   and anchoring moneyline probability to a vig-free market estimate are
   more defensible than trying to out-predict a sharp MLB market from
   scratch.
2. **Chronological label discipline.** Both backfill trainers restrict
   outcomes to dates strictly before the target date.
3. **Appropriate benchmark metrics.** Totals compare MAE/bias with Vegas;
   moneyline compares log loss and Brier with the market rather than
   relying on raw accuracy.
4. **Honest negative-result policy.** Both live game-line markets are
   capped at 2 stars after failing their formal holdout checks.
5. **Useful accountability infrastructure.** Model-version fields,
   snapshots, settlement, score correction, CLV, best-price grading,
   pre-registration, and explicit kill criteria are the right primitives.
6. **Recent live-safety repairs are directionally correct.** The primary
   Python odds path now uses probability-space moneyline consensus and
   provider-plus-Stats-API commence guards.

#### Weaknesses

1. **The advertised baseball-strength features are currently dead.** The
   point-in-time tables exist in schema/code but are empty in production.
   Missing xFIP, K/9, wRC+, ISO, and bullpen values are silently replaced
   with league averages, so row-count checks pass while the model carries
   no player/team information. A green stats workflow can still write zero
   usable rows.
2. **The UI model backtests are mutable.** `our_total_pred`,
   `our_prob_home`, and the reference odds live on `mlb_matchups`; rolling
   backfill rewrites completed games, and the UI queries whatever values
   happen to be present today. There is no immutable prediction-time line,
   feature snapshot, generation timestamp, trained-through timestamp, or
   artifact hash.
3. **Prediction writers still lack a first-pitch guard.** The ledger
   rating path now excludes started games, but the total/ML prediction
   paths define “upcoming” as same-date plus not-final. Intraday refreshes
   can rewrite an in-progress game's mutable prediction.
4. **Odds ingestion has conflicting writers.** The primary Python path is
   partially repaired, while `/vegas` and `/dfs` server actions can still
   match by home team, arithmetic-average American prices, omit complete
   event/commence identity, or update shared matchup odds outside the
   canonical history path.
5. **Odds event identity is not fail-closed.** Live fetching is not bounded
   tightly enough to the target event, and historical backfill collapses
   `home_name -> matchup`, which cannot represent a doubleheader. Both
   teams plus event ID and commence proximity must agree before any write.
6. **One game can create multiple/opposing “bets.”** Ledger uniqueness
   includes `selection_label`. A home/away flip, Over/Under flip, or line
   change inserts a second row rather than evolving one game-market
   recommendation through snapshots. Settlement and generic calibration
   then count every row.
7. **Totals use non-executable price/probability math.** The ledger assumes
   every total is -110 with a 0.5 market reference, even when real prices
   are available. A rounded cross-book average line may not exist at any
   book. Integer-line push probability is left out of binary EV, so
   `p_win * decimal - 1` implicitly treats a push as a loss.
8. **Historical samples mix incompatible regimes.** They combine US-only
   historical snapshots with US+EU live consensus, fixed 20:00 UTC
   historical captures with variable live closes, actual archived weather
   with pregame forecasts, old leaky stats with neutral post-fix features,
   and multiple rating semantics under unchanged model-version strings.
9. **Confirmatory claims are not reproducible.** The underdog script now
   stops because `_SANITY_EXPECTED_N=65` but cleanup leaves 63 stored
   5-star rows. Repairs changed live rows in place without a checked-in,
   versioned repair artifact.
10. **Automated coverage is absent.** Existing tests do not exercise game
    odds identity, consensus math, point-in-time joins, first-pitch
    exclusion, side flips, pushes, settlement, immutable predictions, or
    backtest population rules.

#### Opportunities

1. Build one canonical, event-ID-based odds and decision ledger, then use
   it for both research and the UI.
2. Fail closed on missing/constant features instead of silently degrading
   a multivariate model into a market-only model.
3. Separate a sharp reference close from an executable entry price; grade
   every ticket at an exact book/line/price pair.
4. Rebuild the evidence base from prospective immutable observations and
   evaluate it with rolling-origin folds, date-block bootstrap intervals,
   CLV, and real-price ROI.
5. Once the data layer is trustworthy, improve ML with a fixed market-logit
   offset and totals with an out-of-fold empirical residual or
   negative-binomial distribution.
6. Add genuinely time-sensitive baseball features only with point-in-time
   provenance: reliever-only workload/FIP, confirmed starter/lineup,
   handedness, roof state, forecast weather, and umpire assignment.

#### Threats

1. MLB closing moneylines and totals are highly efficient; public
   season-aggregate features may never provide enough incremental signal
   to clear vig.
2. A wrong-game, stale, or in-play price can manufacture both apparent
   model edge and fictional payout without throwing an exception.
3. Longshot ROI, repeated threshold searches, and correlated series games
   create a high false-discovery risk.
4. Starter scratches, rain delays, shortened games, and book-specific
   settlement rules can make a paper edge non-executable.
5. Silent upstream degradation is more dangerous than choosing Ridge
   versus another algorithm: plausible outputs continue even when all
   advertised baseball features are constants.

### Proposed fixes — implementation order

#### P0 — Stop unsupported actionability and close integrity holes

**Status: in progress. Required before any further game-line model tuning.**

1. **Remove MLB totals actionability from the UI.** In
   `web/src/app/vegas/vegas-client.tsx`, remove the `>=1.0` actionable gate,
   `ACTIONABLE` chip, and historical `~56% / +7% ROI` copy. Display
   `our_total_pred` as a shadow diagnostic with an explicit no-validated-
   edge label. Keep both game-line markets capped at 2 stars.
2. **Canonicalize odds writes.** Move probability-space consensus,
   event-resolution, pre-commence guards, and history insertion into one
   shared service. Route or remove direct MLB odds updates in
   `web/src/app/vegas/actions.ts` and `web/src/app/dfs/actions.ts`.
3. **Make event resolution fail closed.** Bound the Odds API request to a
   target commence window. Require provider event ID when available,
   matching home AND away teams, and a maximum commence-time delta. Reject
   ambiguity; never “use first.” Resolve doubleheaders by event/time rather
   than a home-team dictionary.
4. **Add hard odds invariants.** Reject American prices strictly inside
   `(-100, +100)`, probabilities outside `[0,1]`, same-team matchups,
   post-commence captures, and event/team mismatches. Add production health
   queries/alerts and repair remaining invalid rows through a versioned,
   repeatable script.
5. **Require commence time.** Do not create a prediction or rated game-line
   recommendation when `commence_time` is NULL. Add the same
   `commence_time > NOW()` guard to `predict_and_write()` that exists in
   `rate_slate()`.
6. **Make stats refresh fail loudly.** The workflow must fail if team or
   pitcher history writes zero rows, freshness exceeds the agreed SLA, or
   required feature coverage/variance falls below threshold.

#### P1 — Create immutable prediction and ticket accounting

**Status: in progress. Required before rebuilding the backtest.**

1. Add `mlb_prediction_runs` and `mlb_game_prediction_snapshots` (names may
   vary) with at least:

   ```
   run_id, matchup_id, generated_at, trained_through,
   model_version, git_sha, dependency_lock_hash,
   origin (prospective | retrospective_backfill),
   odds_snapshot_id, book/reference_panel, market, line, price,
   feature_available_at, feature_source/window/sample metadata,
   feature_values + missingness flags,
   raw_prediction, calibrated_probability
   ```

2. Predictions become append-only. Never overwrite the historical record
   used for evaluation. `mlb_matchups.our_*` may remain a latest-value cache
   for rendering, but it must not be a backtest source.
3. Split **recommendations** from **placed tickets**. A recommendation has
   one stable key per `(model_version, matchup_id, market, decision_phase)`
   and evolves through snapshots. A ticket is an explicit immutable event
   with book, exact line/price, stake, placed timestamp, and settlement
   rules. Side/line changes must not accidentally create implied wagers.
4. Store `p_win`, `p_push`, and `p_loss` for totals. Compute
   `EV = p_win * (decimal - 1) - p_loss`; preserve `push` separately from
   cancellations/postponements (`void`).
5. Bump model/data-policy versions whenever point-in-time semantics,
   anchoring, feature sources, pricing, or star caps change. Never repair a
   versioned historical row in place without a separately versioned derived
   record and reproducible migration.

#### P2 — Rebuild a clean prospective backtest

**Status: blocked on P0/P1.**

1. Exclude retrospective backfills from accountability, CLV, and ROI
   claims. They may be labeled and retained for exploratory research only.
2. Use one independent observation per unique game/market/policy. Cluster
   uncertainty by game date (and inspect team/series concentration).
3. Use rolling-origin folds with an untouched final window. Hyperparameter
   or threshold selection must occur inside earlier folds, not on the final
   evaluation sample.
4. Grade moneyline against market log loss/Brier and actual executable
   ROI. Grade totals at the exact line and side price, with pushes and
   turnover handled correctly. Report sharp-close CLV separately from
   best-accessible-book execution.
5. Pre-register the market, selection rule, sample floor, primary metric,
   and kill criterion before examining outcomes. Do not rescue a failed
   market by slicing adjacent totals/odds ranges after the fact.
6. The existing underdog discovery population stays exploratory. Any
   confirmation population must be prospective and independent, not the
   old rows plus enough new rows to cross `n=200`.

#### P3 — Improve the algorithms only after the data gates pass

**Status: deferred.**

1. **Moneyline:** use `logit(market_probability)` as a fixed offset and
   learn only a regularized residual adjustment. This is a stronger market
   anchor than fitting an unrestricted market coefficient and shrinking a
   second time in the rating layer.
2. **Totals:** estimate win/push/loss probabilities from out-of-fold
   residuals by line/regime or a calibrated negative-binomial model rather
   than applying a soccer Poisson distribution to a Ridge mean.
3. Replace mislabeled `bullpen_fip` (currently staff pitching) with true
   reliever-only quality plus 1-3 day workload. Add confirmed starters,
   lineup strength/handedness, roof, forecast weather, and umpire only when
   each has immutable `available_at < commence_time` provenance.
4. Compare every feature addition incrementally against the same frozen
   market-relative evaluation protocol. No bundled feature search and no
   new live rating tier from in-sample lift.

### Validation gates before any MLB game-line can be actionable

All operational gates must pass continuously:

- 0 invalid American prices and 0 event/team identity mismatches.
- 100% non-null commence time for rated/predicted events.
- 0 post-commence odds, prediction, recommendation, or ticket writes.
- Point-in-time team/pitcher history fresh and populated; proposed minimum
  feature coverage is >=95% of eligible games, with no required feature
  silently constant because of fallback.
- 0 duplicate active recommendations per game/market/policy.
- Every evaluated prediction references an immutable odds snapshot,
  feature snapshot, model version, and generation time.
- Checked-in evaluation artifact reproduces from recorded row IDs and
  includes data cutoff, git SHA, dependency versions, seed, feature
  coverage, and exclusion counts.

Then the statistical/economic gates must also pass on the separately
pre-registered **prospective** population:

- The pre-registered minimum number of **unique games** is reached; no
  duplicate/opposing ledger rows count toward the floor.
- Market-relative proper scoring is no worse than the reference market.
- ROI at actual executable prices has a confidence interval whose lower
  bound is above 0.
- CLV against the chosen sharp close is positive and stable, not carried
  by one team, month, odds tail, or half of the sample.
- The result survives the untouched final window and all pre-registered
  split/stability checks.

Until every applicable gate passes, the only honest MLB game-line label is
**shadow / no demonstrated edge**.

### Required automated tests

Add golden and invariant tests for:

1. probability-space consensus across every ingestion surface;
2. exact event/team/date/time matching, including a split doubleheader,
   reschedule, and rain delay;
3. rejection of invalid, stale, in-play, and NULL-commence odds;
4. strict `available_at < commence_time` point-in-time feature joins;
5. prediction immutability and reproducible artifact generation;
6. home/away and Over/Under flips without duplicate recommendations;
7. exact-line total pricing, push EV, cancellation voiding, and idempotent
   score correction/re-settlement;
8. prospective-versus-backfill population filters;
9. one-observation-per-game backtest counts and date-clustered uncertainty;
10. reproduction of the checked-in evaluation metrics from a frozen
    golden dataset.

### UI implementation guidance for the remediation work (2026-07-11)

The Vegas UI should evolve alongside P0-P3 so the user can understand the
system's trust state without reading the database or this file. Prefer
plain-language status, compact cards, small timelines, and expandable
details over additional model jargon.

#### Recommended visual hierarchy

```
System/model trust state
        ↓
Today's model number vs market
        ↓
Prediction inputs and pregame timeline
        ↓
Tracked results, uncertainty, and CLV
        ↓
Requirements remaining before validation
```

#### Components to add as their backing data becomes trustworthy

1. **Model Status Banner** — one prominent state:
   `RESEARCH MODE`, `SHADOW TRACKING`, or `VALIDATED`. Include a one-sentence
   explanation and never infer `VALIDATED` from a point estimate alone.
2. **Data Health Traffic Lights** — green/yellow/red checks for odds event
   identity, commence-time coverage, team stats, pitcher stats, and
   prediction freshness. Clicking a check should reveal the concrete issue
   and affected-game count.
3. **Model vs Market Card** — market line, our number, signed difference,
   prediction time, reference source, and exact price/line when available.
   During P0/P1 the difference is descriptive only: use neutral styling,
   not `Strong`, `Lean`, `Qualified`, or `Actionable` language.
4. **Live vs Backfill Badge** — every prediction/result clearly labeled
   `LIVE` or `HISTORICAL BACKFILL`. Only live prospective rows contribute
   to trusted performance panels.
5. **Prediction Timeline** — a compact pregame line chart/timeline showing
   market movement, model snapshots, selected side changes, and first
   pitch. This should make post-commence writes and side flips visually
   obvious.
6. **One-Game/One-Market Ledger** — one expandable row per matchup/market,
   with recommendation history nested below. Actual placed tickets, if
   ever supported, render separately with book, line, price, and stake.
7. **Performance Confidence Card** — record, ROI, unique-game sample size,
   confidence interval, and CLV shown together. Never show ROI without the
   denominator and uncertainty.
8. **Validation Checklist** — e.g. `5 of 8 requirements passed`, driven by
   the operational/statistical gates above. It explains exactly why a
   market remains in research mode.
9. **Integrity Alerts Panel** — plain-language failures such as
   `12 games missing start times`, `pitcher history has not refreshed`, or
   `2 odds events failed team matching`.
10. **Blocked-State Explanation** — when a safe prediction cannot run,
    show the reason rather than emitting a plausible neutral number.

#### UI rollout aligned to implementation phases

| Phase | UI change |
|---|---|
| P0 | Research-mode banner; neutral model-vs-market display; remove totals actionability; surface current pipeline-health failures |
| P1 | Live/backfill badges; immutable prediction metadata; one-game/one-market expandable ledger; exact line/book/price display |
| P2 | Performance confidence card; prospective-only filters; CLV/ROI uncertainty; validation checklist |
| P3 | Model-version comparison and feature explanations, only after the underlying snapshots are reproducible |

#### UI non-negotiables

- Color must communicate trust state, not merely model direction. Green is
  reserved for passed health/validation gates, not an Over lean.
- Every performance number shows sample size and population type
  (`prospective` vs `backfill`).
- No action-oriented label appears until all applicable validation gates
  pass.
- Missing or stale inputs are visible; they are never silently presented as
  league-average confidence.
- Detailed provenance is expandable, while the default page remains easy
  to scan.

---

## Permanent Delivery Contract — Evidence Before “Done” (2026-07-11)

These instructions apply to every model, betting, analytics, ingestion, and
UI change going forward. They are acceptance requirements, not optional
documentation guidance.

### Definition of done

Every request must be translated into observable acceptance criteria before
implementation. A feature is not complete merely because code exists. Report
its state explicitly as one of:

1. **Built** — implementation exists.
2. **Tested** — required automated checks pass.
3. **Backtested** — evaluated on historical data, clearly labeled with its
   population and point-in-time limitations.
4. **Prospectively validated** — passed pre-registered gates on newly captured,
   immutable observations.
5. **Production actionable** — prospectively validated and currently passing
   every operational, statistical, and economic gate.

Never collapse these states into a generic “done.”

### Source-of-truth declaration

Every material feature must document and enforce:

- its canonical table/service and the exact fields consumed by the UI;
- when records become immutable and which process may update them;
- how corrections are represented without rewriting the audit trail;
- which data is operational cache, diagnostic history, retrospective backfill,
  prospective evidence, recommendation, or placed ticket;
- the model/data-policy version and timestamp provenance.

For MLB Vegas game lines, `mlb_bets` plus its associated snapshot/history
records are the canonical decision ledger. `mlb_matchups.our_*` fields are a
latest-value operational/rendering cache and must never independently confer
actionability or serve as the trusted performance population.

### Executable trust policy

Actionability rules must exist in one tested policy module. UI components may
render the returned decision but may not recreate thresholds, infer validation
from star ratings, point estimates, or colors, or promote a diagnostic result.
Every decision must be one of `blocked`, `research`, `watch`, `actionable`, or
`retired`, with machine-readable passed/failed gates and plain-language reasons.

Promotion gates must be written before examining the confirmation outcomes.
Any change to sample floors, metrics, eligibility, prices, features, or kill
criteria requires a new policy/model version. A failed study may not be rescued
by post-hoc slicing and relabeling.

### Required evidence handoff

Every completed implementation must provide a requirement-to-evidence table:

```
Requirement | Canonical implementation | Automated test | Result | Limitation
```

The handoff must list commands run, pass/fail results, known exceptions, data
cutoff, and whether external/live state was mutated. A limitation that blocks a
required acceptance criterion means the feature is partial, not complete.

### Required end-to-end protections

Where applicable, tests must prove that:

- post-event refreshes cannot alter a frozen decision;
- stale, invalid, ambiguous, in-play, or missing-commence prices fail closed;
- the UI cannot display `Actionable` unless every centralized policy gate passes;
- mutable cache fields cannot change historical decision evidence;
- displayed book/line/price equals the immutable decision record;
- corrections are versioned/re-settled and idempotent;
- scheduled workflows complete ingestion, settlement, and health verification in
  the correct order;
- prospective and retrospective populations cannot be mixed silently.

### MLB Vegas implementation contract

The remaining remediation work must follow this dependency order:

1. Enforce one centralized ledger-backed actionability policy and surface its
   checklist in the Vegas UI.
2. Close P0 ingestion/event/commence/odds invariant gaps.
3. Add immutable feature/prediction provenance missing from the current ledger.
4. Build a prospective-only evaluation artifact with pre-registered gates.
5. Promote markets individually only after their gates pass; add conservative
   staking only after promotion.

Until then, existing star ratings, EV fields, historical ROI, and model-versus-
market differences remain evidence inputs—not permission to recommend a wager.

### Implementation progress

- **2026-07-11 — centralized enforcement started:**
  `web/src/lib/mlb-vegas-trust.ts` now owns the versioned MLB game-line
  actionability policy and its `blocked/research/watch/actionable/retired`
  states. `getMlbActionabilityEvidence()` reads ledger/snapshot evidence only;
  the Vegas UI renders its gate checklist and cannot infer validation from
  `mlb_matchups.our_*` fields.
- **2026-07-11 — live commence guard enforced:** live totals predictions,
  moneyline predictions, and rated game-line records now require a non-null
  `commence_time > NOW()` through shared pregame eligibility. Historical
  backfills remain a separate explicitly non-live path.
- Current policy remains blocked/research as appropriate. Existing historical
  rows reveal missing commence provenance, post-commence writes, no exact-book
  price coverage, incomplete prospective-origin separation, and missing
  immutable feature-run references; none is silently treated as passed.
- **2026-07-11 — canonical MLB odds policy:**
  `ingest/mlb_odds_policy.py` is the required integrity layer for live and
  historical game-line ingestion. It requires provider event ID, exact home and
  away identity, timezone-aware provider and MLB commence times within six
  hours, unambiguous nearest-time resolution for doubleheaders, valid American
  prices, and capture before both start clocks. Known provider-event mappings
  are reused but may not override a team/time mismatch. Historical history rows
  now store the actual requested snapshot timestamp rather than backfill runtime.
  The Vegas action and DFS slate fallback are forbidden from writing MLB odds by
  `web/src/lib/mlb-odds-writer-policy.ts`; the Python refresh is the single
  writer. The decision-ledger version is bumped to `mlb-gameline-v3` so these
  observations cannot mix with legacy v2 integrity semantics.
- **2026-07-11 — prospective prediction provenance:** added append-only
  `mlb_prediction_runs` and `mlb_game_prediction_snapshots`. Each live totals
  and moneyline prediction now freezes its origin, training cutoff, model
  version, git SHA when available, feature vector, missingness, generation
  time, event commence, market context, raw prediction, and latest eligible
  pregame odds-history reference before updating the `mlb_matchups` display
  cache. `mlb_bets.origin` separates prospective from retrospective/legacy
  populations, and every prospective v3 bet must reference an immutable
  prediction snapshot or it fails closed. PostgreSQL triggers reject UPDATE or
  DELETE on both provenance tables; corrections require a new appended run.
  The validation query now uses only
  prospective rows. End-to-end verification created 2 prospective runs, 14
  prediction snapshots (all linked to pregame odds), and 14 pending v3 ledger
  rows across 7 future games; all 14 bets reference a snapshot.
- **2026-07-11 — coverage false-positive repair:** MLB backfill coverage now
  evaluates only canonical `game_id IS NOT NULL` rows and excludes postponed or
  cancelled games from score/odds completeness. Legacy game-id-less rows are
  counted and surfaced separately instead of manufacturing missing-score dates.
  “Complete through” was renamed “Latest complete date” to match the actual
  statistic. The missing-only odds backfill now triggers when moneylines are
  absent even if a total exists. May 31 was re-attempted after restoring MLB
  schedule start times; the one remaining afternoon-game moneyline is correctly
  classified as provider-partial because the available 20:00 UTC snapshot was
  already in-play and was rejected.

---

## MLB Actionable Intelligence Product Spec (2026-07-11)

### Product objective

Turn the MLB Vegas page into a decision-support system that explains where our
model **agrees or disagrees with the market**. The product is not permitted to
claim it can beat Vegas. It may organize favorite parlays, but only as a
transparent view of mutually supported favorites with exact available prices,
not as a promise of profit.

### Decision-engine constraint: no DeepSeek or LLM judgment

DeepSeek, OpenAI models, and other LLMs must not select a side, assign a
probability, set a decision threshold, construct a parlay, or promote a market's
trust state. MLB Vegas decisions must be reproducible from versioned numerical
inputs, deterministic statistical models, and the centralized tested trust
policy.

The existing DeepSeek beat-writer and video extraction utilities are outside the
MLB Vegas decision path. They must remain disconnected from decision features
for the initial implementation. A future proposal may use an LLM only to extract
a candidate structured fact from text; that fact would still require source URL,
publication/capture times, confidence, deterministic eligibility rules, and an
independent prospective validation before it could become a model input. An LLM
may never be the final decision-maker.

The initial implementation therefore uses only:

- MLB Stats API schedule, game identity, start time, starter, status, and result;
- The Odds API event identity, book-specific line/price, and market history;
- point-in-time statistical histories and park factors when their coverage and
  variance health gates pass;
- immutable model prediction snapshots and deterministic derived calculations;
- a centralized decision policy returning `confirmation`, `contrarian`, or
  `pass`, plus machine-readable reasons and trust state.

#### Initial deterministic display policy (`mlb-decision-view-v1`)

This is a research classification policy, not a betting or promotion threshold:
Its fixed first-book rule is a display convenience only and is superseded by the
v2 separation of reference market and best eligible execution quote below.

- Use the first available book in this fixed US-retail order: DraftKings,
  FanDuel, BetMGM, Caesars, Fanatics, BetRivers, ESPN BET, Hard Rock Bet.
- Moneyline market probability is vig-free from that book's exact home/away
  pair. `Confirmation` requires the model and book to favor the same winner and
  the model to be at least 2 percentage points from 50%. `Contrarian` requires
  opposing favored sides and at least a 3 percentage-point model/market gap.
- Totals `Pass` when the model is less than 0.5 runs from the exact book line.
  `Confirmation` requires at least a 0.5-run opening-to-current movement in the
  same direction as the model difference. Other differences of at least 0.5
  runs are `Contrarian` research observations.
- Any missing immutable prediction, exact two-sided retail quote, source-aware
  missingness metadata, or required observed input forces `Pass`.
- All classifications retain `SHADOW / RESEARCH ONLY`; this policy cannot
  promote the centralized actionability trust state.

The system must distinguish three concepts:

1. **Model view** - our frozen pregame probability or projected total.
2. **Market view** - the vig-free probability or total derived from a named,
   timestamped market snapshot.
3. **Decision state** - `confirmation`, `contrarian`, or `pass`, subject to the
   centralized trust policy. A research-state model may describe disagreement
   but cannot label a wager actionable.

### Six product components

1. **Agreement/Disagreement Score**
   - Moneyline: compare our frozen probability with the vig-free market
     probability. Show both percentage-point difference and direction.
   - Total: compare our frozen run projection with the exact market line. Show
     the run difference and Over/Under direction.
   - Display a confidence band and data-completeness state. A large raw delta
     with missing inputs is not a strong signal.

2. **Why We Agree/Disagree**
   - Attribute the difference to named, frozen inputs such as starting pitcher,
     opposing offense/handedness, bullpen quality and workload, park, weather,
     lineup confirmation, and market movement.
   - Show the top three contributors in plain language. Do not display a reason
     when its source is unavailable, stale, retrospective, or captured after
     first pitch.

3. **Favorites Parlay Builder**
   - Candidate legs must be favorites at an exact book/price and must also be
     supported by our model; a market favorite that our model opposes is
     excluded by default.
   - Show leg probability, model-market difference, exact price, combined
     book-implied probability, model-estimated probability, and payout.
   - Default to two or three legs. Never manufacture a parlay from consensus
     prices that cannot all be executed at one named sportsbook.

4. **Parlay Fragility Rating**
   - Grade each leg and the combined parlay for uncertainty, price movement,
     starter/lineup confirmation, weather risk, model calibration, stale data,
     and concentration (same game, team, series, or shared weather exposure).
   - Use `Low`, `Medium`, `High`, or `Blocked` fragility with the concrete reason;
     do not convert fragility into a misleading star rating.

5. **Confirmation / Contrarian / Pass Board**
   - `Confirmation`: market and model support the same side.
   - `Contrarian`: model opposes the market by a pre-registered threshold.
   - `Pass`: insufficient difference, missing data, non-executable price, or a
     failed trust gate.
   - Sort by evidence quality first and magnitude second. Research rows retain a
     visible `SHADOW` badge and cannot be promoted by UI copy or color.

6. **Prospective Decision Tracking**
   - Track confirmation, contrarian, pass, and parlay candidates separately.
   - Report unique games, calibration, Brier/log loss, CLV, exact-price ROI,
     pushes/voids, and confidence intervals. Backfills remain excluded from
     prospective claims.
   - The purpose is to learn which kinds of agreement and disagreement are
     informative, not to search retrospectively for a profitable slice.

### Information-source and API readiness matrix

Every displayed fact must record `source`, `source_record_id`, `captured_at`,
`available_at`, and `freshness_state`. At each immutable decision checkpoint,
`available_at <= decision_at < commence_time`; merely preceding commence is not
sufficient. If a required source is not configured, the component must show
`Unavailable` or `Blocked`; league-average substitution may support an explicitly
marked research estimate but may not support an explanation or action label.

| Information needed | Intended source | Current repository path / canonical storage | Readiness | Required fallback or next step |
|---|---|---|---|---|
| MLB schedule, game ID, teams, start time, status, final score | MLB Stats API (`statsapi.mlb.com`) | Schedule/score ingestion -> `mlb_matchups` | Available now | Fail closed when game ID or commence time is missing; never infer an event from home team alone |
| Pregame probable/confirmed starting pitchers | MLB Stats API schedule/game feed | `mlb_matchups.home_sp_*`, `away_sp_*`; freeze into prediction snapshot | Partially available | Add coverage/freshness health check; label `Probable`, `Confirmed`, or `Unavailable`; block pitcher-based explanation when absent |
| Moneyline and total lines, prices, books, event ID, market movement | The Odds API, `baseball_mlb` | Canonical Python ingestion -> append-only `game_odds_history.books`; linked `odds_snapshot_id` | Available now | Exact parlay construction requires all legs at the same named book and capture; otherwise show comparison only |
| Vig-free market probability | Derived from the exact two-sided price pair | Compute from immutable odds snapshot under versioned pricing policy | Derivable now | Do not use a flat 50% total reference or arithmetic-average American odds |
| Our pregame probability/total and uncertainty | Existing MLB game-line model | `mlb_prediction_runs` + `mlb_game_prediction_snapshots` | Mean prediction available; calibrated uncertainty not yet complete | Add out-of-fold probability/residual distribution before confidence or fragility may be called validated |
| Team offense, handedness splits, starter quality | pybaseball/FanGraphs-derived ingestion plus MLB player/team identity | `mlb_team_stats_history`, `mlb_pitcher_stats_history`; freeze exact values/missingness | Schema exists; production history previously empty | Repair and monitor point-in-time population. Until coverage and variance gates pass, show input as unavailable and keep model in research mode |
| Bullpen quality | FanGraphs/pybaseball-derived team pitching data | `mlb_team_stats_history.bullpen_*` | Field exists, but current value may represent staff rather than relievers | Replace with verified reliever-only calculation and rename/version semantics before using in explanations |
| Bullpen recent workload/availability | MLB Stats API box scores/game logs, derived from reliever appearances and pitches over prior 1-3 days | New append-only team bullpen workload snapshot required | Not built; no dedicated API required initially | Derive only from games completed before prediction time; otherwise omit the factor. Optional news feed may later supplement, not overwrite, the calculation |
| Confirmed batting lineups and batting order | MLB Stats API live game/boxscore feed when posted | New game-line lineup snapshot required; DFS `dk_players` fields are not the canonical Vegas source | Feed may expose it near game time; game-line capture not built | Poll before first pitch, timestamp every capture, and show `Unconfirmed` until complete. Do not make the Vegas model depend on DraftKings lineup metadata |
| Park factors | Annual park-factor dataset, currently intended from FanGraphs/pybaseball | `mlb_park_factors`, versioned by season | Schema/source plan exists; operational freshness must be verified | Use a documented neutral factor only as a visible research fallback; never describe neutral fallback as park intelligence |
| Pregame forecast: temperature, wind, precipitation, roof risk | New weather provider keyed by venue coordinates and event time | `mlb_matchups.weather_*` is only a cache; new immutable forecast snapshot required | No reliable pregame forecast API configured | Select and configure a weather API before implementation. MLB game data may describe conditions later but must not be treated as a prediction-time forecast |
| Ballpark roof type/state | Static venue metadata plus team/venue announcement or weather/game feed | New venue table + immutable roof-state snapshot | Static roof capability can be curated; live state not configured | Show `Roof state unknown` when not confirmed; do not infer open/closed solely from weather |
| Umpire assignment | MLB game/live feed when the crew is published; optional licensed lineup/umpire provider if timing is inadequate | New immutable umpire assignment snapshot required | Not implemented; availability timing uncertain | Treat as optional research input. Do not buy or integrate a provider until a prospective ablation study justifies it |
| Injuries, scratches, roster moves | MLB Stats API transactions/rosters plus existing beat-fact extraction where provenance is auditable | New structured, append-only availability snapshot; existing extracted facts remain supplemental | Partially explored, not a trusted model feed | Start with official roster/transaction changes. Any news extraction must store URL/source, publication time, extraction time, and confidence; absence of news is not proof of availability |
| Sharp/reference close for CLV | Last eligible pregame snapshot from a pre-designated reference book/panel in The Odds API | `game_odds_history.books` under versioned close policy | Data structure available; reference panel/cadence must be fixed | Pre-register reference book, fallback order, and cutoff; missing close means CLV unavailable, not zero |
| Settlement and book-rule outcome | MLB Stats API final score/status plus versioned market settlement rules | `mlb_matchups` results -> `mlb_bets` settlement | Available now for standard full-game markets | Preserve `win/loss/push/void` separately; record special postponement/shortened-game handling and re-settle corrections idempotently |
| Parlay combined price | Same-book leg prices from one eligible Odds API snapshot; preferably direct parlay quote if a future provider supports it | New derived parlay candidate record linked to each leg's odds snapshot | Straight multiplication is derivable; sportsbook parlay quote API not configured | Label multiplied decimal odds as `estimated payout`. Do not call it executable unless an actual same-book quote/acceptance is captured |

### API procurement priorities

Do not add providers merely because a field is interesting. Implement and test in
this order:

1. Use the already configured **MLB Stats API** and **The Odds API** for the
   initial agreement board, exact-price favorite filters, market movement, game
   identity, starters, scores, and settlement.
2. Repair the existing **pybaseball/FanGraphs-derived point-in-time history**
   before adding more model features. Zero-row or constant-feature refreshes are
   hard failures.
3. Add a **weather forecast API** only when immutable forecast snapshots and
   venue coordinates are designed. This is the first clearly missing external
   feed likely to improve understandable explanations.
4. Derive **bullpen workload** from official MLB game logs before paying for a
   separate feed.
5. Treat **lineups, roof state, injuries/news, and umpires** as optional enrichment
   until their availability timing and prospective incremental value are proven.

### Easy-to-understand UI specification

- **Three-column Decision Board:** `Model + Market Agree`, `Model Disagrees`, and
  `Pass / Missing Information`. Each game card shows exact book/price, model
  probability, vig-free market probability, difference, confidence/data state,
  and the top reasons.
- **Source chips:** every reason has a small source/freshness chip such as
  `MLB Stats - 18 min ago`, `Odds API - DraftKings - 4 min ago`, or
  `Weather unavailable`.
- **Why card:** three horizontal contribution bars with expandable provenance.
  Missing factors appear in a separate `What we do not know yet` row.
- **Favorite parlay tray:** users add eligible confirmation legs; the tray shows
  combined estimated probability, same-book estimated payout, and a fragility
  checklist. Block the build if prices come from different books or captures.
- **Evidence footer:** `Research`, `Prospective sample n`, and passed/failed trust
  gates appear on every recommendation surface.

### Initial delivery boundary

The first release should use only data already available or directly derivable:
MLB Stats API schedule/starters/results, The Odds API exact book prices and line
history, immutable model predictions, and ledger outcomes. It can ship the board,
source chips, exact-price favorite filtering, a research-only parlay tray, and
prospective tracking. Pitcher/team explanations remain blocked until history is
populated; weather, confirmed game-line lineups, roof, injuries/news, and umpire
factors remain visibly unavailable until their feeds and snapshots exist.

### Implementation progress

- **2026-07-11 - deterministic decision foundation:** implemented
  `mlb-decision-view-v1`, backed by the latest immutable prospective prediction
  snapshot and latest eligible pregame per-book odds capture. The Vegas query
  now excludes game-ID-less legacy rows and postponed/cancelled games.
- Model prediction writers now freeze an initial missingness map plus per-feature
  standardized contributions. The v2 review found that source-row IDs, real
  availability timestamps, required/optional semantics, and metric-fallback
  identity are still incomplete; these snapshots are not yet full point-in-time
  feature provenance.
- The MLB Vegas UI now renders a three-column `Model + Market Agree`, `Model
  Disagrees`, and `Pass / Missing Information` board with exact-book price,
  model/market difference, reasons, missing inputs, and source timestamps.
- Historical prediction snapshots created before `source-aware-v1` are
  intentionally classified as `Pass`. A future pregame refresh creates the new
  metadata; it does not rewrite old immutable snapshots.
- Favorites parlays, fragility scoring, prospective decision-population tables,
  team/pitcher history repair, reliever-only bullpen quality, and forecast
  weather ingestion remain unimplemented.

---

## MLB Edge Hypothesis and Application Operating Contract v2 (2026-07-11)

### Review conclusion

The additional data does **not** create an edge by itself. Starting-pitcher,
bullpen, lineup, and weather information is public and may already be reflected
in the line. The defensible hypothesis is narrower: point-in-time baseball
features may explain a small residual difference from a sharp market, timely
updates may identify a change before every accessible book adjusts, and exact
book selection may improve execution. Each mechanism must be measured
prospectively and may prove to have no monetary value.

This section corrects and completes the earlier conceptual explanation. It is
the target contract for the next version; it does not declare the current
`mlb-decision-view-v1` board validated or production actionable.

### Errors and omissions corrected by this review

1. **Agreement is not price value.** A book and model can both favor the same
   team while the offered favorite price is still too expensive. The UI must
   separately show `outcome agreement` and `price support`.
2. **Reference probability and observed/access-eligible break-even probability
   differ.** The
   vig-free market probability is the benchmark for disagreement. The exact
   selected book price, including vig, determines break-even and ticket EV.
3. **A projected total difference is not a bet probability.** Totals require
   `p_win`, `p_push`, and `p_loss` at the exact book line and side price. The
   current hard-coded `-110` / `0.50` treatment and soccer Poisson conversion
   cannot support actionability.
4. **Line movement is context, not confirmation of value.** Movement in the
   model's direction may mean the market has already incorporated the same
   information. It must not be double-counted as an independent feature or
   treated as proof of edge.
5. **Illustrative bullpen weights are not learned effects.** Raw workload
   measures should be frozen; coefficients or thresholds must be learned inside
   chronological training folds or pre-registered before confirmation data.
6. **Linear contribution bars are not causal explanations.** A standardized
   coefficient times a feature value explains the fitted model calculation, not
   why the real game outcome will occur.
7. **Parlays amplify error as well as edge.** Multiplying leg probabilities is
   valid only under independence. Cross-game correlation, shared weather,
   common model error, stale quotes, and uncertainty require a joint or
   conservatively penalized estimate.
8. **Tracking only displayed selections creates selection bias.** Every eligible
   game/market checkpoint, including `Pass`, must be frozen so selected cohorts
   have an honest counterfactual population.
9. **A minimum sample count is necessary but insufficient.** Evidence must also
   beat the same-time market benchmark, survive an untouched window, use
   date-clustered uncertainty, and remain stable across time and concentration
   cuts.
10. **Public packages are transport, not signal.** `pybaseball`, an MLB Stats API
    wrapper, or a weather client can improve data reliability; none confers an
    informational edge merely by being installed.
11. **The current moneyline probability is internally inconsistent.** Current
    code fits market probability as a freely estimated standardized feature,
    stores that raw probability in the prediction snapshot/UI, and then shrinks
    it toward market again in the ledger. V2 must define one calibrated decision
    probability and remove the second, conflicting anchor.
12. **The current board is not an immutable decision ledger.** It recomputes a
    category from the latest prediction and an independently selected latest
    odds snapshot. A later refresh can therefore change the displayed historical
    category. Fixed-horizon decision observations are required for evaluation.
13. **Current feature provenance is incomplete.** `feature_available_at` can
    default to prediction-generation time rather than the source record's real
    publication/availability time. Source row IDs are not frozen for each input,
    and the current daily history upserts can update a same-date row. These rows
    cannot prove point-in-time availability yet.
14. **Date-only historical joins remain unsafe.** A condition such as
    `snapshot_date <= game_date` can select a snapshot produced after first pitch
    on the same date. All joins must use timestamp cutoffs. Park factors must be
    joined by actual venue and season, not merely the latest factor for the home
    team, to handle neutral venues and venue changes.
15. **Board and ledger currently select sides differently.** The ledger chooses
    the side with positive model-versus-market edge; the board chooses whichever
    team the model puts above 50%. If model home is 45% and market home is 40%,
    the ledger selects home while the board selects away. Current ledger
    actionability evidence therefore cannot validate `mlb-decision-view-v1`.
16. **Board and ledger totals use different propositions.** The board can show an
    exact book line and price, while the current ledger records a consensus line
    at synthetic `-110`. Existing ledger ROI/CLV cannot validate board totals or
    future parlay legs.
17. **`calibrated_probability` is not calibrated yet.** Current moneyline
    snapshots copy the raw model output into that field. A chronological
    out-of-fold calibrator and calibration artifact are required before the UI
    may use calibrated/confidence language.
18. **Metric fallbacks are mislabeled.** Current starter loading may coalesce
    missing xFIP to ERA and subsequently present it as xFIP. Every value must
    preserve `metric`, `source`, and fallback state; a different metric requires
    a separately versioned model or an explicit missing value.
19. **Schedule revisions can orphan valid-looking snapshots.** A postponed or
    rescheduled game may retain predictions/odds tied to its old commence time.
    Decision eligibility must bind to a schedule/event revision and require the
    snapshot's event identity and commence to match the current canonical event,
    otherwise supersede it visibly.
20. **Observed is not necessarily executable.** An API quote is executable only
    when the user-configured jurisdiction/book set includes that book and the
    quote is sufficiently fresh. Without that configuration, UI copy must say
    `observed quote`, not `executable price`.
21. **Per-book JSON needs stronger provenance.** Quote eligibility requires
    provider event ID, normalized outcome identity, exact total line for both
    sides, valid prices, provider `last_update`, capture time, quote-age limit,
    and pregame status. The immutable decision must freeze the normalized quote;
    mutable JSON alone is insufficient evidence.
22. **Contribution reproduction is incomplete.** Standardized coefficient-times-
    value explanations also require intercept, scaler mean/scale, feature order,
    artifact/training hash, and exact row values. They are associative model
    calculations, never causal claims.
23. **Required and optional inputs are not distinguished.** The model artifact
    must declare each feature `required`, `optional`, or `not_used`. A missing
    required feature forces `Pass`; an optional missing feature produces a
    fragility warning. Treating every absent weather field as blocking would
    conflict with the weather-free initial release.
24. **Push and void are currently conflated.** Current total settlement can store
    an exact-line push as `void`. V2 must preserve `win`, `loss`, `push`, and
    true `void` separately; pushes contribute zero P&L inside the eligible ROI
    denominator, while cancelled/postponed voids are excluded under the
    versioned settlement policy.

### Mathematical definitions

#### 1. Exact market reference and executable price

Convert an American price `o` to raw implied probability:

```
r(o) = -o / (-o + 100),  when o < 0
r(o) = 100 / (o + 100),  when o > 0
```

For an exact two-sided moneyline pair at the pre-registered reference book or
panel:

```
q_home = r(home_price) / (r(home_price) + r(away_price))
q_away = 1 - q_home
```

`q` is the vig-free **reference** probability used to measure model-market
disagreement. The no-vig method is itself versioned because proportional
normalization does not remove every form of favorite-longshot shading.

Reference and execution are separate policies:

- The reference market is a fixed sharp book/panel and coherent timestamp used
  for every game in the study.
- The execution market is the best eligible accessible quote selected by a
  pre-registered line-shopping rule at the decision cutoff.
- Each no-vig pair uses both sides from the same book and capture. A best price
  from one book cannot be paired with the opposite side from another.

For the exact executable side price, convert to decimal `d`:

```
d(o) = 1 + 100 / abs(o),  when o < 0
d(o) = 1 + o / 100,       when o > 0
break_even = 1 / d
```

The application must never substitute consensus odds for the executable price,
select a favorable book after the outcome, or mix capture times.

#### 2. Moneyline residual model

The target moneyline model uses the market as a fixed offset and learns only a
regularized residual:

```
logit(p_home) = logit(q_home) + beta' * z
```

where `z` contains point-in-time standardized features and missingness flags.
This differs from fitting an unrestricted market coefficient and then applying
a second manual anchor. The residual formulation makes the scientific question
explicit: do the baseball features add information after the market?

For selected side `s`:

```
disagreement_delta = p_s - q_s
price_margin        = p_s - break_even_s
EV_per_dollar       = p_s * d_s - 1
```

`disagreement_delta` answers where we differ from the reference market.
`price_margin` and EV answer whether the exact offered price would be supported
if the model were calibrated. They are separate UI fields.

The application stores three independent relationship axes:

```
outcome_relationship = same winner | opposite winner | unclear
market_strength       = model stronger | model weaker | within agreement band
price_value           = supported | too expensive | unavailable
```

For example, market 80% and model 52% is not a simple Confirmation. The outcome
direction matches, but the model is 28 percentage points weaker and the price is
almost certainly unsupported. The board may group cards for scanability, but it
must render all three axes.

#### 3. Uncertainty-aware price support

A binary game has outcome randomness; it does not have a generic per-game "95%
probability interval." Separately, the estimated probability has parameter,
sampling, calibration, and feature uncertainty. Generate time-respecting
bootstrap or ensemble probability draws `p^(b)` and report at least:

```
estimated_EV = mean_b(p^(b) * d - 1)
P(EV > 0)    = mean_b(I[p^(b) > 1/d])
```

A pre-specified lower probability bound `p_lower` may be used as a conservative
product gate. If adopted, research price support requires:

```
p_lower > break_even + execution_buffer
```

The execution buffer covers quote latency, rounding, and model degradation and
must be fixed before the confirmation sample. This lower-bound rule is a
conservative product choice, not a universal statistical theorem. Until
uncertainty is calibrated, the application displays `Price support unavailable`,
not a positive-EV badge.

#### 4. Totals distribution and push-aware EV

Let `T` be the model's calibrated full-game run distribution and `L` the exact
book total. For an Over:

```
p_win  = P(T > L)
p_push = P(T = L)  # non-zero only when the line can push
p_loss = P(T < L)
EV     = p_win * (d_over - 1) - p_loss
```

For an Under, reverse the win/loss inequalities. Estimate the distribution from
out-of-fold empirical residuals by line/regime or a calibrated count model such
as negative binomial. Do not import the soccer Poisson assumption. The exact
Over and Under prices must also be de-vigged when comparing our probability with
the market's probability at that same line.

At an integer line, the de-vigged Over/Under quote represents a conditional
no-push comparison:

```
q_over_cond  = r(over_price) / (r(over_price) + r(under_price))
p_over_cond  = p_win / (p_win + p_loss)
```

Compare `p_over_cond` with `q_over_cond`; preserve `p_push` separately for EV,
settlement, and calibration.

For a line with push probability, positive EV is equivalently:

```
p_win / (p_win + p_loss) > 1 / d
```

This conditional-on-no-push comparison is required; comparing raw `p_win` with
`1/d` is wrong when pushes are possible.

#### 5. Closing-line value

Moneyline CLV is measured from the selected side's pre-registered entry quote to
the chosen sharp/reference close, preferably in vig-free probability or log-odds
space. Positive CLV means the close assigned more probability to our selected
side than the entry snapshot did. Totals CLV must jointly account for line and
price; a half-run improvement and a price improvement are not interchangeable
without an equivalence calculation. Missing reference close means `CLV
unavailable`, never zero.

One transparent price-based moneyline statistic is:

```
CLV_price = entry_decimal / close_decimal - 1
```

Positive CLV is useful evidence that the process obtained a better number than
the close. It is not, by itself, proof of a profitable predictive edge.

#### 6. Parlay math

For genuinely independent legs and multiplied leg prices:

```
P_parlay = product(p_i)
D_parlay = product(d_i)
EV_parlay = P_parlay * D_parlay - 1
```

The economically correct general formula uses the direct same-book quote and a
joint win probability:

```
EV_parlay = P(all legs win) * direct_quote_decimal - 1
```

The application may use `product(p_i)` only as an explicitly labeled
independence estimate. A direct sportsbook quote overrides multiplied displayed
prices. When dependence or joint uncertainty is not modeled, no parlay may be
labeled positive EV. Same-game legs are blocked in the initial version, and the
default remains two or three legs.

### How each data group could add incremental information

| Data | Frozen raw inputs | Hypothesized residual mechanism | Primary failure risk |
|---|---|---|---|
| Starting pitcher | confirmed/probable state, xFIP/FIP, K/BB, pitch count trend, expected innings | Market may not have fully adjusted to a late starter change or changed workload | Public starter quality is already priced; stale or wrong starter creates false edge |
| Team offense | rolling and season-shrunk wRC+/OPS/ISO/K/BB, handedness split, confirmed lineup coverage | Lineup composition may differ from season team average | Current-form noise and post-lineup market adjustment |
| Bullpen quality | reliever-only IP, ER, HR, BB, HBP, K, ERA/FIP | Relief strength matters after the starter exits | Whole-staff data mislabeled as bullpen; role and sample instability |
| Bullpen workload | per-reliever pitches and appearances over 1/3/7 days, consecutive days, high-leverage-arm coverage | Recently used leverage relievers may be less available or effective | Arbitrary fatigue weights, incomplete usage data, manager discretion |
| Park | season/versioned runs and HR factors | Same contact quality converts differently by venue | Annual noise and double-counting with market total |
| Forecast weather | forecast timestamp/valid time, temperature, wind vector, precipitation, roof state | Late wind/temperature/roof changes can alter run environment | Observed weather substituted for forecast, uncertain roof, market reacts first |
| Lineup | player IDs, order, handedness, confirmation timestamp | A confirmed lineup changes expected plate appearances and quality | Late posting, scratches, and treating absence of a lineup as confirmation |
| Market path | same-book opening/current line and prices, sharp reference | Shows whether the disagreement is new, persistent, or already converged | Hindsight choice of opener/close and double-counting movement |

Raw fields are preferred over a hand-built composite. For example, freeze
`bullpen_pitches_1d`, `bullpen_pitches_3d`, `relievers_back_to_back`, and
`high_leverage_coverage`; let the model or a pre-registered rule determine their
effect. Any aggregation formula must be versioned.

Reliever-only FIP can be derived transparently from completed relief
appearances:

```
FIP_relief = (13*HR + 3*(BB + HBP) - 2*K) / IP + C_season
```

The season FIP constant `C_season`, innings, and component totals must all be
available by the decision cutoff; intentional walks require a versioned handling
rule. Starter appearances are excluded rather than blended into this metric.
Expected bullpen exposure can use a frozen expected-starter-innings input, but
its effect is learned in earlier folds rather than hard-coded.

Early-season team/pitcher rates require shrinkage to prevent tiny samples from
creating extreme residuals. One transparent form is:

```
shrunk_rate = w * current_rate + (1 - w) * prior_rate
w = sample_size / (sample_size + k)
```

`k`, the prior population, and the relevant sample unit (PA, TBF, or IP) are
trained or pre-registered inside earlier periods and stored in the model
artifact.

### Application operating flow

The application operates as a series of append-only pregame checkpoints. No
DeepSeek or other LLM participates.

```
MLB schedule/game feed
        +
point-in-time baseball/weather snapshots
        +
exact per-book Odds API snapshot
        |
        v
pregame eligibility + data-health gates
        |
        v
market-offset model + calibrated distribution
        |
        v
immutable decision observation
        |
        +--> Decision Board
        +--> Favorites Parlay research tray
        +--> Prospective tracking and settlement
```

#### Stage A - ingest and freeze sources

1. Append a schedule revision containing canonical MLB `gamePk`, both teams,
   scheduled start, venue, status, probable starters and confirmation states.
   Reschedules supersede a revision; they do not silently move its context.
2. Capture each Odds API request as one immutable batch. Preserve the raw JSON,
   then normalize provider event, book, market, outcome, exact line, American
   and decimal price, bookmaker update time, capture time, and integrity state.
   Batch identity is required for coherent same-capture parlay comparisons.
3. Write immutable team and pitcher captures. Each source capture records
   provider, endpoint/query, source record, `captured_at`, actual `available_at`,
   `stats_through_at`, sample window/size, schema and transformation versions,
   and raw checksum. `pybaseball` may supply advanced
   FanGraphs/Statcast fields, but zero-row, schema-drift, stale, constant, or
   low-coverage pulls fail health checks. A reduced MLB-official-stat fallback
   must be a separately versioned model, not silent substitution.
4. Derive reliever-only appearances and workload from MLB box scores. The first
   game pitcher/starter identity is separated from subsequent relievers before
   aggregation.
5. If adopted, an hourly forecast client such as `openmeteo-requests` writes an
   immutable forecast snapshot keyed by venue coordinates, forecast issue time,
   valid time, and game. MLB game-reported conditions cannot masquerade as a
   pregame forecast.

Package policy: direct `requests` calls may remain the canonical MLB/Odds API
transport; a package such as `mlbapi` is only an optional typed wrapper around
the same MLB source. `pybaseball` remains advanced-stat enrichment with hard
health checks. Adding a package never changes the source/provenance requirement.

#### Stage B - run checkpoints

Use fixed, configurable horizons rather than overwriting one row. Candidate
research checkpoints are `OPEN`, `T-180`, and `T-60`; one horizon is declared
the canonical accountability decision before collection. `CLOSE` is captured
for evaluation only and may never enter an earlier prediction. Additional
lineup checkpoints require a new policy version. Every phase must:

- revalidate event identity and both commence clocks;
- reject missing commence, ambiguity, stale/in-play odds, and started games;
- join only source records whose `available_at <= decision_at < commence_time`;
- record source coverage and missingness before applying research fallbacks;
- append prediction and decision snapshots without altering earlier phases.

A delayed or rescheduled game is re-resolved against the provider event and MLB
clock; a future provider time alone may not reopen an event already started by
the trusted MLB clock.

#### Stage C - create the complete prospective population

Create one immutable decision observation for every eligible
`(policy_version, model_version, matchup_id, market, decision_phase)`, including
`Pass`. It references:

```
prediction_snapshot_id, odds_snapshot_id, feature snapshot IDs,
schedule_revision_id, model_artifact_id, input manifest,
decision_at, event_commence, model/reference/observed-quote probabilities,
book, exact line, exact prices, selection, p_win/p_push/p_loss,
uncertainty method/draw summary, outcome_relationship, market_strength,
price_support,
decision_bucket, gate results, reason codes, origin
```

The current `mlb_bets` ledger remains part of the audit trail. A companion
all-decision observation population is required so `Pass` and non-selected rows
cannot disappear. Actual placed tickets, if ever supported, are a separate
immutable entity and must not be inferred from a displayed model decision.

The user-configured eligible book/jurisdiction set determines whether an
observed quote may be called accessible. Without that configuration, the
decision stores and displays it only as an observed quote.

#### Required persistence model

Names may vary, but the following normalized responsibilities are required:

| Structure | Required role and key fields |
|---|---|
| `mlb_source_ingest_runs` | Provider/query, requested/captured times, status, row count, coverage, schema version, raw hash/URI, error |
| immutable team/pitcher histories | Ingest run/source row IDs, captured/available/stats-through times, sample window and PA/IP/TBF, metric semantics/version, raw hash; no same-date update |
| `mlb_schedule_revisions` | gamePk, revision, commence, venue, teams, starter IDs/status, available/captured times, superseded revision |
| `mlb_relief_appearances` | Game/team/pitcher, actual starter/relief role, outs, pitches, BF, ER/HR/BB/IBB/HBP/K, final/source time |
| `mlb_bullpen_snapshots` | Event revision/team/cutoff, reliever-only season/recent quality, pitches/BF/appearances 1d/3d/7d, consecutive-day and active-arm coverage, sample sizes, derivation version |
| `mlb_weather_forecast_snapshots` | Event revision/venue coordinates, provider/model, issue/valid/capture times, temperature, wind vector, precipitation, humidity, roof capability/state/source |
| `mlb_odds_capture_runs` + `mlb_market_quotes` | Batch/request, provider event, book, market/outcome, line, American/decimal price, bookmaker update/capture times, commence-at-capture and integrity flags; append-only |
| `mlb_model_artifacts` | Feature schema/requiredness, training cutoff, required git SHA, dependency/data hashes, feature order, scaler mean/scale, coefficient/intercept, calibrator/distribution, seed and validation artifact |
| extended prediction snapshots | Event revision/artifact/cutoff, reference quote IDs, exact source-row input manifest, raw score, calibrated probability or total distribution, uncertainty and health/completeness |
| decision runs/snapshots | Policy/horizon/origin/cutoff/trust evaluation plus prediction, event revision, relationship axes, probability/price deltas, quote IDs, bucket/substate, missingness, reasons, fragility and parlay eligibility |
| settlement events | Decision ID, distinct win/loss/push/void, rule version, final source/time/score, P&L, close quote and CLV, correction linkage |
| parlay candidates/legs | Decision run/horizon, same book/batch, leg decision and quote IDs, estimated/direct payout, joint-probability method, fragility, deterministic selection/rank, result events |
| `mlb_trust_evaluations` | Market + artifact + policy + horizon, evaluated time, gate evidence/metrics/sample/calendar span and state; central UI source |

#### Stage D - centralized decision logic

For moneylines, compute three independent labels:

- `Outcome relationship`: same winner, opposite winner, or unclear.
- `Market strength`: model stronger, model weaker, or within a pre-registered
  probability agreement band.
- `Price support`: the uncertainty-aware probability clears the exact book's
  break-even probability plus the fixed execution buffer.

Map these into the existing board without implying actionability:

| Board column | Meaning |
|---|---|
| Model + Market Agree | Same likely winner **and** within the registered probability agreement band. Sub-badge says `price supported`, `too expensive`, or `price support unavailable`. Agreement alone never qualifies a parlay leg. |
| Model Disagrees | Opposing likely winner or a probability-strength gap beyond the registered band, even if both remain above 50%. Show whether the exact price is supported; most disagreement can still be a Pass economically. |
| Pass / Missing Information | Weak difference, incomplete/stale data, unavailable uncertainty, inaccessible/ineligible observed quote, or failed trust gate. |

For totals, do not call line movement `agreement`. The card shows our calibrated
Over/Under probabilities, the exact book's de-vigged probabilities, push risk,
line/price movement as context, and price support. The current v1
movement-alignment classification remains a research display artifact and must
be replaced before totals can advance.

#### Stage E - application presentation

The MLB Vegas page should render in this order:

1. **Research/validation status** - operational trust state and concise blocker
   count.
2. **Decision Board** - cards show model probability, vig-free reference,
   observed/access-eligible break-even, exact book/price, relationship and
   strength axes, uncertainty, price margin, source freshness, countdown, and
   top model contributions. Market/horizon filters select the immutable decision
   observation; a live/latest view cannot replace the canonical evaluation row.
3. **What changed** - opening/current price, starter/lineup/weather changes, and
   which immutable phase is displayed. Closed, rescheduled, invalidated, and
   superseded observations have explicit states rather than disappearing.
4. **Favorites Parlay tray** - only same-book, same-capture, unstarted moneyline
   favorites with complete data. Until validation, it is research-only and
   shows estimated payout rather than executable ticket language.
5. **Fragility panel** - `Low`, `Medium`, `High`, or `Blocked`, driven by data
   freshness, starter/lineup confirmation, uncertainty, price movement, weather,
   concentration, and dependence. It is not a confidence or star rating.
6. **Prospective evidence** - all decisions by bucket/phase with sample size,
   calibration, CLV, ROI, confidence intervals, pushes/voids, and exclusions.
7. **Detailed audit panels** - historical/backfill diagnostics remain below the
   prospective product and are visibly segregated.

Green is reserved for a passed trust or validation gate. Directional leans use
neutral colors while the markets remain research-only. The word `edge` may
describe a numeric difference, but `positive EV`, `actionable`, or staking copy
is forbidden until the central policy passes.

#### Stage F - favorites parlay and fragility rules

A research parlay candidate must satisfy all of the following:

- moneyline favorite at a named accessible book;
- model and market outcome agreement;
- exact two-sided price pair and fresh same-book quote for every leg;
- same Odds API capture batch and current schedule/event revision for every leg;
- immutable source-aware prediction with no blocking missing inputs;
- no started, same-game, duplicate-team, or mutually exclusive legs;
- block duplicate-team/doubleheader exposure in the initial policy;
- two or three legs by default;
- no claim of executable combined price without a direct book quote.

After probability calibration exists, `price support` may become an additional
eligibility requirement. Before that, the tray is an organizer of confirmations,
not an edge or EV builder. The system freezes a deterministic ranked top-K
candidate set separately from user-saved combinations so evaluation cannot
cherry-pick after results. Fragility must increase for common model-version
error, shared weather systems, same series/team exposure, uncertain starters,
late movement, and wide probability intervals. Product probabilities are shown
only as independence estimates until a joint simulation or empirically
validated dependence adjustment exists.

### Validation protocol for the edge hypothesis

1. Define one primary market-relative endpoint before the confirmation window.
   Moneyline primary: log loss or Brier difference versus the same-time vig-free
   reference. Totals primary: proper scoring of the full outcome distribution or
   exact-line log score, not mean MAE alone.
2. Evaluate every eligible game, not only displayed selections. Use rolling
   origin folds and an untouched final window.
3. Add features one group at a time: market baseline; starters; offense;
   reliever-only quality; bullpen workload; weather; lineup. Retain a group only
   if it improves out-of-fold performance and does not destabilize calibration.
4. Report access-eligible exact-price ROI as a secondary economic outcome with
   pushes and voids. Use a date-clustered bootstrap so games on the same
   day/series do not masquerade as independent evidence.
5. Report CLV against a pre-designated reference close and a same-subset entry
   comparison. Do not choose the favorable close or book after results.
6. Control feature/threshold experimentation through pre-registration or
   multiple-testing adjustment. A failed overall result cannot be rescued by a
   post-hoc team, month, odds, or total slice.
7. Evaluate and promote separately for each
   `(market, model_artifact, decision_policy, canonical_horizon)`; a model or
   threshold change begins a new evidence cohort.
8. Require the existing operational gates plus an adequate unique-game sample,
   no worse proper scoring than market, positive and stable CLV, and
   access-eligible exact-price ROI whose confidence lower bound is above zero
   before `actionable`.
9. No Kelly sizing, automated stakes, or production wager language is permitted
   before market-specific promotion. Even after promotion, stake policy requires
   a separate bankroll and drawdown review.

If a market is later promoted, the no-push full-Kelly reference is:

```
kelly_fraction = (p*d - 1) / (d - 1)
```

Production policy would use capped fractional Kelly plus portfolio limits by
game, team, day, model, and embedded parlay-leg exposure. Independent Kelly
calculations on correlated decisions are forbidden.

### Current-versus-target boundary

**Built now:** append-only prospective prediction records, canonical odds
identity/commence guards, centralized trust policy, an initial missingness map
for new model runs, exact observed retail-book comparison, and the research
Decision Board. The review above limits what those records currently prove.

**Still required for this v2 contract:** populated/healthy point-in-time team and
pitcher histories; reliever-only quality/workload; calibrated moneyline
uncertainty; a push-aware total distribution using exact prices; complete
all-decision prospective observations; outcome-agreement versus price-support UI;
same-book parlay candidates; dependence-aware fragility; and the prospective
evaluation artifact. Until those are built and validated, the application
provides structured research intelligence, not a demonstrated betting edge.

---

## MLB Single-User Bet Decision UX Contract v3 (2026-07-12)

### UX objective

The MLB Vegas experience must answer one question first:

> Should I take this exact market, at this exact book/line/price, right now?

Agreement/disagreement, model features, market movement, and validation evidence
support that answer; they are not the primary navigation. Because this is a
single-user application, optimize for fast personal decisions rather than
onboarding, multi-user permissions, or generic education.

Replace the current equal-width Agree/Disagree/Pass board and broad mode banners
with a prioritized decision queue. The first visible section is `TAKE NOW`, then
`WATCH`; the full slate appears below in a compact filterable table. Every
negative or unavailable answer names the exact blocker and the next useful
action.

### Current UI migration map

The first UI pass removes generic market-level badges and long defensive copy
from the decision path. Replace them as follows:

| Current surface | Replacement |
|---|---|
| Research/mode banner | Per-market control: `TAKE enabled` or `TAKE disabled - N gates open`, with a link to the exact gates |
| Moneyline market-efficiency badge | A bet-level `PASS`, `WATCH`, or `BLOCKED` result with the exact team, book, price, and reason |
| Shadow-signal heading | `Today's Decisions`, ordered by action and start urgency |
| Equal Agree/Disagree/Pass columns | One prioritized `TAKE NOW`/`WATCH` queue plus the full-slate table |
| Long backtest warning above the board | Compact trust status; full evidence moves to `Evidence and History` |
| Model-edge percentage alone | Model probability, offered break-even, modeled ROI, price target, and resample stability together |
| Generic refresh button | Staged refresh with Schedule/Quotes/Context/Prediction/Decision status and last-good timestamps |

No market-level badge substitutes for a bet decision. If the moneyline model
cannot qualify at a displayed price, the UI says `PASS` and why; if evidence or
data prevents evaluation, it says `WATCH` or `BLOCKED` and gives the trigger or
repair action.

### Primary answer taxonomy

The central decision service, not React components, returns one primary status:

| Status | Direct meaning | When it is allowed |
|---|---|---|
| `TAKE NOW` | Take the exact displayed selection only at the displayed book/line/price or better, before expiry | The exact market/artifact/policy/horizon trust state is actionable; current event/quote/input identity passes; calibrated uncertainty exists; price and stability rules pass |
| `WATCH` | Do not take it now; a named price, input, refresh, or validation trigger could make it qualify | The calculation is coherent and the next qualifying trigger is known, but at least one non-integrity condition is still open |
| `PASS` | Do not take this market at the current snapshot and price | Complete, fresh evidence exists, but neither side clears the locked decision/price rules |
| `BLOCKED` | The application cannot produce a trustworthy decision | Identity, provenance, required input, quote, distribution, calibration, or local integrity is missing/invalid |
| `CLOSED` | No new pregame action is possible | Game started, market closed/suspended, event cancelled, or old revision was superseded |

`PASS` is price- and snapshot-specific, not a claim that the team is bad.
`WATCH` always includes the trigger and next check. `BLOCKED` always includes the
failed gate and remedy when one exists.

Global prospective validation and local calculation integrity remain separate:

- If a complete local calculation would otherwise qualify but the prospective
  cohort has not passed its promotion gates, use `WATCH` and name the failed
  cohort gates.
- If calibrated uncertainty, required provenance, or a valid paired quote does
  not exist, use `BLOCKED`; the system cannot calculate a trustworthy answer.
- The current MLB models cannot render `TAKE NOW` until the exact market cohort
  passes centralized promotion.

### Secondary evidence labels

Every card also shows a separate market relationship label:

```
Agree - model stronger
Agree - market stronger
Disagree on winner
At market
```

This prevents outcome direction from being confused with price value. For
example, model 52% versus market 70% is `Agree - market stronger`, while the
price status is likely `PASS`. `TAKE NOW` never means merely that model and
market favor the same team.

Additional compact fields remain separate:

```
Price support: passes | too expensive | unavailable
Data completeness: complete | optional inputs missing | required input missing
Fragility: low | medium | high | blocked
Trust: take enabled | take disabled (N gates open)
```

### Proposed locked decision parameters

These are proposed initial policy parameters, not discovered truths. Freeze them
under a policy version before collecting their confirmation population; do not
tune them in the UI:

```
minimum mean estimated ROI                 = +2.0%
minimum model resamples with ROI > 0       = 80%
maximum quote age                          = 10 minutes
minimum manual-verification/start buffer   = 5 minutes
WATCH price distance                       = within 2% of required decimal price
```

For a no-push market with model probability `p`, decimal price `D`, and minimum
ROI `r`:

```
current_ROI = p*D - 1
D_target    = (1 + r) / p
```

For a push-capable total:

```
current_ROI = p_win*(D - 1) - p_loss
D_target    = 1 + (r + p_loss) / p_win
```

The displayed `take to` price is the nearest valid sportsbook tick that clears
both the mean-ROI rule and the model-resample stability rule. A different total
line is a different proposition: `Under 9 -110 or better` cannot silently become
`Under 8.5`.

### Exact status rules and message templates

#### TAKE NOW

Render only when all are true:

1. Central trust state is `actionable` for the exact market, artifact, policy,
   and canonical horizon.
2. Decision snapshot matches the current schedule/event revision and future
   commence time.
3. Every required source record was available by the prediction cutoff.
4. Calibrated probability/distribution and model-resample uncertainty exist.
5. Quote is from a configured accessible book, contains the paired market, is no
   more than 10 minutes old, and leaves at least five minutes to verify.
6. Mean estimated ROI is at least +2.0%.
7. At least 80% of model resamples have ROI above zero.
8. No market-specific blocker remains.

Message template:

> TAKE ATL moneyline at DraftKings -125 or better. Model 58.4%; price requires
> 55.6%; estimated return +5.1% per unit; 86% of model resamples remain positive.
> Quote observed 2 minutes ago and expires at 6:40 PM ET.

`TAKE NOW` is price-specific, not team-specific. Do not attach recommended stake
sizing. The CTA is `Verify at DraftKings`; after the user verifies externally,
`Mark Taken` records the actual book, line, price, time, and optional stake as a
separate immutable ticket.

#### WATCH

Allowed only when the calculation is coherent but cannot currently receive
`TAKE NOW`, including:

- local price/uncertainty rules pass but prospective promotion is incomplete;
- mean estimated ROI is positive but below +2.0%;
- mean ROI passes but fewer than 80% of resamples remain positive;
- the fully qualifying price is within 2% of the current decimal payout;
- probable starter or lineup is pending and a defined checkpoint will rerun the
  model;
- a material input changed and a fresh decision is actively being calculated.

Message templates:

> WATCH - the current price clears the local rule, but TAKE is disabled: 42/150
> prospective games and the positive-ROI resample gate remains open. Next evaluation:
> T-60 at 5:10 PM ET.

> WATCH for LAD -130 or better. Current -145 requires 59.2%; model estimate is
> 58.0%. Alert when the observed price reaches the target.

> WATCH - estimated return is +3.1%, but only 67% of model resamples remain
> positive; policy requires 80%.

> WATCH - confirmed starter is pending. The card will recalculate at T-60; the
> prior decision is superseded and cannot be used.

#### PASS

Use when all data and quote checks are valid but no side clears `WATCH` or `TAKE
NOW` rules. Evaluate both sides independently; a model favorite may still be too
expensive, and an underdog may have the better price margin.

Message templates:

> PASS NYY at FanDuel -180. The model favors NYY, but the maximum supported price
> is -150. Current estimated return is -4.2% per unit.

> PASS - model 52.4%, reference market 51.8%, and neither offered side clears the
> +2.0% price rule.

> PASS Over 8.5 at -115. Win 49.0%, push 0.0%, loss 51.0%; estimated return is
> -8.4% per unit.

#### BLOCKED

Use when no trustworthy current calculation is possible:

- missing/mismatched MLB gamePk, provider event, schedule revision, or commence;
- missing exact quote, paired opposing price, or configured accessible book;
- mixed-book/mixed-capture pair, invalid price, stale/suspended quote;
- started game or inside manual verification buffer;
- missing required starter, feature, source availability, or artifact manifest;
- required input became available only after prediction cutoff;
- calibrated uncertainty or push-aware total distribution unavailable;
- unknown settlement rule, post-commence write, or immutable provenance failure.

Message templates:

> BLOCKED - DraftKings quote is 18 minutes old; policy maximum is 10 minutes.
> Refresh quotes before evaluating this market.

> BLOCKED - starting pitcher is unconfirmed and is required by this artifact.
> Next starter check: 4:30 PM ET.

> BLOCKED - the game was rescheduled. Current prediction and quote belong to the
> old 1:10 PM event revision and cannot be reused.

> BLOCKED - this total model does not yet produce calibrated win/push/loss
> probabilities, so the offered price cannot be evaluated.

#### CLOSED

Message templates:

> CLOSED - game started at 7:10 PM ET. Canonical pregame decision: PASS at
> DraftKings -145.

> CLOSED / VOID - event was postponed. The makeup game will receive a new
> schedule revision and decision.

### Screen 1 - Today's Decisions (`/vegas?sport=mlb`)

#### Header and controls

- Date arrows/calendar and explicit ET clock.
- `My Books` selector using the single local profile; only selected books may be
  described as accessible.
- `Moneyline`, `Totals`, and later `Parlays` tabs.
- Last successful refresh, source age, next scheduled checkpoint, and one
  `Refresh` control.
- Refresh progress is explicit:

  ```
  Schedule -> Quotes -> Context -> Prediction -> Decisions
  ```

  A partial-stage failure names the failed stage and retains the last good card
  as stale; it never displays a false success.
- Per-market trust control: `TAKE enabled` or `TAKE disabled - N gates open`,
  linking directly to the failed requirements.

#### Decision summary strip

Show counts for:

```
TAKE NOW | WATCH | PASS | BLOCKED | CLOSED
```

If no market is eligible for `TAKE NOW`, say why directly:

> No bets qualify now. Moneyline has 5 open validation gates; totals lacks a
> calibrated win/push/loss distribution. View gates.

#### Prioritized queue

Render full-width `TAKE NOW` cards first, then `WATCH`. Do not reserve empty
equal-width columns. `PASS` and `BLOCKED` appear in the full-slate table and may
be expanded. Sort by:

1. primary status;
2. evidence completeness;
3. start urgency;
4. price margin/stability;
5. matchup.

Do not automatically reorder while the user is reading. Show a transition toast
and update the affected card in place until the user acknowledges or refreshes
the queue.

#### Compact decision card

Every card shows above the fold:

- primary status and one-sentence reason;
- exact selection, line, American and decimal price, named book;
- `take to` price when calculable;
- quote capture time, bookmaker update time, age, expiry, and start countdown;
- matchup, venue, Game 1/Game 2 for doubleheaders, event revision/status;
- model probability; vig-free reference; offered break-even; signed price gap;
- relationship label; price support; completeness; fragility;
- canonical horizon, artifact, and policy version;
- top two model associations with actual values and source ages;
- `What changed` and `What is missing` rows.

Card actions:

```
Details | Refresh/Watch | Alert at price | Add to parlay | Verify at book
```

`Add to parlay` is hidden/disabled unless the leg policy passes. `Verify at book`
does not claim the API quote is still offered. `Mark Taken` appears only after
manual verification and stores actual ticket terms separately from the model.

#### Full-slate table

Recommended desktop columns:

| Column | Content |
|---|---|
| Start | ET time, countdown, Game 1/2, event state |
| Matchup | Away @ home, starters, venue |
| Market | ML or exact total side/line |
| Answer | TAKE NOW / WATCH / PASS / BLOCKED / CLOSED |
| Relationship | Agree-model stronger / Agree-market stronger / Disagree / At market |
| Exact quote | Configured book, line, American price, age |
| Model | Calibrated probability or win/push/loss |
| Reference | Vig-free probability at the fixed reference |
| Break-even | Exact offered price requirement |
| Margin | Model minus break-even and estimated ROI |
| Take to | Maximum/minimum qualifying price at the same proposition |
| Stability | Positive-resample percentage and uncertainty range |
| Fragility | Low/medium/high/blocked plus first reason |
| Changed | Price, starter, lineup, weather, or status change |
| Action | Details, alert, verify, parlay |

Filters: answer, market, selected book, relationship, start window, changed
since last view, and completeness. Compact/list toggle is stored locally.

### Screen 2 - Game and Bet Detail

Open as a drawer on desktop and a full screen on mobile. Keep the direct answer
and exact ticket sticky at the top.

1. **Exact quote panel** - selection, line, American/decimal price, observed and
   bookmaker-update times, age, countdown, take-to price, `Verify at book`.
2. **Model vs market panel** - probability bar with model, fixed reference,
   offered break-even, price gap, estimated ROI, model-resample stability. Totals
   show win/push/loss and resolved win rate.
3. **Price shop table** - all configured books with exact line, both-side price,
   update age, eligibility, and best observed quote. Reference market is visibly
   separate from the selected book.
4. **Why this answer** - top fitted-model associations, actual values, direction,
   source, freshness, required/optional state. Copy says `association in this
   model`, never that a feature caused the outcome.
5. **Against this bet** - strongest counter-signals, uncertainty, missing
   optional inputs, concentration, and fragility reasons.
6. **What changed timeline** - schedule/starter revisions, opening/current line
   and price, forecast/lineup changes, predictions, decisions, and superseded
   snapshots at each horizon.
7. **Provenance drawer** - source record IDs, real available-at times, artifact
   and transformation hashes, quote/event identity, settlement rule.

### Screen 3 - Favorites Parlay Builder

Desktop uses a right-side tray; mobile uses a sticky bottom bar opening a full
screen. The user chooses the sportsbook first.

Initial leg eligibility:

- two or three moneyline favorites by default;
- same configured book and same Odds API capture batch;
- current event/schedule revisions and future commence times;
- no same game, duplicate team, doubleheader, or mutually exclusive exposure;
- complete required inputs and eligible individual decision snapshots;
- exact current leg quote and no stale/mixed prices.

Each leg row shows matchup/start, team, exact ML, model/reference/break-even,
primary answer, relationship, freshness, fragility, and remove control.

Summary shows:

| Field | Display rule |
|---|---|
| Observed multiplied payout | Label `estimated`; leg-price product is not a direct parlay quote |
| Direct sportsbook quote | Show only after manually entered or retrieved from a supported source |
| Raw book-implied joint | From the direct quote when available |
| Vig-free market joint | Fixed reference method and timestamp |
| Model joint | Name independence, simulation, or validated dependence method |
| Stressed range | Shared model/calibration uncertainty and dependence stress |
| Fragility | Concrete common risks, not a star rating |
| Return calculator | User-entered stake only; no suggested stake |

Initial CTA:

> Verify parlay price at DraftKings.

The user may enter the actual offered combined price and `Mark Taken`. Parlay
`TAKE NOW` requires its own promoted artifact/policy/horizon cohort, a current
direct quote, individually eligible legs, and a validated joint-probability
method. Single-leg promotion cannot promote parlays.

If prices change, invalidate the tray and show:

> Prices changed. This combination must be recalculated. Accept new prices or
> keep the prior combination closed for comparison.

Freeze a deterministic ranked top-K candidate set separately from user-saved
combinations so prospective evaluation cannot cherry-pick winners.

### Screen 4 - Evidence and History

Tabs:

```
Trust Gates | Prospective Decisions | My Taken Bets | Backtest Archive
```

`Trust Gates` is grouped by market, artifact, policy, and canonical horizon. It
shows identity/timestamp integrity, exact-quote coverage, required-input
coverage, calibration, benchmark proper score, unique games/calendar span,
exact-price ROI with clustered interval, CLV, and version consistency. Each
failed gate includes the concrete count and required remedy.

Prospective summary:

- unique games and date span;
- counts by primary answer and relationship;
- Brier/log loss versus same-time reference for moneyline;
- total probability score/CRPS and push calibration;
- calibration bins and model-resample coverage;
- W/L/Push/Void and exact-price ROI interval;
- CLV under the fixed reference/cutoff;
- quote, feature, and decision coverage;
- concentration by date/team/series.

Prospective table columns:

| Frozen at | Horizon | Game | Market | Answer | Relationship | Exact selection/book/line/price | Model/reference/break-even | Result | Close/CLV | P&L | Artifact/policy |
|---|---|---|---|---|---|---|---|---|---|---|---|

Track the complete canonical population, including `PASS` and `BLOCKED`,
separately from `My Taken Bets`. A version change starts a new cohort and leaves
old evidence visible. A missing prospective sample reads `No settled prospective
decisions yet`, never `0%`.

### Change notifications and alerts

Card transitions generate concise messages:

> Price changed: TAKE NOW at -125 became PASS at -145. Model probability did not
> change.

> Starter changed from Smith to Jones. Prior decision superseded; recalculation
> is running.

> WATCH target reached: DraftKings moved to -130. Fresh decision required before
> status can change.

> Quote refresh failed at the Quotes stage. Last good DraftKings price is 14
> minutes old and the market is BLOCKED.

Alerts may trigger on target price, starter confirmation, lineup publication,
fresh decision availability, or status transition. An alert never carries
forward an old `TAKE NOW`; it requests a fresh immutable decision.

### Empty, stale, and lifecycle states

- No games: `No MLB games scheduled for this date.`
- Schedule/no market: `Waiting for sportsbook markets. Next poll: 10:30 AM ET.`
- No current prediction: `BLOCKED - no prediction matches the current event
  revision.`
- Stale/single-sided quote: `BLOCKED - refresh required`, showing age and limit.
- Material input change: immediately supersede old answer and show
  `Recalculating for [starter/lineup/event revision]`.
- Partial API failure: retain last good snapshot as stale, name the failed source
  and last successful time.
- Started game: move to `CLOSED` without erasing the canonical pregame answer.
- Postponed/cancelled: `CLOSED / VOID`; makeup receives a new revision.
- Artifact/event/commence mismatch: hard `BLOCKED`.

### Mobile behavior

- Fixed bottom navigation: `Board`, `Parlay`, `Evidence`.
- One-column queue; card summary contains answer, exact ticket, countdown,
  price/model gap, and first blocker above the fold.
- Tap expands reasons, source data, and price table; never depend on hover.
- Parlay opens as a bottom sheet/full screen.
- Tables become stacked cards with the same field labels.
- Minimum 44px touch targets; colors always paired with words/icons.
- Relative time opens absolute ET and UTC timestamps.
- Do not reorder while reading; show a change toast and update in place.

### Visual language

- `TAKE NOW`: green, allowed only because all central trust and price gates pass.
- `WATCH`: amber with clock icon and explicit trigger.
- `PASS`: neutral slate with current-price reason.
- `BLOCKED`: red with failed integrity/validation gate.
- `CLOSED`: dark neutral with frozen pregame answer.
- Agreement/disagreement uses blue/violet secondary chips, never the primary
  action color.
- Never use stars as a substitute for the primary answer.
- Put the direct answer, exact quote, quote age, and first reason above the fold.

### Central decision response contract

The UI renders a server-produced immutable decision payload and does not recreate
thresholds:

```
decision_snapshot_id
event_revision_id
market / selection / exact line
primary_status: take_now | watch | pass | blocked | closed
headline / primary_reason_code / primary_reason_text
relationship / market_strength / price_support
reference_quote_id / observed_quote_id / configured_book_eligible
american_price / decimal_price / break_even
model_probability or p_win/p_push/p_loss
estimated_roi / resample_positive_rate / uncertainty_range
take_to_price / valid_until / next_check_at / watch_trigger
completeness / required_missing / optional_missing
fragility / fragility_reasons
top_associations / counter_signals / changed_since_prior
artifact_version / policy_version / canonical_horizon / trust_evaluation_id
parlay_eligible
```

The frontend may format numbers and responsive layout only. It cannot turn
`WATCH` into `TAKE NOW`, recalculate a target price, infer trust from color, or
reuse a status with a different quote.

### Implementation order

| Priority | UI/application work | Acceptance outcome |
|---|---|---|
| P0 | Central primary-status contract; reference versus observed-book separation; normalized immutable quotes; event revisions; fixed decision snapshots; stale/superseded handling | The same frozen decision drives card, ledger, settlement, and evidence; current markets cannot accidentally render TAKE NOW |
| P1 | Today queue, direct messages, full-slate table, details drawer, My Books settings, staged refresh, alerts, Verify/Mark Taken, responsive cards | User can see what to take, watch, pass, or why evaluation is blocked in one scan |
| P2 | Same-book/same-batch favorites tray, repricing, fragility, actual quote entry, deterministic candidate tracking | Parlays are coherent, auditable, and invalidated when any quote changes |
| P3 | Evidence/History screens, canonical complete population, settlement corrections, calibration/ROI/CLV cohorts and gates | Every displayed decision maps to prospective evidence without board/ledger mismatch |
| P4 | Weather/lineup/roof enrichment, richer timeline, source-driven alerts | Additional context appears only after P0-P3 decision integrity is proven |

Do not add more decorative charts before P0-P3. The first successful UI milestone
is not visual polish; it is one exact, immutable answer with a concrete price,
expiry, reason, and next action.

### Implementation status (2026-07-12)

Built in this pass:

- MLB now has a dedicated server/client route boundary. The NBA Vegas page is
  no longer the execution path for MLB.
- The server produces one deterministic decision payload per exact matchup,
  market, sportsbook, observed odds snapshot, prediction snapshot, event
  revision, and policy version. React filters and renders these payloads; it
  does not calculate thresholds or promote statuses.
- Moneyline evaluation checks both sides at each book and selects the stronger
  price proposition. It no longer assumes the model favorite is the bet.
- American-price validation, decimal conversion, two-sided vig removal, exact
  break-even, ROI, qualifying-price rounding, quote age, bookmaker update time,
  start buffer, event/commence matching, feature cutoff, and current-versus-
  reference snapshot identity are centralized in
  `web/src/lib/mlb-vegas-decisions.ts`.
- Totals require a line-matched win/push/loss distribution and distribution
  resamples. A mean total alone cannot produce a price decision.
- Moneylines require a named out-of-fold calibration method and probability
  resamples. A raw probability alone cannot produce a price decision.
- The current models therefore fail closed: existing raw moneyline and mean-
  total artifacts render a concrete `BLOCKED` reason rather than being promoted.
- The new MLB page includes the `TAKE NOW`/`WATCH` queue, complete slate table,
  My Books filtering, market and answer filters, exact-price cards, target-price
  tracking on the local device, game/market detail drawer, per-book price-shop
  table, model/counter-signal sections, snapshot identity, staged pipeline
  output, favorites parlay screen, trust gates, coverage/health, historical
  checks, prospective ledger, line movement, alerts, and audit history.
- The favorites builder accepts only individually qualified same-book favorite
  legs and keeps the combined status blocked until a separate parlay artifact,
  dependence method, validation cohort, and direct combined quote exist.
- The default MLB date is resolved in Eastern time, lifecycle rows are retained,
  doubleheaders are numbered, and the MLB layout uses the wider decision-table
  viewport without changing NBA, soccer, or tennis behavior.

Still required before the first real `TAKE NOW` can appear:

1. Persist normalized quote propositions and append-only decision snapshots
   instead of relying only on deterministic IDs over immutable source rows.
2. Add a genuine out-of-fold moneyline calibrator and probability-resample
   artifact with its canonical horizon.
3. Add a line-specific total distribution with win/push/loss resamples.
4. Key prospective trust evidence by exact artifact, policy, and horizon, then
   pass the locked promotion gates.
5. Persist personal verified tickets and price alerts on the server; current
   target tracking is intentionally device-local.
6. Add the canonical refresh-job trigger and persisted stage/run status. The
   current control reloads the latest output and does not create a second MLB
   odds writer.

### Exact sportsbook-price repair (2026-07-12)

- `mlb-gameline-v4` starts a clean prospective validation cohort. Historical
  v3 rows with a consensus price but no named sportsbook remain historical and
  cannot be relabeled as valid prospective evidence.
- Every new recommendation must persist the exact sportsbook, American price,
  immutable odds-snapshot ID, prediction-snapshot ID, selection, and total line
  when applicable. Missing any required quote provenance fails closed.
- The selected price comes from one configured executable sportsbook in the
  same odds snapshot used by the prediction. Moneylines require both teams at
  that book; totals require Over and Under at the exact frozen line. Vig-free
  market probability is calculated from that paired quote.
- Before first pitch, a changed recommendation updates the one active row for
  that game and market while appending a new audit snapshot. Duplicate pending
  rows are superseded. After first pitch, the recommendation is locked.
- The Evidence gate counts exact-price coverage only when price, sportsbook,
  and linked odds snapshot are all present, and it rejects a bet whose odds
  snapshot differs from its prediction snapshot.

---

## Jira-First Delivery Contract

The DFSVEGAS Jira project (`SCRUM`) is the operational source of truth. Read
the parent Epic and every related issue, search for duplicates, and inspect the
existing repository before creating additional work or writing code.

### Planning requirements

1. Convert the requested outcome into numbered, testable acceptance criteria.
2. Build a dependency-ordered implementation plan.
3. Classify every criterion as complete, partially complete, not started, or
   blocked using direct evidence.
4. Never infer completion from documentation, database columns, existing code,
   passing compilation, or a merged commit alone.
5. Update Jira with the plan, dependencies, blockers, and current evidence
   before coding.

### Required execution order

Complete and verify work in this order unless Jira records a justified
dependency change:

1. Source selection and data contracts.
2. Database schema and immutable provenance.
3. Data ingestion and historical population.
4. Data-quality, freshness, coverage, and leakage checks.
5. Model features and calculations.
6. Model calibration and chronological validation.
7. Decision policy and exact-price handling.
8. Backend queries and API payloads.
9. User-facing UI components and messages.
10. Automated unit, integration, and policy tests.
11. Real-data pipeline execution.
12. Visual verification of the application.
13. Documentation, Jira evidence, commit, and deployment.

Do not begin a dependent step until its prerequisites pass. If a prerequisite
is blocked, record the exact blocker and continue only with genuinely
independent work.

### Status discipline

Track each Jira issue through:

`Planned -> Data Connected -> Backend Complete -> UI Complete -> Real-Data Verified -> Done`

“Built,” “implemented,” and “complete” mean `Done`, not partially complete.

### Definition of Done

An issue may reach `Done` only when:

- Every acceptance criterion passes.
- Required data is populated, not merely defined in the schema.
- Data provenance, freshness, missingness, and sample sizes are recorded.
- Backend calculations use the intended data.
- The feature is visible and understandable in the actual application.
- Missing or stale information is clearly shown to the user.
- Automated tests pass.
- Representative real data has been processed.
- The rendered application has been visually inspected.
- Screenshots demonstrate each user-facing requirement.
- Tested dates, examples, commands, and results are attached to Jira.
- Relevant files and commits are linked.
- No unresolved blocker or deferred acceptance criterion remains hidden.

Before reporting completion, publish this criterion-level matrix:

| Order | Jira issue | Acceptance criterion | Data | Backend | UI | Tests | Real-data verification | Screenshot | Result |
|---:|---|---|---|---|---|---|---|---|---|

Mark every row `PASS`, `FAIL`, or `BLOCKED`. Do not report an overall complete
result while any required row is `FAIL` or `BLOCKED`.

### Communication and documentation rules

- Report material progress in Jira as work proceeds.
- Never silently reduce scope.
- Clearly distinguish planned, partially implemented, and verified work.
- When implementation reveals missing work, create or update the appropriate
  Jira issue and place it correctly in the dependency order.
- Keep `CLAUDE.md` limited to durable architecture, model, data-source, and
  operational decisions. Jira owns scope, progress, blockers, and completion
  evidence.
- Begin with the next unblocked issue in dependency order and continue until
  the outcome is verified or a material decision requires user input.

### Current MLB program

Use `SCRUM-5` as the parent Epic. Begin with `SCRUM-15`: audit the existing
implementation, establish the true status of `SCRUM-6` through `SCRUM-14`, and
then execute the earliest unmet dependency. Do not start with UI polish until
the data and provenance requirements supporting that UI are verified.

### MLB forecast source contract (SCRUM-9)

- U.S. venues use the National Weather Service (`api.weather.gov`) hourly
  forecast as the primary source because it exposes provider generation time,
  forecast valid time, temperature, humidity, precipitation probability, wind,
  grid identity, and the raw source payload without an API key.
- Rogers Centre uses Environment and Climate Change Canada's official Toronto
  citypage feed (`dd.weather.gc.ca`, site `s0000458`). Hourly periods may pass
  the complete gate when the issued forecast covers game time. Longer-range
  daily periods are stored as `partial_daily_resolution`; they preserve the
  official issue time, daily temperature/humidity/precipitation and raw XML,
  but remain non-passing until game-time wind and hourly resolution exist.
- Forecast captures are immutable and must precede first pitch. Mutable
  `mlb_matchups.weather_*` columns are compatibility caches, not prediction
  provenance and are not used as historical model evidence.
- Open-Meteo is a coverage fallback only. It is labeled
  `provider_issue_time_unavailable` and cannot pass the complete forecast-
  provenance gate because retrieval time is not misrepresented as provider
  issue time.
- Static venue metadata may identify `open_air`, `retractable`, or `fixed`
  roof capability. Retractable roof state remains `unknown` until a sourced
  game-specific state is captured; weather alone never implies open or closed.

## Tennis decision data contract — 2023+ (2026-07-13)

This contract supersedes the old odds-only Tennis MVP architecture for all new
work under Jira Epic `SCRUM-17`. It does not overturn the existing no-edge
finding: Tennis stays market-equal and cannot emit a TAKE decision unless a new
feature set passes the preregistered chronological market benchmark.

### Historical scope and validation windows

- The historical corpus starts `2023-01-01`. No pre-2023 match, rating, or
  statistic may initialize or influence a new rating or feature.
- 2023 is rating initialization/burn-in, 2024 is development, 2025 is
  chronological validation, and 2026 is the untouched final test.
- Every ATP/WTA year is an expected partition. A zero-row expected partition is
  a hard failure, never a warning. The existing cached corpus has zero ATP 2025
  rows because a source failure was silently accepted; that cache is invalid for
  `SCRUM-17`/`SCRUM-27` until rebuilt.
- Cached artifact names and manifests include start/end dates, tours, provider
  versions/checksums, build version, row counts, and missingness. A cache built
  for another date scope may not satisfy a 2023+ request.

### Source decisions

| Input | Primary | Supplement/fallback | Contract |
|---|---|---|---|
| Upcoming schedule, event ID, start time, current sportsbook quotes | The Odds API `/v4/sports` plus tournament `/odds` endpoints | None; stale stored state is display-only | API key required; capture pre-start `h2h`, `totals`, and `spreads`, provider event ID, bookmaker key/update time, provider capture time, and raw checksum. Never ingest in-play quotes. |
| ATP historical chronology, surface, results, rank and representative closing odds, 2023+ | tennis-data ATP yearly XLSX | None for chronology | This is the canonical ATP calendar because the TML current-season file was observed stale in 2026 (137 rows versus 1,593 tennis-data rows). Retry/cache with checksum and never silently drop a year. |
| ATP match performance supplement, 2023+ | TML-Database yearly Sackmann-format CSV | None until another performance source is approved | Preserve available raw serve statistics and join to tennis-data chronology by normalized players, tournament and a bounded date window. TML tournament dates are tournament-start dates, not match dates, and cannot define chronology. |
| WTA historical matches, results, rank, points and representative closing odds, 2023+ | tennis-data WTA yearly XLSX | No approved performance-stat fallback | WTA serve/return fields remain NULL with `source_unavailable`; zero is not a substitute. |
| Current results/retirements | TennisExplorer provisional result feed | tennis-data, then TheSportsDB; manual correction record for disputes | Store every provider observation. A result correction appends a revision. Walkovers never update Elo. Retirement/void settlement follows the exact book rule when available; otherwise the decision remains unresolved. |
| Injuries/withdrawals | No approved reliable feed | Explicit manual/provenance event only | Do not infer health from odds or disappearance. Missing withdrawal state is surfaced and can block a decision under the policy. |
| Player rankings/features | Point-in-time fields in TML/tennis-data | None until another source is approved | Store source availability and stats-through dates; never join today's rank to a historical match. |

Free HTML/result feeds are operational observations, not silent authorities.
Provider identity and confidence are stored with each result. When providers
disagree, preserve both observations and require a documented resolution.

### Live odds scope and book roles (frozen 2026-07-13)

- **The Odds API is the sole approved live Tennis odds source.** Its active
  tournament discovery is not a complete ATP/WTA calendar. When a tournament is
  absent from that provider, the application must record `provider_not_covered`,
  skip quote capture, prediction, and decision creation for that tournament, and
  explain that state to the user. It must not imply that no sportsbook has odds.
- A failed `/v4/sports` request, stale capture, or quota exhaustion is a
  **pipeline/data-health failure**. An otherwise healthy run with no active
  provider tournament keys is a **provider-coverage limitation**. These states
  are distinct in Jira evidence, operational health output, and the UI.
- DraftKings' public Tennis page can list events that The Odds API does not
  distribute. DraftKings' state-specific sportsbook content endpoints are not an
  approved fallback: they are undocumented, state/geolocation dependent, and
  returned `403 Access Denied` in the 2026-07-13 verification. Do not scrape or
  depend on that private feed without an explicit source-contract decision.
- **Pinnacle is the primary fair-price reference** when an eligible paired
  Pinnacle quote exists. **DraftKings is the preferred exact execution price and
  displayed fallback price** when it is present in The Odds API payload. A
  DraftKings price cannot also be used as its own fair-price reference: without
  Pinnacle (or another separately approved reference), the comparison is
  `NO_PINNACLE_REFERENCE`/blocked rather than a fabricated zero-edge result.
- Legacy consensus columns remain research/display compatibility fields. They
  must never be substituted for an exact executable price or for the
  Pinnacle-versus-DraftKings decision comparison. SCRUM-22/23/25 own the
  implementation, policy, and user-facing proof of this rule.

### Canonical identity

- `tennis_players` owns a stable internal player ID. Provider names live in a
  separate alias table keyed by provider, tour, normalized name, and effective
  dates. ATP and WTA namespaces remain separate.
- Name normalization is Unicode NFKD, lowercase alphanumeric, with documented
  suffix handling (`Jr`, `II`, etc.). Automated matching records method and
  confidence. Ambiguous aliases fail closed and enter an identity review queue.
- A canonical event is tour + canonical tournament + unordered canonical player
  pair + scheduled window/round. Provider event IDs are aliases/revisions, not
  the sole identity.
- Selections attach to player IDs, not provider `home`/`away` order. A provider
  player-order reversal therefore cannot reverse a bet.
- Reschedules append an event revision with old/new start time and provider IDs.
  Duplicate-name and same-day rematch cases require tournament/round/start-time
  evidence rather than name-only matching.
- Surface is normalized to `hard`, `clay`, or `grass`; `indoor_hard` is retained
  only when the provider explicitly supplies indoor status. Unknown remains
  unknown and cannot default to hard.

### Immutable quote contract

Each normalized quote stores: canonical event/revision, provider event ID,
bookmaker, market, selection player/side, exact line, American/decimal price,
the paired opposite selection/price, bookmaker `last_update`, provider
availability time, our capture time, commence time at capture, region, source,
raw payload checksum, and parser version.

- Moneyline has a NULL line and a complete two-player price pair.
- A total proposition is comparable only when event, book, total line, and
  over/under pair match exactly.
- A spread proposition is comparable only when event, book, selected player,
  handicap, and opposite handicap/price pair match exactly.
- Consensus/reference probability is a derived research value. It is never
  presented as the executable sportsbook price.
- Opening is the first valid, complete, pre-start capture for the exact
  event/book/market/selection/line proposition.
- Current is the latest such capture available at the decision cutoff.
- Close is the final valid comparable capture whose bookmaker update and
  provider capture are both pre-start.
- A move at a different total or handicap is line movement, not price movement.
  Price movement is calculated only at the identical line.
- `source_available_at`/bookmaker update and `captured_at` are distinct. Backtest
  features use the later time as the conservative availability boundary.

Reject or quarantine incomplete pairs, invalid prices, post-start/in-play
captures, event/selection ambiguity, bookmaker timestamps after start, stale
quotes, and raw-checksum/parser conflicts. Never repair them by borrowing the
opposite side from another book or capture.

### Capture cadence and historical limits

- Use adaptive prospective capture: at least every 6 hours outside 24 hours,
  hourly from T-24h to T-6h, and every 15 minutes from T-6h to start when API
  quota permits. Record the expected and achieved cadence.
- Quota exhaustion, discovery failure, or insufficient captures is
  a visible data-health failure; it cannot be described as “no movement.”
- A tournament absent from The Odds API is `provider_not_covered`: skip it and
  show the source limitation without presenting it as a pipeline outage.
- Detailed per-book history begins in July 2026. Historical tennis-data prices
  are representative closing observations and must never be described as
  multi-year opening/current/closing sportsbook history.
- Legacy July 2026 per-book JSON is normalized with source
  `legacy_odds_history_reconstruction`. Moneyline and totals are retained only
  when both exact prices exist. Older spreads lack the away price and remain
  unavailable; they are never reconstructed from consensus or another book.

### Required constraints and tests before dependent work

- Unique provider alias `(provider, provider_event_id)` per event revision.
- Unique normalized quote identity including event revision, book, market,
  selection, line, bookmaker update time, capture time, and raw checksum.
- Paired quote checks for moneyline, total, and spread propositions.
- Idempotent raw ingestion and append-only correction/revision history.
- Contract fixtures cover at least five real ATP/WTA events, player-order
  reversal, reschedule, duplicate names, same-line price move, line move, stale
  quote, incomplete pair, withdrawal/retirement, and post-start rejection.
- Data-quality output is PASS/FAIL with partition counts, missingness,
  duplicates, identity queue, source age, capture cadence, and an exact remedy.

### Surface Elo v1 contract (frozen before population)

- Algorithm version is `tennis-surface-elo-v1`; ATP and WTA are independent.
  Every player begins at 1500 on 2023-01-01 or first appearance. No pre-2023
  result or rating may seed the run.
- Constant K=32 is retained from the legacy model for an auditable baseline.
  Overall and the applicable surface rating update after each eligible result.
  `indoor_hard` updates the hard bucket; it is not promoted as a separate rating
  until validation supports that split.
- Walkovers and retirements do not update Elo in v1. They still produce an
  immutable excluded event with the reason and unchanged before/after ratings.
- Annual sources are date-only. Matches sharing a player/date are evaluated from
  the same start-of-day ratings and their deltas are applied as a batch. This
  prevents an arbitrary file order from leaking one same-day result into another.
  Events are stamped `date_only_batch`; exact future start times may support a
  later, separately versioned chronological rule.
- Surface reliability is `n_surface / (n_surface + 20)`. The conservative surface
  rating used for the combined comparison is
  `overall + reliability * (raw_surface - overall)`. Raw overall, raw surface,
  and blended probabilities are all preserved; validation decides whether any
  surface variant advances.
- Reliability labels are `insufficient` for fewer than 5 prior surface matches,
  `developing` for 5-19, and `established` for 20+. Inactivity days are recorded
  but do not change ratings in v1.
- Each match/player event stores before, expected probability, delta, batch-after,
  sample counts, cutoff/stats-through timestamps, same-day ambiguity, K/prior/
  shrinkage configuration, source checksum, event checksum, run, and algorithm
  version. Rating events are append-only.
- The first preregistered evaluation (`tennis-surface-elo-eval-v1`, run 1) did
  not promote the surface adjustment. On the 2025 validation set, blended-minus-
  overall log loss was +0.000571 for ATP (n=2,616; 95% tournament-cluster CI
  -0.005077 to +0.006424) and +0.000730 for WTA (n=2,463; CI -0.004964 to
  +0.005381). Both intervals cross zero. The encouraging negative 2026 deltas
  are untouched final-test evidence and cannot be used to retune v1.
- Consumers must join the latest `complete` Elo run and its matching evaluation
  run. A populated rating is evidence, not automatic promotion: the API and UI
  must return the ATP/WTA promotion gates with ratings, and the decision policy
  must reject surface-only bets unless both required tour gates pass.
- `/api/tennis/surface-evidence` is the source-aware evidence contract. It returns
  current overall/raw-surface/blended ratings, surface delta, samples,
  reliability, inactivity, form/load, ATP serve/return coverage, explicit WTA
  missingness, algorithm/feature/source versions, promotion gates, and recent
  immutable rating history. The Tennis page displays the same payload and names
  the resulting action rather than hiding a failed gate.

Jira dependency order is `SCRUM-18` → `SCRUM-19` → `SCRUM-20` → `SCRUM-27` →
`SCRUM-21`/`SCRUM-22` → `SCRUM-23` → `SCRUM-24` → `SCRUM-25` → `SCRUM-26`.
Documentation, schema columns, or compilation alone do not advance an issue to
Done.

Dependency boundary: `SCRUM-20` owns canonical identity, immutable raw history,
event/quote revisions, provenance, partition coverage and fail-closed quality
gates. `SCRUM-27` owns populated pre-match feature/Elo snapshots and their
chronological leakage proof. `SCRUM-22` owns adaptive capture-cadence enforcement,
opening/current/close derivation and movement coverage alerts. These dependent
issues may not claim the empty snapshot/cadence state as verified functionality.
# Fantasy Football Draft Board

- The canonical richer-injury data contract, reconciliation rules, rollout phases, and acceptance criteria are defined in [`docs/NFL Injury.md`](docs/NFL%20Injury.md). Injury ingestion is deterministic and independent of the AI draft advisors; richer timeline fields remain shadow data until chronologically calibrated.
- The required player universe is the current-season nflverse weekly roster, not FantasyPros.
- Sleeper is a no-auth enrichment source for fantasy IDs, depth order, position metadata, and injury status; nflverse remains canonical for current roster membership so stale Sleeper records cannot enter the board.
- Independent projections use the three completed seasons immediately preceding the target season. They weight recent per-game production and regress toward position priors using the player's actual eligible historical-game sample plus four equivalent prior games. The displayed season baseline is always 17 active games; injury and availability estimates are stored separately and must not silently reduce that baseline. Bounded depth/role factors may still alter projected active-game opportunity.
- STD, HALF, and PPR are calculated independently. Draft order uses value over a 12-team replacement baseline. Rookies without NFL history use position and draft-capital priors with wider uncertainty.
- Bye weeks are derived from the target-season nflverse schedule by identifying each team's missing regular-season week; player metadata is not authoritative for bye weeks. A refresh fails closed unless all 32 teams resolve to exactly one bye.
- Current 12-team ADP comes from Fantasy Football Calculator's daily Standard, Half-PPR, and PPR JSON feeds. ADP is snapshotted with source hash and match coverage as comparison data only: it powers the displayed rank delta and buy/fade indicators but never changes projections, VOR, or our rank.
- DraftKings' own Best Ball ADP/draft-percentage (`ff_dk_bestball_adp`, distinct from the FFC ADP above) is a **manual, cookie-gated capture** — `GET /rankings/v1/draftgroups/{draftGroupId}/playerpool` requires an authenticated DK session behind Akamai bot detection, tied to a real-money account, so it is never stored in this repo/CI or automated (no `DNN_COOKIE`-style secret). `ingest/ff_dk_bestball_adp.py` ingests a manually-saved response body, matches by unique normalized display name only (the payload carries no position, so an ambiguous shared name is reported as unmatched rather than guessed), and stores a real wall-clock `captured_at` since there's no fixed cadence to floor to. Surfaced as a small "DK <adp>" annotation under the FFC ADP cell on both the rankings table and the Best Ball board, tooltipped with capture time so staleness is visible rather than implied live.
- The rankings table exposes prior-season games played and scoring-format fantasy points from `ff_player_season_features`; kicker points are derived as `FG made * 3 + PAT made` because nflverse's generic fantasy-point fields omit kicker scoring.
- Indicator chips use stable code-specific colors so target, touchdown, team-leader, injury, rookie, team-change, buy, and fade signals are visually distinct across rankings and draft-room surfaces.
- Each scoring board identifies the top three prior-season fantasy-point scorers within every position represented by populated history. `TOP_3_POSITION_POINTS` must use the board's selected STD/HALF/PPR formula, persist the exact rank and point total in evidence, and remain a factual signal rather than changing the forward projection.
- A `NEW TEAM: OLD → NEW` chip is derived by comparing the latest completed-season `prior_team` with the current nflverse roster team. Independent-model rows expose the current team as `team`, while legacy rows may use `team_abbrev`; indicator generation must support both contracts and prioritize this chip in the visible signal order.
- The rankings table provides client-side Name, Position, and Team filters over the full materialized board. Filtering changes visibility only and never changes stored rank, ADP, projections, or indicators.
- Every materialized independent projection persists a human-readable derivation contract: method, scoring format, season-level points/PPG/weights, weighted-history PPG, actual historical-game sample, position prior and equivalent prior games, regressed PPG, 17-game baseline, separate availability estimate, role factor, base/final points, and explicitly unmodeled context. The rankings row exposes this through an expandable `How projected` notation; displayed math must come from the persisted projection payload rather than being recomputed in the browser.
- **DST (team defense): ranks on prior season, projects shrunk — `ff-independent-v1.9` (2026-08-07).** Three separate decisions, each backed by `model/ff_dst_projection_backtest.py` (walk-forward, n=96 team-seasons, tuned on 2023–24 with 2025 held out).
- **Kickers: correct Yahoo distance-tiered scoring, heavier shrinkage, and NO carry-forward — `ff-independent-v1.10` (2026-08-07).** Investigated when asked to give kickers the same 2025-based treatment as DST. Kickers turned out to be the OPPOSITE case, and the request was declined on evidence with two real fixes shipped instead (`model/ff_kicker_projection_backtest.py`, walk-forward, n=61 kicker-seasons with >=8 games, tuned on 2023-24 and held out on 2025).
  **(a) The scoring formula was wrong.** The code scored every field goal as a flat 3 (`fg_made * 3 + pat_made`), but Yahoo's default is distance-tiered — verified live against Yahoo's own express-settings page: **0-39 = 3, 40-49 = 4, 50+ = 5, PAT = 1**. The flat formula undercounted kickers by **~15 points a season (max 34)** and, more importantly, **changed the relative rank of 22 of 42 kickers** (2 of the top 10) — so this affected board ordering, not just the level. `yahoo_kicker_points()` is now the single source of truth, fed by nflverse's per-distance buckets (`fg_made_0_19` … `fg_made_60_`), with the old flat formula retained only as a degraded fallback for rows written before the buckets shipped.
  **(b) Kicker history IS predictive — unlike DST.** Both history methods beat a flat constant by a wide margin (held-out MAE 22.1 and 23.1 vs **26.4** flat). This is the mirror image of the DST finding above, and the reason the two positions get different treatment.
  **(c) The 3-year weighted blend BEATS prior-season-only** (held-out MAE **22.1 vs 23.1**, pooled Spearman 0.18 vs 0.17). So kickers deliberately keep the standard multi-year regression and were NOT switched to the DST-style carry-forward. A test asserts older seasons still move a kicker's projection, so the DST treatment cannot be applied here by mistake.
  **(d) But shrinkage was far too weak.** Three full seasons against the default `REGRESSION_PRIOR_GAMES=4` leaves an effective carry-forward weight of **0.93**, while the tuning period fits **0.58**. New `POSITION_REGRESSION_PRIOR_GAMES = {"K": 37.0}` reproduces 0.58 at a 51-game sample while still shrinking a short-sample kicker harder; held out on 2025 this improves **MAE 23.2 -> 22.1**.
  **Resulting spread** is ~115-167 for 2026 — much wider than DST's ~102-108, and correctly so: kicker signal is real where defensive signal is not.
  **v1.11 override (2026-08-08) — user explicitly chose carry-forward anyway.** After (b)/(c) above were shown to the user as the reason kickers were NOT getting the DST treatment, the user's instruction was: "I want 2025 for both kickers and defense." DST already satisfied this (v1.9, no change needed). For kickers this is a **knowing, informed override** of the accuracy-maximizing choice, not a new finding — (b)/(c)'s numbers are unchanged and still say the 3-year blend is more accurate. `project_player()` now routes K through the same prior-season-carry-forward branch as DST (`method="k_prior_season_carry_forward"`), shrunk via a new `K_CARRY_FORWARD_WEIGHT = 0.54` — the backtest's "prior season only" row (held-out MAE 23.1, rank ρ 0.17), **not** the abandoned 3-year blend's 0.58 effective weight. Cost accepted: ~1 point of held-out MAE (22.1 → 23.1) versus the prior v1.10 approach. `POSITION_REGRESSION_PRIOR_GAMES["K"]=37.0` and the multi-year-regression path are now dead code for K (kept only as the backtest's comparison baseline — see `model/ff_kicker_projection_backtest.py`'s updated verdict). Tests were rewritten to assert the new behavior: `test_kicker_ranks_on_prior_season_only_not_a_multi_year_blend`, `test_kicker_projection_is_shrunk_hard_toward_the_league_prior`, `test_kicker_shrinkage_preserves_order_exactly`, `test_kicker_with_no_history_falls_back_to_the_flat_prior` (mirroring the DST suite) replace the old `test_kicker_projection_still_uses_multi_year_history_not_carry_forward`, whose premise is now the opposite of shipped behavior.
  **(a) History is real and is ingested.** `save_dst_history()` pulls `stats_team_reg_{season}.csv` (nflverse — season-total sacks/INTs/opponent-fumble-recoveries/safeties/defensive-and-return-TDs) and derives points-allowed by mapping each game's actual opponent score (from the `games.csv` schedule already fetched for bye weeks) through Yahoo's tier table, summed **per game** — points allowed is inherently per-game, not a season-total lookup. Scoring verified live against Yahoo's own express-settings default page: Sack +1, INT +2, Fumble Recovery +2, Safety +2, Defensive/Return TD +6, Points Allowed 0=+10 / 1–6=+7 / 7–13=+4 / 14–20=+1 / 21–27=0 / 28–34=−1 / 35+=−4. Deliberately **not modeled: blocked kicks** (+2) — nflverse's team-stats release only records kicks a team's OWN unit had blocked against it, not blocks its defense forced against an opponent (needs game-level opponent cross-referencing this season-aggregate dataset lacks); same "too rare to model, omit" judgment as DK soccer's CG/CGSO/NH scoring, flagged `source_row.blocked_kicks_modeled=false` rather than hidden. This populates the board's real prior-season FPTS column for defenses, previously blank.
  **(b) ORDER comes from the prior season alone.** Carry-forward is the best *rankable* signal tested — Spearman 0.18, top-12 hit rate 42% (vs 38% random) — beating the 3-year weighted blend (0.15). Weak, but real, and a flat constant ranks nothing at all.
  **(c) MAGNITUDE is shrunk hard toward the league prior** (`DST_CARRY_FORWARD_WEIGHT = 0.05`, fitted walk-forward). Shrinkage is a **monotonic transform, so it cannot change the draft order** — it only fixes calibration. Held out on 2025: shrunk **MAE 24.3**, beating both raw carry-forward (28.0) and the flat constant (24.8). This is strictly the best of the three, not a compromise.
  **Why the weight is so small:** every Yahoo DST scoring component is near-noise year-over-year — sacks r=0.20, INTs r=0.11, fumble recoveries r=0.06, defensive TDs r=**−0.03** — and the only component with real persistence, points allowed (r=0.31), is just ~8% of scoring under Yahoo's tiers. The resulting ~102–108 projected spread is narrow **by design**: these defenses genuinely are hard to tell apart, and a wide confident-looking spread would be fiction. The uncompressed prior-season actual is shown alongside so the evidence stays visible.
  **History (do not repeat):** v1.7 shipped a full 3-year history regression *without* backtesting it; it scored held-out MAE 26.1, worse than the flat 105.0 placeholder it replaced. `DST_CARRY_FORWARD_WEIGHT` is the intended tuning knob — raising it widens the displayed spread without changing draft order, trading calibration for visual separation. Re-run the backtest after any change, and do not re-ship an unshrunk or multi-year DST projection without a **new data source** (not prior-season box score) clearing the same held-out bar.
  **(a) History is real and is ingested.** `ingest/ff_independent.py::save_dst_history()` pulls `stats_team_reg_{season}.csv` (nflverse — season-total sacks/INTs/opponent-fumble-recoveries/safeties/defensive-and-return-TDs per team, confirmed via a live request) and derives points-allowed by mapping each game's actual opponent score (from the `games.csv` schedule already fetched for bye weeks) through Yahoo's tier table, summed **per game** — points allowed is inherently per-game, not a season-total lookup. Scoring verified live against Yahoo's own express-settings default page (not a secondary source): Sack +1, INT +2, Fumble Recovery +2, Safety +2, Defensive/Return TD +6, Points Allowed 0=+10 / 1–6=+7 / 7–13=+4 / 14–20=+1 / 21–27=0 / 28–34=−1 / 35+=−4. Deliberately **not modeled: blocked kicks** (+2) — nflverse's team-stats release only records kicks a team's OWN unit had blocked against it, not blocks its defense forced against an opponent (needs game-level opponent cross-referencing this season-aggregate dataset lacks); same "too rare to model, omit" judgment applied to DK soccer's CG/CGSO/NH scoring, flagged as `source_row.blocked_kicks_modeled=false` rather than hidden. This populates the board's real prior-season FPTS column for defenses, which was previously blank.
  **(b) The forward projection is flat, and that is an evidence-based decision, not a stub.** v1.7 briefly shipped a real 3-year history regression on top of (a). `model/ff_dst_projection_backtest.py` — which should have been run *before* shipping, not after — killed it: walk-forward on 2023/2024/2025 (n=96 team-seasons), tuned on 2023–24 and held out on 2025, the flat constant scored **MAE 24.8 vs the regression's 26.1**, and the best shrinkage the tuning period would accept was **λ=0.05** ("almost entirely ignore the history"). Root cause: every Yahoo DST scoring component is near-noise year-over-year — sacks r=0.20, INTs r=0.11, fumble recoveries r=0.06, defensive TDs r=**−0.03** — and the only component with real persistence, points allowed (r=0.31), is just ~8% of total scoring under Yahoo's tiers. A per-component reliability-weighted variant was also tested and also failed to beat flat (held-out MAE 24.5 vs 24.2). Pooled rank correlation of the history model against actuals was ρ=0.15, and *negative* (−0.10) in 2024 — no better than random ordering.
  **Standing rule:** do not re-ship a history-based DST projection without a **new data source** (not prior-season box score) that clears the same held-out bar in `model/ff_dst_projection_backtest.py`. `project_player()` pins this via `method="position_baseline_no_predictive_signal"`, and a test asserts a wildly-better defensive history does not move the number. The most plausible candidates for that new source have since been screened and REJECTED — see "DST feature screen" below and `model/ff_dst_feature_screen.py`; screen any future candidate on predictiveness, not self-stability.
  **Honest consequence:** all 32 defenses project identically, so our board cannot rank them. Prior-season actuals are shown so the user can judge; our model explicitly does not claim to know which defense will be better.

- **Board membership guarantee + curated RB handcuffs — `ff-independent-v1.13` (2026-08-16).** Investigated after the user compared a real Browns FantasyPros depth chart against the app and found 11 of 20 rostered players (all 4 QBs, 2 RBs, 2 WRs, 3 TEs) invisible everywhere — rankings table, Best Ball board, draft room — despite being in `ff_players`. Root cause: `create_ranking_set()` kept only the top `BOARD_SIZE=400` players leaguewide by our model's VOR, and every downstream surface only ever queries `ff_player_rankings`, so a player who missed that cut simply didn't exist there.
  **Fix:** `_must_include_ids()` force-includes, regardless of the VOR cut, each team's top `GUARANTEE_COUNT` players by `our_projected_points` at QB (2)/RB/WR/TE (1 each), inserted into `ff_player_rankings` alongside the top-400 board with their true (deep) rank intact — not fabricated. QB gets 2, not 1: Cleveland's own QB competition is genuinely disputed between sources (Sleeper's `depth_chart_order` has Watson QB1/Sanders QB2; the public ECR-based depth chart the user pasted has it reversed), and guaranteeing both sidesteps having to adjudicate which source is right rather than risk excluding the real starter either way. Verified live: Watson (QB30, rank 410) and Sanders (QB41, rank 812) both now render on `/fantasy-football/rankings`.
  **RB_HANDCUFFS.** The pre-existing `HANDCUFF_CANDIDATE` indicator (`create_indicators()` in `ingest/ff_fantasypros.py`) used a crude `role_rank==2` heuristic — the 2nd-highest-VOR RB on the roster by our own model, which is often a passing-down complement rather than the back who'd actually inherit bell-cow work on injury. Replaced with `RB_HANDCUFFS`, a curated team→handcuff map for all 32 teams sourced from the user-supplied real depth-chart/ECR/ADP table, every pairing cross-checked against the live 2026 roster before being hardcoded (one name variant caught: our DB's canonical "Kenneth Gainwell" vs the table's "Kenny"). The curated pick takes priority in `create_indicators()`; a team missing from the map, or whose named handcuff isn't found on the roster (traded/cut), falls back to the old `role_rank==2` heuristic for that team only — never silently drops the tag. Force-included on the board via the same `_must_include_ids()` mechanism. Verified live: all 32 teams tag their correct curated handcuff (`ff_player_indicators.evidence->>'basis' = 'curated depth chart (starter/handcuff pair)'`); Cleveland's Dylan Sampson and Washington's Jacory Croskey-Merritt confirmed rendering the badge on `/fantasy-football/rankings`.
  **Bonus bug found and fixed in the same pass:** `TEAM_ABBREV_OVERRIDES` (`ingest/ff_independent.py`) was missing `"AZ": "ARI"` and `"JAC": "JAX"`. nflverse's weekly-roster feed codes Arizona as `"AZ"`, not `nfl_teams`' canonical `"ARI"` — every one of the 29 Cardinals offensive players silently got `nfl_team_id=NULL` **and `bye_week=NULL`** (bye weeks are keyed by the same normalized abbreviation), invisible until this fix. One legacy `"JAC"`-tagged orphan row (a duplicate Trevor Lawrence with no sleeper_id/gsis_id and zero downstream references, superseded by a correct `"JAX"` row) was left alone as harmless dead data rather than risk a cross-table FK repair out of scope for this fix. Live rows repaired via a fresh `python -m ingest.ff_independent` run (not a manual UPDATE) — same run that applied the board/handcuff fix above; verified all 30 Arizona players now carry `nfl_team_id` and `bye_week`.

- **Skill-position history constants fitted — `ff-independent-v1.14` (2026-08-23).** `SEASON_WEIGHTS` (0.05/0.20/0.75) and `REGRESSION_PRIOR_GAMES` (4.0) had been set by judgment and never fitted — the same situation `model/ff_kicker_projection_backtest.py` found for kickers, where the default shrinkage turned out far too weak. `model/ff_skill_constant_screen.py` grades them walk-forward: tune on 2022–24 targets, grade **once** on a held-out 2025, PPR points per game, with `role_factor` pinned to 1.0 (it depends on the CURRENT depth chart, which is not reconstructible for past seasons) and a `MIN_TARGET_GAMES=6` floor so a two-game actual cannot masquerade as a per-game rate.

  | pos | fitted weights | prior games | held-out MAE | 95% CI on delta | shipped |
  |---|---|---|---|---|---|
  | RB | 0.15/0.25/0.60 | 2 | 2.900 → 2.762 | [−0.261, −0.014] | yes |
  | WR | *unchanged* | 2 | 2.619 → 2.539 | [−0.156, −0.010] | yes |
  | TE | 0.15/0.25/0.60 | 2 | 2.039 → 1.947 | [−0.173, −0.006] | yes |
  | QB | 0.20/0.30/0.50 | 12 | 3.095 → 2.826 | **[−0.637, +0.105]** | **no** |

  **QB is deliberately NOT shipped, and it had the largest apparent gain of any position.** n=36 held-out quarterbacks against a 90-point grid, and the bootstrap CI spans zero. Directionally it suggests QBs want more history and heavier shrinkage (plausible — QB is the most stable position), but "plausible and not significant" is precisely how v1.7's DST regression shipped. Re-run the screen once more seasons exist; do not ship it on the point estimate. `test_history_weights_and_shrinkage_are_per_position` asserts QB stays on the global default so this cannot be undone by accident.
  **Direction of the fitted shrinkage is worth noting:** RB/WR/TE all fit *less* shrinkage than the 4.0 default (2.0), not more — a skill player with real snap history is a better guide to himself than the position average is. That is the opposite of the kicker result, where the fitted shrinkage was far *heavier* than the default. `POSITION_REGRESSION_PRIOR_GAMES` was previously dead code (only `K`, and `K` had moved off that branch in v1.11); RB/WR/TE now make it live again.

- **DST feature screen — stability and predictiveness are orthogonal (2026-08-22).** Prompted by a real request: a Pro-Football-Reference team-defense dump (pressure rates, drive efficiency, red-zone rates) raised "can we rank defenses with this?". Most of those columns are the same components `model/ff_dst_projection_backtest.py` already rejected, in a different presentation — re-deriving sacks from PFR instead of nflverse does not make them more persistent. But nflverse's team-stats release, which the refresh **already downloads every run**, carries four fields never tested at all: `def_tackles_for_loss`, `def_qb_hits`, `def_pass_defended`, `def_fumbles_forced`. No new source required, so they were screened in `model/ff_dst_feature_screen.py`.

  **The finding is counterintuitive and is the reusable part.** Tackles for loss is by far the most *repeatable* defensive trait measured (r=0.409 year-over-year, twice sacks at 0.194) and is very nearly **useless** for predicting next-season DST fantasy points (r=−0.019). Structural reason: Yahoo does not score TFL, and TFL does not convert into what Yahoo does score. The components Yahoo pays most for — turnovers and defensive touchdowns — are the least repeatable things a defense does (`def_tds` self-stability is −0.119).
  **Standing rule this adds:** screen future DST candidates on **correlation with next-season fantasy points**, never on self-stability, or the search keeps surfacing TFL-shaped dead ends.
  Adding the untested signals to a model made it **worse**, not merely no better: held-out MAE 23.0 → 23.9 and Spearman 0.249 → 0.167. The shipped v1.9 carry-forward stands unchanged.
  **One side observation deliberately not acted on:** given enough training seasons, a ridge on the *incumbent* features alone beat v1.9's held-out MAE (23.0 vs 24.3) — while being **worse** on rank quality (Spearman 0.249 vs 0.258, top-12 50% vs 58%). Since DST magnitude is shrunk almost to a constant and ORDER is what a draft board consumes, that is not an improvement where it counts; it is also one held-out season at n=32 with alpha tuned on training error. It needs its own pre-registered study, not a drive-by promotion.
  **Also corrected here:** the board *can* rank defenses and always could. Shrinkage compresses all 32 into a ~102–108 band that never trips `rank_rows()`' 0.88 tier breakpoint, so every defense returns tier 1 and the board *looks* unranked. The underlying order (2025 actuals carried forward) is real, and agrees with FantasyPros at **Spearman 0.80**; the disagreements are structured — FantasyPros ranks BAL/KC/GB/DAL/SF higher (reputation), we rank CLE/ATL/CHI/MIA/CAR higher (2025 production). Which is more accurate is **not testable today**: only 2026 FantasyPros snapshots exist, with no historical DST ECR archive.

- **Strength of schedule — season-long rejected, weeks 15–17 shipped as an indicator (2026-08-23).** Measured in `model/ff_playoff_sos_screen.py`; the production map lives in `ingest/ff_playoff_sos.py`. Two *independent* effects kill season-long SOS:
  1. **Defenses barely carry over** — year-over-year r is 0.175 (pass yards allowed), 0.231 (rush yards), and **0.028 for passing touchdowns**, the metric most directly tied to QB/WR/TE scoring.
  2. **Seventeen opponents average away most of what remains** — individual defenses span 35–42% (pass) and 52–83% (rush) between best and worst, but a full slate compresses to a 12–14% spread between the most extreme *teams*.

  Spread × carryover leaves a season-long adjustment worth ~**1.2–1.4% to a typical player** — roughly three points on a 250-point receiver, against projection error an order of magnitude larger. **No season-long SOS term exists in the model, and adding one would be the v1.7 mistake again**: a confident-looking number that cannot move accuracy.
  **Weeks 15–17 survive on a structural argument, not a hopeful one.** Three opponents average out far less than seventeen, so the same unstable ratings give a 27–32% spread and ~2.3–3.6% effective signal — 2–3x season-long, on exactly the weeks DraftKings Best Ball scores as separate tournament rounds. It ships as an **indicator** (`PLAYOFF_SOS_SOFT` / `PLAYOFF_SOS_TOUGH`, top/bottom quartile only, `indicator_class='fact'`), never folded into a projection, and renders **only on the Best Ball cheat sheet** (`PLAYOFF_SLATE_VARIANTS`) where those weeks are actually scored separately. Position-appropriate: QB/WR/TE against pass defense, RB against rush defense. Computed once per refresh and reused across all three scoring boards, and **fails soft** — a missing nflverse team-week file must not take down a whole board refresh for a tie-breaker signal.
  **Operational trap found while shipping this, worth knowing:** `create_ranking_set()` returns early (`if existing and player_count >= 100`) when a populated set exists for the same `source_snapshot_id`. Since that id derives from source digests, re-running the refresh after changing only *code* reuses the existing board and never calls `create_indicators()` at all — exit 0, same ranking-set ids printed, looks like it worked. To apply an indicator/projection code change, delete the `ff_player_indicators` and `ff_player_rankings` rows for those sets first, then re-run — and verify the re-run succeeded, because the board is empty in between and `getLatestRankingSet` orders by `created_at DESC` with no player-count filter, so an empty orphan set can win.

- **Printable cheat sheets (2026-08-22).** `/fantasy-football/rankings/print`, `/fantasy-football/best-ball/print`, `/fantasy-football/redraft/print` — position-tiered columns on one landscape page, printed from the browser (no file is generated server-side, so a sheet always reflects the current snapshot). All three read the same ranking snapshot through `getFantasyRankings`; the variant changes which positions print and how deep, never the projections underneath. Depths are sized against each format's real draft: Best Ball 12x20 = 240 picks QB/RB/WR/TE only (K and DST are not draftable there), Redraft 10x15 = 150 with K and DEF, which Yahoo's UI calls "DEF" and the sheet matches. A position deeper than one column spills into continuation columns with position rank running unbroken across the split. **DST prints no tier column** — all 32 return tier 1 for the shrinkage reason above, and printing "T1" 32 times reads as "unranked" when the order is real; it shows rank, the 2025 actual it derives from, and a FantasyPros delta instead.

## DraftKings NFL Best Ball

- `/fantasy-football/best-ball` is a separate DraftKings season-long Best Ball workspace. It must never reuse redraft roster requirements: the eligible pool is QB/RB/WR/TE only; rosters contain 20 players; weekly starters are 1 QB, 2 RB, 3 WR, 1 TE and 1 RB/WR/TE FLEX; the remaining 12 are automatically benched.
- A valid roster must contain at least 1 QB, 2 RB, 3 WR and 1 TE, no more than 5 QBs or 5 TEs, and players from at least two NFL teams. The 3 QB / 6 RB / 8 WR / 3 TE display is the DraftKings default auto-draft guardrail and a planning target, not a manual-draft validity rule.
- Tournament round labels are Weeks 1–14, Week 15, Week 16 and Week 17. The page also surfaces the requirement to edit pre-draft rankings, add a queued player, or make a manual pick.
- The Best Ball draft simulator persists locally per ranking-set snapshot and does not mutate draft sessions or materialized rankings. It fixes the room at 12 user-controlled teams and 20 snake rounds, lets the user designate their own draft slot, assigns each Add to the team currently on the clock, advances automatically, supports last-pick undo/reset, removes drafted players from the board, and validates official roster gates separately for all 12 teams.
- **CPU auto-draft opponents (2026-08-19).** Both `/fantasy-football/best-ball` and `/fantasy-football/redraft` gained an "Opponents: CPU auto-draft" toggle (off by default, so existing self-play behavior is unchanged). This is distinct from the persisted, database-backed "Draft Lab" simulator (`/fantasy-football/draft/new` → Mock vs CPU, `web/src/lib/fantasy-football/auto-draft.ts` + `actions.ts`) that already existed — these two local rooms never touch the database, so they reuse the same pure `selectComputerPick()` decision engine through a new client-side adapter, `web/src/lib/fantasy-football/local-auto-draft.ts`, rather than the persisted flow. `computeCpuDraftBatch()` computes every consecutive CPU pick in one pure pass (stopping the instant the user's team is on the clock or the draft completes) so the board is applied atomically, never mid-batch — mirroring the persisted simulator's own atomic-advancement property. The CPU seed is derived deterministically (`localAutoDraftSeed()`: storage key + ranking-set id + user slot) rather than stored, since these rooms only ever persisted `{userSlot, playerIds}` before now (`cpuEnabled: boolean` is the only new persisted field, parsed with a `false` fallback for old localStorage payloads). Each room maps its own roster shape onto the engine's slot-based `RosterConfig`: Redraft's is a direct, derived translation of `REDRAFT_STARTER_SLOTS`/`REDRAFT_FLEX_SLOTS`/`REDRAFT_BENCH_SLOTS` (`REDRAFT_AUTO_DRAFT_ROSTER_CONFIG`); Best Ball has no fixed lineup, so `BEST_BALL_AUTO_DRAFT_ROSTER_CONFIG` treats DraftKings' own auto-draft guardrail targets (`BEST_BALL_TARGETS`, QB3/RB6/WR8/TE3) as hard "required" slots with FLEX/K/DST zeroed out — since those targets sum to exactly 20 with no bench slack, every CPU Best Ball roster ends up matching the target composition exactly, not just DK's looser real minimums (documented, not hidden, in a test assertion). "Undo" in CPU mode is grouped, not single-pop: because CPU picks are deterministic, popping just the last (CPU) pick would have the effect instantly re-draft the identical player, so undo instead rolls back to immediately before the user's own most recent pick, taking every later CPU pick with it — same grouped-undo behavior the persisted simulator's design already called for. Manual "Add"/"Draft" buttons are disabled for every player whenever CPU mode is on and it isn't the user's turn. Verified via a new pure test suite (`test:local-auto-draft`, mirroring `test:auto-draft`'s style): full all-CPU drafts for both rooms are unique with every roster feasible/exactly-composed, a batch stops exactly on the user's turn and resumes correctly after a simulated manual pick, and CPU output is deterministic for identical inputs.
- Player Selection and Draft Results are two views of the same ordered local draft state. The results grid is derived at render time from the canonical snake-slot sequence; it is never stored separately. It displays 12 team columns, 20 round rows, overall pick numbers, position colors, team/bye context, a persistent My Team marker, and the current on-the-clock cell.
- Best Ball interaction performance is a contract: player/rank lookup is indexed, drafted membership is set-based, roster validity is calculated once per affected team, and the available-player board is viewport-virtualized rather than mounting all 260 rows. Projection explanations are constructed only when opened. Draft actions update in-memory React state immediately, while local browser persistence is deferred to idle time with cross-tab storage synchronization.
- Until a dedicated Best Ball model is chronologically validated, the player board shows the top 260 QB/RB/WR/TE players (a 240-pick 12-team room plus buffer), labels its ranking basis as the existing PPR projection plus ADP, and explicitly discloses that DraftKings yardage bonuses, weekly spike distributions, correlations and Weeks 15–17 opponent context are not included.
- FantasyPros projections are source-attributable comparison data only. They may expose disagreement and support calibration audits, but they must never be blended into, replace, or suppress the independent board; partial/sample coverage must be labeled explicitly.
- The scheduled `Refresh Fantasy Football Draft Data` workflow runs `python -m ingest.ff_independent`; source snapshots and ranking sets record versioned `ff-independent-v1.x` provenance.
- V2 is an approved challenger architecture, not a performance claim. It models running-back output through reconciled team carries/RB targets/team touchdowns, player role allocation, contextual efficiency, separate touchdown share, and weekly uncertainty. It must never exceed team opportunity ceilings.
- The Best Ball UI must separate model status visibly: `V1.5 — live` identifies the displayed 17-active-game baseline, while `V2 challenger — not active` describes the future calculation flow without implying it generates current points. V1.5 weights completed-season PPG at 5% for 2023, 20% for 2024, and 75% for 2025, normalizing across only the seasons a player actually played; it then applies sample-aware regression and the bounded depth-chart role factor across 17 active games. True rookies bypass veteran history and use a position-and-NFL-draft-capital point prior multiplied by current depth order, with 38% confidence and a 62%–142% range. The rookie prior does not yet use college production, age, athletic testing, landing-spot team opportunity, coaching, offensive line, or rookie ADP as projection inputs. FantasyPros remains comparison-only and ADP remains draft-timing context.
- Execute V2 in this dependency order: immutable 2020–2025 as-of history and reproducible V1.5/ADP baselines → chronological validation harness → team opportunity → player allocation → efficiency/TD share → weekly distributions/correlations → final validation gate → decision/backend/UI delivery and visual verification.
- No fixed vacated-opportunity percentage, automatic workload penalty, pair-specific correlation, player/league blend, team adjustment, or shrinkage constant may be presented as calibrated unless it is trained and passes chronological validation. Relationship correlations use type-specific priors with hierarchical shrinkage and obsolete teammate history is reset after team changes.
- FantasyPros historical `type=ADP` PPR responses are the preferred market baseline when their provider timestamp is eligible for the declared simulated draft cutoff. Preserve provider timestamp, capture time, response hash, scoring, season, row count, and player identity coverage. A dated ECR archive is a separate rank baseline and must never be silently relabeled ADP.
- The historical ADP decision cutoff is the first scheduled regular-season kickoff from the nflverse schedule. Real endpoint audits verify eligible PPR ADP coverage for every 2020–2025 season. Endpoint access and cutoff eligibility are separate: a late-mutated ranking response may remain available but is ineligible for an earlier simulated draft.
- V2 promotion evidence must include season-total MAE/bias, weekly CRPS or another preregistered proper scoring rule, P10/P50/P90 coverage, Top-12 precision/recall, spike-week recall, simulated Best Ball counted points, draft-decision regret, and paired bootstrap confidence intervals. Overall season-total bias must be within ±5 points. The final result is `PROMOTE`, `REVISE`, or `RETAIN V1.5`; any FAIL/BLOCKED gate keeps V2 in shadow mode.
- The Best Ball draft room exposes two user-triggered, independent LLM advisors: OpenAI `gpt-5.6-luna` through the Responses API with Structured Outputs and DeepSeek `deepseek-v4-flash` through Chat Completions JSON mode. Credentials are server-only (`OPENAI_API_KEY`, with `OPENAI_API` accepted as a backward-compatible alias, and `DEEPSEEK_API_KEY`); provider output never changes the draft unless the user explicitly presses Add.
- Both advisors receive the same server-reconstructed, request-hashed evidence contract: DraftKings rules/scoring, all ordered picks and roster ownership, the user's slot/next pick/roster/bye profile, and the top legal remaining candidates with V1.5, FantasyPros, ADP, prior-season, confidence, and signal fields. Client-supplied projections are never trusted, providers never see each other's output, and stale responses are discarded after any draft change. Provider-visible payloads omit internal player IDs so the intended selection identifiers are opaque candidate keys (for example, `C01`), which the server maps back to authoritative player IDs. Because DeepSeek JSON mode does not reliably preserve field names, the validator recursively collects only exact live-candidate keys/names, prioritizes winner/primary selection semantics, deduplicates alternatives, and separately finds explanation/strategy/risk fields through bounded nesting. It also normalizes `C1`, snake_case, nested wrappers, `0–1` versus `0–100` confidence, and scalar risk text. When loose output omits structured evidence or alternatives, the UI substitutes server-derived V1.5 projection, ADP, and rank facts and the server's next two legal board candidates, while explicitly labeling omitted confidence, roster fit, risk, or strategy. The primary reference must still map to the current legal candidate set; otherwise one bounded correction retry runs against the same frozen board and then fails closed.
- Advisor availability is checked on the server before rendering. A provider without its deployment credential is shown as `Connection needed` with a disabled action; API credentials remain server-only (`OPENAI_API_KEY`, `DEEPSEEK_API_KEY`) and must be configured in Vercel Production and Preview before redeployment.
- LLM recommendations identify `ff-independent-v1.5` as their active projection evidence until V2 passes its promotion gate. They may interpret the supplied evidence but must not invent absent injury, air-yard, role, schedule, correlation, or matchup data. Provider authentication, billing, rate-limit, timeout, malformed-output, and illegal-player failures remain isolated and visible on the affected provider card.
- Jira dependency chain: `SCRUM-34` source contracts → `SCRUM-35` immutable historical population → `SCRUM-43` chronological validation harness/baselines → `SCRUM-36` V2 model → simulation/decision/backend/UI issues → `SCRUM-41` final champion-versus-challenger gate.
