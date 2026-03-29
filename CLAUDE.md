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
paceFactor  = avg(teamPace, oppPace) / LEAGUE_AVG_PACE
totalFactor = teamImpliedTotal(vegasTotal, homeMl, awayMl, isHome) / LEAGUE_AVG_TEAM_TOTAL
combinedEnv = paceFactor × 0.4 + totalFactor × 0.6
defFactor   = oppDefRtg / LEAGUE_AVG_DEF_RTG
usageFactor = clamp(playerUsage / LEAGUE_AVG_USAGE, 0.5, 2.0)
adjustedEnv = 1 + (combinedEnv − 1) × usageFactor
```

**Stage 2 — Per-stat projections**
Props (pts/reb/ast) are used directly when available — they already bake in matchup,
pace, and injury context. Rolling-average formula is the fallback.
```
pts = propPts  ?? (ppg  × defFactor)
reb = propReb  ?? (rpg  × adjustedEnv)
ast = propAst  ?? (apg  × defFactor × (1 + (combinedEnv−1) × 0.5))
stl = spg  × adjustedEnv            ← always formula (props rarely available)
blk = bpg  × adjustedEnv
tov = tovpg × adjustedEnv
3pm = threefgmPg                     ← no adjustment (shot selection, not pace)
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
LEAGUE_AVG_PACE       = 100.0
LEAGUE_AVG_DEF_RTG    = 112.0
LEAGUE_AVG_TOTAL      = 228.0
LEAGUE_AVG_TEAM_TOTAL = 114.0
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

### Phase 1 — `/analytics` Route (Cross-Slate Calibration)

**Goal:** Surface model calibration trends across all historical slates in the web UI.

**New file:** `web/src/app/analytics/page.tsx` (Server Component)
**New file:** `web/src/app/analytics/analytics-client.tsx` (Client Component)
**Modified:** `web/src/db/queries.ts` — add 4 new query functions

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

| Priority | Phase | Impact | Effort |
|----------|-------|--------|--------|
| 1 | Phase 2 — LineStar soft dependency | Prevents daily slate failures | Low |
| 2 | Phase 3 — Results web upload | Removes manual step after every slate | Medium |
| 3 | Phase 1 — `/analytics` route | Core model calibration visibility | High |
| 4 | Phase 4 — Cash line calibration | Accuracy of strategy leaderboard | Low |

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
