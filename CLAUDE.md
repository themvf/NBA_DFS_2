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
| **Doubleheaders** | Two distinct gamePks — both appear in slate; UNIQUE on game_id handles it |
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
