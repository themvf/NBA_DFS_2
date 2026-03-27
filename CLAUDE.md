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

### DK Scoring
```
PTS × 1.0 | REB × 1.25 | AST × 1.5 | STL × 2.0 | BLK × 2.0 | TOV × -0.5
3PM × 0.5 (bonus) | DD × 1.5 (bonus)
```

### Key Design Decisions
- **Implied team total** (not raw O/U ÷ 2): derive each team's expected points from moneylines using `compute_team_implied_total()`. A -180 home favorite in a 230 O/U game gets ~118 implied, not 115.
- **Usage rate** scales the pace/environment benefit. Stars (30%+ usage) capture more extra possessions in high-pace games. Capped at 0.5×–2.0×.
- **No blowout curve**: NBA teams rarely blow out enough to affect starter minutes — do not apply blowout adjustments.
- **`avg_minutes` directly from the API** — do not derive from min_pct × 48.
- **Assists** get partial pace adjustment (50% of `combined_env`) in addition to defensive factor.
- **DD rate** scaled by `adjusted_env` — more possessions = more double-double chances.
- **Leverage = edge × contrarian × ceiling**, where `edge = our_proj − field_proj`. `field_proj` priority: `avg_fpts_dk` (DK's salary-page projection, which drives most contest ownership) → `linestar_proj` → fallback to raw `our_proj`. Negative leverage = below-field on this player → correct GPP fade. The optimizer filters `leverage > 0` for GPP mode, so negative-leverage players are excluded from GPP pools.

### League Average Constants (2025-26)
```python
LEAGUE_AVG_PACE       = 100.0
LEAGUE_AVG_DEF_RTG    = 112.0
LEAGUE_AVG_TOTAL      = 228.0
LEAGUE_AVG_TEAM_TOTAL = 114.0
LEAGUE_AVG_USAGE      = 20.0
```

## GitHub Actions

### `daily_stats.yml` — runs at 12:10 UTC (7:10 AM ET) daily
- Offset from :00 to avoid stats.nba.com thundering herd
- Steps: seed teams → fetch player/team stats → fetch schedule + odds
- Schedule step uses `if: always()` so odds fetch even if stats step fails

### `load_slate.yml` — manual `workflow_dispatch`
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
