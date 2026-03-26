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
