# NHL Line Terminal

**Started:** 2026-09-29, the 2026-27 regular-season opener (5 games).
**Route:** `/nhl`. **Model:** the CFB line terminal ([spec](cfb-live-data-terminal-spec.md)):
a canonical schedule plus an append-only exact-book quote tape. It observes the
market; it makes no edge claim.

## Data flow

```text
NHL API (free) ──> nhl_teams / nhl_matchups  (identity, start, status, finals)
                          │
Odds API /events (free) ──┤  map provider event -> canonical game
                          │
Odds API /odds (paid) ────┴─> game_odds_history (sport='nhl', books JSONB)
                                   │
                                   ├─> odds_capture_checkpoints (nhl-dense-v1)
                                   └─> event_closing_lines (scheduled_nhl boundary)
```

| Concern | Source | Module |
|---|---|---|
| Schedule, start time, status, final score | `api-web.nhle.com/v1/schedule/{date}` (no key) | `ingest/nhl_schedule.py` |
| Provider-event mapping | Odds API `/v4/sports/icehockey_nhl/events` (free) | `ingest/nhl_schedule.py` |
| Moneyline, puck line, total | Odds API `/odds`, 6 named books × `h2h,spreads,totals` = 3 credits | `ingest/nhl_schedule.fetch_odds` |
| Checkpoint captures + closes | shared worker | `ingest/event_closing_lines.py` |

## Decisions and why

- **The NHL game id is identity.** Odds API event ids are mappings, never keys.
  A reschedule moves the existing row; its old checkpoints are marked `missed`
  (superseded), as are those of a game whose `schedule_state` leaves `OK`.
- **Mapping is exact, not fuzzy.** Full names (`placeName + commonName`) are
  normalized (NFKD, alphanumerics only), so `Montréal Canadiens` and
  `St Louis Blues` match without an alias table. Verified 2026-09-29: all 32
  teams and 33/33 listed events mapped uniquely. A name two teams share, an
  unknown team, or no unique game within ±6h is quarantined in
  `nhl_unmapped_events`, never guessed.
- **Start-time tolerance.** The Odds API lists puck drop ~10 minutes after the
  NHL's scheduled start (21:10 vs 21:00). Captures must precede **both**, so
  the pregame tape and the close use the earlier, scheduled boundary.
- **Settlement follows the books.** The NHL API's final credits a shootout win
  as one goal (3-2 SO), which is how books settle totals and puck lines;
  `last_period_type` (REG/OT/SO) is kept beside the score.
- **Preseason is excluded** (`gameType` 1): no Odds API market to map to.
- **One paid call records every mapped game ≤72h out**, not only the due ones.
  `/odds` is billed per market, not per event, so the extra tape is free.

## Capture cadence — `nhl-dense-v1`

Hockey lines move on starting-goalie news: around morning skate and again at
confirmation near warmups. Windows (minutes before scheduled start):

| Checkpoint | Window |
|---|---|
| `t_minus_24h` | 1440–1200 |
| `t_minus_6h` | 360–330 |
| `nhl_t_minus_180m` | 180–140 |
| `nhl_t_minus_120m` | 120–100 |
| `t_minus_90m` | 90–60 |
| `nhl_t_minus_45m` | 45–35 |
| `t_minus_30m` | 30–20 |
| `t_minus_15m` | 15–5 |
| `closing_candidate` | 5–0 |

No gap exceeds 20 minutes in the final three hours (tested). The same windows
are mirrored in the Vercel dispatcher (`/api/cron/event-closing-lines`), which
is the reliable clock; a parity test fails if they drift.

**Cost.** A simulation of the `*/5` worker against the real October schedule
gives ~86 credits/day (36–180), ~2,600 per 30 days, about 2.6% of the 100k
plan and far inside the 2,000/day shared close-capture cap. It is a
simulation; replace it with `odds_api_usage WHERE sport='nhl'` once measured.

## Operations

- `refresh_nhl_terminal.yml`: full 14-day schedule every 6h; recent finals and
  event mapping hourly; `--health` exits non-zero only on integrity failures
  (post-start capture, final without a score). Coverage gaps are `warn`.
- `capture_event_closes.yml` runs `python -m ingest.nhl_schedule --ensure-schema`
  before capturing: it runs with `--existing-schema` but seeds from
  `nhl_matchups`, so a deploy cannot leave it querying a missing table. Once
  applied the bootstrap is one catalog read.
- The web mirror of the `event_closing_lines` sport CHECK
  (`web/src/db/ensure-schema.ts`) is dropped and re-added on every cold start.
  It must list every sport in `CLOSE_CAPTURE_SPORTS`; a test enforces it.

## Not built yet

- **Movement detectors.** `model/line_alerts.py` thresholds are football-point
  based (spread 1.0, total 1.5, key numbers 3/7/10/14). The puck line is fixed at
  ±1.5 and totals sit at 5.5–6.5, so hockey moves are price-first. NHL
  detectors need hockey-specific, pre-registered thresholds before they are
  enabled; until then the page shows descriptive open-to-latest movement only.
- Starting-goalie confirmations, injuries, and back-to-back context.
