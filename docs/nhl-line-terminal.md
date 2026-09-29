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

## The page (`/nhl`)

Same layout and CSS as `/cfb` (`web/src/app/nhl/`, logic in
`web/src/lib/nhl-market.ts`, tested by `web/scripts/test-nhl-market.ts`):
market watch with sparklines, instrument chart per book, exact book ladder,
market quality, paper blotter, data pulse, checkpoint health. Hockey
differences:

- **Markets are moneyline, puck line, total.** Football terminals chart the
  line; hockey lines barely move (puck line ±1.5, totals 5.5–6.5), so the
  default chart is each book's **vig-free price at the consensus line**. Books
  on a different line gap rather than blend, and a `LINE` toggle shows the
  line path. When the consensus line itself moves, the move is reported as a
  line move, never as a price change across two different bets.
- **FAIR** column: vig removed from that book's own two-sided pair.
- **Largest moves** strip replaces CFB's movement intelligence: open-to-latest
  moneyline change on a stable book cohort. Descriptive only; no detector.
- **Game context:** rest days / back-to-backs derived from the schedule
  (preseason not loaded, so openers read "no prior game"); finals with REG/OT/SO.
- **Status** follows the checkpoint cadence: nothing is owed before T-24h
  closes, and freshness bounds are the worst case per band plus slack.

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

## Moneyline signals (enabled 2026-09-29)

`model/line_alerts.py --sport nhl` runs the four generic moneyline detectors,
**thresholds unchanged** from MLB/tennis/soccer: `pinnacle_divergence` (≥2pp),
`dk_value` (DK EV ≥2%), `steam` (≥3 books move ≥1.5pp between consecutive
captures) and `walking` (≥2pp drift since the first capture). Nothing was fitted
to NHL data. It runs as the last step of `capture_event_closes.yml` and in the
hourly NHL refresh.

- **Grading:** CLV against the verified frozen close only (NHL joins the
  verified_clv_v1 cohort, like CFB/NFL: no close, no grade). Outcomes come from
  `nhl_matchups` only when `completed`; the schedule refresh no longer stores
  running scores for live games. Units are recorded at the price frozen at
  trigger (`exec_decimal`), as CFB does.
- **Steam spans vary.** NHL checkpoints run from 18h apart down to 10m, so an
  NHL `steam` can compare captures hours apart. `interval_minutes` is stamped
  on every NHL steam alert; slice on it before reading steam results.
- `pinnacle_polymarket_delta` is not registered: NHL captures carry no
  Polymarket book. The Python and web detector registries are parity-tested
  for NHL.

## Deferred (decided 2026-09-29: ship on Vegas odds only)

The launch deliberately uses only the odds pipeline above. Everything below is
kept for later.

- **Puck-line and total detectors.** The football detectors' thresholds are in
  points (spread 1.0, total 1.5, key numbers 3/7/10/14). The puck line is fixed at
  ±1.5 and totals sit at 5.5–6.5, so hockey moves are price-first. These need
  hockey-specific, pre-registered thresholds before they are enabled.
- **Starting goalies, injuries, history, a second market.** Candidate sources,
  each probed live on 2026-09-29 so this does not have to be redone:

| Need | Source | Verified 2026-09-29 | Catch |
|---|---|---|---|
| Injuries | FantasyPros `nhl/injuries` (our key) | 621 rows, all 32 teams; status, injury type, note, update date; `sport: NHL` | Licensed, so it satisfies the provenance rule. **Preferred.** |
| Injuries (alt) | ESPN `site.api.espn.com/.../hockey/nhl/injuries` | 111 rows, 31 teams, dated | Undocumented, no license. Not preferred. |
| Starting goalie, live | DailyFaceoff `/starting-goalies` page | Confirmed/unconfirmed status per game | Web scraping only; robots.txt allows the page, disallows `/api/`; terms not verified. |
| Starting goalie, history | NHL API `gamecenter/{id}/boxscore` | `starter: true` per goalie, back to at least 2015 | History only; nothing pregame. The pregame `landing` lists each team's goalies with season stats, not the starter. |
| Results / team stats history | NHL API (`club-schedule-season`, `api.nhle.com/stats/rest`, play-by-play) | Full seasons back to at least 2015; team stats to 2010-11 | Unofficial, undocumented; [Zmalski/NHL-API-Reference](https://github.com/Zmalski/NHL-API-Reference) (MIT) documents it. |
| Advanced stats (xG, goalie quality) | MoneyPuck CSVs (teams, goalies, shot-level) | 2025 files download | Free for **non-commercial** use only; credit MoneyPuck wherever shown. |
| Historical odds | Sportsbook Reviews Online archive | Open/close ML, puck line, total per game | Stops at 2022-23; reference lines, not verified closes. |
| Second live market | Polymarket Gamma (`tag_slug=nhl`) | 42 open game events; 6 markets each | Free and unmetered; thin on some games ($556 to $97k volume). |

**FantasyPros has no NHL starters or depth charts.** `nhl/depth-charts`,
`nhl/starting-goalies`, `nhl/goalies` and `nhl/lines` return the gateway's
route-not-found 403 (`Missing Authentication Token`), as does
`nfl/depth-charts`: the public API has no depth charts for any sport. Goalie
`projections` came back empty (the API ignores `type`/`week` and answers
`preseason, week 0`); goalie `consensus-rankings` are season-long ranks, not
starters. The NHL `news` feed's newest item was 2026-08-29 on opening day, with
no goalie-start items. Recheck projections and news after the season's first
week; a 200 from FantasyPros is never proof of coverage (check `sport`).
