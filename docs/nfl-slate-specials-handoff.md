# NFL Slate Specials — Implementation Hand-off

**Audience:** a team implementing this without the author present.
**Companion:** [`nfl-slate-specials-spec.md`](nfl-slate-specials-spec.md) says *what* and *why*.
This document says *how*: files, signatures, schema, queries, UI, tests, and
the order to do it in. Where the two disagree, this one is newer (it folds in
the v2 review at the bottom) and wins.

Read `CLAUDE.md` first. Three of its rules govern everything below and are
not repeated per section: **walk-forward or nothing**, **fail closed, never a
silent default**, and **bump the version rather than tune in place**.

---

## 0. Ten-minute orientation

You are building one Python simulator that draws ~50,000 "Sundays" and reads
six DK specials markets off the same draws, a small ledger that freezes each
week's ratings before kickoff, and one new tab on `/nfl` that shows our
number next to DK's.

Everything is built on four things that already exist:

| Existing asset | File | You will call it as |
|---|---|---|
| Correlated team box-score sim | `model/nfl_dfs_efficiency.py::simulate_team(forecast, by_player, by_position, config)` | Layer B, conditioned on drawn team points |
| Per-game market script with provenance | table `nfl_game_win_probs`, `nfl_season_games` | Layer A inputs |
| Play-by-play with clock + drive labels + who scored | tables `nfl_pbp_archetypes`, `nfl_pbp_play_participants` | Layer C empirical tables; settlement |
| 250-way power de-vig and star rubric | `model/soccer_first_scorer.py::power_devig_exclusive`, `model/soccer_bet_rating.py::rate_market` | Market side, ratings |

Do not fork any of them. Import.

**Delivery order is fixed** (§8). Ship magnitude markets before timing
markets, and measure DK's overround before modelling anything.

---

## 1. Repo conventions you must follow

- Python owns every table written here. The web app reads only. Pattern:
  `mlb_matchups`, `youtube_picks`.
- New tables go in `db/schema.py` (`TABLES` list). The web app never runs DDL
  for Python-owned tables.
- DB access via `DatabaseManager` in `db/database.py`. For **NFL ingest** the
  living convention is inline SQL in the ingest module — see
  `ingest/nfl_dfs_projections.py`, which does this throughout; `db/queries.py`
  carries only three NFL functions, all from the older odds path. Keep the pure
  logic in separate functions so it is testable without a database.
- Every persisted rating carries `model_version` and frozen `inputs_json`.
- New scheduled work is a workflow in `.github/workflows/`, mirroring
  `refresh_nfl_survivor.yml` (secrets, `pip install -r requirements.txt`,
  `python -m model.<module>`).
- Web queries live in `web/src/db/queries.ts`; page = server component in
  `web/src/app/nfl/specials/page.tsx` + `"use client"` renderer. Reuse
  `nfl-terminal.module.css` classes; do not add a new CSS system.
- Tests: Python `tests/test_nfl_slate_specials.py` (pytest, mocked DB — see
  `tests/test_mlb_prop_program.py` for the tamper-evident-constants style);
  web `web/scripts/test-nfl-specials.ts` wired into `package.json`
  `"test:nfl-specials"`.

---

## 2. Schema (`db/schema.py`)

Add to `TABLES`, in this order.

```sql
-- DK's board, pasted by hand (these markets are NOT on The Odds API).
-- One row per (capture, family, selection). Append-only.
CREATE TABLE IF NOT EXISTS nfl_specials_market_captures (
    id BIGSERIAL PRIMARY KEY,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    family TEXT NOT NULL,          -- see FAMILIES in §3.1
    slate_scope TEXT NOT NULL,     -- 'sunday_all' | 'sunday_1pm'
    selection_key TEXT NOT NULL,   -- normalized player / team / game key (§3.4)
    selection_label TEXT NOT NULL, -- exactly as DK printed it
    american INTEGER NOT NULL,
    book TEXT NOT NULL DEFAULT 'draftkings',
    captured_at TIMESTAMPTZ NOT NULL,   -- real wall clock of the paste
    capture_key TEXT NOT NULL,           -- groups one paste
    raw_text TEXT,
    CHECK (american <= -100 OR american >= 100)
);
CREATE INDEX IF NOT EXISTS idx_nfl_specials_captures_wk
    ON nfl_specials_market_captures(season, week, family, slate_scope);

-- One simulator run. Everything a reader needs to replay it exactly.
CREATE TABLE IF NOT EXISTS nfl_specials_sim_runs (
    run_id UUID PRIMARY KEY,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    slate_scope TEXT NOT NULL,
    model_version TEXT NOT NULL,
    seed BIGINT NOT NULL,
    n_draws INTEGER NOT NULL,
    projection_run_id UUID,            -- nfl_dfs_projection_runs.run_id used
    winprob_model_version TEXT,        -- nfl_game_win_probs.model_version used
    pbp_labeller_version TEXT,         -- drive_labeller_version used for Layer C
    games_json JSONB NOT NULL,         -- [{game_id, provenance, kickoff, included}]
    blocked_reasons JSONB NOT NULL DEFAULT '[]',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    git_sha TEXT
);

-- Our probability for every selection in every family, per run. Append-only.
CREATE TABLE IF NOT EXISTS nfl_specials_probs (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES nfl_specials_sim_runs(run_id) ON DELETE CASCADE,
    family TEXT NOT NULL,
    selection_key TEXT NOT NULL,
    selection_label TEXT NOT NULL,
    our_prob DOUBLE PRECISION NOT NULL,
    p_tie DOUBLE PRECISION,            -- share of draws where this selection tied for the win
    mc_se DOUBLE PRECISION NOT NULL,   -- sqrt(p(1-p)/N)
    status TEXT NOT NULL,              -- 'ok' | 'blocked'
    block_reason TEXT,
    UNIQUE(run_id, family, selection_key)
);

-- The ledger. Same shape as soccer_bets. One ACTIVE row per
-- (family, slate, selection, model_version); locks at slate first kickoff.
CREATE TABLE IF NOT EXISTS nfl_specials_bets (
    id BIGSERIAL PRIMARY KEY,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    slate_scope TEXT NOT NULL,
    family TEXT NOT NULL,
    selection_key TEXT NOT NULL,
    selection_label TEXT NOT NULL,
    model_version TEXT NOT NULL,
    run_id UUID REFERENCES nfl_specials_sim_runs(run_id),
    capture_key TEXT,                  -- market capture the price came from
    our_prob DOUBLE PRECISION NOT NULL,
    market_prob DOUBLE PRECISION,      -- power-de-vigged reference
    market_decimal DOUBLE PRECISION,   -- best offered
    overround DOUBLE PRECISION,        -- family-level, raw sum of implied
    edge DOUBLE PRECISION,
    ev DOUBLE PRECISION,
    stars SMALLINT NOT NULL,
    inputs_json JSONB NOT NULL,
    event_commence TIMESTAMPTZ NOT NULL,   -- slate first kickoff
    locked BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'pending', -- pending|won|lost|push|void|unresolved
    result_detail TEXT,
    settled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(season, week, slate_scope, family, selection_key, model_version)
);

CREATE TABLE IF NOT EXISTS nfl_specials_bet_snapshots (
    id BIGSERIAL PRIMARY KEY,
    bet_id BIGINT NOT NULL REFERENCES nfl_specials_bets(id) ON DELETE CASCADE,
    run_id UUID,
    capture_key TEXT,
    stars SMALLINT, our_prob DOUBLE PRECISION, market_prob DOUBLE PRECISION,
    edge DOUBLE PRECISION, ev DOUBLE PRECISION,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Add the `_ensure_lock_trigger`-style protections used on
`mlb_game_prediction_snapshots`: reject UPDATE/DELETE on `nfl_specials_probs`,
`nfl_specials_sim_runs` and `nfl_specials_bet_snapshots`.

> **Corrected 2026-09-19, and this one is a trap.** This section used to say
> "`nfl_specials_bets` is updatable only while `locked = FALSE`; add the same
> trigger pattern `soccer_bets` uses." Both halves are wrong. `soccer_bets`
> has **no trigger at all** — its lock is enforced in Python, in
> `soccer_bet_rating.record_bet`. And a blanket *no UPDATE when locked* trigger
> would **break settlement every Monday**: settlement happens after kickoff by
> definition, so the row it must write is always locked. Such a trigger passes
> every test written before Sunday and fails in production on the first one
> that matters.
>
> What is implemented instead is **field-level**: on a locked row the 13
> decision fields (`our_prob`, `market_prob`, `market_decimal`, `overround`,
> `edge`, `ev`, `stars`, `inputs_json`, `run_id`, `capture_key`,
> `selection_key`, `selection_label`, `event_commence`) are frozen, and so is
> `locked` itself, so a row cannot be quietly re-opened. `status`,
> `result_detail`, `settled_at` and `updated_at` stay writable. Doing it in the database rather than only in Python means a
> future caller who forgets `WHERE locked = FALSE` fails loudly instead of
> quietly rewriting a committed decision. `DELETE` is rejected outright
> — supersede instead. Verified against a real PostgreSQL 16: see the log
> in §11.

---

## 3. Python modules

### 3.1 `model/nfl_slate_specials.py` — the simulator

```python
MODEL_VERSION = "nfl-specials-v1"
N_DRAWS = 50_000

FAMILIES = (
    "highest_scoring_game", "lowest_scoring_game",
    "highest_scoring_team", "lowest_scoring_team",
    "most_passing_yards", "most_receiving_yards",
    "first_td_scorer", "first_qb_td_pass", "first_qb_int",
)
MAGNITUDE_FAMILIES = FAMILIES[:6]   # need Layers A+B only
TIMING_FAMILIES    = FAMILIES[6:]   # need Layer C

SLATE_SCOPES = {
    "sunday_all": lambda g: g.kickoff_et.weekday() == 6,
    "sunday_1pm": lambda g: g.kickoff_et.weekday() == 6 and g.kickoff_et.hour == 13,
}
```

**Exclusion is by scope, never down-weighting.** A 4:25 game is not in the
1pm market at all. Test it.

Public entry points:

```python
def build_slate(season: int, week: int, scope: str, conn) -> Slate
    # Loads games from nfl_season_games + nfl_game_win_probs (latest
    # model_version, one row per (game, team)); loads the latest populated
    # nfl_dfs_projection_runs run for the week and its player rows.
    # NB nfl_dfs_projection_runs has NO status column: "complete" is
    # player_count > 0, ordered the way every other consumer orders it,
    # ORDER BY as_of_at DESC, created_at DESC LIMIT 1;
    # applies availability (§3.3). Any game with provenance BLOCKED, or
    # with no projection rows for either team, is carried in Slate.games
    # with included=False and a reason. Never dropped silently.

def simulate(slate: Slate, seed: int, n: int = N_DRAWS) -> Draws
    # Returns arrays, all shape (n, ...):
    #   team_pts[n, n_teams]           Layer A
    #   player_stats[n, n_players, k]  Layer B  (k = passing_yards, receiving_yards, ...)
    #   events: list of (draw, time_s, kind, team_idx, player_idx)   Layer C
    #                                  kind in {'td_rush','td_rec','td_other','int'}

def readout(draws: Draws, slate: Slate) -> dict[family, dict[selection_key, Prob]]
    # Pure functions over draws. §3.5.

def run_and_persist(season, week, scope, conn) -> run_id
    # build → simulate → readout → INSERT sim_run + probs. Seed derived
    # deterministically: hash(season, week, scope, MODEL_VERSION).
```

`Slate`, `Draws`, `Prob` are dataclasses. Keep the simulator free of DB calls
below `build_slate` so tests run on fixtures.

#### Layer A — game script

**Do not draw margin and total separately.** Draw `(home_pts, away_pts)`
jointly from an empirical conditional table.

```python
def fit_score_table(games: DataFrame) -> ScoreTable
    # games: nfl_season_games 1999-2025 completed rows with quoted spread/total.
    # Bin by (spread rounded to 0.5, total rounded to 0.5). For each bin,
    # the empirical joint pmf of (home_pts, away_pts) with additive
    # smoothing that borrows from the 8 neighbouring bins, weighted by
    # distance. Persist as artifacts/nfl_specials_score_table_v1.npz with
    # bin edges, counts, and the season range. Frozen; re-fit only under a
    # new MODEL_VERSION.

def draw_scores(table, spread, total, sigma_h, rng, n) -> (home_pts, away_pts)
    # If nfl_game_win_probs.provenance == 'MODEL', the spread is a model
    # estimate with horizon error sigma_h: first draw spread ~ N(spread,
    # sigma_h), THEN look up the bin. Provenance widens; it never narrows.
```

This single table gives you lattice scores, skewed totals, and
margin-total dependence for free. Validation gate (§7, P1): the marginal
distributions of simulated total, margin, and team points reproduce the
1999-2025 empirical ones on a held-out 2024-2025 window (KS p > 0.05 each).

#### Layer B — team box score, conditioned

For each draw and team, call the existing `simulate_team` **once per draw
with a modified forecast**, or — because 50k × 26 teams × the existing sim is
slow — vectorise: preserve `simulate_team`'s allocation *logic* but replace
its scalar `_count(rng, mean)` draws with the team's drawn points decomposed
into a TD budget.

Points → TD budget decomposition, per draw:

```python
def decompose_points(pts: int, rng) -> (tds, fgs, xp, two_pt, safeties)
    # Empirical table from nfl_pbp_archetypes: for each observed team
    # game score s, the distribution of (TD count, FG count). Draw from
    # P(tds, fgs | pts). Never solve pts = 7*tds + 3*fgs arithmetically;
    # 14 points is two TDs OR a TD+2pt+FG+... — use the data.
```

Then, using the team's `forecast["players"]` component shares exactly as
`simulate_team` does: allocate `tds` across rushers/receivers with the
existing `_allocate_with_capacity`, draw attempts/carries/targets from the
existing budgets but **rescaled by `drawn_total / quoted_total`** so a
shootout draw carries more volume. Passing yards/receiving yards per player
follow from the existing rate machinery.

**Change the yardage draw for the leaders families** (v2 item 5): the
existing `_yards` is `normal(rate*opps, spread*sqrt(opps))`, thin-tailed. For
`most_*_yards` readouts use an empirical per-opportunity yardage bootstrap
from `nfl_pbp_archetypes.yards_gained` grouped by (position, play type).
Gate: simulated slate-max receiving yards over 2020-2025 must match the
observed slate-max distribution (P2 in §7).

Who is on the field: `nfl_dfs_player_projections` for the projection run,
after the redistribution layer. Redistribution currently lives in the web
read path (`web/src/lib/nfl-dfs/opportunity-redistribution.ts`); for Python
use `model/nfl_dfs_availability.py::apply()` **with
`positions=("QB","RB","WR","TE")`**, not its default `("QB",)`.

> **Corrected 2026-09-19.** An earlier draft told you to source the OUT flag
> from `nfl_dfs_official_availability`. **No such table exists** — it appears
> nowhere in `db/schema.py` nor in any `.py`/`.ts` file in this repo. `apply()`
> is also pure: it takes `statuses: Mapping[int, str]` keyed by `ff_players.id`
> and never reads a table at all. The OUT flag we actually have is DK's, on
> **`nfl_dfs_slate_players.dk_status` / `.is_out`** (reached via `upload_id`
> → `nfl_dfs_slate_uploads`) — which is what CLAUDE.md's "Out-Player
> Opportunity Redistribution" section already says: the FantasyPros feed
> carried 0 OUT rows on a slate where DK flagged 77. Build the `statuses` map
> by joining `nfl_dfs_slate_players.ff_player_id` to
> `nfl_dfs_player_projections.player_id` — both are FKs to `ff_players(id)`.

#### Layer C — event timing (timing families only)

Reframe from "a drive-hazard model" to **three empirical tables plus the
allocation you already have** (v2 item 3):

```python
def fit_timing_tables(pbp: DataFrame) -> TimingTables
    # From nfl_pbp_archetypes, one row per drive (group by game_id,
    # posteam, drive), seasons 2016-2025, drive_labeller_version pinned:
    #  1. P(drive k ends TOUCHDOWN | k, posteam received opening kickoff)
    #     for k = 1..6, with 'k>6' pooled. Also P(INT on drive k).
    #  2. Drive start time and duration: empirical (game_seconds_remaining
    #     at first play, drive_time_of_possession) by drive index k, as a
    #     joint bootstrap, not independent marginals.
    #  3. Conditional TD type: P(rush | TD on drive k), P(pass | TD).
    # Persist to artifacts/nfl_specials_timing_v1.npz, frozen.
```

Per draw, per team: draw coin toss (Bernoulli 0.5, receive/defer), then walk
drives k=1.. until the team's drawn TD count is exhausted, assigning each TD
a clock time from table 2 and a scorer from Layer B's allocation restricted
to the drawn type. Kickoff time is the real `nfl_season_games.kickoff`; ET
1pm games get a ±90s jitter (empirical from actual first-play timestamps if
available; else uniform ±90s, labelled). Same for INTs.

**Overtime:** a game's TD window is capped at 3600 game-seconds plus OT
only if the drawn margin is 0 — draw OT explicitly for tied draws. This is
the one cross-game dependence that matters for `first_*` and does not for
`highest_*`.

#### 3.4 Selection keys

Deterministic, so ledger rows join to captures and settle without fuzzy
matching at grade time:

- team: `nfl_teams.abbreviation` (canonical; apply `TEAM_ABBREV_OVERRIDES`
  from `ingest/nfl_season_schedule.py` when parsing DK's `WAS`, `LA`, etc.)
- game: `f"{away}@{home}"`
- player: `nfl_dfs_player_projections.player_gsis_id` when present, else
  `normalized_name|team`. Fuzzy-match DK's printed name to the projection row
  **once, at capture ingest**; store the resolved key on
  the capture row. (Implemented with `rapidfuzz`, as `ingest/dk_slate.py` and
  `ingest/mlb_slate.py` already do, rather than the `_levenshtein` first named
  here: that is a private function inside `ingest/nba_schedule.py`, and
  importing an NBA internal into an NFL module — to dodge a dependency already
  in `requirements.txt` — is the wrong trade.) Unresolved → capture row keeps `selection_key =
  'UNRESOLVED:' + label` and is excluded from de-vig with a logged count.

#### 3.5 Readouts

```python
def _argmax_probs(values: ndarray[n, m], keys) -> dict[key, Prob]
    # winners = values.max(axis=1, keepdims=True) == values
    # p_win  = (winners & (winners.sum(1, keepdims=True) == 1)).mean(0)
    # p_tie  = (winners & (winners.sum(1, keepdims=True) >  1)).mean(0)
    # mc_se  = sqrt(p(1-p)/n)
```

`highest_scoring_game`: `_argmax_probs(team_pts[:, home] + team_pts[:, away])`.
`lowest_*`: negate. `highest_scoring_team`: `_argmax_probs(team_pts)`.
`most_passing_yards`: argmax over player_stats[..., passing_yards] restricted
to QBs; `most_receiving_yards`: over receiving_yards, all positions.
`first_td_scorer`: for each draw, min `time_s` over events with kind in
td_*; scorer index; count. `first_qb_td_pass`: kind == td_rec, passer.
`first_qb_int`: kind == int, thrower.

**Tie rules are per DK market and must be read from DK's rules text before
that family's ratings go live.** Store the rule as `tie_rule` in
`inputs_json` (`dead_heat` | `push` | `all_win`). Unknown → family
`status='blocked'`, `block_reason='tie_rule_unknown'`. This is the single
most likely way to mis-settle "highest scoring game".

### 3.2 `ingest/nfl_specials_market.py` — DK board capture

DK specials are not on The Odds API. Manual paste, like Yahoo ADP
(`ingest/ff_yahoo_predraft.py`). Provide:

```
python -m ingest.nfl_specials_market --season 2026 --week 3 \
    --family first_td_scorer --scope sunday_1pm --file paste.txt
```

Parser accepts the two-column `Name\n+ODDS` layout in the screenshots and the
`Label ... +ODDS` row layout. Emits one `capture_key`, resolves selection
keys (§3.4), reports resolved/unresolved counts, refuses to write if any
american price is inside (-100, 100). Idempotent on identical `raw_text`.

Also computes and prints the family **overround** = `Σ american_to_prob` −
1, because P0 (§7) is nothing but this number, collected for two weeks.

### 3.3 `model/nfl_specials_bets.py` — rating + ledger

```python
def rate_week(season, week, scope, conn) -> RateReport
    # 1. latest sim run for (season, week, scope, MODEL_VERSION)
    # 2. latest capture per family (captured_at < slate first kickoff)
    # 3. per family:
    #      raw   = [american_to_prob(a) for each selection]
    #      ref   = power_devig_exclusive(raw)         # soccer_first_scorer
    #      for 'first_td_scorer' ONLY: anchor the exponent so
    #      Σ_p -ln(1 - ref_p) == simulated expected slate TD count
    #      (port the k-solve from soccer_first_scorer._compute_stat_first_probs)
    #      stars, ev, edge = rate_market(our_prob, best_decimal, ref_prob,
    #                                    longshot_odds_cap=True, max_stars=2)
    # 4. upsert nfl_specials_bets (UNLOCKED rows only), append snapshot.
    # 5. Any selection with status='blocked' or unresolved key → not rated.
```

`max_stars=2` is hard-coded, not configurable, until §7 P5 passes. A test
asserts the literal.

Locking: rows with `event_commence <= NOW()` set `locked = TRUE` at the
start of every run, before any upsert. Copy `soccer_bet_rating.record_bet`'s
lock discipline.

### 3.4 `model/nfl_specials_settle.py`

Settles from **our own PBP tables**, no external feed:

- `highest/lowest_scoring_game|team`: from `nfl_season_games` finals for the
  scope's games; apply the family's `tie_rule`.
- `most_*_yards`: from `nfl_dfs_player_week_results` (already populated
  weekly); tie → per rule.
- `first_td_scorer`, `first_qb_td_pass`, `first_qb_int`: from
  `nfl_pbp_archetypes` joined to `nfl_pbp_play_participants`: earliest by
  **wall time**, which needs kickoff + `(3600 − game_seconds_remaining)`.
  Note OT and the ±jitter caveat: if two games' first TDs fall within 120
  wall-seconds of each other by this reconstruction, settle `unresolved`
  and log it — DK settles on broadcast wall clock we do not have.
- **Voids:** a player selection whose DK slate row for that game is flagged
  OUT (`nfl_dfs_slate_players.is_out` / `.dk_status`) settles `void`, not
  `lost`. No slate row for the player at all → `unresolved`. Never
  DNP-as-loss (CLAUDE.md, soccer ATGS). The `nfl_dfs_official_availability`
  table originally named here does not exist — see the correction in §3.1.

Runs after `refresh_nfl_pbp_archetypes.yml` completes (Monday). Idempotent.

### 3.5 `model/nfl_specials_backtest.py`

Implements §7's gate exactly. Reports per family, per season:

- **Calibration**: PIT/rank test — rank of the actual winner in our sorted
  list, expected uniform under calibration; plus CRPS on team-points draws
  vs actual (26 obs/slate, the only place a per-slate distributional check
  has power).
- **Market-relative**: mean log score of our prob vs mean log score of the
  power-de-vigged market prob on the same winners; slate-clustered bootstrap
  CI on the difference.
- Sample floors printed in **slates**: refuses a verdict below 17 slates
  and 200 settled selections, and prints `descriptive-only` above every
  under-floor row.

---

## 4. Workflows

- `refresh_nfl_specials.yml`: `workflow_dispatch` + cron Thu 14:00 UTC and
  Sun 14:30 UTC (after `refresh_nfl_dfs_projections.yml`; add a `needs:` or
  a schedule 30 min later). Steps: `python -m model.nfl_slate_specials
  --season --week --scope sunday_all`, same for `sunday_1pm`, then
  `python -m model.nfl_specials_bets`. Fails loudly if no projection run
  exists for the week.
- `settle_nfl_specials.yml`: Tue 06:00 UTC, `python -m
  model.nfl_specials_settle`, then `python -m model.nfl_specials_backtest
  --report` (informational until P5).
- Market captures stay manual (`workflow_dispatch` is pointless for a
  paste). Document the paste procedure in the tab's UI (§5).

Register the sim as a detector in `DETECTOR_REGISTRY` (both
`model/line_alerts.py` and `queries.ts`) so a silently-empty weekly run shows
up dead on `/vegas/detectors`.

---

## 5. Web UI

Route: `/nfl/specials`. Nav: add `{ href: "/nfl/specials", label: "Slate
Specials", sports: ["nfl"] }` to `PAGE_LINKS` in
`web/src/components/sport-nav.tsx`.

### 5.1 Queries (`web/src/db/queries.ts`)

```ts
export type NflSpecialsFamilyRow = {
  family: string; slateScope: string; selectionKey: string; selectionLabel: string;
  ourProb: number | null; pTie: number | null; mcSe: number | null;
  status: "ok" | "blocked"; blockReason: string | null;
  marketAmerican: number | null; marketProb: number | null;  // de-vigged
  edgePp: number | null; ev: number | null; stars: number | null;
  betStatus: string | null; resultDetail: string | null;
};
export type NflSpecialsBoard = {
  season: number; week: number; scope: string;
  run: { runId: string; modelVersion: string; seed: string; nDraws: number;
         generatedAt: string; projectionRunId: string | null;
         games: Array<{gameId: string; label: string; kickoff: string; included: boolean; provenance: string; reason?: string}>;
         blockedReasons: string[] } | null;
  families: Array<{ family: string; overround: number | null; captureAt: string | null;
                    tieRule: string | null; rows: NflSpecialsFamilyRow[] }>;
  gate: { state: "research" | "capped" | "validated"; slatesSettled: number;
          selectionsSettled: number; note: string };
};
export async function getNflSpecialsBoard(season: number, week: number, scope: string): Promise<NflSpecialsBoard>
export async function getNflSpecialsBacktest(): Promise<...>   // per-family table from nfl_specials_bets
```

One query joins the latest run's `nfl_specials_probs` LEFT JOIN the latest
pre-kickoff capture LEFT JOIN `nfl_specials_bets`. Selections present on DK's
board but absent from our run (an unresolved name) render with `ourProb =
null` and are **listed**, not hidden — a missing row is the bug you want to
see.

`gate.state` is computed server-side from the ledger counts against the §7
floors. The client never derives it.

### 5.2 Page (`web/src/app/nfl/specials/page.tsx` + `specials-client.tsx`)

Layout, top to bottom, using the `nfl-terminal.module.css` instrument
classes already on `/nfl`:

1. **Status strip.** `RESEARCH — no validated edge · 2★ cap · N slates
   settled of 17`. Amber. Green only when `gate.state === "validated"`,
   which cannot happen before P5.
2. **Scope + week controls.** `?season&week&scope=sunday_all|sunday_1pm`.
3. **Run provenance panel** (collapsed `<details>`): model version, seed,
   N, projection run, game list with included/excluded + reason. If
   `run == null`: the page says "No simulation run for this week" and
   nothing below renders as numbers.
4. **Six family panels**, one per DK family, in DK's order so the two
   boards are diffable. Header carries **overround** in large type (e.g.
   `DK overround 340%`) and the capture time. Columns:
   `Selection · Our % · DK % (de-vig) · DK price · Edge pp · EV · ★ · ±MC`.
   Sort by our prob desc. A `blocked` family shows the reason in place of
   rows. `p_tie` renders as a small suffix when > 0.5pp.
5. **Backtest panel**: per family — slates, settled, PIT histogram (10
   bins), log score ours vs market with CI, `descriptive-only` tag under
   floor. Reuse the table styles from the `/nfl` audit `<details>` blocks.
6. **Paste procedure** `<details>`: the exact CLI from §3.2, so whoever
   captures DK's board on Sunday morning has the instructions on the page.

Colour rules (from the MLB v3 contract): status colours only; edge sign uses
neutral text. No star rendered above 2 by construction, but the component
must not special-case it — it renders whatever `stars` the server sends.

### 5.3 Client test (`web/scripts/test-nfl-specials.ts`)

Pure-function tests on the board shaping: unresolved selections are listed
with null prob; blocked families render reason; gate state derives from
counts only; families keep DK order. Add `"test:nfl-specials"` to
`package.json`.

---

## 6. Python tests (`tests/test_nfl_slate_specials.py`)

Minimum set, all on synthetic fixtures:

1. `_argmax_probs` sums to ≤ 1 per family, `p_win + p_tie` per selection
   ≤ 1, and `mc_se` formula.
2. Scope exclusion: a 16:25 ET game contributes zero events to
   `sunday_1pm` families.
3. Provenance widening: a `MODEL` game's team-points variance ≥ the same
   game's variance as `MARKET`.
4. Blocked propagation: one `BLOCKED` game → every family that could be won
   by it has `status='blocked'`; families it cannot affect are `ok`.
5. Decompose-points never returns a combination inconsistent with the
   points (sum check) and is drawn, not solved (two calls, same points,
   can differ).
6. `max_stars == 2` literal in `nfl_specials_bets` (tamper-evident).
7. Locking: a rated row with `event_commence` in the past is not modified
   by a second `rate_week`.
8. Settlement: first-TD within 120 wall-seconds across games →
   `unresolved`; missing availability → `unresolved`; inactive → `void`.
9. Readout consistency: in every draw, the team with the most points is on
   the game with... (no — that's false; instead) the highest-scoring game's
   total ≥ every team's points in that draw. Catches sign bugs.
10. Determinism: same seed → identical `nfl_specials_probs`.

---

## 7. Phases, gates, and what "done" means

| P | Build | Gate (all must pass before the next phase starts) |
|---|---|---|
| **P0** | §3.2 capture tool. Paste every family for two Sundays. **Code shipped 2026-09-19 (§11); the gate now waits on the two Sundays of pastes, which only a human with a DK session can supply.** | Overround table per family in the tab. Any family > 200% is labelled calibration-only in `FAMILIES_CALIBRATION_ONLY` and never rated above 1★. |
| **P0.5** | Layer A only + `highest/lowest_scoring_team`. No player layer. | KS tests §Layer A; team-points PIT uniform on 2024-25 held out. |
| **P1** | Layer B conditioned; `highest/lowest_scoring_game`. Tab live with these four families. | Consistency test 9; tie_rule read and stored for both game families. |
| **P2** | Empirical yardage tails; `most_passing/receiving_yards`. | Simulated slate-max yardage matches observed 2020-25 slate-max distribution (KS p > 0.05). |
| **P3** | Layer C; three timing families. | On held-out 2024-25: PIT of actual first-TD clock time uniform; first-scorer log score vs a "share-of-team-TDs × opening-drive-rate" naive baseline is better with slate-clustered CI excluding zero. If not, timing families stay `blocked` and P3 is re-specced, not tuned. |
| **P4** | Ledger, de-vig, ratings, settlement, workflows, backtest panel. | Two weeks of rows lock and settle without manual intervention. |
| **P5** | Verdict at ≥17 slates and ≥200 settled selections per family. | Calibration PASS and market log-score CI not worse → family may be *considered* for a separately pre-registered uncap study. Calibration FAIL → simulator re-spec. Market-relative worse with CI excluding zero → tenth confirmed negative; permanent 2★. |

Report status per `CLAUDE.md`'s delivery contract: Built / Tested /
Backtested / Prospectively validated / Production actionable. P0-P4
finish at *Tested*; P5 is the first phase that can produce *Backtested*.
Nothing here reaches *Production actionable* in 2026.

Requirement-to-evidence matrix goes in the PR that closes each phase.

---

## 8. Sequencing and estimate

```
P0  ────────────── 2 Sundays of pasting, ~1 day of code
P0.5 ─────────── 3-4 days (score table fit is the bulk)
P1  ───────────── 3-4 days (vectorising the conditioned allocation)
P2  ──── 2 days
P3  ──────────── 5-7 days (new work; may fail its gate)
P4  ──────── 3 days
P5  ▸ waits for the season
```

P0 through P2 can all be done by one person in ~3 weeks. P3 is the only
phase with real research risk; schedule it last and be prepared for it to
end in `blocked`. If it does, the tab still ships six of nine families with
honest numbers, and the joint slate distribution is reusable by pick'em,
survivor and the DFS ceiling model regardless.

---

## 9. What changed from the spec (v2 review, folded in here)

1. Layer A draws `(home, away)` jointly from an empirical conditional score
   table, replacing the spec's separate margin/total draws.
2. `highest/lowest_scoring_team` promoted to P0.5 — it needs no player
   layer and validates Layer A alone.
3. Layer C reframed as three empirical tables + coin toss + existing
   allocation, not a general hazard model.
4. Calibration gate uses PIT/rank tests and CRPS, not reliability bins,
   because 250-way markets produce one winner per slate.
5. Leaders families require an empirical fat-tailed yardage draw with its
   own gate.
6. The tab is designed to be useful with the star column removed
   (provenance + distribution vs DK), so a failed P5 does not leave a dead
   page.
7. Market capture is manual paste with a documented CLI; OT is drawn
   explicitly for tied draws as the one cross-game timing dependence.

---

## 10. Honest prior (unchanged)

Expect calibration to pass and market-relative to fail. The product is a
coherent joint distribution of a Sunday, graded for free against DK's
board, not a source of bets. Build it for that, and let the ledger say
otherwise if it can.

---

## 11. Implementation log

### P0 → shipped 2026-09-19 (code), gate open (data)

**State per CLAUDE.md's delivery contract: Built + Tested.** Not Backtested, not
validated, and no number has been measured yet — P0's gate needs two Sundays of
real DK pastes, and nobody can fabricate those.

| Requirement (§7 P0) | Implementation | Evidence |
|---|---|---|
| Five tables + append-only / lock protections | `db/schema.py` (`TABLES`, `MIGRATIONS`, `INDEXES`) | Applied through the real `DatabaseManager._ensure_schema()` against PostgreSQL 16.13: 5 tables, 5 triggers, 7 indexes; idempotent on re-apply; `_ensure_schema()` still completes in 0.8s |
| Frozen family contract | `model/nfl_slate_specials.py` (constants only) | `test_families_are_frozen`, `test_every_family_declares_what_its_selections_are`, `test_calibration_only_is_empty_until_p0_measures_it` |
| Both paste layouts | `ingest/nfl_specials_market.py::parse_board_text` | Two-column, row, team-context, unicode-minus/thousands-separator, numbered-board, and `San Francisco 49ers` (digits that are not a price) all covered |
| Refuse prices inside (-100, 100) | same, whole-capture refusal | `test_refuses_a_price_inside_the_impossible_band` |
| Selection keys (§3.4) | `resolve_team` / `resolve_game` / `resolve_player` | Live run resolved 8/8 teams, 4/4 games, 5/12 players against seeded fixtures |
| Idempotent on identical `raw_text` | content-derived `capture_key` | Live: re-running the same paste wrote 0 rows and left the count at 24; a moved price wrote a new capture |
| Print family overround | `compute_overround`, `--dry-run` | `-110/-110` two-way returns the textbook 4.76% |

`python3 -m pytest tests/test_nfl_slate_specials.py` → **37 passed**. Full
suite: **876 passed, 3 failed**, and those 3
(`test_alert_audit_floor_is_shared.py`, `test_tennis_walking_study.py`) fail
identically on clean `49f6a03` — pre-existing, unrelated to this work.
Two mutations were injected to confirm the tests have teeth: trusting DK's
print order for game keys, and guessing the best fuzzy match instead of
refusing an ambiguous one. Both were caught.

**Two guards added that this document did not specify**, because P0 measures
exactly one number and both of these corrupt it silently:

1. **A board whose implied probabilities sum below 100% is refused as
   truncated.** No book prices a mutually-exclusive market under 100% — that
   is a free arbitrage — so a sub-100% sum means the paste is partial. Without
   this, pasting the top 20 of a 250-selection first-TD board records a
   generous-looking *negative* overround, which is the one output P0 exists to
   produce. Deliberately no `--allow-partial` flag: a flag that suppresses this
   would be set once and then always. If DK ever genuinely posts a sub-100%
   exclusive board, that is a finding worth a code change and a note here.
2. **A duplicated selection key within one capture is refused**, since a
   double-counted row inflates the overround directly.

**Design decisions worth knowing before extending it:**

- A capture **always succeeds if the prices parse**, even when no projection
  run exists and every player is `UNRESOLVED:<label>`. Market prices are
  perishable and unrecoverable; identities can be resolved any time later. The
  inverse (refusing the capture) loses the only thing that cannot be re-fetched.
- `capture_key` is content-derived, so the tool measures **overround, not quote
  persistence**: re-pasting a genuinely unchanged board records no second
  observation. That is the spec's stated idempotency requirement, and the cost
  is accepted and noted rather than hidden. Persistence would need a
  time-keyed capture.
- Game keys come from **our schedule**, never DK's print order: a board reading
  "Ravens vs Chiefs" stores `KC@BAL`. This is the single cheapest defence
  against the mis-settlement §3.5 warns about.
- An ambiguous name resolves to nothing. Two live `M. Williams` stay
  unresolved rather than being assigned to the higher-scoring fuzzy match.
- `american_to_prob` is **imported** from `model/soccer_bet_rating.py`, not
  reimplemented (§0: "Do not fork any of them. Import."). A test asserts it is
  the same function object.
- There is no re-resolution pass. Back-filling `selection_key` on old captures
  once a projection run lands is a separate job and is **not** built.

**Not started:** P0.5 onward — `build_slate`, `simulate`, `readout`, the score
table, the ledger, settlement, the backtest, the workflow, and the `/nfl`
tab. Nothing in the `nfl_specials_sim_runs` / `_probs` / `_bets` /
`_bet_snapshots` tables is written by any code yet; they exist so the FK chain
and the immutability guarantees are settled before anything depends on them.
