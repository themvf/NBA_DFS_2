# NFL Survivor Pool Optimizer

Status: **Spec only.** Nothing below is built. Numbers in sections 6 and 7 are
measured from real data during spec authoring (2026-08-31) and are reproducible;
they are not estimates.

---

## 1. Summary

A season-long NFL survivor tool at `/nfl/survivor`: a 32-team x 18-week grid of
win probabilities, and an optimizer that plans a full-season pick path under the
"use each team at most once" constraint.

The reference screenshot (STACKED's free survivor tool) is the visual baseline:
teams down the left, weeks across the top, each cell showing opponent, spread,
and win %, with sort modes (W1 / FSV / A-Z) and an Optimize button. This spec
adopts that layout and fixes four things it gets wrong or leaves out.

**What this tool claims, and what it does not.** It does not claim to predict
games better than the market. Every probability it shows is derived from a
market price where one exists, and from a market-implied power rating where one
does not. That is deliberate: this repo has four independent confirmed negatives
on beating closing lines (soccer ML/totals/first-scorer, MLB ML/totals, tennis
ML), and a survivor tool is not the place to relitigate that.

The value added is **combinatorial, not predictive**. Two claims, of very
different strength:

| Claim | Strength | How it gets tested |
|---|---|---|
| Planning a full-season path beats picking the biggest weekly favorite | Strong prior; provable by construction and backtestable over 26 seasons | Section 13.1 backtest, 1999-2025 closing spreads |
| Fading heavily-picked teams raises pool-win rate in large pools | Real mechanism, unproven here | Section 13.3, pre-registered; only one season of field data exists |

The second claim is the one that could turn into a mirage, and it is gated
accordingly.

---

## 2. What the baseline gets wrong

Four defects, each of which drives a requirement below.

**2.1 It shows modeled numbers as if they were market numbers.** The baseline
renders a spread and a win % in every one of 544 cells, in identical styling,
through Week 18. As of 2026-08-31 only **112 of 272** scheduled 2026 games have
a quoted spread anywhere (Weeks 1-6 complete, Week 7 half, Weeks 8-18 almost
nothing). Every cell past Week 6 is therefore a model output wearing a market
costume. This repo's standing rule is that a modeled value is labeled as one
(cf. the MLB `Actionable` removal, the DST spread-compression disclosure). The
grid must distinguish MARKET from MODELED per cell.

**2.2 It presents far-future cells with unearned precision.** Measured
(section 7): a market-implied rating model fit through week *k* predicts closing
spreads 10 weeks out with **RMSE 5.15 points**, and its single best lookahead
play is the eventual best play only **33%** of the time. The number in a Week 15
cell is a planning aid, not a forecast. The UI must say so.

**2.3 It optimizes the wrong objective for large pools.** Maximizing survival
probability is the right objective in a small pool. In a large one, everybody
survives the easy weeks and you win by being alive when others are not, which
requires deliberately unpopular picks. A tool that only maximizes win % will
recommend the most-picked team in the country and produce a pool win rate below
its survival rate. Both objectives are needed, and which applies depends on
pool size.

**2.4 "FSV" is a heuristic where an exact quantity exists.** Future survivor
value has a precise definition under the assignment formulation: the
degradation of the optimal remaining path when a team is consumed. It does not
need to be approximated (section 8.4).

---

## 3. Product decisions (frozen before implementation)

| # | Decision | Rationale |
|---|---|---|
| D1 | Probabilities come from market prices first, modeled ratings second, never from an independent game model | Four confirmed no-edge verdicts in this repo; a survivor tool must not reopen that |
| D2 | Every cell carries a provenance badge: `MARKET` (quoted line for this exact game) or `MODEL` (rating-derived) | Section 2.1 |
| D3 | Modeled cells carry horizon-widened probabilities and a visible uncertainty band | Section 6.4 / 7 |
| D4 | The optimizer is exact, not greedy or heuristic | The problem is 18x32; the exact solve is microseconds (section 8) |
| D5 | Two objective modes, user-selected, with a pool-size-driven default | Section 2.3 |
| D6 | The recommended path is frozen weekly into an append-only ledger before kickoff | Same accountability pattern as every other ledger in this repo |
| D7 | Field/EV mode ships behind a pre-registered study and shows `RESEARCH` until it passes | Section 13.3 |
| D8 | Python owns ingestion, ratings and probabilities; TypeScript owns the interactive solve | Matches the `dfs/actions.ts` precedent; re-optimizing under user constraints must be instant |

---

## 4. Scope

**In scope (v1):** 2026 regular season, weeks 1-18; one pool config; multiple
entries within one pool; the grid; the exact optimizer; FSV; the weekly ledger.

**In scope (v2):** field pick-percentage ingestion; EV-vs-field mode;
multi-entry portfolio diversification.

**Out of scope:** margin-of-victory pools, pick'em/confidence pools, playoff
survivor, college football, in-app pool administration (tracking other members'
picks), and any bet placement.

---

## 5. Data sources

### 5.1 nflverse `games.csv` — canonical season grid (verified 2026-08-31)

`https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv`

Free, no auth, no key, already used in this repo (`ingest/ff_independent.py`
fetches it for bye weeks; `ingest/ff_playoff_sos.py` uses the team-week
release). Verified live:

- 272 rows for `season=2026`, `game_type=REG`, weeks 1-18 complete.
- Columns used: `game_id`, `season`, `game_type`, `week`, `gameday`, `weekday`,
  `gametime`, `away_team`, `home_team`, `spread_line`, `total_line`,
  `home_moneyline`, `away_moneyline`, `result`, `home_score`, `away_score`,
  `div_game`, `roof`, `surface`, `away_rest`, `home_rest`, `location`.
- `spread_line` is **home-team perspective, positive = home favored**. Verified
  against three 2025 rows and against the fitted win rates in 6.1.
- Historical depth: **6,967** REG games since 1999 carry both a closing
  `spread_line` and a `result`. This is the calibration and backtest corpus.

**Team code mismatch — a known bug class in this repo.** nflverse uses `LA` and
`WAS`; `nfl_teams.abbreviation` uses `LAR` and `WSH`. The existing map
`ingest/ff_independent.py::TEAM_ABBREV_OVERRIDES = {"LA": "LAR", "WAS": "WSH",
"AZ": "ARI", "JAC": "JAX"}` must be reused, not re-derived. The v1.13 note in
CLAUDE.md records what the last miss of this cost: every Arizona player silently
carried a NULL team and NULL bye week. Ingestion **fails closed** on an unmapped
code rather than dropping the row.

### 5.2 The Odds API via `nfl_matchups` / `game_odds_history` — live prices

Already built (`ingest/nfl_schedule.py`, `ingest/refresh_nfl_vegas.py`,
`docs/nfl-odds-ingestion-and-movement-spec.md`). Per-book moneylines and spreads
with pre-kickoff capture guards and an append-only history. Covers only games
the provider currently lists — near-term weeks.

**No new Odds API spend.** The survivor tool is a consumer of the existing
capture, not a new one. `docs/the-odds-api.md` must be read before any change to
that, and this spec proposes none. The open NFL cadence question recorded in
CLAUDE.md ("NFL regular-season odds cadence — UNDECIDED") is unaffected: the
tool works at any cadence and gets better at higher ones, but does not require
an increase and must not be used to justify one.

### 5.3 survivorgrid.com — field pick percentage (verified 2026-08-31)

- `robots.txt` is `User-agent: * / Disallow:` — everything permitted.
- `https://www.survivorgrid.com/{season}/{week}` renders a server-side HTML
  table with columns **EV, W%, P%, Team, Opponent+spread**, plus a W/L marker
  once games settle. `P%` is the field pick percentage.
- The 2025 archive is linked and complete (`/2025/1` ... `/2025/18`), giving one
  full season of historical pick distributions for backtesting.

Caveats to carry, not hide: this is an unofficial scrape of an HTML table with
no contract, no versioning, and a structure that can change without notice; its
`P%` is one aggregator's estimate of national pick share, which is not the same
as *your* pool's distribution; and a small local pool may not resemble it at
all. Ingestion stores the raw response hash and fails visibly on a parse change
rather than writing partial rows — the pattern `ingest/ff_dk_bestball_adp.py`
already uses.

### 5.4 Sources deliberately NOT used

- **ESPN Eliminator pick distribution** — probed; the `gambit-api` challenge
  path returns 404 for a 2026 key and no verified public endpoint was found.
  Not pursued.
- **Any independent game-outcome model.** See D1.
- **Circa / DraftKings lookahead line sheets.** Circa posts full-season
  lookahead spreads, which would replace section 7 outright. No free
  programmatic feed identified. If one is ever found, section 7 becomes a
  fallback rather than the primary — that is the single highest-value source
  upgrade available to this tool.

---

## 6. Win probability

### 6.1 Spread to probability (fitted, 6,967 games, 1999-2025)

Logistic regression of home win on closing spread, home perspective:

```
p_home_win = sigmoid(-0.03228 + 0.14327 * spread_line)
```

| spread | fitted p | empirical p | n |
|---|---|---|---|
| 0 | 0.492 | 0.533 | 30 |
| 3 | 0.598 | 0.581 | 1,296 |
| 6.5 | 0.711 | 0.747 | 735 |
| 10 | 0.802 | 0.825 | 377 |
| 14 | 0.878 | 0.858 | 141 |

Two things worth recording. First, the intercept is **-0.032, statistically
indistinguishable from zero**: once the spread is known there is no residual
home-field edge, i.e. the market prices HFA into the number correctly. Do not
add a separate home adjustment on top of a spread-derived probability. Second,
the fit reproduces the baseline tool's own displayed numbers (it shows 70% at
-6.5; this fit gives 71.1%), which is good evidence the baseline is doing
essentially this and that we are not diverging from a known-good reference.

The coefficients must be **refit on ingest, not hardcoded**, and stored with the
fit date, n, and season range, so drift in the relationship is visible.

### 6.2 Source priority ladder

For each (team, week), the probability comes from the first available:

| # | Source | Provenance | Method |
|---|---|---|---|
| 1 | Two-sided no-vig moneyline at a named book, this exact game | `MARKET` | `r(o)/(r(home)+r(away))`, reusing `ingest/mlb_odds_policy.py::consensus_american` and `model.soccer_bet_rating.american_to_prob` |
| 2 | Quoted spread for this exact game (Odds API, else nflverse `spread_line`) | `MARKET` | 6.1 |
| 3 | Modeled lookahead spread from market-implied ratings | `MODEL` | 7 |
| 4 | Nothing usable | `BLOCKED` | Cell renders unavailable with the reason; it is never silently 50% |

Moneyline is preferred over spread because it is the direct quote of the exact
proposition a survivor pick is (win the game outright), whereas spread-to-prob
inserts a fitted transform. Both sides must come from the same book and the same
capture — the no-vig pairing rule already frozen for MLB and tennis in CLAUDE.md.

### 6.3 Ties

15 of 6,967 games (0.22%) ended tied. Most survivor pools score a tie as a
**loss**; some as a survive. This is a pool rule, not a modeling choice:

```
p_advance = p_win                when tie_rule = 'tie_loses'     (default)
p_advance = p_win + p_tie        when tie_rule = 'tie_survives'
```

`p_tie` is estimated as a function of spread from the same corpus (highest near
pick'em, ~0.5% there, effectively zero past 7 points). The effect is tiny, and
that is exactly why it must be explicit: a silent 0.2% that only bites in a
pick'em game is the kind of thing that is wrong for three seasons before anyone
notices.

### 6.4 Horizon widening

A modeled spread is a point estimate with known error (7.1). The honest
probability integrates over it:

```
p_hat = E_eps[ p(spread_model + eps) ],   eps ~ N(0, sigma_h^2)
```

with `sigma_h` from the measured table in 7.1, by 401-point quadrature; cost is
irrelevant at this scale.

Measured effect, and it is **smaller than intuition suggests**:

| modeled spread | h=0 (quoted) | h=4 | h=10 | h=12 |
|---|---|---|---|---|
| 3 | 0.598 | 0.591 | 0.588 | 0.586 |
| 7 | 0.725 | 0.711 | 0.705 | 0.700 |
| 10 | 0.802 | 0.786 | 0.779 | 0.774 |
| 14 | 0.878 | 0.864 | 0.857 | 0.852 |

A modeled -7 ten weeks out is 70.5%, not 72.5%. Roughly two points. Report it
because it is free and correct, but do not oversell it: **the level is nearly
right; it is the ranking that is unreliable** (7.2). That distinction drives the
UI treatment in 12.4.

---

## 7. Lookahead ratings

Weeks with no quoted line need a spread. The model is the standard one and is
deliberately boring: least squares on observed market spreads.

```
spread_hat(home, away) = r[home] - r[away] + hfa
```

fit by ridge regression (`lambda = 1.0`, no penalty on `hfa`) over every game in
the season that already has a quoted `spread_line`. The ratings are therefore
**market-implied**: a compression of what the market already thinks, propagated
to games the market has not yet priced. They are not an independent opinion and
must not be described as one.

Pre-season (no in-season lines yet), the fit uses the Weeks 1-6 quoted lines,
which already exist for 2026 (5.1). In-season it refits weekly on all quoted
lines to date, so it absorbs injuries and form the moment the market prices them.

### 7.1 Measured forecast error by horizon

Fit ratings through week *k*, predict closing spreads in week *k+h*, for
k = 4..14 across seasons 2010-2025:

| horizon h | n | RMSE (points) |
|---|---|---|
| 1 | 2,573 | 3.35 |
| 2 | 2,599 | 3.63 |
| 3 | 2,625 | 3.95 |
| 4 | 2,479 | 4.20 |
| 6 | 2,036 | 4.50 |
| 8 | 1,584 | 4.86 |
| 10 | 1,092 | 5.15 |
| 12 | 591 | 5.78 |
| 13 | 335 | 6.16 |

This table is the `sigma_h` used in 6.4. It must be **regenerated on ingest**
from the then-current corpus rather than frozen as a constant, and stored with
the run.

### 7.2 Ranking stability — the number that governs the UI

Same setup, asking the question the tool actually cares about: is the model's
best lookahead play in a future week the play the market eventually agrees is
best?

| horizon h | top pick exactly right | top pick in eventual top 5 |
|---|---|---|
| 1 | 52.8% | 93.2% |
| 3 | 43.2% | 83.5% |
| 5 | 40.3% | 80.5% |
| 8 | 37.6% | 76.2% |
| 10 | 33.3% | 71.0% |
| 11 | 34.0% | 73.6% |

Read plainly: at ten weeks out the model names the single best future play one
time in three, and lands in the top five about seven times in ten. That is
genuinely useful for **planning** (it reliably identifies the pool of good
future options) and useless for **committing** (it cannot pick between them).

The product consequence, and it is not a footnote: **the grid is a plan, the
current week is a decision.** The UI commits only the current week and
re-optimizes everything downstream weekly. Section 12.4 renders the far columns
accordingly.

### 7.3 Week 18

Week 18 spreads are systematically unreliable — playoff-seeded teams rest
starters and the line moves violently on Saturday news. Many pools also end at
Week 17. Week 18 is included in the grid but flagged, and the pool config can
exclude it. No special model; just the flag and the honesty.

---

## 8. The optimizer

### 8.1 The problem is an assignment problem

Maximize the probability of surviving every remaining week:

```
maximize    prod over weeks w of  p(team_w, w)
equivalently
maximize    sum over weeks w of   log p(team_w, w)
subject to  exactly one team per week
            each team used at most once across all weeks
            team_w plays in week w (not on bye)
            team_w not already consumed
```

The objective is **separable** and the constraints form a bipartite matching, so
this is a rectangular linear assignment problem. It solves **exactly** by the
Hungarian algorithm / min-cost flow on an 18 x 32 cost matrix with
`cost[w][t] = -log p(t, w)` and `+inf` for infeasible cells. No greedy, no beam
search, no heuristic — D4.

Scale: 18 weeks, 32 teams. The optimal solve is microseconds. Every derived
quantity in 8.4 needs at most 64 more solves and still lands well inside one
frame.

Implementation: pure-TypeScript Hungarian in
`web/src/lib/nfl/survivor-assignment.ts`, no new dependency. (`scipy` is not in
`requirements.txt`, and `javascript-lp-solver`, already present, is the wrong
tool — it is a general LP/MILP solver where a specialized O(n^3) algorithm is
both faster and exact.)

### 8.2 Feasibility

An 18-week distinct-team path always exists at season start (32 teams, 18 weeks,
16 games a week). It can become infeasible after enough teams are consumed plus
byes; the solver detects infeasibility and reports **which week cannot be
filled**, rather than returning a partial path.

### 8.3 Objective modes (D5)

| Mode | Objective | Default when |
|---|---|---|
| `SURVIVE` | max `sum log p` | pool size <= 50 entries |
| `EV` | max expected pool-win share (section 9) | pool size > 50 entries |

`EV` is `RESEARCH`-badged and cannot be the default until 13.3 passes,
regardless of pool size. Until then a large-pool user gets `SURVIVE` plus a
visible warning that it is the wrong objective for their pool size — which is
more honest than shipping an unvalidated EV number as the recommendation.

### 8.4 FSV, defined exactly (fixes 2.4)

Let `V*` be the optimal remaining-path log-survival.

**Opportunity cost of a specific pick** — the number that should drive this
week's decision:

```
cost(t, w) = V* - V*(t forced into week w)
```

so the decision rule for the current week is
`argmax_t [ log p(t, w_now) - cost(t, w_now) ]`, which the forced solve gives
directly. 32 solves.

**Future survivor value of a team** — the grid's FSV column:

```
FSV(t) = V* - V*(t banned from every remaining week)
```

i.e. how much the whole plan degrades if this team is never available again. 32
solves. This is the exact quantity the baseline approximates.

Both are reported in log units and also as a probability delta
(`exp(V*) - exp(V*_constrained)`), because "this pick costs you 1.8% of your
season survival probability" is a sentence a user can act on and "-0.018 nats"
is not.

### 8.5 User constraints

The solve accepts, all as hard constraints on the cost matrix: teams already
used (per entry), teams banned by the user, teams forced into a specific week,
weeks excluded from the pool (e.g. pool ends W17), and a minimum-probability
floor per week.

### 8.6 Multi-entry

For N entries, N materially different paths are wanted, not N copies of the best
one. Two options, in order of preference:

1. **K-best assignments** via Murty's algorithm — exact; gives the true top-K
   distinct paths with their exact values, so the user can see what the 2nd-best
   path costs. Feasible at this scale for K up to ~50.
2. **Sequential ban** — solve, ban the first pick, re-solve. Cheaper, and what
   ships first if Murty is deferred.

Correlation between entries is the real question in a large pool (N entries all
alive or all dead is worthless), and it belongs to the EV model, not here. V1
ships option 2 with the diversification caveat stated.

---

## 9. Field / EV mode (v2, gated)

The objective in a large pool is not survival, it is **share of prize**:

```
E[win share] ~= P(I survive to the end) * E[ 1 / (1 + rivals surviving) ]
```

which requires modeling the field. The plan:

1. Ingest `P%` per (team, week) from 5.3.
2. Simulate: for each of M trials, draw game outcomes from the model
   probabilities — **correlated within a week through the shared game outcome**,
   not independently, since every entry on the same team is perfectly correlated
   and ignoring that is the easiest way to get this badly wrong — advance the
   field per the pick distribution, advance your entries per your path, and
   score your prize share.
3. Optimize your path by local search over the assignment solution rather than
   exactly. The EV objective is **not separable** across weeks (your value in
   week 10 depends on how many rivals week 3 eliminated), so the Hungarian
   guarantee does not survive here. This must be stated in the code and the UI:
   `SURVIVE` mode is provably optimal, `EV` mode is a heuristic search.

That loss of exactness is a real cost, and it is why `EV` is gated rather than
shipped as the headline.

---

## 10. Schema

New tables. All timestamps `TIMESTAMPTZ`. Ledger tables are append-only and
carry `model_version`.

**`nfl_season_games`** — the full-season grid, canonical from nflverse.

```
id | season | week | game_type | nflverse_game_id UNIQUE | matchup_id -> nfl_matchups(id) NULL
gameday DATE | kickoff | home_team_id | away_team_id | div_game | roof | surface
home_rest | away_rest | quoted_spread_line | quoted_total_line
quoted_home_ml | quoted_away_ml | quote_source | home_score | away_score | completed
source_captured_at | source_hash
UNIQUE(season, week, home_team_id, away_team_id)
```

`matchup_id` links to the existing Odds API row when one exists; it is a link,
not a duplicate identity. NFL flex scheduling moves kickoff within a week, so
`gameday`/`kickoff` are mutable and every change appends to:

**`nfl_season_game_revisions`** — append-only
`(game_id, revision_hash, gameday, kickoff, captured_at)`.

**`nfl_team_ratings`** — append-only, as-of.

```
id | season | as_of_week | as_of_at | team_id | rating | hfa | n_games_fit
lambda | fit_rmse | model_version
UNIQUE(season, as_of_week, team_id, model_version)
```

**`nfl_spread_horizon_calibration`** — the 7.1 table, regenerated per fit.

```
id | fit_at | season_range | horizon | n | rmse | model_version
```

**`nfl_game_win_probs`** — append-only probability snapshots.

```
id | game_id -> nfl_season_games | team_id | p_win | p_tie | p_advance
provenance ('market_ml_novig'|'market_spread'|'model_spread'|'blocked')
spread_used | spread_source | book | horizon_weeks | sigma_h
odds_snapshot_id | computed_at | model_version
```

**`survivor_pick_popularity`**

```
id | season | week | team_id | pick_pct | source | source_url | captured_at
raw_hash | UNIQUE(season, week, team_id, source, captured_at)
```

**`survivor_pools`** — user config, mutable.

```
id | name | season | entry_count | pool_size | tie_rule ('tie_loses'|'tie_survives')
strikes | start_week | end_week | allow_reuse_from_week NULL | created_at
```

**`survivor_entries`** / **`survivor_entry_picks`**

```
entries: id | pool_id | label | status ('alive'|'eliminated') | strikes_used
picks:   id | entry_id | week | team_id | game_id | locked_at
         result ('pending'|'won'|'lost'|'push') | settled_at
         UNIQUE(entry_id, week)
```

A pick becomes immutable at the earlier of user lock and kickoff.

**`survivor_recommendations`** — the accountability ledger (D6).

```
id | pool_id | entry_id | season | week | recommended_team_id | p_advance
objective_mode | path_json | path_survival_prob | fsv_json | pick_pct_at_rec
alternatives_json | constraints_json | model_version | frozen_at | event_commence
result | settled_at
```

Frozen before the week's first kickoff and never rewritten; a changed
recommendation appends a new row and supersedes, exactly like `mlb_bets`.

---

## 11. Modules

### Python (ingestion, ratings, probabilities)

| File | Responsibility |
|---|---|
| `ingest/nfl_season_schedule.py` | nflverse `games.csv` -> `nfl_season_games` (+ revisions). Fails closed on unmapped team code. Links `matchup_id` where the Odds API row exists |
| `ingest/survivor_pick_popularity.py` | survivorgrid `/{season}/{week}` -> `survivor_pick_popularity`. Stores `raw_hash`; fails visibly on parse-shape change |
| `model/nfl_spread_prob.py` | Fits 6.1 on the historical corpus; exposes `spread_to_prob`, `tie_prob`; writes the fit record |
| `model/nfl_power_ratings.py` | Ridge fit of section 7; writes `nfl_team_ratings`; regenerates `nfl_spread_horizon_calibration` |
| `model/nfl_win_probs.py` | The 6.2 ladder + 6.4 widening -> `nfl_game_win_probs` |
| `model/survivor_backtest.py` | Section 13. Offline, never imported by the web app |
| `ingest/refresh_nfl_survivor.py` | Orchestrator; run weekly. Health gates per 11.1 |

### TypeScript (interactive solve + UI)

| File | Responsibility |
|---|---|
| `web/src/lib/nfl/survivor-assignment.ts` | Hungarian solve, forced/banned variants, K-best. Pure, no I/O |
| `web/src/lib/nfl/survivor-policy.ts` | Objective modes, FSV, constraint assembly, the decision rule. Single source of the thresholds; components never recompute them |
| `web/src/db/queries.ts` | `getSurvivorGrid`, `getSurvivorPool`, `getSurvivorRecommendationHistory` |
| `web/src/app/nfl/survivor/page.tsx` + `survivor-client.tsx` | Section 12 |
| `web/scripts/test-survivor-assignment.ts` | `npm run test:survivor` |

### 11.1 Health gates

`refresh_nfl_survivor.py` fails loudly, and **each gate blocks only what it
covers** — the lesson from the MLB bullpen-gate incident, where one miscounting
health check silently disabled unrelated prop capture for weeks:

- schedule: all 32 teams appear in exactly 18 weeks with exactly one bye.
  Blocks the grid.
- ratings: fit converged, `n_games_fit >= 48`. Blocks modeled cells only;
  quoted-line weeks still render.
- popularity: parse succeeded. Blocks EV mode only; never the grid.

No gate counts rows from an append-only table for equality (the standing rule
from that same incident).

---

## 12. `/nfl/survivor` requirements

### 12.1 Header

Pool selector; entry selector; objective toggle (`SURVIVE` / `EV`, EV badged
`RESEARCH`); week selector; last refresh and source ages; `Optimize`; CSV
export; reset.

Summary strip, matching and extending the baseline's: `PICKED n/18`, `AVG WIN`,
and — the one the baseline omits and is the actual headline —
**`PATH SURVIVAL`**, the product of the path's probabilities. A path of eighteen
75% picks has an average win of 75% and a survival probability of 0.6%. Showing
the average without the product is the more flattering of the two numbers and
the less useful one.

### 12.2 The grid

Rows = teams (sortable: this week's win %, FSV, A-Z, availability), columns =
W1..W18. Each cell: opponent (with `@` for away), spread, win %, heat shading.
Bye cells and consumed teams visually distinct. This is the baseline's layout
and it is good; keep it.

Required additions:

- **Provenance badge per cell** (D2): quoted and modeled cells are visibly
  different — a marker plus a distinct shade family, never colour alone.
- **Modeled cells show a band**, e.g. `71% ±6`, from `sigma_h`.
- **`P%` column** for the current week once 5.3 is ingested, and pick % inside
  current-week cells.
- **`AVAIL`** defined explicitly on hover: share of the team's remaining games
  still usable given consumed teams and the pool's end week.
- Virtualized rows (`@tanstack/react-virtual`, already a dependency) — 32 rows
  is small, but each cell is rich and the FF board already set this pattern.

### 12.3 Optimize result

Highlights the chosen path across the grid, and shows for the current week the
top 5 candidates with `log p`, opportunity cost, net score, and the plain
language delta ("using KC here costs 1.8% of season survival"). A path is a
proposal until the user commits the current week's pick.

### 12.4 The honesty treatment (2.2, 7.2)

Columns beyond the quoted horizon are progressively de-emphasized, and the grid
carries a permanent legend stating the measured numbers: *"Weeks with no quoted
line are modeled from market-implied ratings. Ten weeks out, the model's top
future play is the eventual best play 33% of the time and in the top five 71% of
the time. Plan with these columns; commit only the current week."*

That sentence is the product. A survivor tool that quietly implies it knows the
Week 15 best play is lying, and the measurement to prove it is in section 7.

---

## 13. Validation

### 13.1 Backtest — path planning vs greedy (26 seasons)

The corpus exists: 6,967 games since 1999 with closing spread and result. Replay
every season, walk-forward, using only lines available at decision time:

| Strategy | Description |
|---|---|
| B0 | Biggest weekly favorite by closing spread (the naive baseline, and what most pools' median entry does) |
| B1 | Biggest favorite subject to not-yet-used |
| S1 | This spec's `SURVIVE` assignment, re-solved weekly |

Metrics: mean weeks survived, P(survive to W9 / W13 / W17), and the full
survival curve. Report all seasons pooled and per season, with a bootstrap CI on
the S1 - B1 difference.

**This is not an edge claim and does not need a pre-registration.** S1 is
provably at least as good as B1 on the *modeled* objective by construction; the
backtest measures whether that advantage survives contact with real outcomes and
how large it is. If S1 does not beat B1 on realized survival, that is a finding
about the probability inputs, not about the optimizer, and must be reported as
such.

### 13.2 Calibration

Reliability of `p_advance` by decile against realized results, split MARKET vs
MODEL and by horizon bucket. Modeled cells being *less* calibrated than quoted
ones is expected; the question is by how much, and whether 6.4's widening
actually helps. Publish it either way.

### 13.3 Pre-registered study — does fading the field help? (gates `EV` mode)

Fixed now, before the data is examined.

**H:** In a pool of >= 100 entries, an EV-optimized path produces a higher
expected prize share than the `SURVIVE` path.

**Population:** 2025 season, all 18 weeks, using survivorgrid's archived `P%` as
the field distribution. Simulated pools at 100 / 500 / 2,000 entries, 10,000
trials each; field picks drawn from `P%`; outcomes drawn from the week's market
probabilities with within-week correlation.

**Primary metric:** mean prize share, bootstrap CI over trials.

**Floors:** one season is one season. `n = 1` field-season is **below any
defensible floor**, and this is stated up front rather than discovered later.
The 2025 result can therefore only ever be *directional*; `EV` mode stays
`RESEARCH`-badged until at least **three** seasons of archived `P%` exist and
the effect holds in each independently.

**Kill criterion:** if the 2025 simulation shows no positive effect at any pool
size, `EV` mode is not built. No re-slicing to a specific pool size or week
range afterwards — that would be exactly the multiple-comparisons drift that
produced the soccer totals mirage.

**Honest prior:** the theory is sound and the mechanism is real. The risk is not
that the idea is wrong; it is that `P%` from a national aggregator does not
describe the user's actual pool, in which case a correct model is being fed the
wrong field. That is a data problem the simulation cannot detect, and it is the
most likely way this ships something useless.

### 13.4 Weekly accountability

Every frozen recommendation (D6) settles against the real result. The page
reports realized vs expected advance rate by week and by objective mode, with
the sample size always visible — including at small n, where the number is shown
with its raw record rather than hidden behind a floor.

---

## 14. Testing

**Unit (Python):** team-code mapping fails closed on an unmapped code; spread
sign convention (a known 2025 game reproduces its actual winner); tie handling
under both rules; no-vig pairing rejects mixed-book and mixed-capture pairs;
horizon widening monotonically pulls toward 0.5; the ridge fit recovers a
synthetic rating set.

**Unit (TypeScript, `npm run test:survivor`):** the assignment solve matches
brute force on a 6x8 fixture over all permutations; consumed teams and byes are
respected; infeasibility reports the specific unfillable week; forced-assignment
FSV equals `V* - V*_forced` on a hand-computed fixture; K-best returns strictly
non-increasing values; the solve is deterministic across runs.

**Integration:** a frozen 2026 Week 1 fixture (real nflverse rows plus real
captured odds) produces a byte-stable grid and path.

**Property:** for any random cost matrix, no feasible permutation beats the
solver's answer.

---

## 15. Rollout

| Phase | Scope | Done when |
|---|---|---|
| P1 | `nfl_season_games` ingest + team-code mapping + health gate | 272 rows for 2026; 32 teams x 18 weeks x exactly one bye each, verified against a live run |
| P2 | `nfl_spread_prob` + `nfl_power_ratings` + `nfl_win_probs` | Every 2026 cell has a probability with a provenance value; the 7.1 calibration table is regenerated and stored |
| P3 | Assignment solver + tests | `npm run test:survivor` green including brute-force equivalence |
| P4 | `/nfl/survivor` grid + Optimize + provenance/uncertainty treatment | Screenshot parity plus 12.4, verified in the browser |
| P5 | Pool config, entries, weekly recommendation ledger + settlement | A frozen recommendation settles correctly against a real Week 1 result |
| P6 | `model/survivor_backtest.py` (13.1, 13.2) | Results published in this doc |
| P7 | Pick-popularity ingest + the 13.3 study | Study run and graded against its own kill criterion |
| P8 | `EV` mode | Only if P7 passes; ships `RESEARCH`-badged regardless |

P1-P4 is the usable tool and matches the baseline. P5-P6 is what makes it
trustworthy. P7-P8 is what would make it better than the baseline, and may not
happen.

---

## 16. Open questions

1. **Does the user's pool resemble the national field?** Determines whether
   sections 9 and 13.3 are worth building at all. Cheapest answer: capture the
   pool's actual Week 1 pick distribution once and compare against `P%`.
2. **Pool rule presets.** Circa Survivor, ESPN Eliminator and Yahoo differ on
   ties, deadlines and Week 18. Presets must be verified against each operator's
   current published rules before shipping; this spec deliberately does not
   assert them from memory.
3. **Multi-pick weeks.** Some pools require two picks in later weeks. It is a
   trivial extension of the assignment (duplicate the week's row) but changes
   feasibility; deferred until a real pool needs it.
4. **A free full-season lookahead line source** would replace section 7 entirely
   and is the highest-value upgrade available (5.4).
5. **Cadence.** The grid is only as fresh as the existing NFL odds capture. This
   spec neither requires nor justifies a cadence increase; the standing
   UNDECIDED question in CLAUDE.md is unaffected.
