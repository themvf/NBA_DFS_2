# NFL Slate Specials — Our Own Numbers for DK's Sunday Exotics

**Status: SPEC, for review. Nothing built. No edge claimed.**
Registered 2026-09-19, before any of these markets has been priced by us,
scraped into a ledger, or backtested.

---

## 1. What these markets are

The screenshots cover six DK "Week N Specials" families, all scoped to a
**slate** (all Sunday games, or all 1pm ET games) rather than to a single game:

| Family | Example | Outcomes |
|---|---|---|
| 1st TD scorer, slate-wide | Derrick Henry +2200 | ~250 players |
| 1st QB to throw a TD pass | Lamar Jackson +750 | ~28 QBs |
| 1st QB to throw an INT | Deshaun Watson +700 | ~28 QBs |
| Highest / lowest scoring game | WAS@DAL +550 | ~13 games |
| Highest / lowest scoring team | SF +800 | ~26 teams |
| Weekly leaders (most pass yds, most rec yds) | Dak +750 | ~30-100 players |

**The one thing they have in common, and it is the whole spec:** every one is
an **order statistic over the slate**, not a property of a player or a game.
"Most receiving yards on Sunday" is not a question about Jaxon Smith-Njigba;
it is a question about the joint distribution of ~100 receivers at once. No
amount of per-player point projection answers it, because the answer depends
entirely on the *tails* and on how those tails move together.

That has one architectural consequence: **these markets are readouts of a
single slate-wide Monte Carlo, not six separate models.** One set of draws,
six `argmax`/`max`/`first` functions over it. Building them as six models
would guarantee mutual inconsistency — a board where the team most likely to
score most points is in a game that is not the highest-scoring game.

---

## 2. What we already have, and the exact gaps

Three existing assets cover most of this, and the gaps are specific.

| Asset | What it gives | Gap for this |
|---|---|---|
| `model/nfl_dfs_efficiency.py::simulate_team()` | A **correlated team Monte Carlo**: samples team attempts/carries/targets, allocates them multinomially across the roster under a shared team TD budget, then draws yards and TDs per player. This is exactly the right engine. | Runs **per team in isolation**. No opponent coupling, no game total, no slate. And it has **no clock**: a draw says Henry scored 2 TDs, never *when*. |
| `nfl_game_win_probs` + `nfl_season_games` | Per-game market spread/total/ML with explicit `provenance` (MARKET vs MODEL vs BLOCKED) and horizon-widened sigma. | Gives a game's **mean** total and margin. Gives no joint score distribution and no per-team point split beyond the implied total. |
| `nfl_pbp_archetypes` (per play: `game_seconds_remaining`, `drive`, `drive_archetype`, `posteam`, `yardline_100`, `epa`, `wp`) + `nfl_pbp_play_participants` (who scored) | The only source of **timing** in the repo. Seasons of drive-level terminal outcomes with a clock on every play. | Never yet used forward. It is a labelling layer; this spec is its first predictive consumer, which is a bar to clear, not a shortcut. |

**The missing piece is timing, and only three of the six families need it.**
Highest/lowest game, highest/lowest team, and weekly leaders are pure
magnitude — reachable from a coupled scoring sim alone. 1st TD scorer, 1st QB
TD pass, and 1st INT are `argmin` over **event times across simultaneous
games**, which is a genuinely different object and the part PBP has to supply.

---

## 3. Architecture — one simulator, four layers

`model/nfl_slate_specials.py`, model version `nfl-specials-v1`.
N = 50,000 slate draws. One RNG seed per (season, week, slate_scope), stored.

```
Layer A  GAME SCRIPT        per game: (home_pts, away_pts) joint draw
Layer B  TEAM BOX           per team: stat lines consistent with A's points
Layer C  EVENT TIMELINE     per scoring/turnover event: a clock time
Layer D  READOUTS           six market families, same draws
```

### Layer A — game script

Per game, draw a `(margin, total)` pair from the market's own numbers:
margin ~ N(spread, 13.2), total ~ skewed around `quoted_total_line`.
**13.2 is not a free parameter** — it is the residual SD of margin against
the closing spread, and this repo has already measured it as flat at ~12.6
across every total band (`analyze:underdogs`, pick'em work). Re-measure on
`nfl_season_games` 1999-2025 and freeze; do not tune.

Two rules carried over from MLB totals, both load-bearing:

- **Totals are skewed; do not assume mean = median.** Fit the total's
  predictive distribution from empirical residuals of `actual_total −
  quoted_total_line` by total band, not from a symmetric parametric family.
  The MLB mean-vs-median bug cost a documented −14% ROI by making exactly
  this assumption.
- **Football scores are lattice-valued.** 3 and 7 are common, 5 nearly never.
  Draw points from an empirical score distribution conditioned on
  (implied team total, spread), not from a continuous normal that is rounded.
  A continuous approximation puts mass on impossible scores and systematically
  misprices "highest scoring team," which is decided by 3-point granularity.

`provenance` propagates: a game whose spread is MODEL rather than MARKET
widens, and any market whose argmax depends materially on a BLOCKED game is
itself blocked, not silently completed.

### Layer B — team box score

`simulate_team()` runs conditioned on Layer A's drawn team points, not on a
season-average implied total. Concretely: the drawn points set the team's
**touchdown budget** for that draw (points decomposed into TD/FG/other under
the lattice), and the existing multinomial allocation distributes those TDs
and the pass/rush/target budget across the roster as it already does.

This is the coupling that makes the board internally consistent: in a draw
where DAL@WAS goes 38-34, Lamar's TD count and CeeDee's yards are drawn from
that same high-scoring world, so "highest scoring game" and "most receiving
yards" cannot contradict each other.

**Inputs are the existing pipeline's, unchanged.** `nfl_dfs_player_projections`
supplies `stat_means` and the component shares; the out-player redistribution
layer (`nfl-dfs-redistribution-v1`) applies first so a ruled-out starter's
opportunity is already reallocated before any draw is taken.

### Layer C — event timeline (the new work)

For each drawn team, convert its scoring events into **times**. Estimate from
`nfl_pbp_archetypes`, which already carries `game_seconds_remaining` and
`drive_archetype` on every play:

1. **Hazard of a drive ending in TD, by drive index**, from the actual
   distribution of `drive_archetype = 'TOUCHDOWN'` over drive number. The
   opening drive is not the same as the fifth; the market's first-TD prices
   are dominated by who gets the ball first.
2. **Drive duration distribution**, from `drive_time_of_possession`, so a
   team's k-th drive lands at a real clock time rather than a uniform one.
3. **Who scored it**, from the Layer B allocation restricted to that drive's
   scoring type — a rushing TD goes to the rusher share, not the target share.
4. **Kickoff offsets** across the slate. A 1pm ET slate starts ~simultaneously
   but not exactly; the *actual* variance that matters is which offense gets
   the first possession, and the coin toss is a real 50/50 that must be
   drawn, not assumed. For 1pm-scoped markets, **games outside 1pm ET are
   excluded, not down-weighted** — DK's scope is the market definition.

Interceptions use the same machinery against `turnover_type` / `play_archetype`.

**This layer is the spec's weakest link and is labelled as such.** Everything
above it is a recombination of things already measured. A forward drive-hazard
model from labelled PBP is new, is being asked to predict something, and must
clear Section 6's calibration bar before it prices anything.

### Layer D — readouts

Over the same N draws:

```
highest_scoring_game   argmax_g (home_pts_g + away_pts_g)
lowest_scoring_game    argmin_g (same)
highest_scoring_team   argmax_t pts_t
most_receiving_yards   argmax_p rec_yards_p
first_td_scorer        argmin_e time_e  over all TD events, take scorer
first_qb_td_pass       argmin_e time_e  restricted to passing TDs, take passer
first_qb_int           argmin_e time_e  restricted to INT events, take thrower
```

`our_prob(x) = count(x wins) / N`. **Ties are a real outcome with real book
rules** (two games finishing on the same total; DK's rule decides dead-heat
vs push). Store `p_tie` separately; a market whose rule we have not read is
`BLOCKED`, never assumed.

Monte Carlo error at N=50,000 on a p=0.03 outcome is ±0.15pp — small against
a 2-4pp de-vigged market gap, but report it and never quote an edge smaller
than 2× it.

---

## 4. The market side — de-vigging a 250-way market

These are the highest-hold markets DK offers and the overround must be
measured before anything is called value.

**Reuse `model/soccer_first_scorer.py` wholesale.** First-TD-scorer is
mathematically identical to soccer first goalscorer, a problem this repo has
already solved twice and got wrong once:

- **Power-method de-vig, not proportional.** Books vig longshots harder;
  proportional normalisation deflates favourites. v1 of the soccer model put
  Haaland at 13% against a true ~23% for exactly this reason.
- **Anchor the de-vig to our own total.** Solve the exponent `k` such that
  `Σ_p −ln(1 − p^k)` equals our modelled expected slate TD count. This
  removes vig and ties the market to our number in one step.
- **Drop glitch lines before normalising.** One stale price corrupts a
  mutually-exclusive normalisation across 250 selections.
- **EV uses the best offered price; edge uses the de-vigged reference.**

Expect combined overround of **300-500%** on the 250-way market, as soccer
first-scorer measured. Report it on screen. A 300% overround means almost
every selection is correctly rated "avoid," and that is the honest output.

---

## 5. Correlation — what must couple, and what must not

The failure mode unique to slate markets is treating games as independent when
the readout is a max.

- **Within a game:** coupled through Layer A's joint `(margin, total)`. A
  shootout lifts both teams' players.
- **Within a team:** coupled through `simulate_team()`'s shared budgets. Two
  receivers on one team compete for the same targets — negatively correlated,
  which is precisely what makes "most receiving yards" harder than it looks.
- **Across games:** **deliberately independent.** There is no credible
  mechanism coupling DAL@WAS to SEA@ARI, and inventing one would be an
  unmeasured parameter driving every argmax. Weather is the one real
  candidate and is per-game, already in `nfl_season_games.roof/surface`.
- **QB↔his own receivers:** coupled by construction (a passing TD is
  simultaneously a receiving TD). Never drawn twice.

---

## 6. Calibration and pre-registration

Nothing here is actionable on shipping. The gate, fixed now:

**Primary metric: multi-class log score against the power-de-vigged market,
per market family, walk-forward.** Not accuracy — a 3% favourite being wrong
is not evidence.

**The two-stage bar, in order:**

1. **Calibration (must pass first).** Over a full season, selections we price
   at p must win at rate p, binned. If our 5% bucket wins 1% of the time, the
   simulator is broken and no edge discussion is permitted.
2. **Market-relative (the real question).** Our log score must be **no worse
   than** the de-vigged market's on the same events. This repo's standing
   record is 0-for-9 on beating closing lines; matching it would already be
   the best result here.

**Minimum sample, frozen:** 17 slates (one season) **and** ≥ 200 settled
selections per family, whichever is later. A single Sunday is one draw from
the joint distribution, not 250 observations — the selections within a slate
are **mutually exclusive and maximally correlated**, so the effective sample
is closer to the number of slates than the number of prices. Cluster every
bootstrap by slate. This is the single most likely way to fool ourselves here
and is why the floor is stated in slates.

**Kill criterion:** if calibration fails at 17 slates, the simulator is wrong
and the section stays descriptive-only pending a re-spec. If calibration
passes but market-relative log score is worse with a CI excluding zero, that
is the **tenth confirmed negative** and these markets join the 2★-capped list
permanently. No re-slicing to "just the 1pm markets," no threshold retune.

**Star ratings:** all selections hard-capped at **2★** until the above passes,
via the existing `rate_market(max_stars=2)` in `model/soccer_bet_rating.py`.
`longshot_odds_cap=True` is mandatory — at +4000 a 0.5pp model error
manufactures a fake 5★, which is the exact guard that constant exists for.

---

## 7. Ledger

Reuse the existing pattern; no new shape.

- `nfl_specials_bets` mirroring `soccer_bets`: one row per (market_family,
  slate_scope, selection, model_version), locked at the slate's first kickoff,
  with `inputs_json` freezing the simulator's own inputs (run seed, N,
  provenance map, redistribution state) so any rating replays exactly.
- `nfl_specials_snapshots` append-only, per refresh.
- Settlement from `nfl_pbp_archetypes` itself — it already records the scoring
  play and its clock, so first-TD settles from our own labelled data with no
  new feed. **Timing settlement is exact, not best-effort**, which makes these
  markets unusually gradable compared to soccer ATGS.
- **Void handling stated up front:** a player inactive at kickoff voids at most
  books. We have `nfl_dfs_official_availability`; if it is missing for a
  selection, the row settles `unresolved`, never as a loss. Soccer ATGS's
  DNP-as-loss bias is a known, documented distortion — do not repeat it.

---

## 8. UI — `/nfl` → "Slate Specials" tab

One tab, six collapsible panels, matching the DK families so the two boards
are visually diffable.

Each row: **selection · our prob · de-vigged market prob · offered price ·
edge (pp) · EV · stars · MC error band.**

Non-negotiables inherited from the MLB v3 contract:

- A `RESEARCH — no validated edge` banner until Section 6 passes. Green is
  reserved for a passed gate, never for a positive edge number.
- Market overround printed per family. A 340% board is the headline fact about
  that board and hiding it would be the single most misleading thing this page
  could do.
- Every probability shows its provenance chain; a family containing a BLOCKED
  game says so instead of quoting a number.
- Simulator inputs expandable: seed, N, which games, which players were
  redistributed, what was excluded.

---

## 9. Phases

| P | Scope | Gate |
|---|---|---|
| P0 | Measure the overround on all six families from one real DK board. Two weeks of capture, no modelling. | If hold exceeds ~200% on a family, that family is calibration-only from the start and is never framed as a betting board. Cheap, and it can kill half the work before any is done. |
| P1 | Layers A+B: coupled game-script and team-box sim. Ships **highest/lowest game, highest/lowest team** only — the three magnitude families that need no clock. | Sim's own score distribution reproduces 1999-2025 empirical score frequencies (lattice, ties, margin SD). |
| P2 | Weekly leaders (most pass/rec yards) — same draws, new readout. | Simulated yardage distributions match observed weekly maxima, not just means. |
| P3 | Layer C: PBP drive-hazard timing → first-TD, first-QB-TD, first-INT. | Backtest the hazard model on held-out seasons: does it reproduce the observed distribution of first-TD clock times and first-scorer identity? |
| P4 | Ledger + power de-vig + 2★-capped ratings, live. | — |
| P5 | Section 6 verdict at 17 slates. | PASS / FAIL / INCONCLUSIVE, computed once. |

**P1 before P3 is deliberate.** The magnitude families are reachable from
components already measured; the timing families need a new predictive layer.
Shipping the safe half first means a P3 failure costs nothing already built.

---

## 10. Non-negotiables

- One simulator, six readouts. A market computed by its own separate path is a
  bug, because the board can then contradict itself.
- Never derive a selection from a point comparison of means. Every answer is
  `argmax` over draws. This is the MLB mean-vs-median rule restated for order
  statistics, where it bites harder.
- Sample floors are in **slates**, never in selections.
- PBP archetypes are a labelling layer being asked to predict for the first
  time. That is a bar, not a credential.
- No LLM anywhere in this path.
- 2★ cap until the gate passes, `longshot_odds_cap` always on.

---

## 11. Honest prior

**Expected outcome: calibration passes, market-relative fails, no edge.**

The case for doing it anyway is narrow and real. These are exotic,
derivative, slate-scoped markets — the soft-market class where this project's
*only* measured positive signal has ever appeared (`dk_prop_value`, +1.29%
CLV). They are also almost certainly the highest-hold products DK publishes,
and the same soccer first-scorer work that supplies the de-vig math returned
**n=101, ROI −64.4%, CI entirely below zero** on a structurally identical
market. Both things are true at once.

The defensible reason to build it is the one soccer first-scorer ended up
delivering: **calibration, not picks.** A working slate simulator is the first
thing in this repo that can answer "how likely is this Sunday to produce a
40-point team," and it is reusable by the DFS ceiling model, the pick'em
board, and survivor — all of which currently approximate a joint slate
distribution they do not have. The betting board is the cheapest way to *grade*
that simulator honestly, because the market gives a free per-slate benchmark.

Do not expect it to find bets. Expect it to tell us whether our player and PBP
architecture composes into a coherent picture of a Sunday. If it does not, that
is worth knowing and this is how we would find out.
