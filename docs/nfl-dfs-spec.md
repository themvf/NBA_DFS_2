# DraftKings NFL DFS — Spec (revised 2026-09-05)

Target: a dedicated `/dfs/nfl` workspace covering both DK NFL contest
structures (Classic weekly, Showdown single-game) and both contest
intents (PvP/cash, Tournament/GPP), fed by a manually uploaded DK salary
CSV, producing a DK bulk-entry-uploadable lineup CSV.

**Implementation amendment (2026-09-02):** `/dfs/nfl` now persists the DK
salary pool, links it to an immutable model run, and supports Classic and
Showdown portfolio construction. The UI includes source comparisons,
locks/exclusions, cash/GPP objectives, salary and uniqueness controls,
exposure caps, QB stacks and bring-backs, lineup/exposure review, and DK
entry-template export. Every run stores its settings, input snapshot, digest,
and generated lineups.

FantasyPros, LineStar, and custom CSVs remain labeled comparison or explicitly
selected optimization inputs. They are never blended into `ourProj`. Because
the first walk-forward model did not beat its recency baseline, the page keeps
the research-only status visible without adding a confirmation gate.

## Decisions taken 2026-09-01

User-selected, recorded so they are not silently revisited.

| Decision | Choice | Consequence |
|---|---|---|
| Projection source | **Build our own weekly model** | The page does not ship before the model exists. Dependency order below is data → model → UI, matching the standing Jira-First Delivery Contract. |
| Route | **Dedicated `/dfs/nfl`** | New page, new server actions, new optimizer module. Reuses the sport-agnostic `dk_slates`/`dk_players`/`dk_lineups` tables. Cannot regress live NBA/MLB, which share an 8,551-line `actions.ts` and a 4,177-line client. |
| Export | **DK bulk-entry upload format only** | Requires the user to upload the entry file downloaded from the DK contest; entry IDs come from that file, not from us. A plain review CSV was explicitly not requested and is not built. |

## Architecture amendment — 2026-09-05

The user approved the design review amendments and requested an assessment
of Kelly sizing. This revision specifies the intended implementation; it
does not claim the simulator, field model, portfolio solver, or Kelly
allocator exists. The original route, manual contest selection, and DK
bulk-entry export decisions remain the product contract.

The implementation sequence is:

```
Timestamped inputs -> coherent game scenarios -> legal candidate scores
                   -> contest field and payouts -> portfolio selection
                   -> optional, separately validated bankroll allocation
```

- Compute lineup distributions from aligned scenario draws. Summed player
  quantiles are a comparison baseline only.
- Correct anytime-touchdown market interpretation before building the
  projection model. Never normalize anytime scorers as exclusive outcomes.
- Add ownership upload and field-model contracts; keep LineStar optional.
- Define portfolio utility, exposure counts, overlap, and feasibility.
- Separate deterministic correctness from empirical promotion evidence.
- Include Kelly as a later, disabled-by-default allocation capability,
  conditional on calibrated net-payout distributions (section 8).

Engineering fixtures and the first ranking experiment may be developed
after their data-contract prerequisites pass, before predictive
promotion. User-facing contest-performance claims require the later
chronological gates. Sections 7 and 9 define this distinction.

The user subsequently directed: **"Skip Jira for now. Just start
implementation."** Jira synchronization is waived for this work. The
first implementation slice is the TypeScript scenario scorer, legal
candidate search, and synthetic experiment described in
[`nfl-dfs-scenario-harness.md`](nfl-dfs-scenario-harness.md). Real-data and
model-promotion gates still apply; this waiver does not qualify a model.

## 1. What DraftKings actually requires — verified, not assumed

Everything in this section is now **VERIFIED** against DK's official
Classic and Showdown Captain Mode rules pages, supplied by the user
2026-09-01 after automated access failed (403 to fetch, domain blocked
in the browser tool), plus the user's own Week 1 exports for everything
the files themselves prove.

The scoring table previously recorded here from secondary sources turned
out to be **correct in every line**, including the DST points-allowed
tiers. Two things it was missing or wrong about are corrected below and
called out explicitly rather than quietly patched.

### The real column layout — VERIFIED against Week 1 exports

Both files (Classic, 719 rows; Showdown, 126 rows) carry:

```
Position, Name + ID, Name, ID, Roster Position, Salary,
Game Info, TeamAbbrev, AvgPointsPerGame, Status
```

A UTF-8 BOM precedes `Position`. Two columns matter more than they look:

- **`Status` exists**, with values `OUT`, `IR`, `Q` and blank — 56 of the
  719 Classic players are `IR`/`OUT` and cannot play. The NBA/MLB parser
  in `actions.ts` states the opposite (*"CSV doesn't carry DK injury
  status — rely on LineStar for is_out"*). That is true for NBA and
  **false for NFL**, so NFL needs no LineStar dependency for
  availability. An `IR` player left in the pool silently wastes a roster
  slot.
- **`Position` and `Roster Position` diverge completely on Showdown**
  (`Roster Position` is `CPT`/`FLEX` there), which is why both are read.
  On Classic, `Roster Position` carries the FLEX eligibility directly:
  `QB`, `RB/FLEX`, `WR/FLEX`, `TE/FLEX`, `DST`.

`Q` (questionable) is treated as risk, not absence — whether to accept it
is a contest-type judgement, so the parser flags it and the optimizer
decides.

### Classic (weekly) — VERIFIED

```
QB  RB  RB  WR  WR  WR  TE  FLEX  DST      9 players, $50,000 cap
FLEX = RB / WR / TE.  No kicker in DK NFL Classic.
```

The no-kicker claim is now verified against a real export rather than
inferred: the Week 1 Classic pool is 91 QB / 153 RB / 295 WR / 156 TE /
24 DST and **zero K**. Showdown pools *do* include kickers (2 in the
NE@SEA file), so the position set is format-specific, not sport-wide.

**Correction (2026-09-01).** An earlier draft of this spec recorded the
Classic constraint as *"at least 2 different NFL teams AND at least 2
different games"*, taken from a secondary source. DK's official rule is
narrower — **"must include players from at least 2 different NFL
games"**, and nothing about a team count. The team clause was redundant
anyway (two games implies at least two teams), so the effect is one
fewer constraint for the optimizer to enforce, not a behaviour change.

The games rule is still real and still load-bearing: neither the NBA nor
the MLB optimizer in this repo has anything like it, so it is new code.

### Showdown Captain Mode (single game) — VERIFIED

```
CPT  FLEX  FLEX  FLEX  FLEX  FLEX          6 players, $50,000 cap
CPT scores 1.5x and costs 1.5x salary.
Every slot, CPT included, is eligible to QB / RB / WR / TE / K / DST
  (Showdown is the only DK NFL format where two QBs are legal, and a
   DST or a kicker may be captained).
Lineups must include players from BOTH teams.
```

DK states the duplicate-row trap as a rule in its own words: *"a player
cannot be added at both Captain and another position in the same
lineup."* That is exactly the constraint the parser's CPT/FLEX collapse
exists to make representable.

The DK Showdown salary CSV lists **each player twice** — one `CPT` row
and one `FLEX` row, with different `ID` and different `Salary`. The
ingestion must treat those two rows as one underlying player with two
purchasable roles, or the optimizer will happily roster the same human
twice.

Confirmed on the real NE@SEA file: 126 rows collapse to **63 players**,
every one priced at both CPT and FLEX, with the CPT/FLEX salary ratio
exactly 1.500 across all 63 (min 1.500, max 1.500). The parser still
reads DK's own CPT salary rather than deriving it — the multiplier is
only a fallback, since a derived value would drift the moment DK rounds.

The two rows are **not adjacent**: DK sorts the file by salary, so a
player's CPT row can sit dozens of lines from his FLEX row. Pairing is by
name + team, never by position in the file.

### Scoring — VERIFIED against DK's official rules

Identical between Classic and Showdown, except that Showdown adds
kickers and multiplies the captain.

Offense (full PPR):

```
Passing TD            +4      Rushing TD              +6
Passing yard        +0.04     Rushing yard           +0.1
300+ pass yd game     +3      100+ rush yd game       +3
Interception          -1      Reception               +1
Fumble lost           -1      Receiving yard         +0.1
2pt conversion        +2      Receiving TD            +6
Return TD             +6      100+ rec yd game        +3
```

DST:

```
Sack +1 | INT +2 | Fumble recovery +2 | Safety +2 | Blocked kick +2
Any defensive or special-teams TD +6 | 2pt return +2
Points allowed:  0 = +10 | 1-6 = +7 | 7-13 = +4 | 14-20 = +1
                21-27 = 0 | 28-34 = -1 | 35+ = -4
```

**Kicker — Showdown only** (Classic has no kicker slot at all):

```
Extra point +1 | 0-39 yd FG +3 | 40-49 yd FG +4 | 50+ yd FG +5
```

Two notes on this, both load-bearing:

- DK restricts these categories to kickers: *"Kickers are only eligible
  for extra points and field goals made. Non-kickers are not eligible."*
  So a position player who kicks in an emergency scores nothing for it,
  and the scorer must gate on position rather than on the stat's
  presence.
- This is **exactly Yahoo's distance-tiered kicker formula**, which this
  repo already implements as `yahoo_kicker_points()` in
  `ingest/ff_independent.py`, fed by the same nflverse per-distance
  buckets (`fg_made_0_19` … `fg_made_60_`). The DFS kicker path should
  reuse those buckets rather than re-derive them — and note the trap
  already recorded for fantasy football: nflverse's generic
  `fantasy_points` field is **0 for essentially every kicker**, so
  scoring must come from the buckets, never from that column.

**Points Allowed is not the opponent's final score.** DK: *"Points
Allowed only includes points surrendered while DST is on the field —
doesn't include points given up by team's offense."* A pick-six thrown by
your DST's own offense does not count against it. Special-teams and
blocked-kick return TDs, extra points and field goals all do.

Section 3 therefore derives points allowed from each scenario's scoring
events. Using the opponent's implied total directly both misattributes
offensive concessions and evaluates a nonlinear scoring tier at a mean.
The resulting fantasy-point bias must be measured, not assumed small.

The three yardage **bonuses are threshold events, not linear terms**.
A mean-based projection cannot price them: a 250-yard mean, and a
250-yard mean with twice the variance, carry different bonus
expectation. This is the same mean-versus-distribution error already
recorded in this repo's MLB Totals section, and it is why the model is
simulation-based rather than a point estimate (section 3).

## 2. Data sources

| Input | Source | Status today | Work needed |
|---|---|---|---|
| Player pool, salaries, roster positions, game info, **injury status** | **User-uploaded DK CSV** | ✅ **Built + validated** — `web/src/lib/nfl-dfs/dk-salary-csv.ts` | Done. Validated against real Week 1 Classic (719 players / 12 games / 24 teams) and Showdown (63 players / 1 game / 2 teams) exports, 0 warnings on both. |
| Game environment: total, spread, implied team totals | `nfl_matchups` | **Live**, `refresh_nfl_vegas` 1×/day, 16 credits | Nothing. Already flowing. |
| Player prop lines | The Odds API, `americanfootball_nfl` | **Not ingested** | New `ingest/nfl_prop_odds.py` |
| Weekly actuals + recent form | `ff_player_week_stats` | Prior completed seasons only | In-season weekly refresh |
| Roster, position, team, depth | `ff_players` (nflverse) | Live | Reuse |
| Projected ownership | User upload initially; optional LineStar later | Upload not implemented; LineStar NFL sport id unknown | Timestamped Classic / CPT / FLEX ownership contract in section 4 |
| Contest terms and historical field | User-selected contest metadata, DK entry/standings files | Historical availability not verified | Entry fee, payout ladder, field size, entry limits, lock policy, complete opponent lineups and actual results |

**Initial availability does not need LineStar.** DK's uploaded `Status`
column supplies `OUT`/`IR`/`Q`. Its capture time must be shown: an old CSV
is not a live injury feed. Later availability updates need a timestamped
source and explicit player matching. LineStar remains an optional
ownership provider, never a prerequisite for loading a slate.

### Prop markets — keys confirmed, coverage NOT confirmed

Confirmed against The Odds API market documentation 2026-09-01:

```
player_pass_yds   player_pass_tds   player_pass_interceptions
player_rush_yds   player_receptions player_reception_yds
player_anytime_td
```

All are event-scoped (`/events/{id}/odds`), never bulk.

**Cost correction (2026-09-01).** An earlier draft of this spec said the
`bookmakers=` parameter prices a call at `markets × 1` *regardless of
book count*. That is wrong, and `ingest/mlb_prop_odds.py` records the
measured rule:

```
credits = n_markets × ceil(n_books / 10)     per event
```

8, 9 and 10 books all cost `markets × 1`; **the 11th book doubles the
bill**. So ten books is a hard cap, not a preference. `regions=` remains
strictly worse (`markets × n_regions`).

At 7 markets and ≤10 books that is **7 credits/event**, or roughly
**112 credits for a full 16-game Sunday slate**.

Against the current ~395/day scheduled burn (~11,850/30d of a 20,000
key shared by every sport), a user-triggered fetch once or twice a week
costs ~500–900/month and fits the remaining headroom. It must be
**user-triggered and deduped**, exactly like the existing NBA and MLB
"Fetch Player Props" buttons — never a cron. Read
[`docs/the-odds-api.md`](the-odds-api.md) before changing this.

**P0 gate.** A market key existing in documentation is not the same as
books posting it, and this repo has been burned by exactly that
distinction twice — `batter_home_runs`, where DraftKings posted nothing,
and `batter_runs_scored`, where Pinnacle posted a different line every
time. `ingest/nfl_prop_probe.py` measures it: per market, DraftKings
presence, Pinnacle presence, paired-quote availability and same-line
agreement, across N real events. It writes nothing and prints its exact
credit cost. Run it via the manual `NFL Prop Market Probe` workflow,
which reads `ODDS_API_KEY` from GitHub Secrets — the key is held in
Vercel and GitHub, never in a local `.env`.

### P0 probe RESULT — measured 2026-09-02, 5 events, 35 credits

Run 33628093963, seven days before Week 1 kickoff.

```
market                        events   DK  PIN  DK+PIN  paired  same-line  DK players
player_pass_yds                 5/5     5    2       2       5          0          10
player_pass_tds                 5/5     5    2       2       5          2          10
player_pass_interceptions       3/5     0    0       0       0          0           0
player_rush_yds                 5/5     5    2       2       5          2          18
player_receptions               5/5     5    2       2       5          2          30
player_reception_yds            5/5     5    2       2       5          2          30
player_anytime_td               5/5     5    0       0       0          0          99
```

**Six of seven markets were quoted by DraftKings on every probed event.**
Five have paired over/under quotes; anytime-TD is yes-only. Quote coverage
does not establish calibrated probabilities or a complete stat distribution.

**`player_pass_interceptions` is dead for us.** DraftKings posts it on
**0 of 5** events; only BetRivers quotes it at all (3/5). This is the
`batter_home_runs` situation exactly — the key exists, the book doesn't
post it. Interceptions fall back to history. Cost is low: an
interception is −1 point, the smallest term in the offensive table.

**`player_anytime_td` is the deepest market on the board** — 99 DK
players across 5 games, roughly 20 per game, against 2 per game for
passing yards. Confirmed yes-only (`name=yes, description=<player>,
point=no`), exactly the shape MLB's paired-only rule would have
discarded. It is the most valuable projection input and it is fully
available; not copying that rule was load-bearing, not academic.

**A DK-vs-Pinnacle NFL prop detector is NOT viable at this coverage.**
Pinnacle appears on only 2 of 5 events and **0 of 5** for anytime-TD, and
same-line agreement on passing yards is 0/5. NFL props are not MLB props;
do not assume the `dk_prop_value` architecture ports over. This spec's
consumer is the DFS projection, which needs neither Pinnacle nor paired
quotes, so nothing here is blocked.

**Caveat — this is a floor, not a steady state.** The probe ran seven
days before kickoff and prop boards fill out closer to game time. The
per-game player counts should be re-measured against a live slate before
any coverage assumption is baked into the model. The pool has 719
players and the props cover on the order of 20–25 per game, so the
**history + environment fallback is the majority path, not an edge
case** — particularly for the low-salary players where DFS leverage
actually lives.

**Correction: anytime-TD is not an exclusive market.** Several players
can score in one game. Do not reuse
`model/soccer_first_scorer.py::power_devig_exclusive`, which forces the
probabilities of mutually exclusive first scorers to sum to one.

A yes-only quote is usable evidence, but it cannot identify a fair
probability without an explicit margin assumption or historically fitted
calibration. Store the raw quote, market settlement definition, capture
time, margin method/version, fitted probability, and uncertainty. When a
valid paired yes/no quote exists, de-vig within that player's proposition.
Otherwise use a documented one-sided calibration or history fallback;
label an unvalidated adjustment as an assumption.

`P(TD >= 1)` is not `E[TD]`. For illustration only, a Poisson TD count
would imply `lambda = -log(1 - p)`. That is a distributional assumption,
not a general conversion formula or a mandated NFL model. Fit and validate
zero-, one-, and multiple-TD behavior and reconcile player outcomes with
team TD allocation. Passing TDs are separate from touchdowns scored by
the player under the recorded market's settlement rules.

## 3. Projection model

### Existing historical baseline (retained from main)

The first implementation is `model/nfl_dfs_historical.py`. It is deliberately
**historical first and prop free**. Market inputs do not get to hide a weak
football model: the historical baseline is built, versioned, and graded on its
own before props are permitted to update any component. The output is a
distribution rather than a point estimate.

#### Stage 1 — per-stat expectation

For each player, the baseline uses only completed weeks strictly before the
target kickoff:

1. Resample complete historical stat lines with exponential recency weights.
   Keeping whole games preserves the real correlations between volume,
   yardage, touchdowns, and bonuses.
2. Shrink sparse samples toward comparable players selected by their prior
   production profile. Peer selection happens at the player level; selecting
   individual games close to a mean would manufacture narrow uncertainty.
3. Apply capped opponent and Vegas team-total factors whose values and source
   are stored in the projection snapshot. Missing pregame context is a visible
   fallback, never a zero.
4. Rookies or players with fewer than two eligible games receive an explicit
   `position_prior` status and zero confidence. They are not silently assigned
   a normal veteran projection.

After the baseline passes its gate, de-vigged props may update a named stat
component. A prop overlay must cite the immutable baseline run/player row,
store line and price evidence, and expose the before/after delta. The untouched
baseline remains queryable. Prices are averaged in probability space, never
in American odds.

#### Stage 2 — simulation, not addition

Per player, draw complete correlated stat outcomes, apply DK scoring
**including the three yardage bonuses evaluated per draw**, and keep the
distribution:

```
proj (mean) | floor (P10) | ceiling (P90) | boom rate
```

Bonus expectation falls out of the simulation instead of being bolted
on afterward. Per-player variance comes from weekly history.

#### Stage 3 — DST

DST remains unavailable in the first historical model. The existing stored DST
weeks use Yahoo scoring, while DraftKings has different points-allowed rules.
Re-labelling those rows as DK outcomes would be false precision. DST may ship
only after DK-compatible historical outcomes are derived and pass the separate
flat-constant comparison.

#### Kill criteria — fixed now, before any data is examined

The initial shadow gate is graded walk-forward on the immutable 2020–2025
nflverse snapshots, one week at a time, never in-sample. The convenience
`ff_player_week_stats` table is not the validation source because its current-
roster match creates survivorship bias. Prospective 2026 grading follows once
weeks complete.

- **Primary:** mean absolute error per player against DK actuals, by
  position, versus a DK `AvgPointsPerGame` baseline. The model must beat
  that baseline out of sample at **every** skill position, or the
  position it fails on falls back to the baseline rather than shipping a
  worse number dressed up as a projection.
- **Secondary:** rank correlation within position — for DFS, ordering
  matters more than level, the same finding already recorded for
  season-long DST in this repo.
- **Calibration:** realized P10/P90 coverage must be near 10%/90%. An
  uncalibrated ceiling is worse than useless in GPP, where the entire
  strategy is built on it.
- **DST specifically:** if DST cannot beat a flat positional constant,
  it ships as a flat constant and says so — the exact outcome already
  reached, with evidence, for season-long DST in this repo.

Minimum prospective sample before any 2026 verdict: **4 completed weeks**. No
constant is tuned on a week that has already been graded.

#### Historical v1 shadow result (recorded 2026-09-02)

`artifacts/nfl_dfs_historical_backtest_2025.json` contains the immutable source
hashes, configuration, seed, cutoff rule, and position metrics for 4,523 held-
out 2025 player-weeks. The adjusted candidate did **not** beat the unadjusted
recency baseline at any position (MAE delta: QB +0.036, RB +0.020, WR +0.016,
TE +0.002; lower is better). It therefore remains shadow-only. This is not a
claim about performance versus DK Avg—the historical salary archive required
for that benchmark does not yet exist.

The next model iteration must add pre-kickoff role/opportunity features from
the roster-aware V2 pipeline and improve P10/P90 calibration. Props stay
deferred until that prop-free version passes its fixed gate.

### Joint-scenario extension

The existing prop-free baseline and its failed promotion gate remain in force.
The following contracts extend it; props are deferred until that gate passes.

Proposed modules: `model/nfl_dfs_projections.py` for fitted marginal
inputs and `model/nfl_dfs_scenarios.py` for joint game outcomes. Module
names are implementation targets, not evidence of completion.

The initial implementation keeps scenario validation and lineup scoring
in `web/src/lib/nfl-dfs/scenarios.ts`, calling the existing TypeScript
scorer directly. Its synthetic event generator is a research fixture,
not either proposed fitted Python module. A future Python generator
must satisfy the same data contract and cross-language scoring fixtures.

### Stage 1 — fitted distributions and provenance

1. After the prop-free baseline passes its gate, fit de-vigged prop probabilities at their actual thresholds, with a
   documented treatment of pushes for integer lines. A line and price
   constrain a tail probability; they do not uniquely identify mean,
   variance, skew, or TD counts. Fit different books' lines at their own
   thresholds rather than averaging probabilities for different events.
2. Use role-conditioned weekly history, sample-size shrinkage, and team
   environment priors to fill missing markets and identify distribution
   shape. Record roster/team changes so obsolete teammate relationships
   do not silently carry forward. Historical means alone cannot establish
   current workload after an injury or depth-chart change.
3. Represent active, limited-role, and inactive uncertainty explicitly.
   Redistribute carries and targets within a team's availability branch;
   do not independently scale every teammate upward. Do not infer an
   exact inactivity probability from a `Q` label without validation.
4. Reconcile marginal targets with shared team opportunities. Market-led
   stats must not receive another blanket Vegas/pace multiplier that
   double-counts the same environment. Report material residual mismatch
   or inconsistent inputs instead of forcing a hidden adjustment.

Every fitted input records source, observation/capture time, as-of cutoff,
sample size, model version, fallback reason, and validation status.
Unknown confidence is `unvalidated`, not an invented confidence percentage.

### Stage 2 — retained, synchronized game scenarios

**Do not reuse `compute_monte_carlo()` as the NFL simulator.** The existing
helper draws independently from a zero-clipped Gaussian final fantasy
score, sorts the draws, and returns only three summaries. It cannot
produce joint stat outcomes, preserve scenario identity, evaluate bonuses,
or represent valid negative NFL scores.

Create a scenario run containing a stable player index and, conceptually:

```
run_id, scenario_id, scenario_weight, game_id, player_id,
availability_state, stat_line, base_fpts
```

Production storage can use chunked arrays plus a metadata manifest. Retain
aligned base-FPTS draws and enough event/stat data or deterministic replay
information to audit scoring. Never independently sort player columns.
Off-pool players receive an explicit residual allocation rather than
having their opportunities assigned to rosterable players.

Sample team opportunities and shared scoring environments, then allocate
outcomes consistently. This is an aggregate event model; full play-by-play
simulation is not required for the first experiment. Enforce these
accounting invariants under documented NFL stat conventions:

- Passing completions, yards, and TDs reconcile with receiving totals
  across all passers/receivers, including the off-pool residual.
- Team TDs reconcile with rushing, receiving, defensive, return, and
  other modeled scoring categories. A pass TD is one team scoring event,
  while both passer and receiver correctly receive fantasy credit.
- Interceptions and recovered opponent fumbles agree with the opposing
  defense's credited events; sacks use consistent attribution.
- Team scores include kicks, conversions, and safeties. Kicker outcomes
  share the same scoring opportunities, especially in Showdown.
- DST points allowed follow DK's event attribution, not opponent final
  score or a scoring tier applied to average implied points.
- Count outcomes are valid integers; legitimate negative yards and
  negative fantasy scores remain possible. Bonuses and DST tiers are
  evaluated on each realized stat line.

Validate statistical dependence separately from exact accounting.
Include QB/pass-catcher relationships (including receiving RBs), target
and TD competition among teammates, rushing-versus-passing allocation,
opponent game environments, RB/DST game script, and offense/opposing DST.
Synthetic tests recover dependencies deliberately built into fixtures.
Historical conditional checks assess fitted magnitude and direction;
they must not force every bring-back or RB/DST pair to have the same sign.

### Stage 3 — score complete lineups

`web/src/lib/nfl-dfs/scoring.ts` remains the scoring contract. A Python
simulation implementation must pass shared golden stat-line fixtures
against the TypeScript scorer, including negative scores, bonuses, DST
attribution, kicker gating, and CPT multiplication.

For lineup L in scenario s:

```
lineup_score[L,s] = sum(slot_multiplier[i] * base_fpts[i,s])
slot_multiplier = 1.5 for CPT, otherwise 1
```

Compute displayed P10/P50/P90, mean, and target probability from these
complete-lineup draws and their scenario weights. P10/P90 are percentile
estimates, not guaranteed floor/ceiling values. CPT shares the same
player draw as FLEX. A lineup cannot contain both roles of one player.
All our entries and the opponent field use the same scenario outcomes.

DST starts from team history and game context with explicit uncertainty.
A flat point-estimate baseline may be shown when better mean accuracy is
unproven, but cannot masquerade as a calibrated zero-variance distribution.
Missing required distributions block contest-probability claims.

### Model evaluation and promotion

Freeze the experiment protocol before grading its held-out data. Keep
the original positional MAE and within-position rank comparisons against
as-of DK `AvgPointsPerGame`, and DST against a flat positional baseline.
Add squared-error/bias checks for predicted means (MAE targets a median),
proper distribution scores, quantile coverage, and probability calibration.
Fallback point estimates require separately qualified uncertainty.

Player MAE alone does not authorize lineup-tail or expected-payout claims.
Evaluate lineup quantiles and target probabilities as well as conditional
dependence. Account for discrete-score ties when checking quantile coverage.
Use chronological train/validation/test splits and keep results unavailable
to fitting, candidate generation, and model selection for that slate.

The previous **four completed weeks** floor remains a minimum before a
2026 predictive verdict, not sufficient evidence by itself. Rare top-1%
or profit claims need a pre-registered precision/sample-size requirement
and uncertainty clustered by slate/week. A result can be inconclusive.
Numeric tolerances and minimum meaningful improvement must be frozen in
the section 9 protocol before holdout evaluation; they are not calibrated
constants established by this document.

## 4. Optimizer

Proposed orchestrator: `web/src/app/dfs/nfl/nfl-optimizer.ts`. Keep NFL
format validation separate from NBA/MLB. The first architecture generates
legal candidates, scores them, and selects a portfolio. An exact global
solver is not a prerequisite; report candidate-limited search and timeouts
honestly. A feasible result is not proof of global optimality.

### Legality and strategy are separate

- **Classic:** nine slots; FLEX = RB/WR/TE; $50,000 cap; at least two
  games; one instance of each underlying player. No kicker slot.
- **Showdown:** one CPT and five FLEX; both teams; $50,000 cap; no player
  duplicated across roles. Use uploaded role-specific DK IDs/salaries.
  Enumerate CPT choices explicitly, including K/DST.
- QB stacks, bring-backs, RB/DST pairings, and offense/DST avoidance
  are named strategy preferences or explicit user constraints. They are
  not DK legality rules. Do not universally require stacks for GPP or
  forbid them for cash. Use scenario consequences to evaluate tradeoffs.
- Canonical lineup identity uses the underlying player set for Classic,
  and CPT identity plus the sorted FLEX player set for Showdown. Preserve
  actual slot assignments separately for export and locks.

### Contest contract and objectives

Store contest ID, draft group/slate identity, format, fee, advertised and
expected/actual field size with timestamps, payout ladder, entry limit,
entry count, lock policy, and tie policy. Contest choice stays manual.
The final realized field size is evaluation-only when it was unknown at
the decision cutoff. Entries in different contests need different terms.

| Mode | Objective | Required evidence |
|---|---|---|
| H2H | Expected return against an opponent, including win/tie/loss treatment | Opponent model and contest terms |
| Double-up / cash | Paid-finish probability or expected net payout against a scenario-specific field cutoff | Legal field model, payout and tie rules |
| GPP | Expected net payout; optional explicitly selected top-X finish objective | Field model, entry fee, payout ladder and tie handling |
| Research scorer | Probability above a fixed, predeclared fantasy-score target | Qualified scenario distribution; label as score-target probability |

P10/P90 are descriptive outputs. Lower-tail expectation is an optional
risk preference, not a synonym for cash probability. A fixed fantasy-score
target is not a simulated contest cash line. Missing contest/field inputs
leave modeled payout and finish probabilities unavailable, not fabricated
from salary-based leverage. Support the research scorer independently.

For scenario s, rank our entries alongside the opponent field. Split the
sum of prizes at occupied tied ranks among every tied entry, including
different lineups with equal scores and our own tied entries. Subtract
each entry fee once to obtain net payout; do not subtract rake again
when it is already reflected in the published prize pool. Handle refunds
and special contest tie rules from the recorded contest contract.

### Ownership input and opponent field

Initial ownership source is a user upload matched by slate/player/role
identity. Reject conflicting identities, invalid units/ranges, duplicate
rows, and format mismatch. Unknown ownership is not zero ownership.
Preserve source, capture time, as-of cutoff, sample/validation evidence,
and any override. A salary-derived estimate is labeled `heuristic —
uncalibrated`; it cannot unlock payout confidence or Kelly sizing.

For a complete legal field, marginal ownership percentages sum to 900%
for Classic, 100% for Showdown CPT, and 500% for Showdown FLEX. Each
player's Showdown CPT plus FLEX probability is at most 100%. Report
missing-pool coverage and rounding tolerance; never silently rescale a
partial upload to manufacture a complete projection. Marginals must also
be compatible with roster and salary constraints.

Sample legal opponent lineups conditioned on contest type, salary usage,
stack patterns, CPT selection, and player ownership. Multiple entries by
one opponent may be dependent; record the assumed entry-generation model.
Sampling actual future game outcomes must not give opponents hindsight
when choosing their lineups.

Estimate duplication from canonical full-lineup probabilities or field
samples, not a product of marginal ownership. If q(L) is one opponent
entry's probability of choosing L, M*q(L) is expected other copies for
M identically distributed opponent entries. Independence is additionally
needed for a binomial duplicate-count model; declare it if used. Zero
observed copies in a finite sample is not proof of zero duplication.

Validate absolute ownership error, rank correlation, ownership buckets,
stack and salary distributions, CPT/FLEX choices, and duplicate-count
distributions on held-out fields. Report uncertainty and sensitivity to
alternative plausible field models. Rank correlation alone does not
validate duplication or payout estimates.

### Portfolio selection, constraints, and audit

Choose an explicit objective: expected total net payout, probability at
least one lineup reaches a specified finish/payout target, or a documented
risk-adjusted objective. Use the same scenarios and field for every entry.
Do not add lineup hit probabilities as though target events were disjoint.

For fixed entry payouts, expectation is additive: diversification does
not inherently increase expected profit. It can change portfolio loss
risk and the probability of at least one success. Recompute ranks and
tie sharing when the selected entries themselves affect payouts. Kelly
is a separate optional wealth objective in section 8.

Support player, CPT, stack, and game exposure ranges; minimum unique
players / maximum pairwise overlap; user locks/exclusions; and optional
coverage of named game-script archetypes. Count a stack/game once per
lineup containing it. Define archetypes before selection, rather than
assigning labels afterward to claim diversity.

For N lineups and exposure bounds expressed as fractions:

```
minimum_count = ceil(N * minimum_exposure)
maximum_count = floor(N * maximum_exposure)
```

Use exact decimal/fixed-point handling at integer boundaries. Show counts
and achieved percentages. Overlap uses underlying players, including a
player shared across CPT/FLEX roles; CPT exposure is a separate constraint.
No duplicate canonical lineups within a contest portfolio.

Return requested versus achieved exposures, overlap, target probability,
objective, scenario/model versions, candidate coverage, runtime, and
solver status. Preserve input digests, seed, PRNG/version, draw count,
player ordering, field run, settings, and deterministic tie-breaking.
A fixed seed without fixed inputs and algorithm versions is insufficient.

When infeasible, identify conflicting constraints and propose changes
under documented user-visible priorities/costs. Legality, locked slots,
and spending/entry limits are never relaxed. User-imposed constraints are
not silently relaxed. A claimed minimum relaxation requires a proof;
otherwise label it a feasible suggestion. Distinguish an exhausted
candidate set or timeout from proven infeasibility of the full problem.

### Availability, FLEX placement, and later late swap

Before lock, prefer later-starting eligible selected players in flexible
slots when a reassignment preserves legality and improves pivot options.
Report legal, salary-compatible, unstarted pivots and shared contingency
exposure, including when no pivot exists. A questionable player does not
automatically belong in FLEX regardless of start time and alternatives.

Live late-swap regeneration is a later release. Persist entry-specific
locked players AND their slots, authoritative contest lock policy/state,
salary/eligibility snapshots, and entry IDs now. Regeneration must retain
locked slots, consider observed results only as of the swap decision,
condition remaining outcomes on current information, and revalidate
before export. Missing/stale lock state blocks regeneration. A non-late-
swap contest locks all entries at its contest cutoff.

## 5. Export — DK bulk entry

DK mass entry requires the entry file **downloaded from the contest**,
which carries one row per paid entry with its Entry ID. We fill that
file; we never invent Entry IDs.

```
Upload DK entry file → N lineups generated → each lineup written into
one entry row, player IDs in DK's exact column order → download →
upload to DK
```

Column order is format-specific (`QB,RB,RB,WR,WR,WR,TE,FLEX,DST` for
Classic; `CPT,FLEX,FLEX,FLEX,FLEX,FLEX` for Showdown) and must be read
from the uploaded file's own header rather than hardcoded, so a DK
column change degrades to a clear error instead of a silently misaligned
upload. If the generated lineup count and the entry-row count disagree,
the export fails closed and says so — a partially filled entry file
silently enters the wrong lineups for real money.

Validate every entry against its own contest, format, salary file,
role-specific IDs, and lock state. Round-trip canonical lineup identity
through export/import, including CPT identity and locked slot placement.
The optional allocator in section 8 operates before acquiring new entries;
it must not silently leave already-paid entry rows unfilled. A smaller
allocation recommendation requires an explicit reconciliation of the
uploaded entry set and any cancellations/refunds.

## 6. Deliberately out of scope for v1

- **Automated ownership provider integration.** Timestamped ownership
  upload is in the amended scope. LineStar discovery and an internally
  calibrated ownership predictor are later provider work. No provider is
  silently replaced by a heuristic presented as calibrated ownership.
- **Live late swap.** Preserve lock/entry metadata and pre-lock pivot
  planning in v1; implement live regeneration only after its own gates.
- **Tiers contests.** A third DK format, with no salary cap. Not
  requested.
- **In-season model retraining.** v1 grades weekly; automatic
  recalibration is phase 2, and per the standing discipline in this
  repo a calibration loop may only ever downgrade confidence
  automatically, never promote it.
- **Automatic bankroll sizing and entry purchase.** Kelly is a specified
  later capability, disabled until section 8 passes. v1 uses a manually
  selected contest, entry count, and spending limit. No feature purchases
  entries or changes the user's bankroll allocation automatically.

## 7. Dependency order

Per the Jira-First Delivery Contract, no step starts before its
prerequisites pass. The approval of this specification is not a model
promotion or completion of its implementation issues.

**Session override:** the user has waived Jira synchronization for current
implementation. Continue independently with the data and correctness gates;
restore issue reconciliation later without inventing issue status.

| Order | Work and acceptance IDs | Prerequisites / release gate | Evidence at revision |
|---|---|---|---|
| 1 | Source contracts, scoring and CSV identity (AC1–2) | Verify source timestamps, contest metadata, role IDs and historical availability; Jira sync waived for this session | Parser/scoring fixtures pass; initial scenario contract implemented; production data contracts pending |
| 2 | Schema, immutable snapshots, props and weekly actuals (AC2–3) | Source contracts accepted; coverage/missingness/leakage checks | Probe result recorded for 2026-09-02; remaining data/schema not verified in this review |
| 3 | Corrected marginals, scenario engine, scoring parity (AC3–4) | Point-in-time inputs or explicitly synthetic fixtures; TD method documented | Partial: aligned TypeScript scorer and synthetic event fixture tested; fitted marginals/production engine pending |
| 4 | Approximately 100-candidate experiment (AC5) | Scenario correctness; section 9 protocol frozen | Synthetic Classic/Showdown runs each rank 100 candidates with 2,000 selection + 2,000 evaluation draws; historical inputs still pending |
| 5 | Ownership upload, legal field, contest payouts (AC6–7) | Contest contracts; qualified scenarios; held-out field data | Not started |
| 6 | Constrained portfolio and chronological promotion (AC8–9) | Candidate/field evaluation and independent evaluation draws | Not started |
| 7 | `/dfs/nfl` UI and DK entry export (AC10) | Qualified backend, provenance and mode-specific promotion; real-file and visual verification | Not started |
| 8 | Live late swap (AC11) | Lock state, updated inputs, entry export and scenario conditioning | Later release |
| 9 | Optional Kelly allocation (AC12) | Calibrated payouts/field, bankroll contracts, robust shadow validation | Later release; disabled by default |

Orders 3–4 can establish engineering correctness with labeled synthetic
inputs; that does not satisfy the real-data or predictive gates for later
user-facing claims. A research report is an internal verification artifact,
not a new plain-CSV product export option.

### Numbered acceptance criteria

| ID | Testable requirement | Current criterion status |
|---|---|---|
| AC1 | Classic/Showdown identity, roster rules, salary, CPT eligibility and scoring fixtures pass; real entry export is separately covered by AC10 | Partially complete: parser/scorer and legal-candidate fixtures pass; real-source end-to-end verification pending |
| AC2 | Immutable input/contest snapshots have IDs, source and capture times, cutoff, digests, freshness and explicit missingness; ambiguous identities fail clearly | Partial: scenario metadata/cutoff checks and run digests implemented; production snapshots/contest contracts pending |
| AC3 | TD calibration never normalizes anytime players to one; multi-TD behavior, one-sided assumptions, different-line quotes and history fallback are tested | Not started |
| AC4 | Aligned scenarios pass event accounting, negative-score, bonus, DST and Python/TypeScript scoring parity tests; repeated runs reproduce with fixed manifest | Partial: direct TypeScript scoring, alignment, negative scores, synthetic accounting and replay pass; production generator/Python parity pending |
| AC5 | Frozen candidates have additive, independent and coherent-correlated comparisons, separate selection/evaluation draws, uncertainty and runtime; no one-slate performance claim | Partial: executable synthetic comparison passes in both formats; historical experiment pending input snapshots |
| AC6 | Ownership upload validates role/identity/units and missingness; field sampler produces legal lineups and passes frozen marginal/joint calibration gates | Not started |
| AC7 | Scenario ranks, occupied-rank tie splitting, duplicates, fees/refunds and field-size assumptions match exact small-field fixtures | Not started |
| AC8 | No canonical duplicates; hard exposure/overlap/lock/spending constraints hold; count rounding, infeasibility/timeout distinctions and audit reproduce | Not started |
| AC9 | Pre-registered walk-forward gates evaluate positional and lineup calibration, cash/top-X/payout results with slate-level uncertainty; PASS/FAIL/INCONCLUSIVE recorded separately | Not started |
| AC10 | Real entry files round-trip without changing entry IDs, roles or slots; UI exposes provenance and unsupported metrics; end-to-end and visual verification recorded | Not started |
| AC11 | Regeneration preserves every locked player and slot, checks contest swap policy and fresh lock state, validates pivots and rechecks export | Deferred: later release |
| AC12 | Kelly requirements and fixtures in section 8 pass, including zero-entry and uncertainty cases, exact fees/entry counts, commitments and chronological shadow comparison | Deferred: later release |

### Evidence and operational handoff — 2026-09-05

Reviewed checkout: `.claude/worktrees/focused-rubin-fb1032`. The expected
NFL projection/scenario and optimizer modules were absent at inspection.
The scoring and salary-parser scripts each reported all assertions passed:

```
cd web
npm run test:nfl-scoring
npm run test:nfl-dk-csv
```

These checks substantiate the existing fixture paths only. They do not
re-run the recorded real Week 1 files, verify live data, or certify a
complete DFS feature. The 2026-09-02 prop probe result in section 2 is
historical evidence; the old statement that the probe was still waiting
for a key is superseded by that recorded result.

Jira synchronization remains unavailable: the connected Atlassian search returned
access denied (403, app not installed on the instance). No issue was read,
created, updated, or marked complete, and no issue key has been invented.
The user explicitly waived Jira for implementation, so this is no longer
a coding blocker. Reconcile the NFL parent/related issues and AC1–12 when
access is restored. The initial absence of modules noted above describes
the pre-implementation inspection; the current partial implementation and
replay commands are recorded in the scenario-harness document.

## 8. Kelly criterion — optional later bankroll allocation

### Decision and scope

Include the Kelly criterion in the architecture as a **later, opt-in
allocation objective**. Keep it disabled for v1 and throughout unvalidated
field/payout modeling. It answers how much of a designated bankroll to
commit across contest entries; it does not convert fantasy-point edge
directly into entry counts or certify a profitable lineup.

The general Kelly objective maximizes expected logarithmic wealth growth.
The familiar binary-bet formula does not describe a DFS contest's many
payout ranks, ties, entry limits, and dependent entries. The standing
roadmap shortcut `f = edge / variance` is not an exact DFS sizing rule;
any small-return approximation would require monetary returns and its
own assumptions, not player FPTS variance.

### Required inputs and wealth accounting

- User-supplied, timestamped **dedicated DFS bankroll**, available cash,
  reserve/spending caps, and current unresolved entry commitments. Never
  infer bankroll from salary cap, projection confidence, or account data.
- Contest fees, entry limits, payout ladders, field-size assumptions and
  all proposed entries from the manually selected eligible contests.
- Calibrated scenario distributions of **net monetary portfolio returns**,
  including overlap across contests sharing games and our own rank/tie
  effects. Marginal lineup EVs are insufficient.
- Validation status, model/ownership uncertainty, and sensitivity to
  plausible TD tails, field behavior, duplication and payout outcomes.

Define B as dedicated bankroll immediately before fees for the current
unsettled decision horizon, F_existing as already-paid fees in that
horizon, F_new(A) as additional fees for feasible allocation A, and
P_existing(s,A), P_new(s,A) as their gross payouts in joint scenario s:

```
W_s(A) = B - F_existing - F_new(A) + P_existing(s,A) + P_new(s,A)
A* = argmax_A sum_s weight_s * log(W_s(A) / B)
```

Scenario weights sum to one. Payouts are recomputed for all entries if
our new entries affect ranks or ties. Fees are subtracted once. The
unspent balance stays as cash. Validate B > 0 and W_s(A) > 0; additionally
retain a positive cash reserve and respect the worst-case loss of every
entry fee even if a finite simulation bank omitted that event.

Bankroll and cash are distinct: already-paid entries have reduced cash
but still carry contingent payouts. Do not subtract their fees twice.
If the snapshot starts after those fees, reconstruct the explicit B
convention or use an algebraically equivalent cash-plus-payout ledger.
Missing/unmodeled commitments block sizing rather than being assumed
independent or absent. Fixed already-paid entries cannot be erased to
make a zero-new-entry result appear feasible.

### Integer allocation and conservative deployment

Optimize feasible discrete allocations of actual entries and fees. A
fractional share of a lineup is not purchasable. Always include **zero
new entries** as an option. Respect per-contest entry limits, no-duplicate
portfolio policy, cash/reserve constraints, and user spending caps. Report
search limitations when an exact optimum cannot be proven.

Fractional Kelly is a possible risk preference, not a calibrated default.
During research, compare full Kelly and illustrative fractions such as
one-quarter and one-half against a capped fixed-budget policy and no new
entries. Do not select a fraction on the same weeks used to grade it.

For a discrete operational definition, first find the modeled full-Kelly
allocation, then cap additional spend at the selected fraction of that
allocation's spend (and the user's stricter limits), and re-optimize legal
integer entries under the cap. Label this a **fractional-Kelly spending
cap**, not the exact continuous fractional-Kelly portfolio. If the cap
cannot buy one entry, choose zero; never round up to force participation.
Re-evaluate portfolio utility after integer selection and fee changes.

Evaluate model uncertainty separately from ordinary game randomness.
Compare plausible calibrated models or use a predeclared worst-case
expected-log objective over an uncertainty set. A finite scenario sample
can miss catastrophic losses or overvalue a rare simulated jackpot.
Stress tests and independent evaluation draws are mandatory. Neither
fractional Kelly nor a finite stress suite guarantees a long-term drawdown
bound in changing NFL contests. A formal risk-constrained method requires
its assumptions and bound to be implemented and checked explicitly.

### Enablement gates and user output

Kelly remains unavailable when payout inputs are missing, ownership is
only a salary heuristic, commitments cannot be reconciled, or AC9/AC12
are unqualified. Distinguish `Unavailable — insufficient evidence` from
`0 additional entries — no allocation improves the qualified objective`.
The system can reduce eligibility/confidence automatically; a new
chronological study is required to promote or re-enable sizing.

Before enablement, AC12 must establish:

1. Exact enumeration agrees with the allocator on small synthetic
   multi-rank contests, including ties, loss of all fees, and fee changes.
   A continuous binary reference agrees with the binary Kelly solution;
   the discrete allocator agrees with exact enumeration of feasible
   integer stakes. These are special-case mathematical checks, not the
   production payout formula.
2. Zero new entries beats purely negative-EV additions when no existing
   contingent exposure is present. With existing entries, evaluate an
   addition's hedge effect on total log wealth rather than using a
   blanket standalone-EV rule.
3. Identical correlated return streams never create fictitious
   diversification; paired tests vary dependence while holding marginals
   fixed. Existing and proposed entries share game scenarios.
4. Integer fees, entry limits, reserve, spending caps, missing inputs,
   deterministic replay, and the before/after-fee ledger reconcile.
5. A pre-registered chronological shadow study compares realized log
   growth, net profit, maximum drawdown, total spend, and uncertainty with
   fixed-budget/no-new-entry baselines. The protocol specifies minimum
   sample/precision and stress tolerances before evaluation; no positive
   growth or risk claim is inferred from the first slate experiment.

Display recommended additional spend and exact entry counts, remaining
cash/reserve, expected net payout, probability of a net loss, worst-case
fee loss, model status, assumptions and uncertainty. Never label a
recommendation `safe` or `guaranteed`. User selection of bankroll and
limits is required before an actionable recommendation; entry purchase
remains outside this feature.

### Research basis

- [Kelly (1956), A New Interpretation of Information Rate](https://doi.org/10.1002/j.1538-7305.1956.tb03809.x): expected logarithmic growth foundation.
- [Haugh and Singal, How to Play Fantasy Sports Strategically (and Win)](https://pubsonline.informs.org/doi/abs/10.1287/mnsc.2019.3528): DFS opponent modeling and expected-reward portfolios.
- [Busseti, Ryu and Boyd, Risk-Constrained Kelly Gambling](https://stanford.edu/~boyd/papers/kelly.html): explicit drawdown-risk constraints under stated model assumptions.
- [Sun and Boyd, Distributional Robust Kelly Gambling](https://web.stanford.edu/~boyd/papers/robust_kelly.html): expected-log optimization under distribution uncertainty.

These sources motivate the design. They do not establish calibration,
profitability, a Kelly fraction, or a drawdown guarantee for this model.

## 9. First experiment and chronological validation protocol

### P0 — approximately 100 legal candidates

1. **Feasibility audit first.** Locate one historical slate's salary and
   role IDs, game set, pre-lock projections/props/history/availability
   snapshots and cutoff. Inventory actuals and contest field/payout data
   separately. Reconstructed inputs or missing capture times must be
   labeled; they cannot establish point-in-time predictive performance.
   If unavailable, use a labeled synthetic mechanics fixture and record
   the historical experiment as blocked.
2. **Freeze the protocol and candidates.** Generate approximately 100
   legal candidates spanning stacks, games, salary usage and projection
   profiles without using actual outcomes. Record candidate-generation
   rules and a digest before ranking. Start with one format and report
   its limits; add a separate Showdown/CPT correctness fixture.
3. **Run controlled comparisons.** Hold fitted player marginals fixed.
   Compare summed marginal P90, empirical lineup P90 using independent
   marginals, and empirical lineup P90 using coherent joint scenarios.
   Use equal-weight draws for the pilot. Derive the independence ablation
   by independently permuting aligned player columns from the same draw
   bank; nonuniform scenario weights require weighted resampling instead
   of naive permutation. The ablation is deliberately a baseline
   that breaks cross-player event accounting, never a production model.
   CPT continues to use its underlying player's single outcome.
4. **Separate selection from evaluation.** Use independent draw banks for
   candidate ranking/selection and reporting, with common scenarios
   across candidates within each bank. Keep a fixed predeclared score
   target and evaluate quantiles/target probabilities. Repeat with
   independent seeds or more draws under a frozen convergence rule to
   quantify Monte Carlo ranking noise. Simulation precision does not
   measure model calibration uncertainty.
5. **Report mechanics and limits.** Include accounting assertions,
   scoring parity, candidate legality, rank differences, held-out draw
   metrics, uncertainty, runtime/memory and replay manifest. Approximately
   100 candidates bound what the search can discover. One slate can
   demonstrate ranking differences and correctness, not improved cash-hit
   rate, top-1% rate, or profitable Kelly sizing.

When historical fields exist, evaluate selected entries against them
retrospectively with the appropriate replacement/addition convention and
tie rules. Historical realized ownership/field composition may score the
result, but must not be fed to pre-lock selection. Without those fields,
report simulated contests explicitly and defer observed-finish claims.

### P1 — pre-register before opening holdout results

The executable experiment manifest must specify all of the following.
Unfilled items block a promotion study, not synthetic correctness work:

| Protocol field | Required decision |
|---|---|
| Population | Seasons/weeks, Classic/Showdown, contest sizes/fees, eligibility and exclusion rules, minimum independent slates |
| As-of policy | Decision timestamp, source capture cutoff, training-window boundary, missing/stale inputs and late-swap cutoff |
| Comparators | Additive P90, independent/coherent simulations, fixed-budget strategy, DK point baseline, ownership baseline where valid |
| Fair comparison | Equal budgets and entry limits for lineup-policy comparisons; actual differing spend recorded for bankroll-policy comparisons |
| Primary estimand | Mode-specific cash/top-X/net-payout metric; Kelly log-growth/risk objective separately |
| Calibration | Quantile definition for ties, coverage tolerance, proper distribution/probability scores, conditional slices, ownership and field calibration |
| Evidence threshold | Numeric minimum meaningful improvement, sample/precision floor, confidence level and multiplicity handling frozen before evaluation |
| Uncertainty | Paired slate/week-clustered intervals; portfolio entries and simulation draws do not count as independent historical observations |
| Search and simulation | Candidate budget, runtime/memory limits, draw counts, convergence tolerance, PRNG/seeds and separate evaluation banks |
| Governance | Model/policy versions, immutable protocol digest, PASS/FAIL/INCONCLUSIVE decisions and fallback/disable conditions |

Archive model and source snapshots, settings, candidate/field/scenario
digests, selected lineups, validation outputs and protocol version for
each run. Tune only on permitted earlier training/validation data. A
change after viewing holdout results creates a new study and needs fresh
evaluation data; it cannot retroactively convert an inconclusive result
into a pass.
