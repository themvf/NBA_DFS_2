# NFL DFS field-ownership model

Registered 2026-09-27, before any fitted model existed and with one Classic
slate of actual ownership in hand.

## Why

Every leverage feature in the NFL optimizer — the ownership penalty, the
chalk-fade archetypes, the field-duplication estimate — is gated on ownership
being present and validated. With no ownership the server resolves capability
`unavailable` and the optimizer maximises raw projection. Ownership is the
missing input, not a new feature.

## Phase 1 — stated prior (`nfl-ownership-prior-v1`, shipped 2026-09-27)

`web/src/lib/nfl-dfs/ownership-prior.ts`. Deterministic and pure. The field
drafts points and value, anchored on DraftKings' printed season average, and
concentrates on the top few at each position:

```
blend = 0.6 × our projection + 0.4 × DK season average
score = blend² × (blend per $1K)^1.5 × status   (Q 0.6, D 0.25; OUT 0)
share = score / Σscore within position × position budget, capped 60%
budgets: QB 100 · RB 255 · WR 340 · TE 105 · DST 100  (= 900, FLEX split 55/40/5)
showdown: flex 500 (cap 90), captain 100 on score^1.5 (cap 50)
```

Every constant is a judgement. The prior declares itself heuristic, so
`assessOwnership` caps it at `heuristic_uncalibrated`: leverage runs only
through the user's explicit opt-in and is labelled "Uncalibrated estimate";
the duplication model stays off. LineStar ownership, when present, takes
precedence (`ownSource`).

## Phase 2 — fitted model and promotion gate (frozen now)

Port `model/mlb_ownership_model.py` (`mlb_ownership_v1`: Ridge on log
ownership, slate-relative rank features, one model per role, artifact read by
Python and the web app; scored corr 0.75 / MAE 0.96 pts on 13,404 MLB
player-slates) to NFL positions QB, RB, WR, TE, DST.

Training data: `nfl_dfs_field_ownership` joined to the slate each contest was
imported against. The largest-field Classic GPP each week is the reference
contest; Showdowns are a separate model with Captain and Flex targets.

**Gate to `validated` (unlocks leverage in production without opt-in):**

| requirement | threshold |
|---|---|
| held-out Classic slates | ≥ 4, holdout by slate, never by row |
| Spearman rank correlation, pooled over held-out slates | ≥ 0.70 |
| mean absolute error, pooled, percentage points | ≤ 2.0 |
| bias, pooled | within ± 0.5 |
| coverage / mass / sum checks in `assessOwnership` | unchanged |

The thresholds do not move. NFL ownership is more concentrated than MLB's, so
a higher point error than MLB's 0.96 is expected; 2.0 was set on that basis
before any NFL number was computed. If the fitted model fails, the prior stays
heuristic and the gate stays closed — no re-slicing to the weeks it fits best.

**Governance fix required with Phase 2:** `assessOwnership`'s `validated`
state is a coverage check plus the source not declaring itself heuristic. It
has no accuracy test. Before any source can be promoted, the calibration
record above must be wired in as a precondition, so promotion is by evidence
rather than by declaration.

## Grading

`npm run calibrate:nfl-ownership` grades whatever is deployed against every
imported contest. Descriptive for the prior (it was designed against those
slates); the holdout discipline above applies only to the fitted model.

Import the largest GPP entered each week on the Results step; that is the
training set.

## First grading — 2026-09-27, prior v1 against every imported contest

Constants were set from judgement before this was run; nothing was tuned on
these slates.

| contest | n | Spearman | MAE (pts) | bias |
|---|---:|---:|---:|---:|
| wk2 Classic, 317,082 entries | 670 | **0.80** | **1.03** | 0.00 |
| wk2 Showdown NYG@LAR, 47,562 | 53 | 0.83 | 7.93 | 0.07 |
| wk3 Showdown ATL@GB, 83,234 | 53 | 0.75 | 8.22 | 0.06 |

By position on the Classic slate: TE 0.85, WR 0.81, RB 0.79, QB 0.74,
**DST 0.33 (MAE 4.07)**. For reference `mlb_ownership_v1`, a fitted model,
scores 0.75 / 0.96 on 13,404 rows.

The Classic number clears the Phase 2 gate on one slate; the gate needs four
held out, and this one is descriptive. What the misses say, recorded as
**hypotheses for the fitted model**, not as edits to the prior:

1. **The field follows the market's projection, not ours.** Every large
   over-projection of ownership is a player our model likes more than the
   consensus (Swift 31.6% vs 7.1%, Coker 38.9% vs 17.1%, Watson 34.1% vs
   14.0%, Smith-Njigba 27.8% vs 10.7%); every large under-projection is a
   value or news play the field jumped on that our model did not (Schultz
   $3,200 18.4% vs 3.2%, Aaron Jones 20.0% vs 2.3%). The 0.6 weight on our
   projection is too high for predicting the field; the fitted model should
   carry the DK average, a consensus projection, and our-vs-consensus gap as
   separate features.
2. **DST ownership is a Vegas decision.** The field drafts defenses on spread
   and opponent implied total (Buccaneers 13.9%, 49ers 10.9%); the prior
   has no line features. Add spread and opponent implied total.
3. **Showdown punts.** Fields punt cheap Flex plays far more than points²
   allows (Ferguson $1,800 TE 21.7% vs 1.9%; Corum 28.8% vs 6.9%). A
   salary-tier feature, and a separate kicker/DST treatment, are needed.
4. **Availability at grading time must be the slate's own.** Puka Nacua drew
   0.8% because he was out; the historical upload still carried him active.
   The fitted model must grade against the availability decision pinned on
   that slate, never today's.

Live check on the 2026-09-27 Classic slate: 671 of 671 players carry a prior,
sum 900.0%, source `nfl-ownership-prior-v1`; top chalk Smith-Njigba 53.5%,
St. Brown 51.0%, Henry 38.9%, Allen 18.5% — the same WR-heavy,
QB-light skew as lesson 1, visible before results and left unchanged on
purpose.

## Field structure from the standings lineups — 2026-09-30

`model/nfl_dfs_field_structure.py` (`nfl-dfs-field-structure-v1`) summarizes each
imported contest's full lineups into `nfl_dfs_field_structure`: duplication, who
fills the field, and pair co-ownership. Backfill an already-imported contest with
`python -m ingest.nfl_dfs_field_audit --contest FILE --structure-only` (refuses a
file whose digest differs from the one imported); a fresh `--contest` import now
stores it automatically. The lineups themselves are not stored; the file is the
source. Stored entries run 0.06-0.65% under `entry_count`: those are unfilled
entries (blank lineup, score 0), not parse losses.

| contest | entries | unique lineups | entries in a duplicated lineup | users with 20+ entries -> share of entries |
|---|---:|---:|---:|---|
| wk2 Classic | 316,828 | 299,470 | 8.3% (max 156 copies) | 4,892 -> 31% |
| wk3 Classic | 7,130 | 6,710 | 8.6% (max 25) | 159 -> 45% |
| wk2 Showdown | 47,255 | 9,382 | **89.7%** (max 556) | 1,440 -> 61% |
| wk3 Showdown | 82,798 | 12,242 | **93.3%** (max 394) | 1,989 -> 48% |

Read these as description, not a model. Two things worth carrying forward:

- **Showdown is a duplication contest.** Roughly nine in ten entries share a
  lineup, and 98% of the top 1% do. Classic is the opposite (8-9%), so any
  duplication model must be fitted per format, never pooled.
- **Stacks are visible only at pair level.** Classic QB pairings sit at 3.4-4.3x
  independence (Mayfield-Egbuka 4.3x, Wentz-Jefferson 4.1x, Purdy-Evans 3.7x)
  while the most-owned pairs sit near 1.0x. Pairs are limited to the top 40
  players by ownership, so tail stacks are not measured.

Sample: 2 Classic and 2 Showdown contests. Still short of the 4 held-out Classic
slates the Phase 2 gate needs; this adds labels per slate, not slates.

FantasyCruncher (the 2025 archive's `fc_catalogue` points at it) was probed: its
public pages are a link index with no ownership numbers, so 2025 ownership
history is not recoverable from public pages. Not pursued further.

## Showdown: value exponent and an evaluation harness — 2026-10-04

**What decides whether ownership accuracy is worth buying.** Perfect ownership
barely changes a Classic portfolio at the production leverage setting (about as
much as re-running with a different seed), but it matters in Showdown, where
~90% of entries are duplicated. `model/nfl_showdown_ownership_eval.py` (runner:
`python -m research.nfl_showdown_ownership_eval --contest ID=PATH ...`) builds
80-lineup portfolios from an ownership input and scores them against the real
contest: projected points kept, share of lineups a real entry also played, and
mean real copies. Compare `proj_pts` at similar `share_duplicated`; a perfect
forecast keeps roughly 4-8 more projected points per lineup than the prior at
the same uniqueness (weeks 2-3, one seed set, simplified optimizer, not realized
score).

**`nfl-ownership-prior-v3` (Showdown only).** The value exponent drops 1.5 -> 0.5
(`SHOWDOWN_VALUE_EXPONENT`); Classic is unchanged. Read off weeks 2-3, then
scored on the two week 4 Showdown contests imported afterwards:

| contest | flex MAE v2 -> v3 | captain MAE v2 -> v3 | chalk rank v2 -> v3 |
|---|---|---|---|
| wk2 NYG@LAR (fit) | 7.18 -> 6.30 | 1.63 -> 1.26 | 0.69 -> 0.76 |
| wk3 ATL@GB (fit) | 5.73 -> 5.48 | 1.68 -> 1.52 | 0.79 -> 0.80 |
| wk4 196280117 (holdout) | 4.35 -> 3.82 | 1.63 -> 1.49 | 0.90 -> 0.93 |
| wk4 196204783 (holdout) | 5.23 -> 4.61 | 1.38 -> 1.04 | 0.83 -> 0.85 |

Better on all four contests and all three measures. **Not validated**: one
format, four contests, a constant chosen by looking at two of them. On the
portfolio measure it is roughly neutral (more projected points kept, slightly
less uniqueness, about the same trade-off line), so this is a calibration
improvement, not a proven lineup improvement.

**What it does not fix.** True punts (Ferguson, $1,800 TE, 19% owned against
0.8% predicted; Treadwell $3,200, 19.3% against 2.4% in week 4) and kickers
(Shrader K 67% against 33.3% still over-projected) are role and news effects
with no feature in this prior. Next candidates: a salary-tier or role feature,
and a separate kicker treatment; judge each on the holdout contests, not the
ones above.

## `nfl-ownership-prior-v4` — committed before the week 6 Monday Showdown, WAS@SF (2026-10-19)

Owner's decision: commit stated corrections now and grade them, rather than
wait for a fitted model. Every constant is a judgement read off the misses in
`docs/nfl-ownership-review-2026-10-19.md`; none is fitted, and the scorecard
(`docs/nfl-ownership-scorecard-v1.md`) grades v4 beside v3 on frozen forecasts.
v3 stays callable (`variant: "v3"`) and the Python mirror still pins it.

| change | evidence | constant |
|---|---|---|
| Classic TE budget 105 -> 135 (RB 230, WR 335) | recorded TE totals 137.71 / 134.09 / 137.12% on the weeks 3-5 Millionaires, i.e. 34-38% TE FLEX against the 5% implied | `CLASSIC_POSITION_BUDGETS_V4` |
| Showdown punt tier: players under $3,000 share a reserved 60 of the 500 flex points on points^1, cap 30; unused slice returns to the main pool | Ferguson $1,800 19-21.7% actual vs 0.8-1.9%; Lance 23.87% vs 1.45%; Treadwell $3,200 19.3% vs 2.4% (Treadwell is above the tier) | `SHOWDOWN_PUNT_*` |
| Kicker flex score x 0.6 | Shrader 67% projected vs 33.3% actual after v3 | `SHOWDOWN_KICKER_MULTIPLIER` |
| DST x 1.3 favorite / 0.7 underdog when the slate resolved a favorite | DST ownership follows the line (Classic DST Spearman 0.33) | `DST_GAME_SCRIPT_MULTIPLIER` |
| Captain score x TE 0.7 / K 0.25 / DST 0.25 | direction only; magnitudes are judgements. Fit on full-field captain shares (`drafted_by_slot.CPT`), not winners | `CAPTAIN_POSITION_MULTIPLIER` |

Not changed: the 0.6/0.4 blend, the points^2 term, the 60% Classic cap, the
Showdown value exponent. The field-points input is whatever the page shows,
so anchored players already carry the market number; the scorecard splits
grades by the anchor evidence stored per player.

The calibration script now averages tied ranks and treats a missing label as
a reconciled zero only when the contest's recorded mass is at least 99.5% of
the roster slots; otherwise the row is unresolved and excluded. The 0.80
Spearman figures above were computed without tie averaging and are withdrawn
as headlines (tie-correct week 2 full pool: 0.663).

## `nfl-ownership-prior-v5` — drafted 2026-10-20, not the default

Registration: `docs/nfl-ownership-prior-v5.md`; design grade:
`artifacts/nfl-ownership-scorecard/v5-design-2026-10-20.md`. Built on v3's
shape (v4 was no better), callable as `variant: "v5"`; production stays on v4
until the owner switches it.

| change | constant | chosen on the weeks 2-6 design slates |
|---|---|---|
| Every allocation weight is score^alpha | Classic 0.75, Showdown 0.45 (flex and captain) | 0.05 grid, full-pool MAE per format |
| Showdown captain budget split by position, then by v3 captain score^alpha within | QB 22.82 / RB 28.82 / WR 31.92 / TE 11.39 / DST 3.25 / K 1.80; missing positions renormalised | mean of observed full-field captain labels per slate, normalised |
| v4 punt tier kept, no kicker term in it | $3,000 / 60 / cap 30 | flex MAE 4.30 and Spearman 0.855 with it vs 4.52 and 0.845 without |
| No kicker cut, no DST game script | - | v4's x0.6 put kickers at 11.8% against 20.6% actual |
| Classic budgets | v4's | v3's were 0.007 better pooled, all from week 2; v4's better on 4 of 5 slates |

Design-slate result (in-sample, scorecard v3 latest rule): Showdown flex MAE
5.47 (v3) / 5.57 (v4) -> 4.30, Spearman 0.845 / 0.801 -> 0.855; captain MAE 1.54
/ 1.62 -> 1.32; Classic MAE 1.256 / 1.258 -> 1.205 with Spearman 0.808 -> 0.800.
Graded for real only on week 7 onward beside v3 and v4 from the same frozen
inputs; no constant changes before 8 Showdown and 4 Classic non-design slates.
Not addressed: news- and value-hype chalk (no input carries it).

## Phase 2 fit — `nfl-ownership-fit-v1` (registered 2026-10-28, before any fit was run)

Owner's decision ("build and test"). Everything in this section was written
before the first fit; the result is appended below it, never edited into it.

**Data.** For each 2026 week with a linked Classic contest whose recorded
ownership mass is at least 890 of 900, the REFERENCE contest is the largest
field (the stat-api Millionaire); any other Classic contest on the same slate
(the owner's exports, weeks 2 and 3) is extra evaluation only, never pooled into
the gate. The forecast population is the newest owner optimizer run on that
slate created BEFORE its first kickoff (`origin='upload'`, not a research study,
status complete or partial), the same population the Monday three-way grade
reads; players marked out in that snapshot are excluded. Labels are the
contest's `drafted_pct` matched by normalized name; an unmatched active player
is 0 (the mass rule above makes that a reconciled zero, not a guess). Seven
reference slates exist today (weeks 1-7).

**Features**, all from the pregame snapshot, one model per position (QB, RB,
WR, TE, DST), standardized on the training rows:

| feature | definition |
|---|---|
| `log_salary` | log(salary / 1000) |
| `proj`, `proj_missing` | our projection as the run saw it (anchor, absence commit included), 0 and a flag when absent |
| `dk_avg`, `dk_missing` | DraftKings' printed season average, 0 and a flag when absent |
| `value` | proj / (max(salary, 3000) / 1000) |
| `rank_proj`, `rank_value`, `rank_dk`, `rank_salary` | tie-averaged percentile rank (0-1) within the position's active pool on the slate |
| `team_share` | the team's summed projection over the slate's summed projection (active players) |
| `game_rank` | percentile rank of the game's summed projection among the slate's games |
| `team_pos_rank` | the player's rank within his team at his position by projection, 1 = top, clipped at 4 |
| `status_q`, `status_d` | DraftKings status flags |
| `log_pool` | log(active players at the position on the slate) |

`team_share`, `game_rank` and `team_pos_rank` are the concentration features the
seven-week grade said the v5 prior lacks (the most-owned team under-predicted
every week; QB/TE/DST tops too flat).

**Model.** Ridge regression on `log(drafted_pct + 0.1)` (the MLB model's
target offset), closed form, intercept unpenalised, `alpha = 1.0` on
standardized features. Alpha is fixed here and is not tuned on any held-out
slate. Rows are weighted so each training slate counts equally.

**Allocation (primary).** `exp(prediction) - 0.1`, floored at 0, scaled within
each position to that position's mean recorded total over the training slates,
with the v5 cap of 60% and the excess redistributed (`allocate_detailed`).
Secondary, reported but not gating: the raw predictions rescaled to 900 with no
position budgets.

**Folds.** (a) Leave-one-slate-out over the reference slates: the gate. (b)
Walk-forward, train on earlier weeks only, held out weeks 4-7: reported beside
it as the stricter check, not gating. In both, the v5 prior is graded on the
identical held-out rows.

**Gate** (unchanged from Phase 2 above, judged on the primary allocation,
pooled over the leave-one-out held-out rows): at least 4 held-out slates,
Spearman (tie-averaged) >= 0.70, MAE <= 2.0 points, |bias| <= 0.5. PASS also
requires the pooled MAE to beat the v5 prior's on the same rows; a model that
clears the bar but is no better than the formula replaces nothing. On FAIL
the prior stays and the next variant is a new registration; features and
alpha are not changed after seeing the result.

Code: `model/nfl_ownership_fit.py` (pure), `research/nfl_ownership_fit.py`
(runner; prints and writes `artifacts/nfl-ownership-fit-v1/`).

### Result — first run, 2026-10-28 (`artifacts/nfl-ownership-fit-v1/result-20261028.json`)

Seven reference slates (weeks 1-7, 3,512 held-out active players, leave-one-
slate-out), the v5 prior graded on the identical rows:

| | Spearman | MAE | bias |
|---|---:|---:|---:|
| fitted, primary allocation | **0.826** | **1.243** | 0.000 |
| fitted, raw rescaled to 900 (secondary) | 0.826 | 1.287 | +0.007 |
| v5 prior | 0.797 | 1.256 | +0.006 |

**Gate: PASS** on every registered bar (slates 7 >= 4, Spearman 0.826 >=
0.70, MAE 1.243 <= 2.0, |bias| 0.000 <= 0.5, MAE below the prior's 1.256).

Read it plainly, because the pass is narrow on one axis and clear on another:

- **Rank order improves on every slate** (0.761/0.823/0.814/0.843/0.855/0.853/
  0.861 against the prior's 0.694/0.756/0.805/0.811/0.840/0.847/0.857) and at
  every position; DST most (0.518 vs 0.316), WR next (0.823 vs 0.782).
- **Point error improves by 0.013 overall, and only on weeks 1-4.** Weeks 6-7
  are WORSE on MAE (1.46 vs 1.27, 1.62 vs 1.45). The walk-forward check (train
  on earlier weeks only, held out weeks 4-7) has the fit better on rank (0.848
  vs 0.838) and worse on MAE (1.504 vs 1.394). Weeks 2-6 are the prior's
  design slates, so the prior's point error there is not a clean comparison
  either; week 1 and week 7 are the only slates neither saw, and they split
  (week 1 fit better on both, week 7 fit better on rank, worse on points).
- **The chalk is still missed.** Players the field owned 20%+ (n=47, mean
  28.9%) are predicted at 16.5% by the fit and 14.4% by the prior. The team
  and game concentration features moved this by two points, not fifteen. The
  cheap-starter chalk (Golden 39%, Marks 23%, Hurst 19% in week 7) is a role
  and news story that no pregame snapshot feature carries.
- The two extra contests (owner's exports, weeks 2 and 3) agree with the
  reference slates: 0.804/1.05 vs 0.756/1.22 and 0.829/1.07 vs 0.830/1.05.

Budgets fitted on all seven slates: QB 100, RB 233, WR 332, TE 132, DST 100
(the v4/v5 budgets were QB 100 / RB 230 / WR 335 / TE 135 / DST 100, so the
recorded totals confirm them). The all-slate model's coefficients are in the
artifact (`model_all_slates`) for a web port.

**What follows from a pass this size is the owner's call**, recorded here
rather than decided by the script: the registered gate is met, so the fit may
replace the prior as the ownership source; the honest expectation is a better
ORDERING of the field (which is what the leverage fade and the duplication
estimate consume) at about the same point error, not fewer chalk misses.
Features and alpha are frozen; the next variant (a news or published-ownership
input for the chalk) is `nfl-ownership-fit-v2` with its own registration.

## Classic shadow — `nfl-ownership-fit-v1` beside v5 (registered 2026-10-31, before any graded shadow slate)

Owner's decision 2026-10-31: run the fitted Classic model as a shadow rather
than switch. It cleared its gate on rank (0.826 vs 0.797, every slate) but only
narrowly on points (MAE 1.243 vs 1.256), lost on points walk-forward (1.50 vs
1.39), and still predicts 20%+ chalk at ~16 against ~29.

**What runs.** `python -m research.nfl_ownership_fit --freeze` froze the
all-slates fit (the seven week 1-7 Millionaires) to
`web/src/data/nfl-ownership-fit-v1.json`, `fitted_at` 2026-10-31 18:45 UTC.
`web/src/lib/nfl-dfs/classic-ownership-fit.ts` ports it (pinned to the Python by
`tests/fixtures/nfl_ownership_fit_fixture.json`, both sides tested). On every
Classic slate the workspace read computes it from the same projections as the
prior; the pool shows it as a violet "fit" line under the Own box, and every
saved build records it in `ownership.shadow` with its version and `fitted_at`.
Nothing builds from it: the optimizer, leverage and duplication read `ownPct`
(v5 or LineStar or a typed number) exactly as before.

**How it is graded.** `python -m research.nfl_ownership_three_way --week N`
prints a "fitted shadow" line beside the prior, the FantasyPros-bounded prior
and the FantasyPros midpoint, using the number the build saved (recomputed from
the frozen file for older builds) and saying whether the slate counts.

**Switch rule, frozen now.** Population: the largest linked full-field Classic
contest of each week (recorded mass >= 890/900) whose first kickoff is after
`fitted_at`, graded on the newest pre-kickoff owner run (the Monday grade's
rows). At the fourth such slate, once:
- SWITCH the Classic default to the fit if all hold: the fit's MAE beats v5's
  on at least 3 of the 4 slates, its pooled MAE and pooled Spearman both beat
  v5's, and its pooled |bias| is at most 0.5;
- otherwise the default stays v5 and the shadow is retired or replaced by a
  separately registered v2 (a news or published-ownership input).

The frozen coefficients are not refitted until that verdict; a refit before it
restarts the count. The owner's own exports on the same slates are reported but
are not the counted population.

### Amendment 2026-10-31 (same day, before any counted slate): the fit goes live, v5 becomes the shadow

Owner's decision: run both, with the fitted models live. From this change the
workspace builds from `nfl-ownership-fit-v1` on Classic (and, as before,
`nfl-ownership-fit-showdown-v1` on Showdown), and the v5 prior is computed on
the same projections, shown as a violet "v5" line under the Own box and saved in
each build's `ownership.shadow` (`shadowVersion`, plus `liveVersion` and
`liveFittedAt`; Showdown also saves the shadow captain/flex split). LineStar and
typed numbers still override the live number exactly as before.

The comparison and its rule are unchanged; only what happens at the verdict is
restated. At the fourth counted Classic Millionaire, KEEP the fit if all four
registered conditions hold; otherwise REVERT the Classic default to v5. The
Monday grade recomputes both from the build's own inputs either way, so the
comparison does not depend on which one was live.

## Rank-shape calibration layer — `nfl-ownership-rank-shape-v1` (registered 2026-11-07, before any week 10 capture)

Owner's decision 2026-11-07 ("Register it"), after the week 9 Thursday import
and regrade. Code: `model/nfl_ownership_rank_shape.py` (pure), frozen curves
`model/nfl_ownership_rank_shape_v1.json` (digest `bbd895213850…`, written once
by `python -m research.nfl_ownership_rank_shape --from-pickle … --freeze`),
tests `tests/test_nfl_ownership_rank_shape.py`. Nothing live reads it.

### What was measured first (weeks 2-9, 33 imported contests)

The ownership error decomposes into ordering (which players the field drafts)
and shape (how much the r-th most-drafted player gets). The shape is close to
a fixed function of rank: on the complete-label reference contests the mean
actual ownership at rank r, by Classic position or Showdown slot, has a
week-to-week coefficient of variation of 0.08-0.13 from rank 3 down, and only
rank 1 is unstable (CV 0.18-0.37). Our forecasts already have the right spread
(slope of log actual on log forecast 0.96 Classic, 0.98 CPT, 0.97 FLEX, bias
~0), so what remains is ordering (Spearman 0.80 / 0.87 / 0.88) and the top rung
(Classic top-5 bias +4.7 points, Showdown captains -1.5).

### The forecast

For player i in group g (Classic: position; Showdown: every player, one curve
per slot):

```
own_i = sum_r P(rank_i = r) * curve_g(r),   rescaled so the group sums to its frozen mean total
P(rank) : rank the base forecast after adding N(0, sigma) to log(base + 0.05), averaged over 200 draws, seed 7
sigma   = 0.6 (Classic), 0.3 (Showdown)      frozen; the best of {0, 0.3, 0.6, 0.9, 1.2} on weeks 2-9
base    = nfl-ownership-prior-v5, recomputed from the frozen capture by the scorecard
```

sigma = 0 (a hard rank map) hands rank 1's curve value to whoever we rank
first and was WORSE than v5 on Classic (2.13 vs 2.07); the noise term is what
makes the layer work, so it is part of the version. The layer is applied to
every player in the frozen capture (ruled-out players included, at a base of
about 0), then graded on the gate labels as every other version is.

### Evidence before registration (descriptive, the design sample)

Leave-one-slate-out on the scorecard's primary canonical frames, curves from
the other slates each time:

| format / slot | slates | v5 MAE | rank-shape MAE | slates won | Spearman v5 → rs | top-5 bias v5 → rs |
|---|---:|---:|---:|---|---|---|
| Classic | 7 | 2.070 | 2.024 | 5 | 0.800 → 0.803 | +4.69 → +2.26 |
| Showdown CPT | 16 | 1.396 | 1.300 | 13 | 0.866 → 0.864 | −1.54 → +0.46 |
| Showdown FLEX | 14 | 4.256 | 3.832 | 11 | 0.881 → 0.881 | −1.36 → −0.93 |

The scorecard's own design-week rows (curves in sample, labels-complete
reference contests only): Classic 1.326 → 1.234 (4 of 4), CPT 1.361 → 1.205
(13 of 15), FLEX 4.409 → 3.947 (12 of 15). Those rows are tagged `design` and
count for nothing.

### Prospective grade (frozen now)

- **Population:** `nfl_ownership_scorecard_rows` under scorecard v3 (v4 from the 2026-11-07 completeness amendment, which adds the stat-api top-100 contests), reference
  contest with complete labels, canonical primary forecast, `full_pool`
  population, 2026 weeks 10 and later (the first kickoff after this
  registration; week 9's Sunday and Monday slates are tagged `design` although
  their contests are not yet imported, to keep the boundary a week boundary).
  Classic and each Showdown slot are graded separately and never pooled.
- **Primary metric:** per-slate MAE of `nfl-ownership-rank-shape-v1` against
  `nfl-ownership-prior-v5` on the same rows (paired).
- **Floors:** Classic 4 counted slates; Showdown CPT and FLEX 8 counted slates
  each. The verdict for a slot is computed once, on the first scorecard run
  after its floor is reached, and recorded under this section.
- **PROMOTE** a slot when all hold: the layer's MAE is lower on at least 3 of
  the 4 Classic slates (6 of 8 Showdown), its pooled mean MAE is lower, and its
  pooled Spearman is not lower by more than 0.01. Otherwise **RETIRE** it for
  that slot. No re-slicing by position, salary or week, and sigma and the
  curves do not move; a different sigma or a refreshed curve set is
  `nfl-ownership-rank-shape-v2` with its own registration.
- **What PROMOTE licenses:** a TypeScript port applied to the LIVE ordering
  (the fitted model where that is live, v5 otherwise) as a shadow line beside
  the Own box, with a separately registered grade before it drives leverage.
  It never changes the live number by itself. Spearman is expected to be flat:
  the layer cannot fix ordering, only the shape.

### Known limits

The Classic curves rest on 7 slates and rank 1 of every curve is the least
stable number in them. The layer gives each player an expected ownership, not
yet the per-player distribution the leverage term would want (E[(1 − own)^k]
rather than (1 − E[own])^k); that is the next layer, not this one. It is not
graded on the fitted ordering because the scorecard recomputes only the stated
priors from frozen captures.

## Outside ownership: stat-api's projection per slate — `nfl-ownership-outside-v1` (owner's decision 2026-11-12, a trial)

On a slate with a stored pull, stat-api's projected ownership replaces our
model for the players it covers. Our own numbers stay on each player and in
the saved build (`ownership.model`), so the two can be graded against the
real field afterwards. Requested for the WAS @ NYG Showdown (2026-11-12) and
the Sunday morning NE @ DET Showdown (2026-11-15), while the Pro subscription
runs.

### Why

Tonight our model and stat-api disagreed most on the quarterbacks: their share
of captains was 21% for us and 40% for stat-api. Feeding stat-api's own points
through our fit still gave 23%, so the gap is the ownership model, not the
projections. Against real ownership on 20 past Showdown flagships (2026-10-01
to 2026-11-09, stat-api's stored pre-game numbers, engine 2.2):

| average miss, ownership points | stat-api | our fit |
|---|---:|---:|
| captain, per player | 0.71 | 0.86 |
| flex, per player | 2.09 | 3.59 |
| quarterbacks' share of captains | 4.1 | 4.7 |
| top quarterback, total | 4.5 | 6.2 |

stat-api was closer on flex in all 20 slates. Our fit kept the quarterbacks'
captain share between 15% and 29% while the real share ran 10% to 39%, and it
was low in all five slates at 30% or more. Cautions: "our fit" was run on
stat-api's points, and 17 of the 20 slates are in its own training sample, so
it is flattered; stat-api's stored numbers are stamped 3 to 8 minutes before
kickoff, later than a build, and hindsight cannot be ruled out; stat-api ran
a little high on quarterback captains (25.4% mean against 21.9%). Closer
ownership has not been shown to improve payouts. That is untested.

### How it works

- `pullNflOutsideOwnership(uploadId)` finds stat-api's slate by DraftKings
  player ids (at least 80% of ours), reads `/slates/{id}/ownership` for the
  slate's main contest, and stores one row in `nfl_dfs_outside_ownership`,
  keyed by slate signature. Append-only; the newest row decides.
  `clearNflOutsideOwnership` appends an inactive row (back to our model).
- The page read applies it right after our own ownership step
  (`attachOutsideOwnership`). Rules, in `lib/nfl-dfs/outside-ownership.ts`:
  stat-api's number for every active player it gives more than 0%; an active
  player it has at 0% keeps our number when ours is at least 1%, and
  stat-api's players are scaled down in that slot to make room; a player we
  have ruled out gets nothing and is listed; captain plus flex is held to 95%.
  First slate: Cam Skattebo and Treylon Burks were at 0% on stat-api while it
  projected both to play, so they kept ours (35.8% and 15%) and Daniels went
  from stat-api's 89.7% to 81.9%.
- A typed ownership still wins over it. It is still treated as an estimate:
  leverage runs only through the opt-in, and the Showdown lineup ownership
  floor reads it per slot.
- The pull is a button on the build page when the deployment has
  `STAT_API_KEY`, and otherwise `web/scripts/pull-nfl-outside-ownership.ts`
  from a machine that holds the key. It is never scheduled. stat-api's numbers
  change until kickoff, so pull again shortly before building. A slate that
  has kicked off cannot be pulled.
- Classic slates are supported (totals only) but stat-api's Classic ownership
  has not been compared with real ownership, so it has not been applied to one.

## Showdown lineup model — `nfl-ownership-field-showdown-v2` (owner's decisions 2026-11-13; v1 that morning, v2 that evening)

Our own Showdown ownership once the books' player lines are in. It replaces the
per-player fitted model (`nfl-ownership-fit-showdown-v1`) on those slates; that
model stays live before the lines arrive and becomes the shadow after. stat-api's
pull, when one is stored for the slate, still replaces either.

### Why

stat-api's files publish the shape of its own ownership model: a choice over
whole lineups under the salary cap (a choice temperature, a cap term, a crowd
mixture). Fitted to the real field on stat-api's own point projections, a model
of that shape reproduced stat-api's accuracy (captain 0.67 against 0.83, flex
2.66 against 2.50, 23 slates, 5-fold). So the form is recoverable, and what was
left was the inputs.

### The model

- Players: the slate's top 24 by crowd points. Lineups: every legal one among
  them (one captain, five flex, both teams, under $50,000) that leaves at most
  $6,000 unspent, cut to the 80,000 with the most crowd points less 0.5 a
  $1,000 unspent.
- Value of a lineup: crowd points of its five flex seats, 1.5x the captain's,
  an offset by position for each seat (wide receiver is the reference), a
  charge per $1,000 unspent, and a bonus when a WR or TE captain has his own
  quarterback in the flex.
- The low-end term (v2): a seat's crowd points are bent before they are
  valued. A player the crowd expects under 4 points loses a further 1.92 for
  each point under it, so a 2-point player is valued at 2 - 1.92 x 2 = -1.85.
  Which players are kept and which lineups survive the cut still go by plain
  crowd points. See "v2: the low-end term" below.
- The field picks a lineup with probability proportional to exp(value / tau).
  Captain and flex ownership are the shares of that mass each player is in, so
  they sum to 100% and 500% and one player's two shares cannot pass 100%.
  Players outside the top 24 get 0%.
- Crowd points: our projection as the ownership step sees it (after the market
  anchor, before the range calibration), except a kicker is 3.53 + 0.2325 x his
  team's Vegas points, a defense is 14.23 - 0.363 x the opponent's, and once
  the lines are in a player with only a touchdown line is scaled by 0.70 and a
  player with no line by 0.34. Those are where our projections and a consensus
  projection differ most (kickers 1.1x ours, defenses 1.26x, touchdown-only
  players 0.70x, players with no line 0.34x on the training slates); for players
  with a full set of lines ours and the consensus are the same thing (0.97x).
- "The lines are in" means at least twelve ownable players have a full set
  (`MIN_FULL_ANCHORS`). Before that a missing set says nothing about a player.
  The rule was six until the evening of 2026-11-13 (owner's decision). Two days
  before NE @ DET ten players had a full set, so the lineup model was live, but
  five of the thirteen skill players priced $4,500 or more had none yet, among
  them the Patriots' lead back, and each was scaled by 0.70 as if he were a
  backup (DeMario Douglas read 10.7% flex against 29.1% under the fitted model,
  Mack Hollins 12.9% against 25.2%). On the eight training slates, all frozen
  about an hour before kickoff, 12 to 17 players had a full set and at most two
  at that price had none, every one a backup under 10 points. Twelve is the
  lowest count seen on a slate the model was fitted on. It is still a stated
  guard: a slate with twelve sets and a starter unposted reads that starter low.
- Fitted numbers (`web/src/data/nfl-showdown-field-model-v2.json`): tau 3.70,
  unspent salary -2.74 a $1,000, quarterback stack +1.43, low-end penalty 1.92
  under 4 points, flex offsets QB -0.15 / RB +0.94 / TE +0.75 / K +0.35 / DST
  +0.44, captain offsets QB -0.64 / RB +3.00 / TE +0.66 / K -3.01 / DST +2.51.
  (v1, without the low-end term: tau 3.55, unspent salary -1.81, stack +1.47,
  flex offsets QB -0.44 / RB +0.79 / TE +0.61 / K +0.82 / DST +1.01, captain
  offsets QB -0.74 / RB +3.07 / TE +1.09 / K -1.16 / DST +3.84.) A separate
  value for a player's
  salary is not identified (a captain costs exactly 1.5x his flex price, so it
  adds up to the unspent-salary term) and is not in the model. A ceiling term
  fitted to zero and was dropped.

### Evidence

Training slates: eight 2026 Showdown slates (weeks 7-10) frozen by the ghost
test about an hour before kickoff with the lines in, each joined to the real
captain and flex ownership of that night's largest contest. Each slate was
predicted by numbers fitted on the other seven. Miss per ownable player, in
percentage points:

| | captain | flex |
|---|---:|---:|
| lineup model v2 (with the low-end term), held out | 0.88 | 2.75 |
| lineup model v1, held out | 0.89 | 3.36 |
| v1 without the fringe factors | 0.91 | 3.54 |
| our projections alone, no stand-ins (fitted on all twelve slates) | 0.95 | 3.96 |
| the fitted model it replaces (in sample for two of the eight) | 1.18 | 4.85 |
| what the app showed at the time | 1.30 | 4.84 |
| stat-api's projected ownership | 0.81 | 2.31 |

By position the v1 held-out totals are close to the real ones (QB captain 24.5%
against 23.3%, DST 4.6% against 4.2%, kicker flex 45.3% against 44.7%). The
largest misses are news-driven cheap players (a backup running back at 30.6%
of flex against 12.7% predicted by v1 and 16.6% by v2), which no input here
carries.

Four more slates were frozen before any line had been captured. They are not
in the fit. On them v1 missed captain by 1.04 and flex by 4.59 and v2 by 1.01
and 4.30, no better than the fitted model (0.82 and 4.12, and at least two of
the four were in that model's fit), which is why the lineup model is only used
once the lines are in.

### v2: the low-end term (owner's decision 2026-11-13)

Asked what our model could learn from stat-api's, v1's inputs were swapped for
stat-api's projections one group of players at a time, on the same eight
slates, each still predicted from the other seven. Flex miss per player:

| crowd points taken from stat-api for | flex |
|---|---:|
| nobody (v1) | 3.36 |
| kickers and defenses | 3.31 |
| players with a full set of lines | 3.24 |
| players without a full set | 3.07 |
| everyone | 2.96 |
| stat-api's own projected ownership | 2.31 |

Their projections explain 0.4 of the 1.05 gap. For players with a full set of
lines the two projections are the same number (correlation 0.993, 0.66 points
apart on average). The rest was the shape of v1's seat value. A seat was
valued in a straight line, so a 1-point player was only 3 points worse than a
4-point one and his price, which frees salary for stars, more than paid for
it. Flex ownership by salary on the eight slates (227 players):

| salary | players | real | v1 | v2 | stat-api |
|---|---:|---:|---:|---:|---:|
| under $1,000 | 38 | 0.7% | 2.5% | 1.2% | 0.6% |
| $1,000 to $2,500 | 38 | 2.9% | 5.1% | 3.4% | 2.7% |
| $2,500 to $4,000 | 34 | 12.9% | 11.3% | 12.4% | 11.1% |
| $4,000 to $6,000 | 43 | 20.7% | 19.6% | 21.0% | 20.3% |
| $6,000 to $8,500 | 34 | 22.8% | 22.2% | 23.2% | 23.9% |
| $8,500 and up | 40 | 42.3% | 43.3% | 42.8% | 43.5% |

On players really owned 10% or more v1 already missed by about what stat-api
did (10-25%: 2.93 against 2.76; 25-50%: 4.48 against 4.00; 50% and up: 4.45
against 5.10). The 60 players really owned under 2% were where it lost: 2.77
against 0.40.

The term: seat points = crowd points - penalty x max(0, 4 - crowd points),
penalty fitted with everything else. Held out on the eight slates it took flex
from 3.36 to 2.75 (-0.61, 90% interval [-0.78, -0.44], better on 8 of 8) and
left captain where it was (0.89 to 0.88). The threshold is not fitted: 3, 4
and 5 were tried on these eight slates (2.85, 2.75, 2.75), which makes the
2.75 a little kind.

The check that does not share that problem: twelve other Showdown slates
(Oct 1 to Oct 25), none of the eight, with stat-api's stored projections as
the crowd points because we hold no frozen projections of our own for most of
them. Each predicted from the other eleven, threshold fixed at 4: flex 2.47
without the term, 2.20 with it, better on 11 and level on 1; captain 0.65 and
0.67. stat-api's own ownership on those slates: 2.14 and 0.72. So with their
inputs the model's form now matches theirs.

Tried and not kept: reading a player without a full set of lines off a
straight line in his salary (stat-api's projection for such players is close
to 0.32 + 0.79 x salary in $1,000s; held out it reproduces their number to
0.75 points against 0.93 for the flat 0.70 and 0.34 factors). With the
low-end term in, it added nothing (2.78 against 2.75).

What is left after v2 is mostly which fringe players have a real role: with
stat-api's projections for players without a full set on top of the term the
miss is 2.49. We project some third-string players at 4 to 6 points where the
consensus has them near 2. Salary does not separate them.

### What it does not show

Better lineups. Six stored slates were rebuilt the way the page builds
(Winners shape v14, simulated-games scoring, leverage on, the saved lineup
count and max exposure, 10 seeds) with the fitted model, the lineup model
(numbers that had not seen the slate) and, for five of them, the real field's
ownership, and every lineup was paid out of the five stored contests of its
slate. Lineup model minus fitted model, mean over slates with a 90% interval:

| | difference | interval |
|---|---:|---|
| actual points a lineup | +0.91 | [-0.22, +2.04] |
| top-10% finishes, points | +1.40 | [-0.13, +2.94] |
| top-1% finishes, points | -0.04 | [-0.14, +0.06] |
| return per $1, one prize capped at 50x | 0.00 | [-0.08, +0.07] |
| copies of the lineup per 10,000 entries | -0.53 | [-1.56, +0.50] |

Real ownership against the fitted model was no better (points +0.57, top-1%
-0.26, capped return -0.10, all intervals through zero). On these builds a more
accurate ownership forecast, even a perfect one, did not change results by an
amount six slates can see. The model is shipped as a better estimate of the
field, not as an edge. That check was run on v1; v2 has not been through it,
and nothing above suggests it would come out differently.

### Known limits

- The 4-point threshold of the low-end term was chosen on the eight slates it
  is scored on, and its independent check ran on stat-api's projections, not
  ours.
- Eight slates of one season, all prime-time games frozen an hour before
  kickoff. A slate read the day before, with only some lines posted, is outside
  what was tested; the twelve-full-sets rule is a stated guard, not a fitted one.
  Raising the rule does not make an early read a good one: Rhamondre Stevenson
  read 12.5% flex under the fitted model two days out (9.7% under the lineup
  model), low for a lead back, and the fitted model does not look at lines at
  all. Ownership read before the lines post is an early read under either model.
- The kicker and defense lines and the fringe factors were fitted to a
  consensus projection from licensed contest files, which is why the training
  file is not in the repository. `research/nfl_showdown_field_model.py --slates`
  refits from such a file and needs torch.
- No status term: a questionable player is read at his projection.
- Classic is not covered. Its lineups cannot be listed outright and would need
  sampling.

## Showdown after-slate grade — `nfl-showdown-ownership-grade-v1` (owner's decision 2026-11-13)

Every number in the two sections above came from one-off research runs. This
grade is the standing record: after each Showdown, how far every ownership
estimate that was stored before kickoff was from the field's real captain and
flex ownership. It is written by the weekly post-week job and shown on
`/dfs/nfl/results` ("Showdown ownership: how close were we").

### What is graded

- **The forecast** is the last saved build or automatic freeze at least 20
  minutes before kickoff that stored a captain and flex split
  (`nfl_dfs_optimizer_runs.input_snapshot`, `nfl_dfs_ghost_freezes.input_snapshot`).
  Each frozen player carries up to three estimates (`snapshotPlayer` in
  `web/src/app/dfs/nfl/actions.ts`): the number the build read, our own
  model's number when stat-api was shown in its place, and the comparison
  model recorded beside it.
- **Nothing is recomputed.** A later code change cannot move an old grade, and
  a model that produced no number before kickoff has no grade for that game.
  The lineup model is therefore absent from every slate before 2026-11-13, and
  from any later slate whose lines were not in at the freeze.
- **The real field** is the largest contest imported for the slate with
  complete captain and flex ownership. A contest whose stored captain shares
  sum to at least 99.5 and flex shares to at least 497.5 holds the whole
  field's split (a DraftKings export, or a full stat-api standings file) and is
  read value for value, a slate player it does not list being 0%. Any other
  contest keeps the scorecard's label rules
  (`model/nfl_ownership_provenance.py`). The whole-field rule is this grade's
  own: the scorecard's rules predate full standings files and leave out a
  player whose captain share no fetched top-100 lineup shows (9 of BUF @ MIN's
  players and 28 of TB @ CHI's on the first run).
- **Miss** is the mean absolute gap between estimate and real ownership, in
  percentage points a player, captain and flex apart. Every model of one slate
  is graded on the same players: active in the snapshot, real ownership known
  in that slot, and a number from every model. Players we had ruled out that
  the field drafted are counted beside the grade, not inside it.

### Where it runs and what it stores

`python -m ingest.nfl_showdown_ownership_grade` (pure logic in
`model/nfl_showdown_ownership_grade.py`), a step of
`refresh_nfl_dfs_postweek.yml` after the flagship import, which is where
Thursday's and Monday's real ownership comes from. Any other Showdown is
graded on the next run after its contest standings are imported; running the
command by hand does the same at once. It calls no outside service.

- `nfl_showdown_ownership_grades`: one row per slate and grade input,
  append-only. A slate whose forecast and stored contest have not changed is
  skipped without reading either. A changed input (a later import, a fuller
  file) appends a row and readers take the newest.
- `nfl_showdown_ownership_grade_runs`: one row per run, written whether or not
  anything was graded. It is the row the health checklist watches
  (`nfl_showdown_ownership_grade`), and it names every slate of the last 30
  days still waiting and why (no contest imported, no forecast saved before
  kickoff, no captain and flex split).

Both tables are created by the job itself
(`db/nfl_showdown_ownership_grade_schema.py`), not by `db/schema.py`, so adding
them did not trigger a schema pass in every other job.

### How to read it

Two models are compared only on the slates where both were stored; an average
over different games says nothing about which is closer. Each slate counts
once. First run, 2026-11-13, 17 slates from week 4 on:

| stored together on | flex miss | captain miss | closer on flex |
|---|---|---|---|
| per-player model vs rule of thumb, 7 slates | 4.91 vs 4.80 | 1.12 vs 1.40 | 3 vs 4 |
| stat-api vs per-player model, 1 slate | 1.78 vs 4.69 | 0.72 vs 1.27 | 1 vs 0 |

So on live slates the per-player fitted model has been closer than the older
rule of thumb on captains and no closer on flex. Seven games is a description,
not a verdict. The lineup model has no live slate yet.

### Known limits

- One slate is about 30 players of one game, and players of one game move
  together. Several slates are needed before two models can be told apart.
- The Sunday Showdowns have no free flagship import, so they wait for the
  owner's contest export or a stat-api pull made on request.
- A slate before 2026-10-01 stored no captain and flex split and cannot be
  graded.
- `research/nfl_ownership_inventory.attach_contests` matches an unlinked
  contest to its slate through the schedule's team codes, which say WSH where
  DraftKings says WAS, so an unlinked Washington contest never matched. This
  grade matches on the slate's own game string as well. The ownership
  scorecard still uses the schedule match alone and is unchanged here.
