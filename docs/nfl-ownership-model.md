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
