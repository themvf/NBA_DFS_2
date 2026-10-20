# `nfl-ownership-prior-v5` — registration (drafted 2026-10-20, not the default)

Status: BUILT (`variant: "v5"` in `web/src/lib/nfl-dfs/ownership-prior.ts`),
TESTED (TypeScript and Python pinned to each other within 0.02 on two shared
fixtures), BACKTESTED on the 2026 weeks 2-6 slates, which are all v5 DESIGN
slates. Not validated. `NFL_OWNERSHIP_PRIOR_VERSION` stays on v4; the owner
switches the default after reviewing this document.

Owner decisions this follows (2026-10-20): start from v3's shape (v4 was no
better: Showdown flex Spearman 0.801 vs v3 0.845 under the latest rule); add one
flattening parameter; replace the hand captain multipliers with position shares
read off observed full-field captain labels; no kicker cut; keep the punt tier
only if it lowers design-slate MAE without hurting Spearman; Classic on v4
budgets unless the design grade says v3's are better; no value-hype term.

Grading contract: `docs/nfl-ownership-scorecard-v1.md`, scorecard v3 (the
canonical forecast is the LATEST capture at or before first kickoff minus 20
minutes, from any source). Design numbers: `artifacts/nfl-ownership-scorecard/v5-design-2026-10-20.md`
(`python -m research.nfl_ownership_prior_v5_design --from-pickle PATH`); the
v3/v4/v5 regrade those rows sit in: `artifacts/nfl-ownership-scorecard/scorecard-v3-2026-10-20.md`
(3,970 rows appended to `nfl_ownership_scorecard_rows` under
`nfl-ownership-scorecard-v3`, every v5 row tagged `design`).

## 1. The algorithm

Showdown:

1. Score each player as v3: `points^2 x value^0.5 x status` (points = 0.6 x our
   projection + 0.4 x DK average; value on at least $3,000; Q 0.6, D 0.25). No
   kicker multiplier, no DST game-script term.
2. Captain: weight = (v3 captain score at captain salary, ^1.5)^alpha. The 100%
   captain budget is split into position shares (section 2.2); within a position
   the share goes in proportion to the weight, capped at 50% per player.
3. Flex cap per player = min(90, 95 - captain share).
4. Punt tier as v4: players under $3,000 (not out) share a reserved 60 of the 500
   flex points in proportion to points^1 x status, capped at 30; what the tier
   cannot absorb returns to the main pool.
5. Everyone else shares the rest of the 500 flex points by v3 score^alpha.

Classic: v3 score (value exponent 1.5) raised to alpha, allocated within v4's
position budgets (QB 100 / RB 230 / WR 335 / TE 135 / DST 100), cap 60.

## 2. Constants and how each was chosen

All on the reference contest of every 2026 weeks 2-6 slate with a frozen input,
canonical horizon, latest capture: 11 Showdown slates; 3 Classic slates with
complete labels (weeks 2, 3, 5) plus 2 with observed labels only (weeks 4, 6),
reported descriptively. Only `observed` and `reconciled_zero` labels are scored.

### 2.1 Flattening exponent alpha (`CLASSIC_FLATTEN_ALPHA_V5`, `SHOWDOWN_FLATTEN_ALPHA_V5`)

Why: under v3 and v4 the five highest-predicted players of every slate and slot
were over-predicted and everyone else under (summed top-5 error per slate: Classic
+94 / +95 points, Showdown flex +81 / +66, captain +18 / +20).
`score^alpha` with alpha < 1 moves share from the top of each allocation group to
the rest.

How: a 0.05 grid, full-pool MAE per format, the minimum taken.

| format | grid | chosen | MAE at 1.0 (v4 budgets) | MAE at chosen | alternative |
|---|---|---:|---:|---:|---|
| Classic (3 complete slates) | 0.40-1.00 | **0.75** | 1.258 | 1.205 | 0.70 1.207, 0.80 1.206 |
| Showdown flex (11 slates, punt tier on) | 0.30-0.75 | **0.45** | 5.665 (v3 shape + position shares, alpha 1, no punt tier; v3 itself 5.473) | 4.301 | 0.40 4.310, 0.50 4.381 |

The two optima differ clearly (Classic at 0.45 gives 1.360; Showdown at 0.75
gives 5.006), so the formats are fitted separately. In Showdown one alpha covers
both the flex main pool and the captain weight within a position, so the format
has one flattening parameter. The captain alone would prefer about 0.65 (captain
MAE 1.225 vs 1.317 at 0.45); that second parameter was not taken.

Rank metrics. `score^alpha` is monotone, so it never reorders players inside one
allocation group (Showdown flex Spearman without the punt tier is 0.845 at every
alpha, exactly v3's). It can reorder ACROSS groups: Classic positions, Showdown
captain positions, the punt tier against the main pool, and any player whose cap
binds. Classic mean Spearman falls 0.808 -> 0.800; Showdown flex rises 0.845 ->
0.855 (the punt tier); captain 0.844 -> 0.841.

### 2.2 Captain position shares (`CAPTAIN_POSITION_SHARES_V5`)

Per slate, OBSERVED full-field captain labels (stat-api CPT seats or the
DraftKings export; never a synthesized zero) are summed by the frozen input's
position, normalised to 100, and averaged over the 11 slates:

| | QB | RB | WR | TE | DST | K |
|---|---:|---:|---:|---:|---:|---:|
| mean raw sum | 22.69 | 28.63 | 31.76 | 11.34 | 3.23 | 1.79 |
| **v5 share (normalised mean)** | **22.82** | **28.82** | **31.92** | **11.39** | **3.25** | **1.80** |
| slate SD | 7.83 | 7.30 | 8.57 | 4.59 | 1.79 | 0.98 |

Observed captain totals were 97.07-100.00 per slate (the shortfall is labels on
players missing from the frozen input or OUT). The slate SDs are large: a fixed
share cannot know that SEA@DEN put 49% of captains on receivers or WAS@SF 19% on
tight ends. Per-slate tables are in the design artifact, section 1.

Missing positions: a position with no eligible captain (every player out, none
listed, or no positive score) gives up its share and the remaining positions are
renormalised to 100; the result reports it in `notes.captainPositionsMissing`.
A position outside the table (none occurs on DraftKings Showdown today) gets no
captain share. If no listed position has an eligible player the whole captain
budget is reported unallocated.

### 2.3 Punt tier: kept

Each variant at its own best alpha (Showdown flex, 11 slates):

| | best alpha | flex MAE | flex Spearman |
|---|---:|---:|---:|
| with v4 punt tier | 0.45 | **4.301** | **0.855** |
| without | 0.55 | 4.517 | 0.845 |

It lowers MAE and raises Spearman, so it stays, with v4's constants unchanged
($3,000 / 60 points / cap 30) and without v4's kicker term inside it.

### 2.4 Kickers: no cut

v4's x0.6 halved predicted kicker flex ownership (23 kicker rows: mean actual
20.6%, v3 20.6%, v4 11.8%). v5 has no kicker term: mean predicted 20.8%, kicker
MAE 3.9 points (v3 8.1, v4 9.3).

### 2.5 Classic budgets: v4's

At alpha 0.75 the v3 budgets were 0.007 points better pooled on the three
complete slates (1.198 vs 1.205), all of it from week 2 (1.089 vs 1.121, the
week whose recorded TE total was 115% against 134-139% in weeks 3-6). v4 budgets
were better on the other four Classic slates (week 3 1.029 vs 1.035, week 5
1.550 vs 1.561, observed-only weeks 4 and 6 3.178 vs 3.204 and 2.822 vs 2.835)
and on observed-only Spearman (0.735 vs 0.727), and they match the recorded TE
totals. The pooled difference is below anything the grade can resolve, so v4's
stand.

## 3. Design-slate grade (latest rule, canonical, reference contest, full pool)

Every row is a design row for v5. MAE and bias in percentage points; intervals
resample slates (2,000 reps). Top-5 error: summed predicted minus actual of the
five highest-predicted players, mean per slate. Captain-by-position MAE: mean
over slates and the six positions of |predicted captain sum - observed captain
sum|. Kicker: flex rows of kickers.

| format | slot | version | slates / n | MAE [95%] | bias | mean Spearman [95%] | top-5 err | captain-by-position MAE | kicker actual / pred / MAE |
|---|---|---|---|---|---:|---|---:|---:|---|
| Classic | overall | v3 | 3 / 1,621 | 1.256 [1.106, 1.644] | +0.005 | 0.808 [0.754, 0.848] | +93.9 | - | - |
| Classic | overall | v4 | 3 / 1,621 | 1.258 [1.094, 1.633] | +0.005 | 0.808 [0.752, 0.850] | +95.0 | - | - |
| Classic | overall | **v5** | 3 / 1,621 | **1.205** [1.029, 1.550] | +0.004 | 0.800 [0.756, 0.840] | +37.0 | - | - |
| Classic (observed only) | overall | v3 | 2 / 343 | 3.276 | -0.108 | 0.728 | +63.0 | - | - |
| Classic (observed only) | overall | v4 | 2 / 343 | 3.278 | -0.111 | 0.733 | +67.4 | - | - |
| Classic (observed only) | overall | **v5** | 2 / 343 | **3.010** | -0.245 | 0.735 | +18.6 | - | - |
| Showdown | CPT | v3 | 11 / 402 | 1.540 [1.322, 1.793] | +0.008 | 0.844 [0.785, 0.892] | +18.3 | 4.82 | - |
| Showdown | CPT | v4 | 11 / 402 | 1.619 [1.393, 1.874] | +0.010 | 0.842 [0.772, 0.895] | +19.6 | 5.39 | - |
| Showdown | CPT | **v5** | 11 / 402 | **1.317** [1.157, 1.494] | -0.045 | 0.841 [0.777, 0.886] | -6.9 | 4.13 | - |
| Showdown | FLEX | v3 | 11 / 461 | 5.473 [4.659, 6.244] | +0.071 | 0.845 [0.792, 0.889] | +81.4 | - | 20.6 / 20.6 / 8.1 (23) |
| Showdown | FLEX | v4 | 11 / 461 | 5.572 [4.935, 6.216] | +0.078 | 0.801 [0.746, 0.853] | +66.2 | - | 20.6 / 11.8 / 9.3 (23) |
| Showdown | FLEX | **v5** | 11 / 461 | **4.301** [3.837, 4.772] | +0.058 | 0.855 [0.812, 0.896] | -2.3 | - | 20.6 / 20.8 / 3.9 (23) |

Captain mean signed error by position (predicted minus observed sum, points per
slate): v3 QB +8.05 / TE -5.05; v4 QB +10.10 / TE -6.58; v5 within 0.65 at every
position. That v5 figure is close to zero by construction (the shares are the
mean of the same slates); the honest captain-by-position number is the MAE,
4.13 vs 4.82, which is mostly slate-to-slate spread a fixed share cannot follow.

Reading it: on the slates its constants came from, v5 cuts flex MAE by about a
fifth and nearly removes the top-5 over-prediction, and it trades a little
Classic rank (-0.008) for level accuracy. Those are in-sample numbers. With 11
Showdown and 3 complete Classic slates and three constants chosen on them, the
week 7+ grade may land well short of these.

## 4. What v5 deliberately does not address

- **Value and news hype.** The largest misses that remain are players the field
  drafts for reasons no input carries: late news, beat-writer buzz, a cheap
  starter in a role change (Wilson 11.35% vs 45.59%; Lance 1.45% vs 23.87% under
  v3). Flattening raises everyone below the top a little; it cannot pick out
  which cheap player the field will crowd.
- Slate-specific captain mix (SD 5-9 points per position).
- Game script, team totals, DST and kicker context beyond points and price.
- The 0.6/0.4 projection blend, the points^2 term, value exponents, caps, the
  punt-tier constants and Classic's 60% cap are unchanged from v3/v4.

## 5. Prospective plan

- v5 is graded only on week 7 onward slates, under scorecard v3 (latest capture
  at the canonical cutoff), always beside v3 and v4 recomputed from the SAME
  frozen input (exact recomputation, as the grader already does), on the
  reference contest's full pool with complete labels. Week 2-6 rows stay tagged
  `design` and never count toward a verdict.
- No v5 constant changes until there are at least 8 Showdown and 4 Classic
  non-design slates with complete labels. A change before then is a new version
  (v6) with its own registration and its own design slates.
- At that point report per format and slot: pooled MAE, bias, mean Spearman
  with slate-clustered intervals, top-5 concentration error, captain-by-position
  MAE, kicker error, and the paired per-slate difference v5 minus v3 and v5 minus
  v4. Showdown still has no registered promotion gate; switching the production
  default is the owner's call on this evidence, not an automatic rule. The
  Classic Phase 2 gate in `docs/nfl-ownership-model.md` is for a FITTED model
  and is not changed by this stated prior.
- Optimizer impact (duplication, payout) is not part of this grade; it belongs
  to the ghost factorial and the portfolio study.
