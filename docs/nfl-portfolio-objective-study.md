# NFL DFS: does a field-relative joint objective pick better portfolios than the ceiling heuristic? (pre-registered 2026-10-14)

Status: **v3 registered 2026-10-30 (see "v3" at the end). Earlier: registered before any 2026 week-6 slate was captured; freeze gate
FAILED for both formats on 2026-10-14 (v1) and again on 2026-10-15 (v2, the
stacked field; see "v2" at the end), so no week-6 slate enrolls.** Nothing
here changes the live optimizer, its presets or its defaults. The challenger is
a research arm saved beside the owner's real build; it never exports and is
never "our build" to any reader (every reader excludes `researchStudy` runs).

Gate result (`artifacts/nfl-portfolio-objective-v1/freeze.json`, realized
top-1% vs the model's implied median, allowed miss = that format's week-to-week
SD): Classic 1 of 2 (w4 miss 1.5 of 11.8; w5 miss 28.5), Showdown 2 of 3
(ATL@NO 11.2 and IND@WAS -14.2 of 20.0; PIT@CLE 24.2). Where it misses, the
realized top 1% is higher than the model's: implied medians are about right,
so the sampled field is less dispersed than the real one. That is the
stacking limit named below (opponents drawn slot by slot do not concentrate on
correlated stacks the way entrants do). Two sampler defects were found and
fixed before this result and are not outcome tuning: the first calibration
collapsed its own acceptance rate, and the salary reservation let early slots
spend the cap so the DST slot held only the cheapest defence (27.7% prior
ownership sampled at 1.2%). The residual ownership deviation after calibration
is recorded per slate (max 14-45pp on the checked slates) and is itself a
reason not to freeze.

What a field model v2 needs before re-gating, as a new version with its own
registration: a field-wide stacking rate measured from the full standings
(every entry's stack shape is in the stat-api standings rows; it is not stored
today), not from the top finishers; and a calibration that reproduces the
prior within a stated tolerance. The runner refuses `--save` for the
portfolio study until a freeze passes for the slate's format.

## Why this exists

The production optimizer (`objective()` in `web/src/app/dfs/nfl/nfl-optimizer.ts`)
scores a lineup by adding capped player ceilings plus a boom bonus, scaled by
an ownership factor, then searches lineup by lineup under exposure and
uniqueness rules. It is a search heuristic: it does not model how teammates
move together, where the field's top scores will fall, or what a contest pays.

The read-only attribution studies (PRs #427, #428, #429) measured selection
regret, the gap between our best lineup and a hindsight optimum under the same
rules, at a median of about 23 points per Showdown and about 40 per Classic,
against construction-rule costs of a few points. That gap is headroom, not a
forecast of improvement: it includes outcome variance no pregame method can
remove. It does say that if anything in the build is worth changing, it is how
lineups are chosen, not which rules shape them.

Haugh and Singal (2019) show that for top-heavy payouts the right single-entry
target is the chance of beating the field's top score, which depends on the
covariance between a lineup and the field, and they approximate it with a
mean-variance surrogate because they lack joint draws. This study scores
candidates and a sampled field on the SAME joint draws, so that covariance is
realized by construction and no surrogate is needed.

## Hypothesis

H: on the flagship contests, a portfolio of n lineups chosen from the
production optimizer's own candidates to maximise P(at least one lineup reaches
the field's top 1%) under a joint player model and an ownership-based field
model, lands in the contest's top 1% more often than the n lineups the owner's
build produced, and has at least as good a median lineup score.

## Arms (identical settings; only the selection differs)

- **A, current:** the owner's saved build, rebuilt from its own frozen
  settings and snapshot (Winners shape resolved if it was Winners). The runner
  records how many of the source's lineups the rebuild reproduced; a rebuild
  that differs is still arm A (it is the same construction), and the count is
  reported.
- **B, challenger:** the same settings, locks, exclusions, projections and
  ownership. Candidate pool = arm A's lineups plus a run of the production
  optimizer at the same settings with `nLineups = min(200, max(60, 5n))`
  (the same construction, exposure cap and uniqueness, more lineups).
  Selection = greedy marginal coverage of P(any lineup >= the field's
  within-draw top-1% score) under the source's `maxExposure`,
  `maxExposureByPlayer` and `minUnique`. Constraints the selection does not
  enforce (per-player exposure minimums, exposure policies, captain ranges,
  archetype quotas, bring-back and QB/DST shares) are recorded per slate as
  `unenforcedSourceConstraints`; they shape the candidate pool, not the pick.

Both arms are saved at the source's lineup count before the slate's kickoff.

## The challenger's model, frozen (`web/src/lib/nfl-dfs/portfolio-objective.ts`)

| part | version | what it is | stated priors |
|---|---|---|---|
| joint draws | `nfl-joint-copula-v1` | Gaussian copula from the captain simulation, extended to a multi-game slate: one factor per game, one per team, DST loads negatively on its opponent; marginals = the model's P10/median/P90 bent so the mean is the projection | same-team correlation 0.40, same-game 0.15, DST-opponent -0.30; 4,000 draws; a player with no quantiles draws from 0.74x-1.28x of his projection and is counted |
| field | `nfl-field-ownership-v1` | 2,000 opponent lineups drawn slot by slot from the ownership prior (captain share for CPT, flex share for FLEX), salary-feasible by construction; weights calibrated to the prior's own ownership by iterative proportional fitting on pilot samples; realized vs prior deviation recorded | the prior is the target; no outcome is used |
| objective | `nfl-portfolio-objective-v1` | within-draw field top-1% = the 20th-best of the 2,000 sampled opponents in that draw; greedy coverage of P(any lineup >= it) | selection on seed 20261016, forecast on independent seed 20261017, field seed 20261018 |

Every constant lives in `PORTFOLIO_OBJECTIVE`; changing one is a new version
and a new cohort. The freeze gate (below) hashes the module files.

### Freeze gate, run once before enrollment

`python -m research.nfl_portfolio_freeze` writes
`artifacts/nfl-portfolio-objective-v1/freeze.json`. Per format, the challenger
is frozen only if, on every frozen discovery slate that carries an ownership
prior (week 4-5 Classic and Showdown inputs of the attribution studies), the
realized top-1% score of the flagship contest lies within the format's
week-to-week spread (the SD of realized top-1% across the stored flagships) of
the model's implied median top-1%. A format that fails is not enrolled; the
runner refuses `--save` for it. The gate checks the inputs; nothing is tuned
to the realized values, and the discovery slates never enter the cohort.

## Population and timing

From 2026 week 6: the weekly Classic main slate (the Millionaire) and each
game day's biggest DraftKings Showdown that stat-api serves in full, the same
flagships the pool-rule studies use, in formats whose freeze gate passed.
Weeks 1-5 are discovery and excluded. One pair per slate; both arms saved
before that slate's first kickoff; a pair saved after kickoff voids the slate.
A slate with no saved pregame build is a missed slate, not a failure of either
arm. Classic and Showdown are separate cohorts and are never pooled.

## Metrics (fixed now)

Per slate and arm, graded from the stored contest curve and the week's actual
DK points (players with no recorded score resolved as in the attribution
studies; an unresolved player leaves a lineup unscored and is counted):

1. **Top-1% finishes** (primary): lineups at or above the contest's realized
   top-1% score. Paired difference B - A.
2. **Median lineup points**: paired difference B - A; B must not be lower in
   total.
3. **Brier score of the recorded forecast**: each arm's pregame forecast
   `forecast.successTop1Pct` = P(at least one of its lineups reaches the
   field's top 1%), against the realized event (metric 1 > 0). The forecast
   for arm A comes from the same model, so the Brier scores compare the
   model's calibration on both constructions, not the constructions.
4. **Best lineup rank** (secondary; one draw).
5. **Field-model check** (descriptive): implied median top-1% and top-0.1%
   against the realized ones, and the realized ownership of the sampled field
   against the prior.

## Decision rule (fixed now)

- No verdict before **8 eligible slates** in a format.
- **B is supported** in a format if its total top-1% finishes exceed A's,
  the paired per-slate difference is positive in at least 6 of 8 slates, and
  the mean paired median-points difference is not negative with a
  slate-clustered bootstrap 95% interval.
- **B is rejected** if the total top-1% finishes are lower or the median-points
  interval is below zero.
- Otherwise **inconclusive**; continue to 16 slates with the same bars.
- One look per milestone (8, then 16). The bars do not move. A failed format
  is not rescued by re-slicing; a changed model is a new version and cohort.

## What this test cannot say

It compares two selections of the same projections under one joint model and
one field model. A win says the selection and its model are useful together;
it does not establish the model's calibration (the Brier scores do that,
descriptively), future profitability, or an edge. Eight slates detect only a
large, consistent effect; at about three flagships a week the first Showdown
look is around week 9 and the first Classic look around week 13. Failure to
detect does not establish equivalence.

Field-model limits named in advance: ownership is a heuristic prior
(Classic Spearman ~0.80 against imported contests, Showdown weaker); opponents
drawn slot by slot do not stack the way real entrants do; the within-draw
top-1% is estimated from 2,000 opponents, so the contest winner is out of
reach and no first-place or payout claim is made. Full-field rosters exist only
for top finishers (outcome-selected) and are not used to fit anything.

## Runner

```powershell
python -m research.nfl_pool_rule_study --study portfolio --source-run <UUID> --flagship-showdown --out artifacts/portfolio-w6-<game>-dry
python -m research.nfl_pool_rule_study --study portfolio --source-run <UUID> --flagship-showdown --out artifacts/portfolio-w6-<game>-saved --save
python -m research.nfl_pool_rule_study --study portfolio --source-run <UUID> --main-slate --out artifacts/portfolio-w6-classic-saved --save
```

Same mechanics as the pool-rule studies (`docs/nfl-classic-te-wr-pool-rule-study.md`):
read-only dry run, one transaction for both arms, failures preserved, lock
rechecked after the inserts, enrollment unique per slate, clean tracked working
tree for `--save`, artifacts with SHA-256 hashes. `--save` additionally
requires a passing, current freeze for the slate's format. Regression tests
(`npm run test:nfl-pool-rule-study`) require arm A to match the production
build exactly on the frozen week-5 inputs and arm B to differ only in which
lineups it kept.

The grader (`research/nfl_pool_rule_grade.py`, shared with the pool-rule
studies) must be committed before the first 8-slate look.

## Field stacking rate, measured (2026-10-15; the input v2 needs)

The freeze gate failed on a field that does not stack. The stacking rate is
now measured, not assumed: `model/nfl_dfs_field_stacks.py`
(`nfl-dfs-field-stacks-v1`) reads every stat-api standings row's stack label
(Classic) and the stored top finishers' lineups (Showdown, because stat-api
labels no showdown row: checked live on three flagships at rank 1 and rank
50,000), and the weekly flagship import stores one row per contest in
`nfl_dfs_field_structure`. `python -m research.nfl_field_stack_rates --season
2026 [--backfill]` fills older contests and prints the pooled rates.

Classic, the whole field, five Millionaires (weeks 1-5, 1,490,188 entries):

| | all entries | top 1% | top 100 |
|---|---:|---:|---:|
| QB stacked with a teammate | 90.2% | 95.3% | 97.0% |
| QB + 1 / 2 / 3+ pass catchers | 52.5 / 28.3 / 1.9% | 45.2 / 40.7 / 5.3% | 48.2 / 40.6 / 5.4% |
| QB + TE | 28.3% | 36.2% | 41.4% |
| QB + RB | 18.5% | 12.1% | 6.8% |
| bring-back (any) | 45.2% | 61.5% | 57.2% |

Showdown, the held top-100 users' every lineup (eight flagships, 40,891
lineups; outcome-selected, NOT the field): team split 3-3 32% / 4-2 46% / 5-1
21%; a QB in 94.5% of lineups, two QBs in 35.6%; the QB with 2 / 3+ teammates
42% / 34%; a kicker in 41.3%, a DST in 33.9%; captain WR 36% / RB 30% / QB 22%.
Within that population the top 100 ranks lean 3-3 (57%) and use fewer kickers
(29%), which is where the Winners shape's 5-1 / 3-3 rule came from.

What this licenses: a v2 field sampler that draws a QB stack for ~90% of
Classic opponents with the catcher and bring-back mix above (and the Showdown
split, flagged as a top-user estimate), then re-runs the freeze gate as a new
version. What it does not license: tuning any of these against realized
thresholds, or reading the Showdown numbers as the field's.

## v2: the stacked field (`nfl-portfolio-objective-v2`, `nfl-field-stacked-v2`, 2026-10-15), gate FAILED again

What changed, and only this: the field sampler draws each opponent's SHAPE
from the measured rates above (`FIELD_STACKING` in the module, the pooled
"all entries" Classic rates and the top-user Showdown split, converted to
conditional shares by dividing by the QB-stack share), seats the stack, then
fills the open slots from the ownership prior exactly as v1 did. The joint
copula, the draws, the seeds, the selection and every other constant are v1's.
The sampled lineups' realized shapes are recorded per slate (`fieldShapes`)
and sit within a few points of the stated rates (Classic QB stack 88-90%,
bring-back 62%, TE-with-QB 40% because the ownership-weighted fill adds
teammates the shape did not ask for; Showdown split 32 / 47 / 21).

Re-run on the same frozen inputs and the same rule
(`artifacts/nfl-portfolio-objective-v2/freeze.json`):

| slate | realized top 1% | implied v1 | implied v2 | miss v2 | allowed | |
|---|---:|---:|---:|---:|---:|---|
| w4 Classic | 189.1 | 187.7 | 191.4 | -2.3 | 11.8 | pass |
| w5 Classic | 199.1 | 170.6 | 174.7 | 24.5 | 11.8 | FAIL |
| w4 ATL@NO | 133.0 | 121.8 | 120.9 | 12.1 | 20.0 | pass |
| w4 PIT@CLE | 113.4 | 89.2 | 89.4 | 24.1 | 20.0 | FAIL |
| w4 IND@WAS | 82.5 | 96.8 | 97.6 | -15.1 | 20.0 | pass |

Read: stacking the field is worth about 4 points of Classic tail (170.6 to
174.7 on w5, 187.7 to 191.4 on w4) and nothing in Showdown, where the split
constraint changes which players sit together but not how the scores spread.
The stacking limit the v1 registration named was real and is now measured
in, and it is not the main cause of the misses. Two things the comparison
shows instead, recorded for whoever registers a v3, not acted on here:

1. **The Showdown misses are level, not dispersion.** On PIT@CLE the
   realized MEDIAN entry scored 89.6 against an implied median of 69.2, and
   the top 1% missed by the same 24 points; on ATL@NO the median missed by 9
   and the top 1% by 12. The whole sampled field scores low on those games,
   which is the projections (the joint bank's marginals) running under the
   game that was played, not the field model. A single game's outcome
   cannot say whether that is bias or one draw; the Classic medians (w4
   133.6 vs 116.9 realized, w5 124.6 vs 131.8) go both ways.
2. **The gate rule is harder than it reads.** It asks every checked slate's
   one realized outcome to land within the format's week-to-week SD of the
   model's implied median. For a perfectly calibrated model whose implied
   top-1% band is as wide as the recorded p5-p95 (Classic ~50 points,
   Showdown ~70), a single slate passes well under 100% of the time, and
   three Showdown slates all passing is roughly a coin flip. Every realized
   top 1% here does sit inside the model's own p5-p95 band (`inside90` is
   true on all five slates). This is a comment on the registered rule, made
   after seeing it fail twice, so it changes nothing: a different rule (for
   instance, realized inside the implied p5-p95 on every slate AND the
   pooled signed miss within SD/sqrt(n)) is a v3 registration the owner
   makes before any new slate, not a re-reading of v2.

The runner still refuses `--save` for both formats. Nothing in v2 was tuned
against a realized threshold: the stacking rates came from the standings,
and the gate was run once on the frozen inputs and written once.

## v3 (`nfl-portfolio-objective-v3`, registered 2026-10-30 before any v3 gate number was computed)

Registered on the owner's instruction of 2026-10-30 ("work on #3"), which
adopted the v3 plan: a new gate rule fixed before the gate runs, more draws,
and a selection-stability check. Written and committed before the gate
script was changed or run, and before the realized top 1% of any gate slate
below was read. Only entry counts, score-curve presence, teams, build times
and ownership coverage were looked at to choose the slates.

### What changes, and only this

1. **Draws: 4,000 to 20,000** for both the selection and the evaluation
   bank. The joint copula (`nfl-joint-copula-v1`), the field model
   (`nfl-field-stacked-v2`, `FIELD_STACKING` unchanged), 2,000 field
   lineups, the seeds, the candidate pool and the greedy objective are v2's.
   Reason: v2 picked a different portfolio under a different selection seed
   (Jaccard 0.44 at 4,000 draws on the week-5 BUF@LAR dry run), so the pick
   was mostly draw noise.
2. **Same results, less work.** The field threshold is computed one draw at a
   time instead of holding every opponent's 20,000 scores, and the greedy
   selection keeps a running coverage mask instead of re-scanning every
   chosen lineup. A test pins both to the v2 code's exact output on fixtures.
3. **The gate rule** below replaces v1/v2's rule.

### Gate slates (fixed now)

A gate slate is one the v1/v2 gate never checked, from week 6 on, where the
contest has a stored full-field score curve with at least 10,000 entries (the
floor v1/v2 used for the spread), and the owner saved an ordinary GPP build
before the first kickoff whose snapshot carries the ownership prior on every
player. The source is the latest such build before kickoff.

| format | slate | contest | entries | curve from | source build |
|---|---|---|---:|---|---|
| Showdown | w6 SEA@DEN | 196689718 | 84,384 | stat-api (full) | 36c70002, 22:12 UTC, kickoff 00:15 |
| Showdown | w6 WAS@SF | 196786695 | 47,562 | owner's DraftKings export | 21bf2279, 23:13 UTC, kickoff 00:15 |
| Showdown | w7 NE@CHI | 196947078 | 47,562 | owner's DraftKings export | 3091aa86, 23:20 UTC, kickoff 00:15 |

Excluded, and why: w8 CAR@GB (197208280) has 5,945 entries, under the floor;
every other week 6-8 flagship has stat-api's top 100 only, no curve.
**Classic has no gate slate**: no Classic contest since week 5 has a full
curve. Classic stays unchecked and unenrolled until three fresh Classic
slates meeting the same conditions exist (the owner's weekly export with
`--keep-all` of an entered Classic contest provides the curve). It is then
checked once, by the same rule, in its own freeze file.

### Gate rule (fixed now)

Per format, over its n gate slates (n at least 3, else unchecked):

- **Where the realized top 1% falls.** u = the share of the 20,000
  evaluation draws whose implied within-draw top-1% score is below the
  realized one (a tie counts half). For a calibrated model u is uniform.
- **Coverage:** at most k slates may have u outside [0.05, 0.95], where k is
  the smallest number with P(Binomial(n, 0.10) > k) at or below 0.05.
  For n = 3, k = 1.
- **Bias:** with z = the normal quantile of u (u clamped to half a draw from
  0 and 1), the mean z must lie within plus or minus 1.96 / sqrt(n).
- **Stability, every gate slate:** arm B rebuilt from the source build's
  candidate pool with selection seed 20261016 and again with 20261019 (field
  and evaluation seeds unchanged) must choose the same lineups at a Jaccard
  overlap of at least 0.80.

A format passes only if all three hold. For a perfectly calibrated model the
two calibration checks together fail about 8% of the time at n = 3 (coverage
2.8%, bias 5%), where v1/v2's rule failed such a model about half the time.

**Power, stated now.** The Showdown implied top-1% band has been about 70
points wide from p5 to p95, a standard deviation near 21. A consistent
24-point level miss on all three slates gives a mean z near 1.1 against a
limit of 1.13: this gate catches gross miscalibration, not a modest bias.
That is the honest reach of three slates, and it is why the forward test
(metrics and decision rule above) still decides whether B is any good.

### Mechanics

`python -m research.nfl_portfolio_freeze --format showdown` writes
`artifacts/nfl-portfolio-objective-v3/freeze-showdown.json` once (the file
cannot be overwritten). The runner (`research/nfl_pool_rule_study.py`)
reads the freeze for the slate's format and refuses `--save` unless it
passed and its module hashes still match. A failed format is not re-gated on
these slates under any new rule; a different model is a new version with new
gate slates.

If Showdown passes, enrollment starts with the first flagship Showdown after
the freeze is written, with an owner build saved before kickoff and both
arms saved by the runner before that kickoff. Population, metrics and the
8-then-16 decision rule are v1's, unchanged, and the v3 cohort never pools
with anything earlier.
