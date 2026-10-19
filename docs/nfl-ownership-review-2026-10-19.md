# NFL DFS ownership projection — review and five improvements (2026-10-19)

Scope: how projected field ownership is produced for the NFL DFS page, how it
has graded against real DraftKings tournament ownership, and five changes that
would improve it for Classic and Showdown GPPs of 50,000+ entries. Read-only
review from the repository; the live database was not reachable from this
session, so every number below is one the repo already records. Nothing here
changes code or capability.

## 0. Corrections after the independent review (2026-10-19)

An independent review with database access graded frozen pregame forecasts
against stored 50,000+ entry contests. Where it contradicts this doc, it wins:

- Classic rank order is weaker than the 0.80 Spearman quoted below. The
  calibration script does not average tied ranks, and with ~500 near-zero
  players that inflates Spearman; a full-export recount of the week-2 contest
  with current code gave Spearman 0.663, MAE 0.99 over all 670 players but
  4.32 among the 135 players owned at least 1%. Frozen week 4 and 5 Classic
  forecasts graded 3.54 and 3.92 MAE (missing labels excluded, not zeroed).
  Grade on the owned-at-least-1% population from now on.
- The TE FLEX budget is measurably wrong: TE ownership sums in weeks 3-5 imply
  34-38% TE FLEX usage against the prior's 5%.
- Captain positional frequencies and "never K or DST" in section 2.2 and 3.4
  come from top-100 users and winners, an outcome-selected sample. Fit captain
  priors on full-field captain shares (`nfl_dfs_field_ownership` stores them),
  not on winners.
- The two Showdown "holdouts" cited for v3 had 8,917 and 22,136 entries; they
  do not validate v3 for 50,000+ fields. The frozen ATL-NO forecast graded
  9.27 FLEX MAE.
- Section 1.2's claim that the accuracy gate is unwired was wrong; see the
  corrected bullet.
- "Simulation is the only route" to feasibility (3.3) is too strong:
  constrained probabilistic and enumeration methods also enforce it. The
  acceptance test for any field generator is agreement with complete fields
  on exposures, stacks, salary spend and duplication, not seed count.

## 1. What runs today

### 1.1 Production: `nfl-ownership-prior-v3` (`web/src/lib/nfl-dfs/ownership-prior.ts`)

A stated prior, not a fitted model. Computed in `attachOwnership`
(`web/src/app/dfs/nfl/actions.ts`) from the workspace players AFTER
availability zeroing, the absence commit and the market anchor, so the
"our projection" input is the number the page shows, not the raw model.

```
blend  = 0.6 x ourProj + 0.4 x DK printed average          (one alone if the other is missing)
value  = blend / max(salary, 3000) per $1K
score  = max(blend, 0.5)^2 x max(value, 0.2)^k x status     (Q 0.6, D 0.25, OUT 0)
         k = 1.5 Classic, 0.5 Showdown (v3)
Classic:  share = score / sum(score) within position x budget, cap 60%, excess re-shared
          budgets QB 100 / RB 255 / WR 340 / TE 105 / DST 100  (FLEX split 55/40/5 RB/WR/TE)
Showdown: captain = score(captainSalary)^1.5 -> 100% budget, cap 50%
          flex    = score -> 500% budget, cap min(90, 95 - captain)
```

Inputs it uses: our projection, DK average (`avgFptsDk`; `ppg` from the DK pool
poll on system slates), salary, position, DK status, captain salary.
Inputs it does not use: spread, team implied total, salary tier, depth/role,
targets or carries share, week-over-week salary change, status change timing,
opponent, game time, any consensus projection, any prior-week ownership.

### 1.2 How the number is consumed

- `ownPct` (slot-level `captainOwnPct` / `flexOwnPct` in Showdown) feeds the
  GPP objective `(capped ceiling + boom) x (1 - own)^0.5`
  (`nfl-optimizer.ts`), the chalk-fade archetypes and the exposure reports.
  The duplication model stays off (`duplicationModel: false`) because the
  source declares itself heuristic.
- The leverage opt-in is ON by default since 2026-10-12. Between 2026-10-04
  and 2026-10-12 it was forced off, so the Sunday builds in that window did
  not use ownership at all.
- `assessOwnership` caps the prior at `heuristic_uncalibrated`. The
  non-heuristic path already requires a passing calibration record
  (`ownershipAccuracyReason(options.calibration, ...)`) before it returns
  `validated`, so the Phase 2 gate IS wired; what is missing is a source
  that declares itself non-heuristic and carries qualifying evidence.
  (Corrected 2026-10-19 after independent review; the first version of this
  doc, and the "governance fix required" line in `docs/nfl-ownership-model.md`,
  said the gate was unwired.)
- The ghost factorial holds ownership fixed across its 32 arms and varies only
  the leverage exponent (0 / 0.5 / 1.0), so it measures how much to trust
  ownership, not how good the ownership is.
- The field sampler for the portfolio objective (`nfl-field-stacked-v2`) draws
  opponents from the prior plus measured stacking rates; its freeze gate
  failed twice, and the Showdown misses were level (projection), not shape.

### 1.3 Two dormant code paths

1. `research/nfl_ownership.py` + `model/nfl_ownership.py` (`nfl-ownership-v1`,
   runbook `docs/nfl-ownership-weekly-runbook.md`): a ridge on log ownership
   with 16 features (salary, projection, DK average, missingness flags,
   within-position salary / projection / value ranks, pool size, backup role,
   position dummies), separate Classic / Captain / Flex fits, roster-total
   normalisation and a salary-feasibility penalty. Its artifacts
   (`artifacts/nfl-ownership-v1/`, 2026-10-04) were trained on two contests,
   report `trained_forward_folds: 0`, and the forecast was built on a
   snapshot that "inherits a salary/projection snapshot last updated
   October 3". It has never been integrated and has no later artifact.
2. `model/nfl_ownership_model.py`: a three-feature logistic (salary,
   projection, value) marked `exploratory_marginal_ownership_not_field_generator`.

Three ownership implementations exist; one is in production and it is the
least featured. The ridge in (1) is closer to the registered Phase 2 design
than the prior is.

### 1.4 Ownership evidence now in the database

| table | what it holds | coverage |
|---|---|---|
| `nfl_dfs_field_ownership` | per-player %Drafted per contest (overall; captain and flex split in Showdown) | DK exports: wk2 Classic 317k, wk3 Classic 7k, wk2/wk3/wk4 Showdowns; stat-api flagships weekly from wk4 (the Millionaire and the biggest Thu/Mon Showdowns), full fields |
| `nfl_dfs_field_structure` | duplication, multi-entry share, QB-stack / bring-back rates, pair lift (top 40 players) | 5 Millionaires wk1-5 (1,490,188 entries), 8 Showdown flagships (top-100 users only) |
| `nfl_dfs_field_top_entries`, `nfl_dfs_field_user_builds` | top finishers' every lineup, exposure vs field | per flagship |
| `nfl_dfs_published_winners` | the Millionaire rank-1 lineup each week, with draft % per player | 46 weeks, 2023 w1-17, 2024 w1-18, 2025 w1-11 |
| `nfl_dfs_pool_captures`, DK pool status poll | pregame pool, status, DK ppg every 15 min in the two hours before kickoff | since late September |

So the training corpus for Classic is roughly one labelled slate per week of
2026 (six to seven so far), and Showdown two to three per week. There is no
pre-2026 ownership except the nine players of each published winner.

## 2. How it has graded against real tournament ownership

Every figure below is recorded in `docs/nfl-ownership-model.md`,
`ownership-prior.ts` or `CLAUDE.md`. The calibration script
(`npm run calibrate:nfl-ownership`) has not been re-run in the repo record
since the week-4 flagship imports, and never since the market anchor started
changing the projection input (2026-10-12), so the Classic numbers describe a
prior whose main input has since changed.

### 2.1 Classic

| contest | n | Spearman | MAE (pts) | bias | note |
|---|---:|---:|---:|---:|---|
| wk2 Classic, 317,082 entries (v1) | 670 | 0.80 | 1.03 | 0.00 | by position TE 0.85, WR 0.81, RB 0.79, QB 0.74, DST 0.33 (MAE 4.07) |
| wk4 Millionaire (stat-api, v3 Classic = v1) | 629 | 0.80 | 1.06 | 0.01 | first stat-api grade |

Reads: rank order is good and stable; the 1.0-point MAE is flattering because
~550 of the 630 players are near zero on both sides. The misses that matter
are concentrated and all of one kind:

- **Over-projected chalk is where our model likes a player more than the
  market**: Swift 31.6% projected vs 7.1% actual, Coker 38.9 vs 17.1, Watson
  34.1 vs 14.0, Smith-Njigba 27.8 vs 10.7 (and 53.5% on the 2026-09-27 live
  slate). The 0.6 weight on our projection is a weight on our disagreement.
- **Under-projected chalk is value and news**: Schultz $3,200 at 18.4% vs 3.2
  projected; Aaron Jones 20.0 vs 2.3 (a starter ruled out, backup became
  chalk). The prior has no role, depth, salary-tier or status-change feature.
- **DST is a Vegas decision**: Spearman 0.33, MAE 4.07. The field drafts
  defenses on spread and opponent implied total; the prior has no line input.
- The 60% cap and the position budgets are stated; no Millionaire has been
  checked for how often a player exceeds 60% or what the realized FLEX split
  is, even though every full-field flagship now carries it.

### 2.2 Showdown

| contest | version | flex MAE | captain MAE | chalk rank |
|---|---|---:|---:|---:|
| wk2 NYG@LAR 47,562 (fit) | v2 -> v3 | 7.18 -> 6.30 | 1.63 -> 1.26 | 0.69 -> 0.76 |
| wk3 ATL@GB 83,234 (fit) | v2 -> v3 | 5.73 -> 5.48 | 1.68 -> 1.52 | 0.79 -> 0.80 |
| wk4 196280117 (holdout) | v2 -> v3 | 4.35 -> 3.82 | 1.63 -> 1.49 | 0.90 -> 0.93 |
| wk4 196204783 (holdout) | v2 -> v3 | 5.23 -> 4.61 | 1.38 -> 1.04 | 0.83 -> 0.85 |
| wk4 TB@DAL (stat-api) | v3 | 4.82 overall, Spearman 0.89, bias 0.00 | | |

Showdown MAE is four to six times Classic's because a 50-player pool with a
600% budget puts 10-30% on most real players, and the prior's misses are
structural: true punts (Ferguson $1,800 TE 19-21.7% actual vs 0.8-1.9
projected; Treadwell $3,200 19.3 vs 2.4), kickers (Shrader 67% projected vs
33.3 actual even after v3), and captain choice (captain among top users is
WR 36% / RB 30% / QB 22%, never a K/DST/TE; the prior's captain is just
score^1.5 with no positional term). On the portfolio harness
(`model/nfl_showdown_ownership_eval.py`) a perfect forecast keeps 4-8 more
projected points per lineup than the prior at equal uniqueness, so the
accuracy gap is worth real points in Showdown; in Classic, perfect ownership
at the production leverage setting moved a build about as much as a new seed.

### 2.3 Field structure the prior cannot express

The prior is a vector of marginals. The measured field is not:

- Classic (whole field, 1.49M entries): 90.2% of entries stack the QB with a
  teammate (top 1%: 95.3%), 45.2% carry a bring-back (top 1%: 61.5%), QB
  pairings sit at 3.4-4.3x independence. Duplication is low (8-9% of entries
  in a duplicated lineup).
- Showdown: 90-93% of entries share a lineup with another entry; 98% of the
  top 1% do. Team split among top users 3-3 32% / 4-2 46% / 5-1 21%; a kicker
  in 41%, a DST in 34%, two QBs in 36%.
- Millionaire winners (stat-api review, week 4): ownership sum 64-115% per
  lineup across the winner's ten lineups; our 107-lineup build averaged 64%
  and its lineups differed from their nearest twin by 2.1 players against the
  winners' 3.6-5.7. The 2023-25 published winners (46 lineups with draft %)
  have not yet been profiled against the prior at all, which is the one
  multi-season check available.

Two consequences. First, the duplication model, the part of GPP ownership
that pays in a 50,000-entry Showdown, is off, and will stay off while the
input is a marginal with no salary-cap or stacking consistency. Second, the
prior sums to 900% / 600% by construction, but nothing guarantees those
marginals can be realised under the $50,000 cap: a slate where the five
highest-scoring players are all expensive gets five large shares the field
cannot afford together.

### 2.4 What the review did not find

- No evidence that any calibration slate was used to tune Classic constants
  (v1 Classic constants predate the first grade; the Showdown value exponent
  was read off weeks 2-3 and held up on weeks 4). Governance has been honest.
- No evidence the prior is systematically biased in level (bias 0.00-0.01 on
  every graded contest), so the problems are allocation, not scale.

## 3. Five ways to project ownership better

Ordered so each one can ship and be graded on its own against the stat-api
corpus; each applies to both formats unless stated. None promotes anything:
promotion stays with the registered Phase 2 gate, wired into
`assessOwnership` as item 2 requires.

### 3.1 Replace "our projection" with the field's number, and use our gap as a NEGATIVE feature

The recorded Classic misses are one defect: the field drafts the consensus
(DK average, the sportsbook lines, the big public projection sites), and our
projection enters the prior at 0.6 weight, so every place we disagree with
the market becomes a false chalk forecast. The repo now computes a
market-implied projection for every player with a full set of prop lines
(`model/nfl_prop_implied.py`, `nfl-market-anchor-v1`: 144 of 407 Classic
players on week 5) and the anchor already overwrites `ourProj` for those, so
the prior's input is now a mixture of market numbers and our numbers without
saying which.

Change: field points = DK average blended with the market-implied projection
(calibrated), and for players without lines, DK average blended with our
projection shrunk toward the position's salary curve. Carry
`ourProj - fieldPoints` as a separate term that LOWERS predicted ownership
when positive (we are above the market; the field is not) and raises it
slightly when negative only for cheap players. Grade: the Swift / Coker /
Watson / Smith-Njigba class of miss should collapse on weeks 2-4 (descriptive)
and on weeks 5+ (holdout). Showdown gets the same input; its captain term
should use the same field points.

### 3.2 Fit the registered residual model on the flagship corpus, with the prior as the offset

The ridge in `research/nfl_ownership.py` is the right shape and has sat at two
contests since 2026-10-04 while the flagship import added a full-field
Millionaire and two Showdowns every week. Fit it as a residual on top of the
prior (log target, prior log-share as a fixed offset, like the market offset
in the MLB moneyline contract) so a small sample cannot pull it off a
sensible base, holdout by slate, one model per position for Classic and per
slot for Showdown. Features the prior lacks and the data already has:

- Vegas: `nfl_season_games.market_spread_line`, `market_total_line`, team
  implied total (DST ownership is 0.33 Spearman without them).
- Salary tier dummies (under $3,000 / $3,000-4,000 / ... ) and the
  week-over-week salary change from the stored pool captures.
- Role: `priorOpportunity` (three-week targets, carries, team target share),
  depth-chart rank from the Sleeper evidence, backup-became-starter flag
  (the absence-commit record already marks it).
- News timing: DK status at lock vs 48 hours earlier, from the 15-minute pool
  captures.
- DK average and the market-implied projection separately, never blended
  (item 3.1 is the prior's version of the same thing; the ridge learns the
  weights).

The gate is already enforced in `assessOwnership`; the fitted source must
declare itself non-heuristic and supply the calibration record. Keep the prior
as the fallback and the offset, and compare the residual fit against a direct
fit and the prior under the same chronological test rather than assuming the
residual form wins (it inherits the prior's position budgets). Retire `model/nfl_ownership_model.py` or fold it in; three code paths
for one number is how the fitted one was forgotten.

### 3.3 Simulate the chalk: generate ownership from what public-input optimizers build

A 50,000-entry field is mostly lineups from optimizers and from the DraftKings
projection column. Those lineups must fit $50,000, hold nine positions, and
(90% of the time) stack the QB. A marginal prior cannot know that two $9,000
WRs and an $8,500 RB cannot all be 50% owned, or that the QB's WR1 inherits
ownership from the QB. Build the field forward: run the production optimizer
(or the stacked field sampler already in `portfolio-objective.ts`) with the
FIELD's inputs (DK average and market-implied projections, no leverage, the
measured stack rates, salary floors matching the field's observed spend),
across many seeds and a spread of settings (max exposure 20-60%, min salary),
and take exposure as the ownership forecast. Blend it with 3.1/3.2 by
position. This is the only one of the five that produces pair co-ownership
(the 3.4-4.3x QB-WR lift the field structure table measures) and so is the
input the duplication model has been waiting for. Grade on marginals as usual
AND on pair lift against `nfl_dfs_field_structure`.

### 3.4 Showdown gets its own model: slots, tiers, team split, and the duplicated lineups themselves

Every Showdown miss recorded is a Showdown-specific structure the Classic
formula cannot see. Build the Showdown forecast in three parts:

- **Captain** as a positional choice first (WR / RB / QB priors from the
  stored top-user and field data; K, DST and TE near zero), then within
  position by captain-salary affordability (captain at 1.5x must leave room
  for five flex plays the field actually buys), then by points. The current
  score^1.5 captain term has no positional or affordability term.
- **Flex** with salary-tier terms (punts under $2,500 are drafted at 15-30%
  on every graded slate; the prior gives them 1-2%), a kicker term fitted on
  its own (41% of top-user lineups carry one; the prior still over-projects
  Shrader at 67%), and the DST term with the spread.
- **Structure**: team split 3-3 / 4-2 / 5-1, two-QB share, QB with 2+
  teammates, all measured. With 90% of entries duplicated, the right output
  is not only marginals but the expected count of each of the few hundred
  lineups the field will actually submit; the duplication model
  (`salary-duplication.ts`) can then be turned on for Showdown with an input
  that was built for it. Grade on the portfolio harness (projected points
  kept at equal real-field duplication), which is the measure that showed a
  perfect forecast is worth 4-8 points a lineup here.

### 3.5 Make ownership a function of time, and freeze it so it can be graded

The prior is computed whenever the page is read, with no notion of when.
Ownership in a 50,000-entry field is set in the last 24 hours: Saturday
projection updates, Sunday 11:30 ET inactives, Showdown's 90-minute
inactives. The Aaron Jones miss (20% vs 2.3%) is a Sunday-morning event; the
Showdown punt misses are late-week value drift. The 15-minute DK pool
captures and the status poll already exist. Change:

- Compute the forecast from the capture nearest the decision time and record
  which capture, so a Wednesday number and a Sunday 12:45 number are
  different forecasts with different expected error.
- Add a late-news redistribution for the field (not for our projection): when
  a starter is ruled out after the Friday capture, move his ownership to the
  backup and to same-salary pivots at a fitted rate, and in Showdown to the
  same-team flex. Fit the rate on the flagship slates where it happened.
- Freeze the ownership forecast into the pregame pool capture (the ghost
  factorial already freezes projections there) and grade every forecast
  automatically in the Tuesday post-week job, per format and per time-of-
  forecast, into an append-only table. Today the calibration is a manual
  script nobody has run since week 4.

## 4. Housekeeping that should happen before any of the five

1. Re-run `npm run calibrate:nfl-ownership` now: weeks 5-6 Millionaires and
   Showdowns are imported, and the market anchor changed the prior's input on
   2026-10-12, so every Classic figure in section 2 is out of date.
2. Profile the 46 published 2023-25 Millionaire winners against the prior's
   shape (their ownership sum, number of sub-5% players, chalk count); it is
   the only multi-season field evidence and it has not been looked at.
3. Measure from the full-field Millionaires what the prior currently assumes:
   the realized FLEX split (the prior says 55/40/5, the dormant ridge 35/55/10),
   how often a player exceeds 60%, and the actual position budgets.
4. Decide the one code path. The dormant ridge is closer to the registered
   design than the production prior; either integrate it (3.2) or delete it.
