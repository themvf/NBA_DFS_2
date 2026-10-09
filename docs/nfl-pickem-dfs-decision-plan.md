# Using NFL evidence to improve pick'em and DFS decisions

Date: 2026-09-27
Status: proposed decision and evaluation contract, not a production model promotion.

Implementation handoff: [NFL matchup data: pick'em and DFS projection
improvement specification](./nfl-matchup-data-projection-spec.md). The new
spec defines the shared feature contracts, separate forecast paths,
contest policies, page behavior, delivery packages, and release gates.

## User objective and contest modes

The user plays straight pick'em for both weekly and season-long prizes, and
wants separate DFS recommendations for single-entry/three-entry tournaments
and large-field multi-entry tournaments. Confidence weights are not part of
this user's pick'em format: every correct selection earns one point.

The common objective is to improve contest outcomes. Forecast accuracy,
expected correct picks, expected fantasy points, chance of finishing first,
and expected prize money are different quantities. Report the differences.
Use exact contest rules and payouts before calling any entry optimal.

If the same pick'em card counts for weekly and season prizes, it is one
decision with two consequences. Compare the weekly-oriented card and the
season-oriented card, then select a single card under the user's prize
priorities. Do not imply both can be submitted to one entry. With supplied
payouts, expected weekly prize plus expected remaining season prize provides
an explicit combined monetary objective; probability of first is shown
separately. Do not silently invent a weighting when payouts are unknown.

## Findings from current main and this task

1. `web/src/lib/nfl/pickem-strategy.ts` supports straight pools but simulates
   a one-week, winner-take-all split prize. A standings-aware remaining-season
   policy is absent. Modeled favorite bias and the all-favorite entry fraction
   are stated assumptions, not calibrated observations.
2. The same file's `fieldHomeShare` returns an observed whole-field share,
   but `simulateWorld` uses that share for the non-chalk rivals and then adds
   a separate all-favorite block. This changes the combined simulated share.
   For a home favorite, observed 60% and effective chalk fraction 25% yield
   25% + 75% * 60% = 70%. This contract mismatch must be corrected before
   presenting results as using the entered observed shares.
3. `web/src/app/dfs/nfl/nfl-optimizer.ts` uses per-player upper-tail/boom and
   ownership-weighted scores with sequential constraints. A sum of player
   P90 scores is not a lineup P90, nor a probability of beating the field.
4. Existing exposure, legality, game-script, salary, overlap and export checks
   are useful constraints and should be retained. The missing decision layer
   is joint lineup outcomes against a contest field and payout structure.
5. `captain-simulation.ts` uses fixed unvalidated correlations and top-scorer
   frequency. Being the highest scorer is not the same as appearing at
   Captain in the highest-scoring legal salary-constrained lineup.
6. The duplication helper accepts a field model and field size, but the
   optimizer does not supply them on its current path. Current duplication
   output remains a heuristic, not measured expected duplicate counts.
7. PFR advanced charting has been collected for 33 completed 2026 games. Its
   first retained capture was September 27. Local pick'em integration reads
   descriptive snapshots; that connection is not a qualified DFS feature.
8. The local pick'em PFR reader selects the database snapshot ID but drops it
   from its displayed evidence object. Add snapshot ID, parser/schema version
   and source identity before registering any model-feature dependency.

These findings are based on current GitHub main read on September 27 and the
local PFR work in this task. Local uncommitted changes and deployed behavior
must be reconciled before implementation. This document does not claim that
the repairs below have shipped.

## One evidence layer, four decision policies

Store football evidence once, freeze the exact eligible inputs for each
decision, then use the appropriate contest policy.

| Mode | Decision | Inputs beyond the football forecast |
|---|---|---|
| Weekly straight pick'em | Select the complete card for the weekly prize objective; show expected correct picks sacrificed versus favorites | Pool size, observed or estimated entrant picks, tie rules, payouts, locked selections |
| Season-long straight pick'em | Select a card conditional on present score deficits, rivals and remaining schedule | Standings, weeks remaining, known rival selections/behavior, season payouts, weekly prize overlap |
| DFS single-entry / three-entry | Compare legal complete lineups for that contest; for three entries assess the three together | Site/scoring, salaries, entry count, payout curve, field lineup model, ownership uncertainty |
| DFS large-field multi-entry | Select a joint portfolio whose lineups cover useful winning outcomes at the allowed entry count and cost | Same inputs, plus entry limits, player/team/game exposures, lineup dependence and duplication |

Leading does not automatically mean copy favorites; trailing does not mean
pick every underdog. Simulate the relevant standings and consequences. Early
in a season the expected-score cost of unnecessary divergence matters; later,
the ability to overtake particular rivals can change the best decision.

## Where each dataset helps

| Data | Football question | Action it can support | Limit |
|---|---|---|---|
| Moneylines, spread, total, movement | What is priced into the game now? | Market baseline for winners; existing team environment for DFS; freshness alerts | Do not count the same news or defensive strength twice |
| Availability, depth, platform status | Who can play and who receives the work? | Remove ineligible players; conserved redistribution of carries, targets and QB opportunity; participation scenarios | Active does not prove full workload; replacement efficiency stays player-specific |
| PBP opportunity and role | Who gets attempts, targets, red-zone and goal-line work? | Player volume and scoring distributions; coherent stack and game-script candidates | Use only pre-target history and qualified features; repaired PBP is not automatically pre-lock evidence |
| PBP efficiency, drives and field position | How were points generated and how fragile were they? | Separate turnover/short-field effects from sustained offense; propose residual features | Excluding bad plays is a sensitivity view, not a forecast |
| PFR pressure and contact | What matchup mechanisms might change efficiency or downside? | Research pressure susceptibility, rushing efficiency, and receiver opportunity quality | Game totals cannot yield per-play pressured EPA, clean-pocket splits or four-rush rates |
| Pick shares and prior entrant cards | What will this particular pool choose? | Compare win likelihood against differentiation and tie risk | Public shares are a proxy for a private pool; hidden picks cannot be backfilled before lock |
| DFS ownership and contest entries | Which legal lineups will this contest field use? | Field simulation, ownership sensitivity and duplication-aware selection | Contest-specific; missing ownership is not zero; marginal ownership alone is not a joint lineup model |
| Results and contest standings | Did the forecast and decision actually help? | Paired forecast and decision grading, calibration, promotion and rollback | Late results cannot change the frozen candidate or selection policy |

## Immediate engineering priorities

### 1. Correct and measure the pick'em field model

Treat entered shares as total-field marginals. If a separate deterministic
chalk group is retained, define for each game:

```
q_noisy = (q_total - c_effective * chalk_home_pick) / (1 - c_effective)
```

`c_effective` uses the actual integer chalk-rival count, not the requested
fraction. Infeasible settings must be rejected or adjusted with a visible
warning and a saved effective value. Do not silently alter observed shares.
Handle zero rivals, all-chalk fields, exact 0/100 shares, home and away
favorites, and confidence-mode compatibility for other users. Version and
freeze this policy. Test formula boundaries and simulated marginal recovery.
This repair does not solve the unmeasured dependence between entrant cards.

Collect the user's pool size, tie rules, payouts, standings and prior entrant
cards when available. Build observed and uncertain-field scenarios. A switch
recommended only under one speculative crowd setting should be labeled
fragile. Report the exact expected-correct-picks cost and simulated contest
benefit with Monte Carlo uncertainty; use separate evaluation draws after
candidate search to avoid selecting simulation noise.

### 2. Preserve the opponent-term study and finish its grader

Follow `docs/nfl-dfs-opponent-term-spec.md`: grade frozen `context_variants`,
keep the freeze alive, and retain the registered `opp_carries` formula,
eight-scorable-week gate, promotion and rollback rules. Derive valid paired
rows by study and non-null variant payload, not a stale hard-coded row count.
No PFR feature is added to this variant. Points-allowed mode remains off.

### 3. Connect joint outcome scenarios to DFS selection

Use the current legal-lineup generator to produce candidates first; add a
contest evaluator that re-ranks those candidates before replacing the
generator. Reuse existing scenario and portfolio infrastructure. Draw a game's pace,
scoring and availability state, allocate conserved team opportunities, then
derive player outcomes with the approved projection distributions. Shared
draws must represent teammate and opponent dependence; do not sample every
player independently or sum individual percentiles. New dependence
parameters require empirical estimation and validation, not fixed arbitrary
correlations presented as truth.

Evaluate complete legal lineups on the same outcome draws against plausible
legal field lineups. Include actual contest size, payout rules, ties and
duplicate-lineup splits. Validate the opponent field separately by contest
type. A model registered for the largest GPP is not automatically calibrated
for single-entry or three-max. Keep Showdown Captain and Flex distinct.

Produce separate single-entry/three-max and multi-entry recommendations.
Single-entry is still a tournament: it must not silently become a floor or
cash-game objective. Portfolio diversification is a tradeoff, not a guarantee
of higher expected value.
For multi-entry, select the portfolio jointly rather than taking the highest
N individual scores. Show probability of at least one first-place finish,
expected gross and net prizes, top-percentile finish rates, duplication
sensitivity, exposures and the main game scripts on which the portfolio
depends. Treat these as simulation estimates, not proven profit.

### 4. Test a small number of new football features

Propose separate registrations, not new production boosts:

1. **Pressure mismatch:** offense/QB pressure susceptibility against opponent
   pressure generation, shrunk by exposure and adjusted for opponents.
   Primary targets are sacks/lost passing opportunity and prediction tails;
   do not assume identical effects on every receiver or promote a DST boost
   merely because the narrative sounds plausible.
2. **Rushing contact:** pre-contact/post-contact outcomes, usage and opponent
   run performance as candidates for per-carry efficiency after existing
   workload and market effects. Isolate designed RB runs where possible;
   do not let QB scrambles and kneels define RB blocking quality.
3. **Availability-conditioned opportunity:** evaluate the existing shared
   resolver and replacement-role scenarios on realized participation and
   conserved opportunities before widening their optimizer authority.

Respect the recorded failed studies: opponent passing volume, archetype pace,
team red-zone trips and previous injury redistribution do not become approved
features because they have intuitive explanations. The earlier player
red-zone-share construction had selection/denominator issues; correcting its
definition requires a new study. Scenario labels may describe these football
conditions without claiming a new calibrated predictive effect.

Drops, bad throws, broken tackles and drive labels can help explain these
tests. Do not register every available field at once or assume automatic
regression to a favorable outcome. Compare additions against the same
market-aware baseline, using earlier periods for fitting and untouched
periods for evaluation. Any historical corrected-data screen is labeled
exploratory until point-in-time eligibility can be demonstrated.

## Practical Bucs–Vikings example

The reviewed September 27 market snapshot gave Minnesota about 51.8% and
Tampa 48.2%. Switching a straight pick from Minnesota to Tampa sacrifices
about 0.035 expected correct picks under that baseline. Whether it improves
the chance of winning the pool depends on actual field choices and, for the
season prize, standings. A hypothetical 80% field share on Minnesota would
justify testing Tampa in the contest simulation; it would not prove the
switch profitable by itself.

Minnesota's observed pressure and Baker's seven sacks on 15 charted pressures
justify a matchup-risk flag. They do not supply a calibrated new win
probability. In DFS, the same evidence motivates evaluating a Tampa passing
failure scenario and a run-led scenario. Baker can still be usable at an
appropriate salary and ownership in a lineup that benefits when that thesis
fails. A team win pick is never automatically an instruction to roster or fade
all players on that team. No player-specific projection adjustment or lineup
recommendation is approved by this example.

## Decision report contract

Every recommendation should answer:

1. What is the baseline, and what contest objective are we optimizing?
2. Which observed facts materially change eligibility, opportunity or a
   validated forecast? Which are only warnings or untested scenarios?
3. What does the opponent field likely do, and what is uncertain about it?
4. What action changes: team pick, player allocation, stack, Captain,
   lineup selection or portfolio exposure?
5. How much expected score is sacrificed, and what modeled contest benefit
   is received? Is the change robust to reasonable forecast/ownership error?
6. What new information reverses the recommendation, and by what deadline?

Freeze source snapshots, identity resolution, eligibility, model and policy
versions, contest rules, ownership/pick-share provenance, random seeds,
candidate entries and selected entries before lock. Use exact snapshots for
later explanation. Never substitute current data to explain old selections.

## Evaluation and operating rhythm

Forecast grading and decision grading are separate:

- Pick'em forecasts: calibration, Brier/log loss versus the timestamp-matched
  market baseline. Strategy: expected-correct-picks cost, weekly finish and
  prizes, ties, season position and eventual season prizes on frozen cards.
- DFS forecasts: opportunity error, mean/median accuracy, tail coverage and
  dependence calibration. Strategy: paired equal-entry-count/equal-cost
  portfolios versus the current optimizer, realized prizes and net return,
  top finishes, duplication and drawdown. Cluster uncertainty by slate/week,
  not correlated players or lineups. Rare wins need more evidence than a
  handful of slates; simulations alone cannot establish ROI.

Set challenger-specific materiality and uncertainty gates before looking at
held-out results. Do not alter existing registered gates. Promote only the
consumer/use case actually evaluated; keep a frozen fallback and rollback.

Friday morning refresh builds the initial slate assessment. Monday and
Tuesday refreshes capture weekend/Monday results and revisions and support
postgame grading. These are not sufficient for Sunday lock: add a pre-lock
availability/price check and supported late-swap review with locked players
and legal slots preserved. Friday's final practice reports may arrive after
the morning run. New schedules or paid acquisitions are not activated by
this document.

## Information still needed

Pool entry count, current standings, weekly/season payout amounts, tie rules,
whether one card covers both prizes, and any accessible pick-share history.
For DFS: the exact contests, entry counts/limits, payouts and salary slates.
Until supplied, present explicit parameterized scenarios rather than invented
contest-specific win probabilities.

## References

- Existing opponent-term, availability, ownership, context-consumer and GPP
  portfolio specifications in this repository remain authoritative.
- [Pick'em strategy implementation](https://github.com/themvf/NBA_DFS_2/blob/main/web/src/lib/nfl/pickem-strategy.ts)
- [DFS optimizer](https://github.com/themvf/NBA_DFS_2/blob/main/web/src/app/dfs/nfl/nfl-optimizer.ts)
- [Clair and Letscher: Optimal Strategies for Sports Betting Pools](https://www.stat.berkeley.edu/~aldous/157/Papers/clair.pdf)
- [Haugh and Singal: How to Play Fantasy Sports Strategically (and Win)](https://pubsonline.informs.org/doi/10.1287/mnsc.2019.3528)
- [Picking Winners in Daily Fantasy Sports Using Integer Programming](https://arxiv.org/abs/1604.01455)
