# Winners + Diversified Classic construction v3

Owner requested implementation on 2026-10-31 for November 1 lineups. This is
an opt-in construction profile, not a promotion of a predictive model or the
portfolio-objective study. Choose **Winners + Diversified** under Build shape
on a Classic GPP slate. Standard and Winners remain available.

## Acceptance contract (defined before release)

- Return exactly the requested lineup count or fail with an actionable error;
  do not save a partial Diversified portfolio.
- Preserve canonical eligibility, salary, player locks/exclusions, explicit
  exposure bounds, opportunity requirements and leader-coverage identities.
- Independently recount final position caps, combination caps, roster legality
  and pairwise overlap. Never widen a hard bound to make a search succeed.
- Save resolved policy, algorithm/build identity, candidate roster keys,
  candidate count/attempts/stopping reason, and final exposure diagnostics.
- Preserve the existing Standard/Winners and pinned ghost factorial behavior.
- Describe forecast scores and concentration as construction diagnostics.
  Improvement in tournament results is unvalidated.

## Defaults and precedence

| Risk preference | Default |
|---|---:|
| QB / RB / WR / TE / DST maximum | 20 / 55 / 40 / 45 / 25% |
| Pair outside primary stack | 25% |
| Triple outside primary stack | 15% |
| Player + DST pair | Smaller of 20% and generic pair cap |
| Soft concentration threshold | 25% |
| Soft penalty strength | 0.5 search points; quadratic cumulative cost |
| Minimum distinct QBs | 0: automatic, optional user minimum |
| Candidate target | 500 distinct rosters |
| Minimum differences | At least four players |
| Salary used | At least $48,100; a higher requested floor wins |
| Own offensive player versus DST | Forbidden |

Percentages floor to counts, with at least one appearance allowed for small
portfolios; UI shows effective counts. A global exposure maximum can tighten
position defaults. An explicit player maximum replaces its default position
cap. Locks explicitly set that player's minimum and maximum to the full
portfolio. Combination caps still apply to locks: conflicting locks produce
a named error. An explicit minimum exceeding its resulting maximum fails.

Primary stack means the selected QB and his same-team WR/TE. A pair/triple
wholly within that stack is exempt from the generic combination cap. QB/RB
combinations are counted. Two same-team receivers outside their QB's lineup
are counted; the solve models the exemption conditional on selecting that QB.
The player/DST cap is applied to every player plus DST combination, whether
or not the players oppose one another.

This profile forces ownership influence off on both the server and optimizer
paths, including restored requests. It does not rewrite ownership estimates.
It requires a QB pass catcher and restores the Winners composition defaults:
40% double stacks, 50% bring-backs, 30% two-TE lineups, at most 35% four-WR
lineups and at most 35% lineups with a same-team RB/WR/TE pair outside the
QB game. All five controls are adjustable in the form. Exact targets round
to the nearest lineup; the two caps round down. Initial construction does
not relax these rules, and every accepted replacement is checked against
the final composition. Inability to find a complete compliant portfolio is
a build error, not proof that no feasible portfolio exists. The 30% TE
target is a historical compromise, not a fitted optimum. Existing input projections and
source qualifications are retained.

## Search

The first pass constructs a feasible portfolio, with exposure soft costs and
saturated-combination constraints applied during each solve. The incremental
soft penalty is derived from the same cumulative quadratic cost used during
reselection; it is normalized to 50 entries. This controls repeated exposure
without claiming to model joint game outcomes.

Additional candidates rotate eligible QBs across ceiling, mean-projection,
bring-back and double-stack proposals. Candidate rosters must be distinct;
they need not differ by four players from the entire candidate bank. Final
portfolio exposure/combination budgets are not spent on unselected candidates.

Candidate generation stops at the requested target, twice that many attempts,
or 25 seconds, whichever comes first. Each candidate solve has a 150ms limit;
first-pass solves have a 3-second limit to accommodate the combined constraints. Integer output and completed rosters
are checked. Time-limited searches do not imply that the target was reached
or optimality was proved. The exact roster-key bank is saved for audits.

Two bounded passes consider single-roster replacements (at most 12,000
evaluations) using the common ownership-neutral search objective minus
concentration cost. Every accepted replacement must preserve the complete
portfolio contract, including minimum exposure, overlap, signal requirements,
salary bands and leader coverage. The existing scenario selector is not used.

## Verification and research boundary

`web/scripts/test-nfl-diversified.ts` exercises full builds, ownership
neutrality, explicit overrides, exclusions, optional QB breadth, conflicting
locks, impossible plans, combination exemptions and deterministic fixed-target
replay. Existing Winners, Showdown, exposure, QA, ghost and joint-selection
checks protect the unchanged profiles.

`web/scripts/replay-nfl-diversified.ts INPUT_JSON [CANDIDATE_TARGET]` reads a
frozen `{run:{input_snapshot,settings}}` and performs a database-free replay.
The owner's November 1 snapshot is discovery/construction verification only.
The earlier v1 replay (before restoring Winners composition) had a
historical baseline mean of 137.79; a bounded replay returned 50
legal lineups at 139.67, with four players at 40%+, versus eight previously.
It considered 237 distinct candidates and disclosed its 25-second candidate
search limit rather than claiming the 500-candidate target was reached.
These are model projections, not observed improvement.

No existing study hypothesis, stability threshold, gate slate or ghost arm
is changed. The model-completion implementation manifest carries a dated
provenance amendment for the changed optimizer and its new dependency; prior
implementation hashes remain recorded and evidence cohorts remain separate. Future performance comparisons require separately registered
paired arms and a verified complete-contest grading path. Classic remains
unqualified for field-relative simulated top-1% claims. The portfolio-objective
study's 0.80 roster-stability gate and spent-slate exclusions remain intact.

## November 1 v2 construction verification

The frozen 50-lineup input produced 50 legal lineups with 20 double stacks,
25 bring-backs and 15 two-TE lineups, respecting both 35% composition caps
and all diversification limits. This is construction verification, not
outcome evidence. Earlier saved portfolios retain their original settings
and version; newly generated diversified builds use v2 defaults.

## More diversity button (v3)

Owner requested a one-click preference for less shared exposure across the
portfolio. Under Winners + Diversified, choose **More diversity**, then
**Generate & save**. The button changes numeric preferences; it does not
regenerate, modify, or replace a saved lineup set. **Default diversity**
restores the original concentration preferences. Manual edits show Custom.

| Preference | More diversity |
|---|---:|
| QB maximum | 12% |
| RB / WR / TE maximum | 35% each |
| DST maximum | 20% |
| Non-primary pair / triple maximum | 18% / 10% |
| Player + DST maximum | 12% |
| Soft exposure threshold / strength | 20% / 1.0 |

Composition targets, candidate count and explicit minimum QBs survive both
buttons. Locks and player-specific exposure overrides retain precedence and
are disclosed in the UI and saved report. The same full-portfolio validation
applies; no automatic relaxation is introduced. Settings comparison marks a
changed preset as requiring regeneration, and saved numeric settings restore
which preset matches. Old saved defaults remain defaults.

These are user-selected risk preferences, not fitted optimal weights. Less
shared player exposure does not measure or guarantee uncorrelated lineup
scores: games, teammates, and shared outcomes can still correlate entries.
No scenario covariance or profitability claim is made. Forecast scores may
fall when stricter limits require different players. v3 changes preset UI
and defaults selection only, not the underlying solver or study arms.

Frozen current-run replay with More diversity returned 50 lineups, 14 QBs,
no player at 40%+, a 34% highest exposure, and 1.81 mean shared players
between lineups (saved baseline: 8 QBs, 48% highest exposure, 2.35 shared).
It preserved 20 double stacks, 25 bring-backs and 15 two-TE lineups; all
composition and combination caps passed. The 500-target search considered
214 candidates within its budget and accepted no replacements. This replay
is discovery/construction evidence only and is not a return forecast.

## Explicit minimum scheduling repair

The November 1 request with eight player minimums and a Higbee maximum
stopped at 49/50 in a frozen More diversity replay. Prioritizing unmet
explicit minimums during initial construction returned all 50 with every
minimum, combination cap and Winners target satisfied. This priority bonus
is a construction heuristic (1000 per still-needed appearance), not a player
forecast or reported score. It is disabled for the unselected candidate bank;
final selection retains its common score and validates all hard bounds.
No-minimum builds have identical construction scores. Failure messages now
list any unmet player minimums. Feasibility is not guaranteed for arbitrary
conflicting requests. Standard/Winners behavior is unchanged.
