# NFL DFS Classic: do the Winners-shape TE and WR pool rules cost us? (pre-registered 2026-10-13)

Status: **registered, not started.** No optimizer or setting change follows
from this document; it fixes the test before any outcome is seen.

## Why this exists

The read-only Classic attribution study
(`artifacts/nfl-classic-attribution-2026-10-13/report.md`, PR #428) found, on
the only two slates where the rules can be measured:

- With perfect information, the Winners-shape TE rule (keep tight ends under
  $4,000 with 4+ targets a game) and WR rule (keep receivers with 25%+ of
  their team's targets, or under $5,000) together cost the week-5 optimum
  about 32 points on their own, and removing either from full Winners
  recovered 20-23 points.
- Every one of week 5's verified top-100 entries, and 98 of week 4's, used a
  player those rules exclude (96/100 an excluded tight end, 90/100 an
  excluded receiver in week 5).

That is discovery evidence from the same two slates, hindsight-only, with a
partially reconstructed week 4. It motivates this test; it cannot confirm it.
The study does not support blaming most of our Classic underperformance on
these rules: selection regret is far larger and is descriptive only.

## Hypothesis

H: on Classic main slates, the current Winners shape with only its TE and WR
pool rules removed produces portfolios at least as good as current Winners
shape, by the metrics below.

## Arms (identical except the two rules)

- **A, current:** Winners shape (`nfl-winners-shape-v4` or its successor at
  the time; the version is recorded per slate).
- **B, test:** the identical configuration with the TE and WR pool rules
  switched off. RB and DST pool rules, salary floor, stacking, bring-back
  share, QB cap, DST-vs-offense share, exposure, uniqueness, plan, leverage
  setting, projections, ownership, locks and exclusions are all identical.

Both arms are built pregame from the same frozen pool and saved as optimizer
runs, at the same lineup count as the slate's real build (default 20 if none).
Leverage is held at whatever the real build used; it is not a factor here.

## Population and timing

Every DraftKings Classic main slate from 2026 week 6 on that has a stored
contest with a score curve and top-100 entries (the weekly flagship import).
Weeks 1-5 are discovery and are excluded. Both arms must be saved before the
slate's first kickoff; an arm saved after kickoff voids that slate.

## Metrics (fixed now)

Per slate and arm:

1. **Feasibility:** lineups requested vs built.
2. **Median lineup points** (primary). Paired difference B - A per slate.
3. **Top finishes:** number of lineups inside the contest's top 1% and top
   0.1% (from the stored score curve).
4. **Best lineup rank** (secondary only; a single draw).
5. **Excluded-player participation:** share of arm-B lineups using a player
   arm A's rules exclude, and the same share among that contest's verified
   top-100 entries (entry-weighted and unique lineups).

Players with no recorded score are resolved with
`research/nfl_attribution_resolve_missing.py`; verified non-participants score
0, unresolved players leave a lineup unscored and are counted.

## Decision rule (fixed now)

- No verdict before **8 eligible slates**.
- **B is supported** if the paired median difference B - A is positive in at
  least 6 of 8 slates and its mean is positive with a slate-clustered bootstrap
  95% interval above zero, and B's top-1% count is not lower in total.
- **B is rejected** if the mean paired median difference is negative with the
  interval below zero.
- Otherwise **inconclusive**; the rules stay and the test continues to 16
  slates, with the same bars.
- One look per milestone (8, then 16). The bars do not move.

## What this test cannot say

It compares two portfolio constructions on the same projections. It does not
establish future profitability, an optimal exposure level, or that the rules
explain our results. Eight slates are little evidence; the bars are set to
detect only a large, consistent effect.

## Required before the first slate

A small research script that, given a saved Classic slate and a real build,
writes both arms as saved optimizer runs (read-only with respect to settings;
it builds, it does not change defaults). Not yet written.
