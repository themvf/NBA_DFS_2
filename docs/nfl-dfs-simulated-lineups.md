# NFL DFS: simulated-game lineup scoring and the absence mixture (2026-11-03)

Prompted by the CHI@SEA Showdown on 2026-11-02. The build put Roschon Johnson
(median 1.95, mean 7.37, $3,400) in 25 of 50 lineups, Jason Myers and Luther
Burden in exactly 25 each, and paired Smith-Njigba with Swift in 3. Every
lineup in the contest's top 1% had both of them.

## What was wrong, mathematically

1. **Caps set exposure, not the model.** Joint selection maximised a sum of
   player scores under exposure caps. A linear objective puts the optimum on
   the constraints, so every player who beat his replacement was used up to
   his cap. Myers and Burden sat at 25 of 50 in all three builds that night.
2. **A lineup's ceiling was the sum of player ceilings.** Summing P90s equals
   the lineup's P90 only if every pair is perfectly correlated. That overpaid
   high-variance cheap players (their spread added in full instead of in
   quadrature) and gave correlated pairs no credit.
3. **Role changes stretched a backup's range.** The absence adjustment
   multiplied every quantile by one factor, keeping a backup's shape (median a
   quarter of the mean) for a player whose premise is that he has the job.
4. **The randomness never changed a choice.** Uniform noise of +/- 8% of the
   projection (0.59 points for Roschon) against a 4.8-point margin.
5. **Leverage had the wrong form.** Duplication is about field size times the
   product of slot ownerships; the old factor scaled each player's ceiling.
6. **The boom bonus was inert** (at most 1.4 points).

## What changed

**`gppScoring: "draws"` (`nfl-lineup-draws-v1`, `web/src/lib/nfl-dfs/lineup-draws.ts`).**
Lineup L is solved against one correlated simulated game. Every player is
drawn from his own quantile curve (P10, median, P90, bent so the mean is his
projection, shared with the captain simulation). Dependence is a Gaussian
copula: game factor 0.15 and team factor 0.25 of variance (teammates 0.40,
opponents 0.15), DST -sqrt(0.3) on the offense it faces, separate games
independent. These are stated priors, not fitted. A player then appears in
about the share of draws in which he belongs in the best lineup, so exposure
follows P(in the optimal lineup). Correlated players boom together, so they
are built together. The draws replace the jitter; joint re-selection is
skipped in this mode, because re-picking by projection would undo it. The
draws are reproducible by `drawSeed`. Cash and Diversified builds are
unchanged.

**Lineup-level leverage.** In draws mode, when leverage is on, each slot earns
`pointsPerHalving x log2(1 / ownership)` (default 1 point, a stated prior),
because ln(duplicates) = ln N + sum of ln(slot ownership). The captain's term
is not multiplied by 1.5.

**Absence commit v2 (`nfl-absence-commit-v2`).** An upward own-role adjustment
is now a mixture, `(1 - pi) * own range + pi * absent starter's range`, with
`pi = (target mean - own mean) / (starter mean - own mean)`. The projected
mean is exactly the v1 mean (the audit below found v1 means sound); only the
shape changes. The starter's range is his pre-availability range, recorded
when the pipeline zeroed him. If there is no starter range, or the starter
projects no higher than the target, v1 scaling is used and the record says
why. Roschon on 2026-11-02: pi 0.32, median 1.95 -> 4.6, P90 22.9 -> 17.2,
mean unchanged.

**Defaults.** New page builds use simulated games ("Lineup scoring" control in
Build shape). A restored run saved before this keeps Player ceilings, and
`sameGenerationSettings` treats a missing value as ceiling. The ghost
factorial and research studies keep `ceiling`: the optimizer default is
unchanged and only the build form sets `draws`.

## Replay evidence (descriptive, not a validated edge)

`research/nfl_draw_portfolio_replay_export.py` then
`web/scripts/replay-nfl-draw-portfolio.ts`. Every 2026 Showdown slate with a
saved GPP run and an imported contest score curve was rebuilt with its own
settings under both scorings (5 seeds for draws). Lineups were scored with
real points and placed in the real field.

- Ten slates were graded. Three week 1-3 snapshots predate saved medians and
  were dropped. When the saved settings no longer build, the same relaxation
  was applied to every variant, and the output says which.
- **Paired, draws minus ceiling, mean over 10 slates (95% bootstrap CI over
  slates):**

  | Metric | Difference | 95% CI | Draws better / worse |
  |---|---|---|---|
  | Players exactly at a cap | **-1.54** | [-2.48, -0.52] | |
  | Best lineup's field percentile | +0.95 | [-1.03, +3.14] | 6 / 4 slates |
  | Top-10% share | +2.0pp | [-1.3, +5.3] | |
  | Share above the field median | -0.7pp | [-5.6, +5.0] | |
  | Mean lineup score | +0.10 | [-1.45, +1.73] | |

- **CHI@SEA:**

  | | Ceiling | Draws (5 seeds) |
  |---|---|---|
  | Smith-Njigba + Swift together | 4 lineups | 7-11 lineups |
  | Roschon Johnson | 24 | 11-17 (11-15 with the v2 mixture) |
  | Myers | 25 | 13-21 |

Reading: the mechanical repairs work, and results are not worse. Ten slates
cannot show that they are better. Grade the change live through the weekly
review.

## Range calibration audit (weeks 2-8, pregame pool captures)

The tail audit (2,582 player-weeks) found that range miscalibration is mostly
about who plays, not about the shape of the range:

- **21.5% of players not ruled out recorded no stat line.** Scored as DK paid
  (those players at 0), 59% of results fell below the median and 23% below
  P10. Excluding them, the median is exactly calibrated (49.9%, bias -0.03).
  The ranges need a did-not-play probability for unconfirmed backups, not a
  lower median.
- **For players who played, P90 is too narrow:** results beat it 13.7% of the
  time [12, 15] against 10%. RB1/RB2 run about 16%, and RB2s are
  under-projected (+1.5 points).
- **Mean-20+ projections go the other way:** they beat P90 only 2.5% of the
  time, with bias -2.0. Do not widen the top tier.
- **Market-anchored ranges are the best calibrated** (10.0% beat P90 and 50.3%
  finished below the median). Absence-adjusted ranges show no
  miscalibration, but n = 81.

Follow-up, same day (`nfl-range-calibration-v1`,
`web/src/lib/nfl-dfs/range-calibration.ts`; full numbers in CLAUDE.md):
- **Did-not-play mass: built.** It is a chance of a 0 by depth-chart rank.
  Simulated games draw the if-he-plays range and score a 0 that share of the
  time.
- **Means pulled toward the middle: built.** The slope of actual on projected
  was 0.87-0.90.
- **Wider P90s: not built.** The long-memory range split (Oct 26) already
  calibrates RB/WR/TE. Widening the rest made replayed lineups worse
  (best-lineup percentile -0.93 [-2.09, 0.00]).

Operational note: the CHI@SEA build ran about 2 hours before kickoff. By
kickoff the market anchor had moved Roschon to 3.15 / 0.83 / 9.20. Rebuilding
after the pre-kickoff prop capture would have changed him more than any fix
here.
