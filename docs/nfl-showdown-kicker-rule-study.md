# NFL DFS Showdown: does the Winners-shape kicker ban cost us? (pre-registered 2026-10-14)

Status: **registered before any week-6 Showdown was built; capture runner
implemented.** No optimizer or setting change follows from this document; it
fixes the test before any outcome is seen. Owner's choice of contrast
(2026-10-14): the kicker ban alone, over "kicker ban + salary floor" and
"Winners vs Standard".

## Why this exists

The read-only Showdown attribution study
(`artifacts/nfl-showdown-attribution-2026-10-13/report.md`, PR #427), weeks 1-5,
ten games:

- The verified top-100 entries played a kicker in 14 of 108 (NE@SEA) to 78 of
  116 (ATL@GB) entries, and in 61 of 113 in TB@DAL. The Winners shape plays a
  kicker in none.
- With perfect information the ban cost 5.6 (DET@CAR) and 9.8 (IND@WAS) points
  in the two games where a kicker belonged in the best lineup, and nothing in
  the other eight.
- Selection regret (median about 23 points) is far larger than any rule cost
  and is descriptive only. The ban cannot explain our Showdown results; this
  test asks only whether removing it helps the portfolio.

That is discovery evidence from the same ten games. It motivates this test; it
cannot confirm it. The captain QB/RB/WR rule (cost 0 in every game, followed by
every top entry) and the other Winners rules are not under test.

## Hypothesis

H: on the game day's flagship DraftKings Showdown, the current Winners shape
with only its kicker ban removed produces portfolios at least as good as the
current Winners shape, by the metrics below.

## Arms (identical except the one rule)

- **A, current:** Winners shape (`nfl-winners-shape-v4` or its successor; the
  version is recorded per slate): 5-1/3-3 splits, no kicker, captain QB/RB/WR,
  captain cap 30%, minimum salary $49,500.
- **B, test:** the identical resolved settings with `excludeKickers` off. The
  captain rule still applies, so a kicker can play only flex. Projections,
  ownership, leverage setting, plan, exposure, uniqueness, locks and manual
  exclusions are identical.

Both arms are built pregame from the same frozen pool, at the lineup count of
the explicitly selected saved Showdown GPP build for that game, and saved as
research optimizer runs. If the selected build is itself Winners, its recorded
Winners exclusions must match the reconstruction or enrollment is refused.

## Population and timing

From 2026 week 6 on, each game day's biggest-prize DraftKings Showdown that
stat-api serves in full (the weekly flagship import, Thursday and Monday at
least): it must have a stored contest with a score curve and top-100 entries.
Weeks 1-5 are discovery and are excluded. Both arms must be saved before that
game's kickoff; an arm saved after kickoff voids the slate. A game without a
pregame saved build is not enrolled (it is a missed slate, not a failure of
either arm). One pair per game; the runner refuses a reroll.

## Metrics (fixed now)

Per slate and arm:

1. **Feasibility:** lineups requested vs built.
2. **Median lineup points** (primary). Paired difference B - A per slate.
3. **Top finishes:** lineups inside the contest's top 1% and top 0.1% (from the
   stored score curve).
4. **Best lineup rank** (secondary only; a single draw).
5. **Kicker participation:** share of arm-B lineups with a kicker, and the
   share of that contest's verified top-100 entries with a kicker
   (entry-weighted and unique lineups).

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
- Otherwise **inconclusive**; the ban stays and the test continues to 16
  slates, with the same bars.
- One look per milestone (8, then 16). The bars do not move.

A slate where arm B plays no kicker in any lineup still counts: the optimizer
was free to and chose not to, which is part of the answer.

## What this test cannot say

It compares two portfolio constructions on the same projections. It does not
establish future profitability, an optimal kicker exposure, or that the ban
explains our results. Eight slates are little evidence; at about two flagship
Showdowns a week the first look is around week 9.

## Runner

```powershell
python -m research.nfl_pool_rule_study --study showdown --source-run <UUID> --flagship-showdown --out artifacts/showdown-kicker-<game>-dry
python -m research.nfl_pool_rule_study --study showdown --source-run <UUID> --flagship-showdown --out artifacts/showdown-kicker-<game>-saved --save
```

Same mechanics as the Classic study (`docs/nfl-classic-te-wr-pool-rule-study.md`,
"Required before the first slate"): read-only dry run, one transaction for both
arms, failures preserved, lock rechecked after the inserts, advisory lock and
existing-pair check scoped to the game, no retries or loosened settings, clean
tracked working tree required for `--save`, artifacts with SHA-256 hashes. The
Showdown shape has no pool rules, so no prior-opportunity capture is made.
Regression tests (`npm run test:nfl-pool-rule-study`) require arm A to match
native Winners generation on the frozen week-5 BUF@LAR inputs and the arms to
differ only in `excludeKickers`.

The grader (per-slate metrics above and the milestone bootstrap) is not yet
written; it must exist and be committed before the 8-slate look.
