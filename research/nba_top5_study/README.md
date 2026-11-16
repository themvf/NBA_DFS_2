# NBA three-player contest: how to pick the five lineups (study, Nov 14-15 2026)

Contest: an entry names three NBA players; the score is their summed actual points (a player who
sits scores 0); picks lock at the first tip. The owner's goal is the best points projection, so
every measure here is field-free: how high the best of five lineups scores, and how often it is
one of the night's best trios. Splitting a prize is out of scope.

Research only. Nothing here feeds a page, and no registered constant or hash-pinned file changed.
The simulated-field work from the first day is kept only in the data copy (`approaches/`).

## Data

Not in git. The full working copy (187 MB: line captures, box scores, injury reports, stat-api
rows, caches, the first day's approach folders) is in `data/nba_top5_study/` of the main checkout;
`_paths.py` finds it from a worktree, or set `NBA_TOP5_DATA`. To rebuild from the database
(read-only): `build_dataset.py <out>/data`, then `proj/dump.py`, `proj/dump_inj.py`,
`proj/dump_statapi.py`, `proj/build_table.py`.

Two samples: 24 real nights (Oct 20 - Nov 13 2026) replayed one minute before the first tip, and
130 nights of 2025-26 where the "line" is a stand-in built from each player's recent average
(`top5_bed.py`; it runs high for the top two players, so use it for structure, not levels).
Intervals are night-clustered bootstraps.

## Findings

| Question | Script | Result |
|---|---|---|
| Is a later line better than an earlier one? | `proj/analyze.py` | Barely. Lock vs first of day -0.009 MAE [-0.033, +0.013]. Line moves are about 65% real. |
| Is any book sharper than the consensus? | `proj/analyze.py` | No (all within 0.02). A price-adjusted mean of the books' lines beats the consensus by 0.013 MAE [0.004, 0.023]: real, tiny. |
| Teammate ruled out (this season, vs the books' line) | `proj/step56.py` | First game a 15+ scorer misses: teammates +1.40 vs the morning line [+0.18, +2.85], +0.47 vs the last line [-0.33, +1.34], n=29. The books take about two thirds. |
| Teammate ruled out (last season, vs own average) | `proj/study24.py` | First game +1.9 points [+1.6, +2.2], later games +0.9. Players averaging 22+: nothing. As a rule on the second half: squared error -1.7 [-3.2, -0.2] for the first game only. |
| Questionable players | `proj/step56.py` | Play about 6 in 10 at the lock (10 of 16; repo count 12 of 21). Four or more books quoting does not clear them (9 of 15). When they play: -3.2 minutes [-4.6, -1.9], -1.15 points [-3.1, +0.6]. |
| Which statistic should rank trios? | `proj/study1.py` | The average total. Percentiles (60th to 95th), chance-to-be-best and coverage rules are level on last season and about a point worse on the real nights. |
| Best source for the ceiling | `proj/study3.py` | The books' ladder runs high (6.4% beat its 90th percentile). The walk-forward top-end correction fixes most of it (pinball -0.032 [-0.047, -0.016]) and matches stat-api. |
| How much of a miss is minutes? | `proj/study24.py` | About a quarter (23% for 18+ scorers). Minutes news matters mainly for role players. |
| Players beyond the top 20 lines | `proj/study5.py` | Not worth it. The best trio on the whole slate beats the best inside the top 20 by 1.5 points on average, zero on 17 of 24 nights. The books overstate deep players' 30-point nights (8.7% said, 4.2% happened for ranks 21-30). |
| How to spread five lineups | `proj/study6.py` | Nothing reliably beats the five biggest line sums. A mild spread (about 8 players, each lineup within 6 of the top) is +1.1 [-1.9, +4.7] real and +0.8 [-0.2, +2.0] last season. Locking #1 and #2 into all five loses about a point in both; splitting the stars loses 6 to 9. |

## What follows from it

1. Project with the books' line; rank trios by the summed line.
2. Default build: the five biggest line sums, always on 2-4 game slates. Do not lock the top two
   into every lineup.
3. Leave a Questionable player out of a single entry.
4. One candidate rule: a 15+ scorer ruled out after the last line reading is worth about 2 points
   to teammates averaging under 22. Untested against lines at scale.
5. The gap to the night's best trio (12-13 points) is inside the top 20. It is scoring luck, not
   a missing player or a ranking rule.

Open: a forward test of the mild spread (about 150 nights to settle a one-point gain), and why
only 44% of last readings carried a usable ladder.

## Sizing a test

The miss (points minus line) has SD 6.3 (5.4 under a 12 line, 8.8 at 25+), and nights are close to
independent (within-night correlation -0.0004). An effect d needs about (2.8 x 6.3 / d)^2
player-games: 313 for one point, 1,250 for half a point. Shrink a small cell with n / (n + 40).
