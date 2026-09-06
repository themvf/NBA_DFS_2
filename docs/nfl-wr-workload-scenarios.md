# WR workload scenario increment

Player Context (`/dfs/nfl/history`) now includes a WR teammate selector and present/absent comparisons for targets, mean DK points and P10/P50/P90. Estimates combine complete scored receiver games with matching-state games; the matching-state weight is n/(n+8). At least six scored games and three observed games in each state are required. This keeps tiny splits from becoming unqualified projection changes.

Absence requires a roster entry, zero recorded offensive plays, and complete scrimmage-personnel coverage. Missing roster/participation is unknown. Positive recorded participation establishes presence, not full-game health or a starting role. All estimates use the same historical team; no opponent bonus, route count, injury reason or causal target-transfer claim is inferred.

The replay selects the leading other WR/TE by earlier team targets and estimates each scored WR game from strictly earlier weeks. Target-game personnel is an oracle input observed after the game, so the experiment cannot justify live deployment. It is an already-inspected 2025 retrospective diagnostic. Same-week/future observations cannot enter the fitted estimate. Missing scoring rows are excluded, which limits the cohort.

63 cases met the split requirements; 30 were teammate-absent cases. Mean absolute error worsened from 5.478 to 5.542 DK points and 80% interval score worsened from 23.572 to 24.862. The absent subset also worsened. Optimizer activation is withheld. Existing projections remain unchanged.

Run `npm run test:nfl-workload` and `npm run replay:nfl-workload` from web. The saved report contains cohort exclusions, source SHA-256 and recipe SHA-256. Tests cover shrinkage, incomplete personnel, temporal cutoff, team isolation and ordered quantiles.

Next research step: recover timestamped pregame availability and additional historical seasons, then model team pass volume and receiver target shares jointly. This initial full-score empirical conditioning model has not shown sufficient predictive value.
