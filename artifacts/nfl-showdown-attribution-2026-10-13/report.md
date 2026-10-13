# Showdown attribution study (read-only)

Generated 2026-10-13T19:20:00.614Z. Inputs: `research/nfl_showdown_attribution_export.py` files in this folder; analysis: `web/scripts/research-nfl-showdown-attribution.ts`. No setting or optimizer code was changed.

## 1. Coverage

| Week | Game | Contest | Entries | Score curve | Top-100 entries | Saved run (lineups) | Model | Optimizer then | Eligibility report | Pool | No recorded score |
|---:|---|---|---:|---|---:|---|---|---|---|---:|---:|
| 1 | NE@SEA | 193391013 | 126,020 | yes | 108 | yes (18) | nfl-dfs-historical-v2 | nfl-dfs-ilp-v5-audited-situations | no (salary-only punt rule) | 68 | 11 |
| 2 | NYG@LAR | 195786068 | 88,235 | yes | 109 | yes (40) | nfl-dfs-historical-v3 | nfl-dfs-ilp-v6-punt-policy | no (salary-only punt rule) | 53 | 3 |
| 3 | ATL@GB | 195943234 | 88,235 | yes | 116 | yes (20) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v7-showdown-legality | no (salary-only punt rule) | 53 | 4 |
| 3 | PHI@CHI | 196036238 | 88,235 | yes | 109 | yes (20) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v7-showdown-legality | yes | 56 | 12 |
| 4 | ATL@NO | 196285155 | 88,235 | yes | 101 | yes (40) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v11-dst-script | yes | 56 | 6 |
| 4 | DET@CAR | 196285055 | 35,671 | **no** | 52 | yes (31) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v8-defensive-dk-fallback | yes | 50 | 2 |
| 4 | IND@WAS | 196204783 | 22,136 | yes | 0 | yes (20) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v8-defensive-dk-fallback | yes | 56 | 3 |
| 4 | PIT@CLE | 196187074 | 88,235 | yes | 104 | yes (20) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v8-defensive-dk-fallback | yes | 51 | 8 |
| 5 | BUF@LAR | 196480476 | 500 | yes | 100 | yes (26) | nfl-dfs-historical-v5 | nfl-dfs-ilp-v12-lineup-integrity | yes | 49 | 7 |
| 5 | TB@DAL | 196438550 | 88,235 | yes | 113 | **none** | - | - | - | 52 | 14 |

Players with no recorded score (no DraftKings contest number and no stat row) are excluded from every pool and counted above; they are almost all $200 depth players and inactive starters, who scored 0 on DraftKings. Source disagreements between the contest's DraftKings points and the results table: 0.

**Declared eligibility** (shared by both solvers): the frozen run's pool, minus pregame availability exclusions (inactive, doubtful, backup-kicker role), which are never relaxed. The cheap-player rule (absolute $1,000 block and the role checks below $3,000) is applied in the *No punts* column and lifted in *punts allowed*. Kicker bans, the Winners-shape pool rule and manual exclusions are not part of declared eligibility; the study switches the kicker ban itself. Runs without an eligibility report (weeks 1-3) get only the $1,000 salary block as their cheap-player rule. TB@DAL has no saved run: its pool is the uploaded salary file and it is hindsight-only. Every recorded score enters the oracle pool, zeros and negatives included: under the split and salary-floor rules a $200 player who scored 0 can be what makes a lineup legal (column 'zero-point gain' in results.json).

## 2. Hindsight optimum (perfect information, one lineup)

Both solvers maximize actual DK points with the captain's 1.5x applied once and DraftKings' cap and both-teams rule. Agreement is required on the optimal score and on legality, for all 16 rule combinations and both cheap-player settings.

**Solver agreement: 316 of 320 cases agree; 4 do not.**

| Game | Cheap players | Rules | Exhaustive search | Optimizer | Optimizer lineup legal? | Optimizer error |
|---|---|---|---:|---:|---|---|
| IND@WAS | No punts | no K + $49.5k floor | 74.27 | 73.77 | yes |  |
| IND@WAS | No punts | no K + $49.5k floor + CPT QB/RB/WR | 74.27 | 73.77 | yes |  |
| TB@DAL | No punts | no K + $49.5k floor | 133.59 | 132.81 | yes |  |
| TB@DAL | No punts | no K + $49.5k floor + CPT QB/RB/WR | 133.59 | 132.81 | yes |  |

Where the optimizer's legal lineup scores below the exhaustive optimum, its integer solver (javascript-lp-solver, default settings) stopped short of the true optimum. Every cost below uses the exhaustive search.

### Cost of each rule (points below the unrestricted optimum, No punts)

| Week | Game | Unrestricted | no K | no 4-2 | $49.5k floor | CPT QB/RB/WR | All four (Winners shape) | Sum of single costs | Interaction | Unrestricted optimum: K? split | Punts allowed: unrestricted (gain) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| 1 | NE@SEA | 101.02 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | no K, 3-3 | 101.02 (0) |
| 2 | NYG@LAR | 149.33 | 0 | 0 | 6 | 0 | 13.7 | 6 | 7.7 | no K, 5-1 | 149.33 (0) |
| 3 | ATL@GB | 152.1 | 0 | 1.45 | 3.12 | 0 | 3.12 | 4.57 | -1.45 | no K, 4-2 | 152.1 (0) |
| 3 | PHI@CHI | 100.22 | 0 | 1.8 | 4.1 | 0 | 10.2 | 5.9 | 4.3 | no K, 4-2 | 100.22 (0) |
| 4 | ATL@NO | 145.45 | 0 | 0 | 1.55 | 0 | 1.55 | 1.55 | 0 | no K, 3-3 | 145.45 (0) |
| 4 | DET@CAR | 179.56 | 5.6 | 0 | 5.6 | 0 | 6.58 | 11.2 | -4.62 | K, 3-3 | 179.56 (0) |
| 4 | IND@WAS | 85.67 | 9.82 | 0 | 2.2 | 0 | 11.9 | 12.02 | -0.12 | K, 5-1 | 90.37 (4.7) |
| 4 | PIT@CLE | 120.74 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | no K, 5-1 | 120.74 (0) |
| 5 | BUF@LAR | 166.57 | 0 | 0.3 | 0.3 | 0 | 5.65 | 0.6 | 5.05 | no K, 4-2 | 166.57 (0) |
| 5 | TB@DAL | 136.99 | 0 | 0 | 2.15 | 0 | 6.99 | 2.15 | 4.84 | no K, 3-3 | 136.99 (0) |

Interaction = cost of all four together minus the sum of the four single costs (0 means the costs add). Rank of the unrestricted optimum against each contest is in results.json; it is not achievable pregame and is kept apart from every pregame number below.

## 3. Rule premise: what the verified top-100 entries did

| Week | Game | Top-100 entries (matched) | Unique constructions | Kicker (entries / unique) | 4-2 split (entries / unique) | Both QBs (entries / unique) | Captain QB/RB/WR (entries) |
|---:|---|---:|---:|---|---|---|---|
| 1 | NE@SEA | 108 (108) | 7 | 14/108 / 3/7 | 2/108 / 1/7 | 0/108 / 0/7 | 108/108 |
| 2 | NYG@LAR | 109 (109) | 11 | 21/109 / 3/11 | 33/109 / 4/11 | 0/109 / 0/11 | 109/109 |
| 3 | ATL@GB | 116 (116) | 18 | 78/116 / 5/18 | 21/116 / 6/18 | 0/116 / 0/18 | 116/116 |
| 3 | PHI@CHI | 109 (109) | 24 | 32/109 / 8/24 | 36/109 / 9/24 | 94/109 / 17/24 | 109/109 |
| 4 | ATL@NO | 101 (101) | 3 | 0/101 / 0/3 | 0/101 / 0/3 | 0/101 / 0/3 | 101/101 |
| 4 | DET@CAR | 52 (52) | 9 | 25/52 / 5/9 | 47/52 / 6/9 | 3/52 / 1/9 | 52/52 |
| 4 | IND@WAS | none stored | - | - / - | - / - | - / - | - |
| 4 | PIT@CLE | 104 (104) | 6 | 22/104 / 1/6 | 10/104 / 2/6 | 32/104 / 3/6 | 104/104 |
| 5 | BUF@LAR | 100 (100) | 61 | 23/100 / 13/61 | 50/100 / 31/61 | 80/100 / 47/61 | 97/100 |
| 5 | TB@DAL | 113 (113) | 14 | 61/113 / 4/14 | 33/113 / 3/14 | 28/113 / 5/14 | 113/113 |

Entries are those ranked 1-100 in the contest (`nfl_dfs_field_top_entries`), not the top 100 users. 'Matched' = every player resolved to the slate pool by name. Games with no stored top entries (IND@WAS) have nothing to report.

## 4. Selection regret and controlled replays (pregame, achievable)

### 4a. Original saved portfolio (its own size) and regret against the hindsight optimum under the rules it used

| Week | Game | Lineups (unscored) | Best | Rank | Median | Top-1% | Above field median | Rules it used | Hindsight optimum, same rules | Regret (optimum - our best) |
|---:|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| 1 | NE@SEA | 6 (12) | 78.43 | 18723 | 69.75 | 0 | 4 | unrestricted (min $49,000) | 101.02 | 22.59 |
| 2 | NYG@LAR | 39 (1) | 110.77 | 19145 | 67.98 | 0 | 10 | unrestricted (min $45,000) | 149.33 | 38.56 |
| 3 | ATL@GB | 20 (0) | 122.43 | 20570 | 102.3 | 0 | 6 | unrestricted (min $45,000) | 152.1 | 29.67 |
| 3 | PHI@CHI | 20 (0) | 80 | 11732 | 63.15 | 0 | 10 | unrestricted (min $45,000) | 100.22 | 20.22 |
| 4 | ATL@NO | 40 (0) | 114.29 | 15284 | 81.72 | 0 | 3 | unrestricted (min $45,000) | 145.45 | 31.16 |
| 4 | DET@CAR | 31 (0) | 137.85 | - | 90.82 | - | 7 | unrestricted (min $45,000) | 179.56 | 41.71 |
| 4 | IND@WAS | 20 (0) | 74.47 | 2683 | 57.47 | 0 | 3 | unrestricted (min $45,000) | 85.67 | 11.2 |
| 4 | PIT@CLE | 20 (0) | 97.61 | 18422 | 89.21 | 0 | 9 | unrestricted (min $45,000) | 120.74 | 23.13 |
| 5 | BUF@LAR | 26 (0) | 142.95 | 92 | 120.08 | 0 | 5 | no K + no 4-2 + $49.5k floor + CPT QB/RB/WR | 160.92 | 17.97 |

Regret mixes forecast uncertainty and the selection objective; it is not projection error. The optimum uses the No punts pool, the cheap-player rule the runs used.

### 4b. Controlled replay: 20 lineups from each frozen pregame pool, today's optimizer code

Same frozen pool, the run's own other settings (plan, exposure, salary floor, defensive mode), 20 lineups. Arms: Standard or Winners shape, ownership leverage on or off. Randomness is a fixed-seed hash, so arms are paired. Weeks 1-3 snapshots carry no ownership, so their leverage arms are identical by construction.

| Week | Game | Arm | Best | Rank | Median | Top-1% | Above field median | Regret vs hindsight (same rules) | Note |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 1 | NE@SEA | standard, leverage off | 86.62 | 3378 | 47.69 | 0 | 1 | 14.4 | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (34 historical and 9 workload)); retried with defensive off and projection source 'our'; built 17/20; 13 unscored |
| 1 | NE@SEA | standard, leverage on | 86.62 | 3378 | 47.69 | 0 | 1 | 14.4 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (34 historical and 9 workload)); retried with defensive off and projection source 'our'; built 17/20; 13 unscored |
| 1 | NE@SEA | winners, leverage off | 78.64 | 17930 | 72.27 | 0 | 2 | 22.38 | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (32 historical and 9 workload)); retried with defensive off and projection source 'our'; built 17/20; 13 unscored |
| 1 | NE@SEA | winners, leverage on | 78.64 | 17930 | 72.27 | 0 | 2 | 22.38 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (32 historical and 9 workload)); retried with defensive off and projection source 'our'; built 17/20; 13 unscored |
| 2 | NYG@LAR | standard, leverage off | 97.63 | 30444 | 77.78 | 0 | 8 | 51.7 | snapshot predates the observed-games field: that rule off |
| 2 | NYG@LAR | standard, leverage on | 97.63 | 30444 | 77.78 | 0 | 8 | 51.7 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 2 | NYG@LAR | winners, leverage off | 117.53 | 12779 | 73.4 | 0 | 8 | 18.1 | snapshot predates the observed-games field: that rule off |
| 2 | NYG@LAR | winners, leverage on | 117.53 | 12779 | 73.4 | 0 | 8 | 18.1 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 3 | ATL@GB | standard, leverage off | 130.33 | 9610 | 102.66 | 0 | 5 | 21.77 | snapshot predates the observed-games field: that rule off |
| 3 | ATL@GB | standard, leverage on | 130.33 | 9610 | 102.66 | 0 | 5 | 21.77 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 3 | ATL@GB | winners, leverage off | 122.1 | 21010 | 101.84 | 0 | 5 | 26.88 | snapshot predates the observed-games field: that rule off |
| 3 | ATL@GB | winners, leverage on | 122.1 | 21010 | 101.84 | 0 | 5 | 26.88 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 3 | PHI@CHI | standard, leverage off | 93.13 | 2705 | 63.82 | 0 | 12 | 7.09 | snapshot predates the observed-games field: that rule off |
| 3 | PHI@CHI | standard, leverage on | 93.13 | 2705 | 63.82 | 0 | 12 | 7.09 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 3 | PHI@CHI | winners, leverage off | 86.33 | 6312 | 62.45 | 0 | 10 | 3.69 | snapshot predates the observed-games field: that rule off |
| 3 | PHI@CHI | winners, leverage on | 86.33 | 6312 | 62.45 | 0 | 10 | 3.69 | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off |
| 4 | ATL@NO | standard, leverage off | 114.29 | 15284 | 85.03 | 0 | 6 | 31.16 | snapshot predates the observed-games field: that rule off |
| 4 | ATL@NO | standard, leverage on | 120.99 | 5879 | 90.72 | 0 | 4 | 24.46 | snapshot predates the observed-games field: that rule off |
| 4 | ATL@NO | winners, leverage off | 120.99 | 5879 | 86.1 | 0 | 5 | 22.91 | snapshot predates the observed-games field: that rule off |
| 4 | ATL@NO | winners, leverage on | 120.99 | 5879 | 91.41 | 0 | 5 | 22.91 | snapshot predates the observed-games field: that rule off |
| 4 | DET@CAR | standard, leverage off | 117.02 | - | 82.25 | - | 2 | 62.54 | snapshot predates the observed-games field: that rule off |
| 4 | DET@CAR | standard, leverage on | 133.75 | - | 98.42 | - | 4 | 45.81 | snapshot predates the observed-games field: that rule off |
| 4 | DET@CAR | winners, leverage off | 110.88 | - | 86.25 | - | 0 | 62.1 | snapshot predates the observed-games field: that rule off |
| 4 | DET@CAR | winners, leverage on | 114.75 | - | 97.95 | - | 3 | 58.23 | snapshot predates the observed-games field: that rule off |
| 4 | IND@WAS | standard, leverage off | 77.57 | 1492 | 70.65 | 0 | 13 | 8.1 | snapshot predates the observed-games field: that rule off |
| 4 | IND@WAS | standard, leverage on | 69.57 | 5994 | 56.17 | 0 | 3 | 16.1 | snapshot predates the observed-games field: that rule off |
| 4 | IND@WAS | winners, leverage off | 69.79 | 5828 | 62.35 | 0 | 9 | 3.98 | snapshot predates the observed-games field: that rule off |
| 4 | IND@WAS | winners, leverage on | 69.15 | 6265 | 56.49 | 0 | 6 | 4.62 | snapshot predates the observed-games field: that rule off |
| 4 | PIT@CLE | standard, leverage off | 107.76 | 3187 | 89.91 | 0 | 10 | 12.98 | snapshot predates the observed-games field: that rule off |
| 4 | PIT@CLE | standard, leverage on | 107.64 | 3305 | 90.16 | 0 | 11 | 13.1 | snapshot predates the observed-games field: that rule off |
| 4 | PIT@CLE | winners, leverage off | - | - | - | - | - | - | failed: Exposure plan is infeasible before generation: • Player 44315898 has a captain minimum but is not captain-eligible. |
| 4 | PIT@CLE | winners, leverage on | - | - | - | - | - | - | failed: Exposure plan is infeasible before generation: • Player 44315898 has a captain minimum but is not captain-eligible. |
| 5 | BUF@LAR | standard, leverage off | 147.97 | 64 | 119.97 | 0 | 7 | 18.6 |  |
| 5 | BUF@LAR | standard, leverage on | 146.85 | 71 | 116.75 | 0 | 3 | 19.72 |  |
| 5 | BUF@LAR | winners, leverage off | 142.85 | 94 | 121.75 | 0 | 8 | 18.07 |  |
| 5 | BUF@LAR | winners, leverage on | 142.85 | 94 | 121.75 | 0 | 5 | 18.07 |  |

Replays measure the strategy on frozen inputs with today's code, not the build that was entered. Best-lineup numbers from 20 lineups and from the original portfolio size are not comparable.

## 5. Attribution table (one row per game)

Hindsight columns bound what a rule can cost; pregame columns are achievable. Replay columns are best-of-20 actual points (rank in parentheses where the contest has a score curve).

| Week | Game | Hindsight optimum (unrestricted) | Winners rules cost (hindsight) | Original best (lineups) | Regret, original | Standard, lev off | Standard, lev on | Winners, lev off | Winners, lev on |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NE@SEA | 101.02 | 0 | 78.43 (6) | 22.59 | 86.62 (3,378) | 86.62 (3,378) | 78.64 (17,930) | 78.64 (17,930) |
| 2 | NYG@LAR | 149.33 | 13.7 | 110.77 (39) | 38.56 | 97.63 (30,444) | 97.63 (30,444) | 117.53 (12,779) | 117.53 (12,779) |
| 3 | ATL@GB | 152.1 | 3.12 | 122.43 (20) | 29.67 | 130.33 (9,610) | 130.33 (9,610) | 122.1 (21,010) | 122.1 (21,010) |
| 3 | PHI@CHI | 100.22 | 10.2 | 80 (20) | 20.22 | 93.13 (2,705) | 93.13 (2,705) | 86.33 (6,312) | 86.33 (6,312) |
| 4 | ATL@NO | 145.45 | 1.55 | 114.29 (40) | 31.16 | 114.29 (15,284) | 120.99 (5,879) | 120.99 (5,879) | 120.99 (5,879) |
| 4 | DET@CAR | 179.56 | 6.58 | 137.85 (31) | 41.71 | 117.02 | 133.75 | 110.88 | 114.75 |
| 4 | IND@WAS | 85.67 | 11.9 | 74.47 (20) | 11.2 | 77.57 (1,492) | 69.57 (5,994) | 69.79 (5,828) | 69.15 (6,265) |
| 4 | PIT@CLE | 120.74 | 0 | 97.61 (20) | 23.13 | 107.76 (3,187) | 107.64 (3,305) | failed | failed |
| 5 | BUF@LAR | 166.57 | 5.65 | 142.95 (26) | 17.97 | 147.97 (64) | 146.85 (71) | 142.85 (94) | 142.85 (94) |
| 5 | TB@DAL | 136.99 | 6.99 | no build | - | - | - | - | - |

## 6. Player forecasts (descriptive)

| Week | Game | Players scored | MAE | Bias (actual - projected) | Inside P10-P90 | Mean P10-P90 width |
|---:|---|---:|---:|---:|---:|---:|
| 1 | NE@SEA | 25 | 4.37 | -3.05 | 22/25 | 14.94 |
| 2 | NYG@LAR | 25 | 6.92 | -2.53 | 17/25 | 16.98 |
| 3 | ATL@GB | 23 | 5.41 | 2.35 | 16/23 | 12.88 |
| 3 | PHI@CHI | 24 | 4.64 | -1.71 | 16/23 | 13.23 |
| 4 | ATL@NO | 22 | 5.4 | 0.26 | 16/22 | 14.38 |
| 4 | DET@CAR | 22 | 6.7 | 1.92 | 17/22 | 17.69 |
| 4 | IND@WAS | 20 | 4 | -1.53 | 15/19 | 11.58 |
| 4 | PIT@CLE | 24 | 3.67 | 2.09 | 17/24 | 10.63 |
| 5 | BUF@LAR | 21 | 5.52 | 1.96 | 18/21 | 17.1 |

Week 5 BUF@LAR, 16 players with a full set of pregame lines: books' calibrated DK-point mean (market anchor) MAE 6.08, our model's own number before the anchor MAE 5.25. Per-market lines against their own stats for weeks 1-5 are in `research/nfl_prop_outcomes.py` (rushing overs 44.6% vs 49.8% implied; receptions and receiving yards calibrated); weeks 1-4 lines are pregame snapshots captured after the fact.

P10-P90 coverage is a description, not a calibration test: a nominal 80% interval is judged over many player-games, and players in one game move together.

## 7. What this can and cannot say

- 10 games (9 with a saved pregame build). That is enough to see which rules cost points in hindsight and how our selections behave, not to establish an exposure percentage, a profitable strategy, or a winner between arms.
- Hindsight optima are not achievable; they bound what a rule can cost, not what removing it would have won.
- Replays use today's code on old inputs; earlier weeks ran older projection versions (coverage table).