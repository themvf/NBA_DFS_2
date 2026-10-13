# Classic attribution study (read-only)

Generated 2026-10-13T22:01:54.026Z. Inputs: `research/nfl_classic_attribution_export.py`; exclusions and report: `web/scripts/research-nfl-classic-attribution.ts`; independent oracle: `research/nfl_classic_attribution_cbc.py` (CBC via pulp). No setting, optimizer code or stored record was changed.

## 1. Coverage and input completeness

| Week | Slate | Contest | Entries | Score curve | Top-100 entries | Build used (lineups) | Shape | Model | Optimizer then | Eligibility report | Eligible pool | Eligible with a recorded score | Prior-window plays labelled before the run | Pool-rule inputs |
|---:|---|---|---:|---|---:|---|---|---|---|---|---:|---:|---|---|
| 1 | 12 games | 193028206 | 832,342 | yes | 100 | 4875c71f (20) | standard | nfl-dfs-historical-v2 | nfl-dfs-ilp-v1 | no | 719 | 289 | - (no prior weeks) | none: RB/WR/TE rules skipped by the product |
| 2 | 13 games | 195648007 | 172,761 | yes | 100 | 79839671 (20) | standard | nfl-dfs-historical-v3 | nfl-dfs-ilp-v5-audited-situations | no | 593 | 481 | 0 / 2,616 | **unavailable**: RB/WR/TE rules skipped |
| 3 | 13 games | 195905122 | 161,764 | yes | 100 | 45346d1b (25) | standard | nfl-dfs-historical-v5 | nfl-dfs-ilp-v7-showdown-legality | no | 586 | 353 | 0 / 5,192 | **unavailable**: RB/WR/TE rules skipped |
| 4 | 12 games | 196151357 | 161,764 | yes | 100 | af17aac3 (107) | standard | nfl-dfs-historical-v5 | nfl-dfs-ilp-v8-defensive-dk-fallback | yes | 475 | 292 | 7,045 / 7,856 | **partial** reconstruction |
| 5 | 11 games | 196403752 | 161,557 | yes | 100 | 4c50a8d6 (100) | winners | nfl-dfs-historical-v5 | nfl-dfs-ilp-v12-lineup-integrity | yes | 442 | 270 | 7,934 / 7,934 | complete reconstruction |

- **Build used** is the largest optimizer run created before the slate's first kickoff (latest on ties), i.e. the entered build rather than a test run.
- **Prior opportunity** (the input to the RB/WR/TE pool rules) was not stored in any of these frozen pools. It is **reconstructed** with the workspace's own query: play-by-play weeks w-3..w-1 labelled no later than the run, team targets for the share. Plays relabelled after a run carry the later label time, so the gate drops them rather than use later information: weeks 2-3 have none left (the product would have skipped the three rules, and so does this study), week 4 keeps 90%. Week 1 has no prior weeks.
- **Reconstruction check (week 5):** that week's Winners run recorded 217 pool exclusions. Today's rules on the reconstructed inputs exclude 217: 217 in common, 0 only in the run's record, 0 only in the reconstruction.
- Players with no recorded score are excluded from every pool and counted (eligible minus scored, above); they are mostly depth players who did not play. Defence scores where DraftKings' contest number and the results table differ: Buccaneers 9 vs 8; Seahawks 9 vs 8 (DraftKings' number is used).

### What the Winners pool rules exclude, as the product computes them

| Week | RB excluded | WR excluded | TE excluded | DST excluded | Product notes |
|---:|---:|---:|---:|---:|---|
| 1 | 0 | 0 | 0 | 9 | Winners shape: no prior-week opportunity data for any RB, so the RB rule (backs with touches a game in the slate's top quarter (n/a+) or 4+ targets a game) was not applied. Winners shape: no prior-week opportunity data for any WR, so the WR rule (receivers with 25%+ of their team's targets, or under $5,000) was not applied. Winners shape: no prior-week opportunity data for any TE, so the TE rule ( |
| 2 | 0 | 0 | 0 | 10 | Winners shape: no prior-week opportunity data for any RB, so the RB rule (backs with touches a game in the slate's top quarter (n/a+) or 4+ targets a game) was not applied. Winners shape: no prior-week opportunity data for any WR, so the WR rule (receivers with 25%+ of their team's targets, or under $5,000) was not applied. Winners shape: no prior-week opportunity data for any TE, so the TE rule ( |
| 3 | 0 | 0 | 0 | 9 | Winners shape: no prior-week opportunity data for any RB, so the RB rule (backs with touches a game in the slate's top quarter (n/a+) or 4+ targets a game) was not applied. Winners shape: no prior-week opportunity data for any WR, so the WR rule (receivers with 25%+ of their team's targets, or under $5,000) was not applied. Winners shape: no prior-week opportunity data for any TE, so the TE rule ( |
| 4 | 104 | 18 | 109 | 8 | Winners shape: RB pool kept 18 of 122 (backs with touches a game in the slate's top quarter (15.0+) or 4+ targets a game); 104 left out, each named in the build's eligibility list. Winners shape: WR pool kept 172 of 190 (receivers with 25%+ of their team's targets, or under $5,000); 18 left out, each named in the build's eligibility list. Winners shape: TE pool kept 6 of 115 (tight ends under $4,0 |
| 5 | 93 | 16 | 101 | 7 | Winners shape: RB pool kept 19 of 112 (backs with touches a game in the slate's top quarter (14.0+) or 4+ targets a game); 93 left out, each named in the build's eligibility list. Winners shape: WR pool kept 160 of 176 (receivers with 25%+ of their team's targets, or under $5,000); 16 left out, each named in the build's eligibility list. Winners shape: TE pool kept 9 of 110 (tight ends under $4,00 |

## 2. Hindsight optimum (perfect information, one lineup)

Both solvers maximize actual DraftKings points under DraftKings' Classic rules plus the optimizer's always-on invariants (at most 8 players from one game; a defence with at most 3 opposing offensive players), except 'DraftKings rules only', which CBC alone solves without the second invariant. The defence-against-own-offense limit (5%) and the bring-back share (50%) are portfolio rules: a single Winners lineup may have either, so full Winners leaves both open; strict single-lineup versions appear only as labelled diagnostics. CBC's certificate (status, objective, bound, gap, time) is required before its number is used.

**CBC certificates:** 180 of 180 solves proved optimal with zero gap; max solve time 0.2 s.

**Optimizer vs CBC:** 170 of 170 solves agree on the optimum and legality.

### Cost of each restriction (points below Unrestricted, CBC, No punts)

| Week | Unrestricted | DST script invariant | RB pool rule alone | WR pool rule alone | TE pool rule alone | DST pool rule alone | $49.5k floor alone | QB + WR/TE stack alone | Full Winners | Sum of singles | Interaction | Standard defaults |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 292.14 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | 258.46 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 4.6 |
| 3 | 256.8 | 0 | 0 | 0 | 0 | 0.08 | 0 | 0 | 0.08 | 0.08 | 0 | 0 |
| 4 | 280.86 | 0 | 3.84 | 0 | 0 | 4.44 | 0 | 2.08 | 16.7 | 10.36 | 6.34 | 2.08 |
| 5 | 284.76 | 0 | 0 | 17.4 | 14.3 | 0 | 0 | 0 | 40.2 | 31.7 | 8.5 | 8.5 |

### Leave-one-out: what removing each restriction from full Winners recovers (CBC, No punts)

| Week | Full Winners | without RB pool rule | without WR pool rule | without TE pool rule | without DST pool rule | without $49.5k floor | without QB + WR/TE stack | Diagnostic: strict DST | Diagnostic: bring-back required |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 292.14 | +0 | +0 | +0 | +0 | +0 | +0 | 0 | 0 |
| 2 | 258.46 | +0 | +0 | +0 | +0 | +0 | +0 | 0 | -4.6 |
| 3 | 256.72 | +0 | +0 | +0 | +0.08 | +0 | +0 | 0 | -0.92 |
| 4 | 264.16 | +5.12 | +8.36 | +0 | +9.5 | +5.22 | +10.44 | 0 | 0 |
| 5 | 244.56 | +5.9 | +19.98 | +22.8 | +4.6 | +7.6 | +0 | 0 | -4.4 |

Weeks 1-3 have no usable prior opportunity, so their RB/WR/TE rules exclude nothing (the product skips them) and only the DST pool rule binds; week 4 rests on a partial reconstruction, week 5 on a complete one. Punts-allowed results are in results.json. Hindsight optima are not achievable; they bound what a restriction can cost.

## 3. Rule premise: what the verified top-100 entries did

| Week | Entries (matched) | Unique lineups | Uses a player the pool rules exclude (entries / unique) | RB | WR | TE | DST | Under $49.5k | No WR/TE stack | Bring-back | DST vs own offense |
|---:|---:|---:|---|---|---|---|---|---|---|---|---|
| 1 | 100 (100) | 91 | 58/100 / 53/91 | 0/100 | 0/100 | 0/100 | 58/100 | 2/100 | 12/100 | 61/100 | 2/100 |
| 2 | 100 (100) | 99 | 28/100 / 28/99 | 0/100 | 0/100 | 0/100 | 28/100 | 4/100 | 6/100 | 32/100 | 7/100 |
| 3 | 100 (100) | 98 | 25/100 / 25/98 | 0/100 | 0/100 | 0/100 | 25/100 | 1/100 | 1/100 | 75/100 | 4/100 |
| 4 | 100 (100) | 98 | 98/100 / 96/98 | 39/100 | 69/100 | 57/100 | 35/100 | 6/100 | 6/100 | 75/100 | 4/100 |
| 5 | 100 (100) | 98 | 100/100 / 98/98 | 29/100 | 90/100 | 96/100 | 39/100 | 7/100 | 4/100 | 43/100 | 5/100 |

Per-position columns are entry-weighted. Entries are those ranked 1-100 (ties kept whole), not the top 100 users. Pool-rule columns use the exclusions in section 1, so weeks 2-4 inherit the reconstruction caveat.

## 4. Selection regret (original build at its own size)

| Week | Lineups (unscored) | Best | Rank | Median | Top-1% | Above field median | Rules it used | Hindsight optimum, those rules | Regret | Sensitivity: no-score players as 0 (best, rank) | Players with no recorded score |
|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---|---|
| 1 | 0 (20) | - | - | - | - | - | Standard (min $49,000) | 292.14 | - | 161.5 (207,295) | Hunter Dekkers, Connor Bazelak, Jalon Daniels, Jalen McMillan |
| 2 | 20 (0) | 137.82 | 34,849 | 109.02 | 0 | 8 | Standard (min $49,000) | 253.86 | 116.04 | 137.82 (34,849) | - |
| 3 | 23 (2) | 149.18 | 32,534 | 103.34 | 0 | 3 | Standard (min $45,000) | 256.8 | 107.62 | 149.18 (32,534) | Sam Howell |
| 4 | 106 (1) | 141.82 | 28,235 | 93.02 | 0 | 8 | Standard (min $45,000) | 278.78 | 136.96 | 141.82 (28,235) | Troy Franklin |
| 5 | 100 (0) | 161.4 | 20,942 | 116.98 | 0 | 22 | Winners shape | 244.56 | 83.16 | 161.4 (20,942) | - |

Regret mixes forecast uncertainty with the selection objective; it is not projection error. For a Winners build the comparison optimum is the single-lineup Winners optimum. The sensitivity column scores players with no recorded score as 0, which is what DraftKings pays a player who did not play; it is labelled, not used elsewhere.

## 5. Paired replays: 20 lineups from each frozen pool, today's code

Same frozen pool and the run's other settings; arms Standard vs Winners x ownership leverage on vs off. Randomness is a fixed-seed hash, so arms are paired. Weeks 1-3 Winners arms apply only the DST pool rule; week 4 reads a partial reconstruction.

| Week | Arm | Requested / built | Best | Rank | Median | Top-1% | Above field median | Sensitivity: no-score as 0 (best, rank) | Note |
|---:|---|---|---:|---:|---:|---:|---:|---|---|
| 1 | standard, leverage off | 20 / 20 | - | - | - | - | - | 157.3 (246,848) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (690 historical and 8 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback; 20 unscored |
| 1 | standard, leverage on | 20 / 20 | - | - | - | - | - | 157.3 (246,848) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (690 historical and 8 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback; 20 unscored |
| 1 | winners, leverage off | 20 / 20 | - | - | - | - | - | 158.2 (238,281) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (681 historical and 8 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback; 20 unscored |
| 1 | winners, leverage on | 20 / 20 | - | - | - | - | - | 158.2 (238,281) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (681 historical and 8 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback; 20 unscored |
| 2 | standard, leverage off | 20 / 20 | 137.82 | 34,849 | 101.7 | 0 | 4 | 137.82 (34,849) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (564 historical and 1 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback |
| 2 | standard, leverage on | 20 / 20 | 137.82 | 34,849 | 101.7 | 0 | 4 | 137.82 (34,849) | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (564 historical and 1 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback |
| 2 | winners, leverage off | 20 / 20 | 160.88 | 8,930 | 102.4 | 0 | 4 | 160.88 (8,930) | snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (554 historical and 1 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback |
| 2 | winners, leverage on | 20 / 20 | 160.88 | 8,930 | 102.4 | 0 | 4 | 160.88 (8,930) | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; run settings refused (Cannot build lineups from mixed objective sources (554 historical and 1 dk_avg_fallback). ); retried with defensive off, projection source 'our', no DK-average fallback |
| 3 | standard, leverage off | 20 / 20 | 150.7 | 30,096 | 118.68 | 0 | 5 | 150.7 (30,096) | snapshot predates the observed-games field: that rule off; 4 unscored |
| 3 | standard, leverage on | 20 / 20 | 150.7 | 30,096 | 118.68 | 0 | 5 | 150.7 (30,096) | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; 4 unscored |
| 3 | winners, leverage off | 20 / 0 | - | - | - | - | - | - | snapshot predates the observed-games field: that rule off; stopped: Stopped after 0 lineup(s): the Standard ceiling quota or remaining exposure, uniqueness, salary, and DST game-script rules cannot all be met. |
| 3 | winners, leverage on | 20 / 0 | - | - | - | - | - | - | no ownership in snapshot: leverage inert; snapshot predates the observed-games field: that rule off; stopped: Stopped after 0 lineup(s): the Standard ceiling quota or remaining exposure, uniqueness, salary, and DST game-script rules cannot all be met. |
| 4 | standard, leverage off | 20 / 20 | 126.8 | 56,682 | 94.02 | 0 | 2 | 126.8 (56,682) | snapshot predates the observed-games field: that rule off |
| 4 | standard, leverage on | 20 / 20 | 161.62 | 8,566 | 92.5 | 0 | 2 | 161.62 (8,566) | snapshot predates the observed-games field: that rule off |
| 4 | winners, leverage off | 20 / 0 | - | - | - | - | - | - | failed: Puka Nacua has a minimum exposure but can't be used: winners shape: an unknown target share at $7,500; the preset keeps receivers at 25%+ or |
| 4 | winners, leverage on | 20 / 0 | - | - | - | - | - | - | failed: Puka Nacua has a minimum exposure but can't be used: winners shape: an unknown target share at $7,500; the preset keeps receivers at 25%+ or |
| 5 | standard, leverage off | 20 / 20 | 163.04 | 18,962 | 130.8 | 0 | 8 | 163.04 (18,962) |  |
| 5 | standard, leverage on | 20 / 20 | 153.22 | 33,264 | 122.22 | 0 | 5 | 153.22 (33,264) |  |
| 5 | winners, leverage off | 20 / 20 | 156.44 | 27,928 | 113.24 | 0 | 4 | 156.44 (27,928) |  |
| 5 | winners, leverage on | 20 / 20 | 134.46 | 74,199 | 98 | 0 | 1 | 134.46 (74,199) |  |

Replays measure the strategy on frozen inputs with today's code, not the build that was entered; their best-of-20 is not comparable with an original build of another size.

## 6. Player forecasts (descriptive)

| Week | Players scored | MAE | Bias (actual - projected) | Inside P10-P90 | Mean P10-P90 width |
|---:|---:|---:|---:|---:|---:|
| 1 | 285 | 5.13 | -0.1 | 224/285 | 14.59 |
| 2 | 460 | 5.02 | -2.91 | 380/460 | 14.36 |
| 3 | 317 | 4.58 | -0.14 | 234/317 | 12.85 |
| 4 | 249 | 5.01 | 0.01 | 183/248 | 13.91 |
| 5 | 234 | 5.12 | 0.38 | 170/234 | 12.77 |

Coverage is description only; players in one game move together, and pooled players with no recorded score are excluded.

## 7. What this can and cannot say

- 5 slates identify mechanical exclusions, solver shortfalls and retrospective selection regret. They cannot say which strategy has better future returns, nor set an exposure level.
- The RB/WR/TE pool rules are measurable on week 5 (complete, and exact against that week's recorded exclusions) and week 4 (partial); weeks 1-3 cannot test them.
- Hindsight optima bound what a restriction can cost, not what removing it would have won.