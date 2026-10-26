# NFL DFS projection ranges for players who play (pre-registered)

Study id `nfl-dfs-range-calibration-v1`. Registered 2026-10-25, before any
2022-2025 row was replayed for this question, before the tuning constants
exist, and before 2026 week 8 was played. Research only: nothing here changes
production projections, and `model/nfl_dfs_historical.py` is not edited.

## Scope

The DFS page shows each player's P10 (floor), projection and P90 (ceiling).
The owner asked why the ranges miss. The participation-ledger study
(`nfl-dfs-participation-ledger-v1`, `docs/nfl-dfs-rb-floor-participation-study.md`)
owns players who never take the field, and named the remaining question, the
width of the range for players who do play, as out of scope there (its
"Played-tail width" item). This study is that question and nothing else. It
does not re-test the ledger.

**"Played" means a regular-season stat line in the target week.** A player
who dressed and recorded nothing has no stat line and is outside this study
(the ledger's zero rows cover him).

## Evidence before registration

### 2026 saved builds (production rows, already seen)

`research/nfl_dfs_range_coverage_2026.py --weeks 1-7`, run 2026-10-25: for
each player the latest saved non-research build before his kickoff, projected
5+, not ruled out (same selection as the ledger study). Week 7 holds only the
Thursday game. Rows are stored in
`artifacts/nfl-dfs-range-calibration/coverage_2026.json`.

Played rows, weeks 1-7 pooled (targets 10% below P10, 10% above P90):

| pos | n | below P10 | above P90 | inside |
|---|---|---|---|---|
| RB | 298 | 13% [10, 17] | 10% [7, 14] | 76% |
| TE | 206 | 13% [9, 18] | 13% [8, 17] | 74% |
| WR | 481 | 10% [7, 12] | 13% [10, 16] | 77% |
| QB | 175 | 10% [5, 14] | 13% [8, 18] | 78% |

Intervals are 95%, resampling whole games (88 games). By week the rates swing
widely (WR above P90: 21% in week 3, 9% in week 6; WR below P10: 1% in week
6), so one week's "too wide" or "too narrow" is mostly that week's scoring
environment, which every player in it shares. Including players who did not
play, RB below-P10 is 20% (n=405); the extra is the ledger's zero mass.

Of 31 played RBs below P10 in weeks 1-7, 12 got under half their projected
touches (carries + receptions), against 19 of 168 at or above P10.

So for players who play, the ranges are somewhat too narrow on both sides,
by about 3 points of coverage per tail, not grossly wrong. The prompt's
week-6 "WR 90% coverage" is one week.

### Model facts

`project_player` resamples a player's last 34 stat lines (half-life 3 rows,
`MODEL_CONFIG["player_half_life_games"]`), mixed with 400 peer lines at
weight `4 / (n + 4)`, and reads P10/P90 off 2,000 draws. Half-life 3 leaves an
effective sample of about 8.7 games. It was chosen on MAE alone
(`model/nfl_dfs_half_life_screen.py`, tuned 2023-24, graded 2025); no quantile
was ever graded. The market anchor (`web/src/lib/nfl-dfs/market-anchor.ts`)
multiplies every quantile by `k = market mean / model mean`.

## Data

nflverse `stats_player_week_{season}.csv`, `roster_weekly_{season}.csv` (names
and identities) and `games.csv`, cached under `data/nflverse_cache`, loaded by
`load_sources()` of `model/nfl_dfs_participation_ledger_gate_h.py`. Source
SHA-256s are written into each replay artifact.

Every projection runs `model/nfl_dfs_historical.py` at SHA-256
`63d0353099727a292ef2a1255baf312a6cadf74bc3da38bc507709e41e3afc30` (the same
pin as the ledger study; the scripts refuse any other file). History is the
target season and the two before it, strictly before the target week; team
implied total from the nflverse closing total and spread; no opponent term
(`opponent_mode` is `off`); no market anchor; projection seed 20260902.

`research/nfl_dfs_range_calibration.py::projection_draws` reproduces
`project_player` exactly (same rows, same random draws) but scores each
history row once and returns the draw array, so several half-lives are read
off identical draws, and `PeerIndex` reproduces `_peer_rows` without rescoring
every peer line per call. `tests/test_nfl_dfs_range_calibration.py` checks
equality of mean, P10, P50 and P90 at two half-lives and of the peer
selection; a live check on 2022 week 9 (60 QB/RB/TE/WR players, half-lives 3
and 8) matched `project_player` on all 120 comparisons.

**Population.** QB, RB, TE and WR player-weeks, weeks 4-18 of the regular
season, with a target-week stat line, a team game in `games.csv`, and a
baseline (v5, half-life 3) projection of 5 or more. RB, TE and WR are gated;
QB is reported only.

**Seasons.**
- Tuning: 2022 and 2023 (history from 2020).
- Held out: 2024 and 2025. Disclosure: both were used before, by the
  half-life MAE screen (2024 tuning, 2025 grading, MAE only) and by the
  ledger study's Gate H (P10 pinball and a P90 point estimate, on a different
  population that includes non-players). Neither graded P90 coverage for
  players who played, and no candidate here has been scored on them. The
  ledger's Gate H artifacts contain baseline P10/P90 for 2024-25 rows; they
  were not read for this registration and will be summarised only in the
  result section.
- Forward: 2026 weeks 8-15.

## Candidates (fixed now; constants come from the tuning seasons only)

Each is compared with the baseline on identical rows. They are not combined;
a combination is a new version.

**A. Half-life chosen by quantile loss.** Grid
`{2, 3, 4, 6, 8, 12, 17, 34}` games. On the tuning seasons, choose the
half-life with the lowest mean range loss (below) over RB+TE+WR rows pooled;
ties go to the shorter half-life listed first. One half-life for all
positions, as in production. If the choice is 3, candidate A is the baseline
and is recorded as DROPPED, not graded.

**B. Mean-preserving tail scaling, per position.** With `m` the projection:

```
P10' = m - a_pos * (m - P10)
P90' = m + b_pos * (P90 - m)
```

`a_pos` and `b_pos` for RB, TE and WR, each the value in
`0.50, 0.52, ..., 3.00` minimising the mean pinball loss at tau 0.10 (for
`a`) or 0.90 (for `b`) over that position's tuning rows; ties to the smaller
value. The projection does not move, so MAE cannot change. Because the
transform is linear in (m, P10, P90) it commutes with the market anchor's
k-scaling: it can be applied before or after the anchor with the same result,
which makes it valid on anchored rows in the forward gate. A role-loss
mixture was considered and not chosen: it adds a shape assumption that two
tuning seasons cannot separate from simple widening, and it moves the mean.

The tuning step (`tune`) writes the frozen constants to
`research/nfl_dfs_range_calibration_frozen.json`, once; they are committed and
appended to this file before any held-out row is replayed. The held-out replay
refuses to run without that file.

## Grading

**Range loss** for one player-week:
`L = pinball(0.10, P10, actual) + pinball(0.90, P90, actual)`, with
`pinball(tau, q, y) = (tau - 1[y < q]) * (y - q)`. Actual is the DraftKings
points of his stat line.

**Primary metric.** Paired mean of `L(candidate) - L(baseline)` over RB+TE+WR
rows, each held-out season separately, with a 95% interval from 10,000
resamples of whole nflverse games (both teams' players together, seed
20261025).

**Guards, each season.**
1. No gated position worse: per position (RB, TE, WR), the point estimate of
   the range-loss delta is at most +0.01.
2. Calibration closer to target: pooled
   `|below-P10 rate - 10%| + |above-P90 rate - 10%|` is smaller for the
   candidate than the baseline.
3. MAE: paired MAE delta, 95% upper bound below +0.05 DK points (binding only
   for A; B leaves the mean unchanged).

**Reported, not gated:** the same interval resampling whole weeks (a
sensitivity check, because 2026 shows strong week-level swings), P10 and P90
pinball separately, coverage by position, QB baseline coverage, and the
candidate constants.

### Verdict per candidate, decided now

- **KILL** if the primary interval's upper bound is at or above 0 in 2024 or
  in 2025. No rescue by slicing (one position, one tail, one season), by a
  different grid, or by combining A and B: each is a new registration.
- **H_CLEARED** if the interval is wholly below 0 in both seasons and every
  guard holds in both. The forward gate still runs.
- Otherwise **INCONCLUSIVE**: production unchanged, nothing re-run on the
  same data.

Two candidates are tested; each needs both held-out seasons, which is the
multiplicity control.

### Gate F, forward (2026 weeks 8-15)

Rows: the 2026 production selection above (latest saved non-research build
before kickoff, projected 5+, not ruled out), RB/TE/WR, who played.
- B is applied to the saved row's projection, floor and ceiling as stored
  (anchored or not; the transform commutes with the anchor).
- A needs both arms recomputed from database history at the run's cutoff and
  anchored identically, as the ledger's Gate F does. That code does not exist
  yet; it is written before week 8 results are read, and only if A is
  H_CLEARED.

Floor: at least 400 played RB+TE+WR rows over at least 6 of the 8 weeks;
short of that the gate waits for weeks 16-18 and is graded once then.
**PASS** only if the candidate is H_CLEARED, Gate F's pooled range-loss delta
point estimate is below 0, and its calibration error is smaller than the
saved builds'. A PASS licenses a proposal for the owner's decision, not a
release.

## Prediction, stated before scoring

The 2026 table suggests both tails are a little narrow for RB and TE and the
upper tail for WR. Expected: B's `a` and `b` mostly between 1.0 and 1.3, and a
small, real cut in range loss in both held-out seasons. A is more likely
than not to choose a longer half-life (more spread, at a small MAE cost) or
to be dropped. The played-tail excess is small (about 3 coverage points a
tail), so even a passing candidate moves the page's ranges modestly.

## If a candidate ever ships

- **A** changes `MODEL_CONFIG`, so `model/nfl_dfs_historical.py`'s hash and
  `MODEL_VERSION` change, and means move. Re-pin at the next boundary:
  `ingest/nfl_dfs_shadow.py` (source digest), `ingest/nfl_dfs_research.py`
  and `ingest/nfl_dfs_efficiency.py` (implementation digests), the ledger's
  Gate H pin and this study's pin, `docs/nfl-matchup-studies.json`
  (`model_version`, `baseline_config_hash`), the coherent-scenario study
  (`research/nfl_coherent_scenario_study*.json`, `baseline_config_hash`),
  `ingest/nfl_defensive_capture_requests.py` (`HISTORICAL_V5`), the
  freshness-capture eligibility check, `nfl-qb-spot-start-v2` (built on v5
  projections), and the replacement-upside grade (its baseline and
  if-starter ranges come from v5). If the ledger's v6 also ships, the two
  changes need one combined version, not two in sequence.
- **B** can ship as an overlay after the model, like the QB starter overlay,
  leaving the model file and v5 unchanged; `production_config()` would gain a
  key, which changes `baseline_config_hash` for the coherent-scenario and
  matchup studies. Ranges feed the optimizer (the GPP objective credits
  `min(P90, 2.5 x projection)`) and the ghost factorial's arms, so
  `nfl-ghost-factorial-v1` would need a boundary too.

## Reproduction

```
python -m research.nfl_dfs_range_coverage_2026 --weeks 1-7
python -m research.nfl_dfs_range_calibration replay --season 2022
python -m research.nfl_dfs_range_calibration replay --season 2023
python -m research.nfl_dfs_range_calibration tune
python -m research.nfl_dfs_range_calibration replay --season 2024
python -m research.nfl_dfs_range_calibration replay --season 2025
python -m research.nfl_dfs_range_calibration grade
```

## Amendment 1: tuning result and frozen constants (2026-10-25, before any 2024 or 2025 row was replayed)

`replay --season 2022` and `--season 2023` (artifacts `replay-2022.json`,
`replay-2023.json`; 2,969 and 2,916 rows with baseline 5+; the logged
runtimes include about ten hours of the machine sleeping), then `tune`, run
once (`tune.json`). Nothing in the registration changed.

Baseline coverage on the tuning seasons, played rows (targets 10 / 10 / 80):

| pos | n | below P10 | above P90 | inside |
|---|---|---|---|---|
| RB | 1,571 | 15.3% | 12.1% | 72.6% |
| TE | 861 | 13.1% | 12.7% | 74.2% |
| WR | 2,451 | 14.0% | 11.5% | 74.4% |
| QB (reported) | 1,002 | 15.2% | 14.0% | 70.9% |

The same direction as 2026, a little larger: both tails too narrow, the lower
one more.

Half-life grid, RB+TE+WR pooled (range loss, MAE; per position below/above):

| half-life | range loss | MAE | RB | TE | WR |
|---|---|---|---|---|---|
| 2 | 2.7866 | 5.9038 | .17/.14 | .16/.15 | .16/.13 |
| **3 (v5)** | 2.6846 | **5.8553** | .15/.12 | .13/.13 | .14/.12 |
| 4 | 2.6481 | 5.8434 | .14/.11 | .13/.12 | .13/.10 |
| 6 | 2.6018 | 5.8388 | .14/.10 | .11/.12 | .12/.10 |
| 8 | 2.5838 | 5.8491 | .14/.10 | .11/.12 | .12/.10 |
| 12 | 2.5771 | 5.8661 | .13/.09 | .11/.12 | .12/.10 |
| **17** | **2.5760** | 5.8837 | .13/.10 | .10/.12 | .12/.10 |
| 34 | 2.5816 | 5.9037 | .14/.10 | .10/.12 | .12/.10 |

Frozen (`research/nfl_dfs_range_calibration_frozen.json`):

- **A: half-life 17.** Range loss 2.5760 against 2.6846 at half-life 3 (-4.0%),
  at a tuning MAE cost of +0.028. The loss is nearly flat from 8 to 34, so
  the exact value matters little; the registered rule picks the minimum.
  The MAE guard (upper bound below +0.05) is the one most likely to bind.
  Note the tuning MAE optimum is 6, not 3 as in the half-life screen (which
  tuned on 2023-24 with a different population); recorded, not acted on.
- **B: RB a 1.08, b 0.96; TE a 1.08, b 0.94; WR a 1.12, b 0.94.** Pinball
  wants the floor about 10% further below the mean and the ceiling about 5%
  closer to it, although the baseline's above-P90 rate is already 11.5-12.7%.
  Pinball and the coverage count disagree here because the mean-relative
  scale is one number for players of very different spreads: pulling in the
  ceilings of high-variance players costs less than it saves on the rest.
  B may therefore fail guard 2 (calibration closer to target) through its
  upper tail even if its loss falls. That is a reading the registration
  allows; the rule is not changed.

Prediction revised only in its numbers, not its rules: B smaller than first
expected (scales nearer 1); A larger, with its MAE guard the open question.

## Held-out result (2026-10-25): A INCONCLUSIVE, B KILL

`replay --season 2024` and `--season 2025` (2,911 and 2,937 rows with
baseline 5+; 2,405 and 2,425 gated RB/TE/WR rows over 224 games each), then
`grade`, run once (`verdict.json`, `grade.log`). Both seasons were replayed
after the frozen constants were committed (`450087ce`).

**Baseline coverage, played rows** (the 2024-25 half of task 1; read only
now):

| | RB below/above | TE below/above | WR below/above | QB below/above (reported) |
|---|---|---|---|---|
| 2024 | 14.6% / 11.7% | 12.8% / 13.0% | 12.2% / 13.0% | 13.6% / 17.0% |
| 2025 | 15.6% / 12.5% | 14.4% / 13.2% | 15.5% / 10.8% | 13.9% / 10.2% |

Pooled RB+TE+WR: 13.1% below / 12.6% above in 2024, 15.3% / 11.8% in 2025.
Same as the tuning seasons and 2026: both tails a few points too narrow, the
lower one more.

**Primary metric**, range-loss delta (candidate minus baseline; baseline mean
range loss 2.696 in 2024, 2.737 in 2025), 95% game-clustered interval:

| | 2024 | 2025 |
|---|---|---|
| A (half-life 17) | -0.062 [-0.114, -0.011] | -0.100 [-0.153, -0.047] |
| A, week-clustered (reported) | [-0.105, -0.022] | [-0.138, -0.063] |
| B (tail scaling) | +0.0003 [-0.013, +0.013] | -0.019 [-0.032, -0.007] |

**Candidate B: KILL.** Its 2024 interval contains zero. Its floor scaling
worked (below-P10 13.1% -> 10.0% in 2024, 15.3% -> 11.2% in 2025), but its
ceiling pull-in pushed above-P90 from 12.6% to 13.6% in 2024 and cost more P90
loss than the floors saved for TE and WR. No rescue by keeping only its floor
half: that would be a new registration.

**Candidate A: INCONCLUSIVE.** Its range-loss interval is wholly below zero
in both seasons (about 2-4% of the loss), under both clusterings, and its
coverage moves to the target: below/above 13.1/12.6% -> 10.5/11.4% in 2024
and 15.3/11.8% -> 11.5/10.8% in 2025. Every position's point estimate
improves in both seasons (RB -0.035/-0.070, TE -0.038/-0.134, WR
-0.087/-0.103). But the MAE guard fails in both seasons: MAE delta +0.044
[-0.033, +0.119] in 2024 and +0.070 [+0.005, +0.131] in 2025 (bound: upper
below +0.05). Baseline MAE 5.874 -> 5.919 and 5.821 -> 5.891. So the longer
memory gives the right width and a slightly worse centre: a recent window
predicts the mean better and the spread worse. Per the registration,
production is unchanged and nothing is re-run on 2024-2025.

**What this says about the question.** For players who play, the model's
ranges are too narrow because half-life 3 sees about 8.7 effective games; a
17-game memory spreads the draws to roughly the right width. The cost is
centre, not width. This was not the prediction (B was expected to pass; A
was expected to be dropped or marginal); recorded as wrong.

**Follow-up, not tested here (a new registration if pursued):** take the
centre from half-life 3 and the spread from a longer half-life, for example
`P10 = m3 + (P10_17 - m17)` and the same for P90, which keeps MAE identical to
v5 by construction. 2022-2025 have now all been seen for this question, so
it needs fresh data: 2026 weeks 8+ forward, or 2020-2021 replays
(nflverse back to 2018 for history) as a blind historical gate. Gate F of
this study is not run for either candidate: B is killed, and A did not reach
H_CLEARED.
