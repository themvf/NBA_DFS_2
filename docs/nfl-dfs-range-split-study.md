# NFL DFS ranges: centre from the short memory, spread from the long one (pre-registered)

Study id `nfl-dfs-range-split-v1`. Registered 2026-10-26, before 2026 week 8
kicks off (Thursday 2026-10-29) and before any 2020 or 2021 row was replayed.
Research only: production projections are not changed.

## Why

`nfl-dfs-range-calibration-v1` (`docs/nfl-dfs-range-calibration-study.md`)
found that for players who play, the P10/P90 range is a few points too narrow
on both sides (2022-2026: about 13-15% below P10 and 12-13% above P90 against
10% targets). Resampling a 17-game memory instead of the production 3-game
memory gave the right width in both held-out seasons (range loss -0.062 and
-0.100, intervals below zero, coverage moved to the target) but a worse
projection (MAE +0.070 [+0.005, +0.131] in 2025), so it failed its MAE guard.
A short memory predicts the centre better; a long memory predicts the spread
better. This study tests taking each from where it works. It was named as the
follow-up in that study's result and is registered here as a new version, not
as a rescue of the old one.

## The candidate (one, nothing tuned)

With `m` the projection and `m17`, `P10_17`, `P90_17` the mean and quantiles
of the same player's half-life-17 distribution (same rows, same draws,
`research/nfl_dfs_range_calibration.py::projection_draws`):

```
P10' = m * (P10_17 / m17)
P90' = m * (P90_17 / m17)
```

- The projection does not move, so MAE is identical to production by
  construction (reported, not gated).
- The ratio form, not an additive offset, because the market anchor multiplies
  every quantile by `k = market mean / model mean`. The candidate commutes
  with it: applying it to an anchored row gives the same range as anchoring a
  candidate row. An offset would not, and could put a floor below zero for a
  player whose long-memory mean is well above his short-memory mean.
- If `m17 <= 0` the candidate keeps the baseline range (counted).
- Half-life 17 is frozen: it came from `nfl-dfs-range-calibration-v1`'s
  tuning on 2022-2023 (`research/nfl_dfs_range_calibration_frozen.json`) and
  is not re-chosen.

The baseline is production: `nfl-dfs-historical-v5` (half-life 3) in the
historical gate, and the saved build's own floor and ceiling in the forward
gate.

## Metric

Per player-week, range loss `L = pinball(0.10, P10, y) + pinball(0.90, P90, y)`
with `pinball(tau, q, y) = (tau - 1[y < q]) * (y - q)` and `y` his DraftKings
points. Primary: paired mean of `L(candidate) - L(baseline)` over RB+TE+WR
rows, 95% interval from 10,000 resamples of whole games (both teams' players
together; seed 20261026). Reported, not gated: the week-clustered interval, P10
and P90 pinball separately, coverage by position, QB.

Guards (wherever a gate says "guards hold"):
1. Each of RB, TE, WR: range-loss delta point estimate at most +0.01.
2. Pooled `|below-P10 rate - 10%| + |above-P90 rate - 10%|` smaller for the
   candidate than the baseline.

## Population

Players who played: a regular-season stat line in the target week. Players
who never take the field belong to `nfl-dfs-participation-ledger-v1`; this
study does not touch them. QB is reported only.

## Gate B: blind historical replay, 2020 and 2021

- Same harness, data and pin as `nfl-dfs-range-calibration-v1`: nflverse
  `stats_player_week`, `roster_weekly` and `games.csv`; `project_player` at
  SHA-256 `63d0353099727a292ef2a1255baf312a6cadf74bc3da38bc507709e41e3afc30`
  (the scripts refuse any other file); history the target season and the two
  before it (2018-2021); nflverse closing implied totals; no opponent term, no
  anchor; weeks 4-18; baseline projection 5+.
- 2020 and 2021 have not been used for any range question. The RB-absence
  study (`nfl-rb-absence-reallocation-v2`) used them for backs' points in
  absence games, a different question. 2022-2025 are excluded: all four have
  now been seen for range width, and the candidate is not computed on them
  before both gates are graded.
- **KILL** if the primary interval's upper bound is at or above 0 in 2020 or
  in 2021. **B_CLEARED** if it is wholly below 0 in both and the guards hold
  in both. Otherwise **INCONCLUSIVE**. No rescue by slicing (position, tail,
  season), another half-life, or an offset form: each is a new registration.

2020 had empty stadiums and an unusual schedule; it stays in. A gate that
fails only in 2020 is still a failure.

## Gate F: forward, 2026 weeks 8-15

- Rows: for each player the latest saved, non-research optimizer build made
  before his kickoff (`load()` in `model/nfl_week_projection_review.py`, as
  in `research/nfl_dfs_range_coverage_2026.py`), RB/TE/WR, projected 5+, not
  ruled out, who played (a scored DraftKings result).
- Baseline: the saved floor and ceiling. Candidate: the saved projection times
  the half-life-17 ratios, computed by `projection_draws` from nflverse
  history 2023 onward (the depth of production's table) before the target
  week, the player found through `ff_players.gsis_id`, the team's nflverse
  implied total. The 2026 nflverse files are re-downloaded at grading time. A
  player with no nflverse identity or history keeps the saved range (counted).
  Anchored rows are included: the saved projection already carries the anchor
  and the ratio form commutes with it. Overlays that reshape the range (the
  first-game "if starter" mixture) are replaced by the ratio range for the
  candidate. The artifact records how many rows were anchored.
- Graded once, after week 15's results are in. Floor: 400 rows over at least
  6 weeks. Short of that, nothing is written, and it is graded once over weeks
  8-18 after week 18.
- **PASS** only if Gate B is B_CLEARED, the forward pooled delta point
  estimate is below 0, and the forward calibration error is smaller than the
  saved builds'. Otherwise INCONCLUSIVE (or NOT_RUN if Gate B did not clear).

## Prediction, stated before scoring

The candidate should keep most of half-life 17's range gain (it was about
-0.06 to -0.10 a row) at no MAE cost: Gate B cleared in both seasons, with
coverage close to 10/10. The forward gate, at ~400-600 rows, can only show
the sign. A risk: for players whose role changed recently, the long memory's
ratio mixes two roles and may be too wide; that is part of what is tested.

## If it passes

A PASS licenses a proposal for the owner's decision, not a release. It would
most naturally ship as a range overlay after the model (the model file and
`nfl-dfs-historical-v5` unchanged), which adds a key to `production_config()`
and so changes `baseline_config_hash` for the coherent-scenario and matchup
studies (`research/nfl_coherent_scenario_study*.json`,
`docs/nfl-matchup-studies.json`). Ranges feed the optimizer (the GPP
objective credits `min(P90, 2.5 x projection)`), the replacement-upside
"if starter" mixture, and the ghost factorial's arms, so
`nfl-replacement-upside-grade-v2` and `nfl-ghost-factorial-v1` would need a
boundary. If the participation ledger's v6 also ships, the two go out as one
version.

## Reproduction

```
python -m research.nfl_dfs_range_split replay --season 2020
python -m research.nfl_dfs_range_split replay --season 2021
python -m research.nfl_dfs_range_split blind
python -m research.nfl_dfs_range_split forward --weeks 8-15      # after week 15
```

`python -m research.nfl_dfs_range_split forward --weeks 1-7 --smoke` was run
before this registration as a plumbing check; it prints and stores row counts
only (`artifacts/nfl-dfs-range-split/forward-smoke-counts.json`) and computes
no loss: 1,066 played RB/TE/WR rows over weeks 1-7, 218 of them anchored,
every row given a candidate range (no identity or history fallback). That
rate (~150 rows a week) clears the 400-row floor by about week 10. Tests: `tests/test_nfl_dfs_range_split.py`.

## Gate B result (2026-10-26): B_CLEARED

`replay --season 2020` and `--season 2021` (3,069 and 3,104 rows with
baseline 5+; history from 2018), run after the registration commit
`e948ecfd`, then `blind`, run once (`blind-verdict.json`, `blind.log`). No
row fell back to the baseline range (`m17` was positive everywhere).

| | 2020 | 2021 |
|---|---|---|
| gated rows (games) | 2,588 (208) | 2,585 (224) |
| baseline range loss | 2.709 | 2.678 |
| range-loss delta, game-clustered | -0.078 [-0.114, -0.041] | -0.062 [-0.101, -0.023] |
| week-clustered (reported) | [-0.112, -0.046] | [-0.092, -0.032] |
| below / above, baseline | 14.1% / 11.2% | 13.7% / 11.7% |
| below / above, candidate | 10.2% / 9.4% | 11.1% / 10.2% |
| RB delta | -0.077 [-0.140, -0.009] | -0.124 [-0.195, -0.056] |
| TE delta | -0.008 [-0.073, +0.056] | -0.024 [-0.109, +0.058] |
| WR delta | -0.106 [-0.167, -0.046] | -0.035 [-0.096, +0.025] |

The interval is wholly below zero in both seasons under both clusterings,
every position's point estimate improves, and pooled coverage moves to the
target. MAE is unchanged by construction. The gain is about 2.3-2.9% of the
range loss, the same size as half-life 17's in the earlier study, now without
its MAE cost. TE is the weakest: its intervals cross zero, and in 2020 its
ceiling over-widened (above-P90 8.9% -> 6.4%). That does not break a guard
(each position's point estimate only has to be at most +0.01), and it is the
cell to watch forward.

Gate F (2026 weeks 8-15, saved builds) still decides. It is graded once after
week 15's results are scored, about 2026-12-22, or after week 18 if the
400-row floor is not met. Production is unchanged until then and after it
unless the owner decides otherwise on a PASS.

## Amendment 1 (2026-10-26, before any weeks 1-7 loss was computed): Gate Q

The owner decided the ranges should change before week 8 rather than wait for
Gate F (an 8-week forward test ends with the season nearly over). Gate Q is a
one-time check on data that already exists, written down before it was run.

- Rows: exactly Gate F's construction (`forward_rows`) on 2026 weeks 1-7. The
  plumbing check above counted 1,066 such rows and computed no loss. These
  weeks were seen for baseline width by `nfl-dfs-range-calibration-v1`; the
  candidate's loss on them has never been computed.
- Graded once (`quick` subcommand, `artifacts/nfl-dfs-range-split/quick-verdict.json`,
  refuses to run twice).
- **SHIP_OK** only if the pooled range-loss delta point estimate is below 0
  and calibration error is smaller than the saved builds'. Otherwise
  **DO_NOT_SHIP**; production keeps the saved ranges and Gate F proceeds as
  registered.
- SHIP_OK licenses the owner's decision to ship the candidate as a range
  overlay for week 8 onward. It is not a Gate F verdict. Gate F still grades
  weeks 8-15 against the range each build would have had without the overlay,
  so a shipped overlay must record that range.

## Gate Q result (2026-10-26): SHIP_OK, and shipped

Run once (`quick-verdict.json`): 1,107 rows over weeks 1-7, every row given a
candidate. Range-loss delta -0.076 [-0.124, -0.028] (game-clustered; week-
clustered [-0.129, -0.019]); below P10 / above P90 11.4% / 12.1% -> 11.1% /
10.5%. WR -0.107 [-0.178, -0.034]; RB -0.031 and TE -0.069, both intervals
crossing zero. The owner shipped it the same day as `nfl-dfs-range-split-v1`:

- `ingest/nfl_dfs_range_ratios.py` writes each RB/WR/TE's (P10_17/m17,
  P90_17/m17) for the upcoming week into `nfl_dfs_range_ratios`, computed
  with `forward_rows`' exact construction, in the production projection
  workflow (an optional step; every run stamps `nfl_dfs_range_ratio_runs`).
- The DFS read (`web/src/lib/nfl-dfs/range-split.ts`, applied after the market
  anchor) sets floor = projection x ratio_p10 and ceiling = projection x
  ratio_p90, clamps the median inside them, and keeps the replaced range in
  `rangeSplit.before`, saved in every run snapshot.
- Gate F is unchanged in rule and window. Its baseline is now the saved
  `rangeSplit.before` where a build carries the overlay (the range the build
  would otherwise have had), the saved range otherwise; its candidate is
  recomputed exactly as registered.
- The matchup (defensive) capture's reproduction check reads `before`, and an
  applied candidate's range takes the same ratios, so defensive adjustments
  stay on. Ghost arms re-apply the ratios to whichever projection an arm uses,
  so every arm carries them.
