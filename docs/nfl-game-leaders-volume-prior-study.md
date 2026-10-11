# Game leaders: last-season usage prior (pre-registered 2026-10-11)

Study id `nfl-leaders-volume-prior-v1`. Registered and committed **before any
tuning or holdout number was computed**. Implementation:
`research/nfl_game_leaders_volume_prior.py`.

## Why

The game-leaders model (`model/nfl_game_leaders.py`, roles from
`model/nfl_game_leaders_roles.py`) builds each player's share of his team's
targets and carries from **current-season games only** (`recent = current or
own`; the roles-v1 scenario reads the same window). By week 5 a role rests on at
most four games. On 2026-10-11 this produced the model's largest disagreements
with DraftKings lines (Chase 5.2 projected targets vs 11.6 a game in 2025; Likely
8.8 vs 3.3). Re-running the week-5 forecasts one setting at a time showed the
efficiency shrinkage (peer prior, prior-season weight) moves these players only a
few yards: projected volume is almost the whole gap.

## What has already been seen (disclosed, not hidden)

An exploratory check on **2024 and 2025 weeks 2-8** (212 games) used an analytic
replica of the volume logic with one untuned prior weight (K = 4):

- error (actual − roles-v1 projection) regressed on (last-season rate − current
  rate): slope +0.50 targets [+0.44, +0.56], +0.29 carries [+0.21, +0.37];
- K = 4, any-team prior: target MAE 1.655 → 1.546, carries 1.288 → 1.240;
- an any-team prior beat a same-team-only prior;
- players with no current-season games are projected at zero (actual 3.1
  targets); the blend overshoots them (4.4).

Those weeks are therefore examined. **2025 weeks 2-8 are excluded from every
evaluation below.** 2024 is used only for tuning. The "any team" choice and the
returner rule below were shaped by that look; this is stated, not concealed.

## Variants (fixed list)

For player `i`, action `a` in {targets, carries}, using the team's
current-season window exactly as roles-v1 does (up to six games, games-played
rate shrunk by one game toward the all-games rate, `r_cur`, with `n` games
played):

- **C0 (control):** roles-v1 unchanged.
- **V1:** `rate = (n · r_cur + K_a · r_prev) / (n + K_a)`, where `r_prev` is the
  player's previous-season targets (or carries) per game played, **on any team**,
  required only when he played at least 4 games that season. Otherwise
  `rate = r_cur`. The prior fades by itself as `n` grows.
- **Returner rule (part of V1):** when `n = 0` and `r_prev` exists,
  `rate = ρ_a · r_prev`.

Rates become shares exactly as roles-v1 does (normalized over the active
players, with the same newcomer reserve), and projected volume = team budget ×
share, with the model's team budget (current-season window, three league
pseudo-games). Team budget and the opponent volume effect are **not changed**
by this study.

## Tuning (2024 only)

- Data: 2024 regular season, weeks 2-18; previous season 2023.
- Grid: `K_a` in {1, 2, 3, 4, 6, 8, 12, 16}; `ρ_a` in {0.5, 0.6, 0.7, 0.8, 0.9, 1.0}.
- Selection: for each action separately, the (K, ρ) pair with the lowest mean
  absolute error of projected volume per player-game. Ties go to the smaller K.
- The chosen constants are written to this document (Amendment 1) and committed
  **before** the holdout is run.

## Holdout (one look)

- Data: 2025 regular season, **weeks 9-18**; previous season 2024. Weeks 2-8
  are excluded (seen above). Earlier evaluations graded 2025 weeks 9-12
  leader shares for a different model version; this mechanism and these
  volume errors have not been computed on those weeks.
- Population: every player-game with a box score for that game. The active list
  is the players who appeared, for all variants alike (a cleaner availability
  picture than a live forecast; disclosed limitation).
- **Primary metric:** paired difference in mean absolute error of projected
  targets per player-game, V1 − C0, with a 95% bootstrap interval resampling
  whole games (2,000 resamples, seed 20261011).
- **Secondary:** the same for carries; the slope of error on (last-season −
  current) rate under V1 (should move toward zero); MAE by week (9-12, 13-18);
  the returner group (n = 0) mean error.

## Decision rules (fixed now)

- **PASS (volume):** the primary interval's upper bound is below 0.
- **Carries:** V1 is adopted for carries only if its own interval's upper bound
  is below 0; otherwise carries keep roles-v1 and V1 applies to targets only.
- **FAIL:** primary upper bound ≥ 0. V1 is dead. No re-tuning on the holdout; any
  change is a new registration.
- A PASS licenses building V1 into `model/nfl_game_leaders_roles.py` as a new
  role version (`nfl-game-leaders-roles-v2`). It does **not** by itself change
  the published page. Before that, the full simulation must be graded:

## Leader-level gate (after a PASS, before publishing)

The full forecast with V1 roles is graded on the same holdout games with the
existing harness (`research.nfl_game_leaders` grading): fractional-winner log
loss and Brier score for rushing yards, receptions and receiving yards, against
C0 and the recent-average baseline. V1 is published only if the pooled log-loss
difference V1 − C0 has point estimate below 0 and 95% upper bound below +0.005
(non-inferior). Forward 2026 weeks 6+ are then graded weekly against C0, the
baseline and the stored market-implied shares (`nfl_game_leader_market`). Market
comparison is descriptive: no market history exists before 2026-10-11.

## Out of scope

Team target and carry budgets (the source of low rushing projections, e.g.
Gibbs and Henry on 2026-10-11) are a separate phase pending the owner's choice
between an odds-free team prior and spread/total inputs. Efficiency settings are
unchanged. No claim of betting edge is made or tested.

## Amendments

(none yet)
