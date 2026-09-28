# First game of an RB absence: blind test (pre-registered 2026-09-28)

Study id `nfl-rb-first-game-blind-v1`. Registered and pushed **before any
2014–2018 points, carries or error was computed**. Implementation:
`model/nfl_rb_first_game_blind.py`.

## Why

[`nfl-rb-absence-reallocation-v2`](nfl-rb-absence-reallocation-study.md) was
NOT_PROMOTED: the fixed pie moved carries correctly but did not improve points
for backs as a whole. Slicing its output afterwards pointed at one case where
it did look useful: **the first game a starting back misses, for the back who
takes over.** In that case the baseline has not yet absorbed the absence, so a
backup averaging a few carries is projected as a backup while he actually
plays as the starter.

| post-hoc slice (φ 0.5/0.5, lead remaining back) | 2022–25 MSE | 2020–21 MSE |
|---|---:|---:|
| first game of the absence | 86.0 → 67.2 (−22%) | 86.9 → 80.2 (−8%) |
| first game, backup < 6 carries replacing a 12+ starter | 89.4 → 61.4 (−31%) | 103.0 → 81.7 (−21%) |
| absence already under way | 65.8 → 62.3 | 52.0 → 51.9 |

Those slices were cut **after** seeing both studies' outcomes, on seasons both
studies graded. They are a hypothesis, not evidence. This study tests them on
seasons no absence study has graded.

## Blind data

- Gameday inactive status (`INA`) exists in nflverse weekly rosters only from
  2019. Before that, inactive players are listed `ACT`.
- **Activity proxy:** a rostered player is active in a game if he has a stat
  row or a snap-count row for it (snap rows are joined by PFR id, else by
  normalised name within the team-week). A player listed `ACT` who is in
  neither is treated as inactive. Other statuses (`RES`, `PUP`, …) are kept
  unless the player played, in which case he is active. A team game with stats
  but no snap rows is dropped rather than guessed (there are none).
- nflverse snap counts for 2012 are empty (header only), so **2013 is warm-up
  history and 2014–2018 are the blind seasons.**
- Team codes are collapsed to one key per franchise. The three nflverse files
  disagree: schedules keep historical codes (STL, SD, OAK), stats files use
  today's (LA, LAC, LV) for every season, and 2013–2015 weekly rosters use
  GSIS codes (ARZ, BLT, CLV, HST, SL). Without the mapping those teams' rows
  silently fail to join. (The same defect means v1/v2 never loaded the 2019
  Raiders' stats; it affected only warm-up history for early-2020 LV games.)

## Hypothesis (frozen)

In the **first game of a material RB absence**, the v2 fixed pie at
**φ_carries = 0.5, φ_targets = 0.5** predicts the **lead remaining back's** PPR
points better, by squared error, than his unchanged baseline.

Definitions, all from v2 and v1 unchanged (8-game window, half-life 4, ≥ 2
active games to be established, full-strength reserve, 4× cap):

- **Absence game:** a v2 event, i.e. an established RB/FB/WR/TE with a carries
  baseline ≥ 5 is inactive or on reserve, budgets and reserves are defined, and
  at least one established back is active.
- **First game:** at least one such material donor was active in the team's
  previous game.
- **Lead remaining back:** the active established back with the highest carries
  baseline (ties: smallest player id). The pie is allocated across the whole
  backfield; only the lead back is scored. One row per event.

## Frozen setting (selected on already-seen data)

Selection rule: lowest lead-back points squared error on 2020–2025 first-game
events (true `INA` status; 2019 warm-up), ties to smaller φ_targets then
φ_carries. It picked the same setting v2 froze.

| φ_carries, φ_targets | points MSE | points MAE |
|---|---:|---:|
| BASE (no change) | 86.37 | 6.989 |
| 0.5, 0 | 74.86 | 6.847 |
| **0.5, 0.5 (frozen)** | **72.92** | 6.889 |
| 0.5, 1 | 75.04 | 7.031 |
| 1, 0 | 77.35 | 7.042 |
| 1, 0.5 | 79.79 | 7.215 |
| 1, 1 | 85.99 | 7.493 |

Discovery, frozen versus BASE (353 events): points MSE −13.45 [−21.46, −5.56];
MAE −0.10 [−0.51, +0.30]; bias 3.73 → 0.46; carries MSE −23.1 [−29.2, −17.1].
By season the MSE delta is +2.9 (2020), −14.8, −8.6, −14.6, −31.0, −20.0
(2025).

## Proxy validation (2019–2025, true status available; passed)

Pre-set checks, required before unmasking:

| check | threshold | result |
|---|---|---|
| V1 recall: true first-game events reproduced with the same lead back | ≥ 90% | 346 / 353 = 98.0% ✓ |
| V2 precision: proxy events that are true events with the same lead back | ≥ 90% | 346 / 364 = 95.1% ✓ |
| V3 the proxy reproduces the discovery result (CI below 0) | CI upper < 0 | −13.53 [−21.84, −5.65] vs true −13.45 ✓ |

Player-game agreement for RB/FB/WR/TE: 37,953 ACT→ACT, 5,102 INA→INA, 369
ACT→INA (dressed, no stat or snap row matched), 1 INA→ACT. Snap rows matching
no roster player: 0.83% (2019–25) and 1.08% (2013–18).

## Blind structure (counts only, computed before registration)

These depend on who played, never on anyone's points or carries in an event
game. 643 absence games, **259 first-game events** (2014: 48, 2015: 39,
2016: 66, 2017: 51, 2018: 55), 384 already under way, 41 in the narrow slice.
Roster rows without a scheduled game: 5,341, exactly one week per team in
2013–2015 (bye weeks).

## Gate (primary: first-game lead back, 2014–2018; all required)

Event bootstrap (one row per event), 10,000 draws, seed 20260928.

- **G1 points squared error:** 95% CI of the delta (frozen − BASE) entirely
  below 0.
- **G2 points MAE non-inferiority:** 95% CI upper bound below **+0.50**. The
  expected CI half-width at ~260 events is about ±0.44 (discovery ±0.40 at 353),
  so a tighter margin would fail even with no true MAE effect. +0.50 is about
  7% of the lead back's typical miss (~7 points).
- **G3 mechanism:** carries squared-error delta below 0 (point estimate).
- **G4 sample:** ≥ 150 events.

Verdicts: `PROMOTE`, `NOT_PROMOTED` (G4 passes, another gate fails),
`INSUFFICIENT` (G4 fails). If the proxy validation had failed the study would
have been `VOID` and never unmasked; `unmask` re-runs it and refuses to grade
on a failure.

**Secondary, stated in advance, gating nothing:** the narrow slice (backup
< 6 carries replacing a 12+ starter; 41 events, underpowered). Prediction:
MSE delta below 0. Also reported: other backs in first-game events, lead backs
once the absence is under way (expected near zero), and by season.

## What a verdict licenses

- **PROMOTE:** a *live shadow* on the NFL DFS slate. For a back the slate rules
  out who played the team's previous game, the adjusted estimate for the lead
  remaining back is shown next to the baseline and graded against DraftKings
  results each week. Optimizer projections do not change; that switch is a
  separate decision after forward evidence.
- **NOT_PROMOTED / INSUFFICIENT:** baselines stay (v3 behaviour). No retuning
  of φ, thresholds, window, proxy or metric on 2014–2018. Any further variant
  is a new registration on 2026+ data.

## Protocol and disclosures

1. Registration, code, tests and the three pre-registration artifacts
   (`artifacts/nfl_rb_first_game_blind_v1_{discovery,validate,blind}.json`)
   are committed and pushed before `unmask` runs.
2. `python -m model.nfl_rb_first_game_blind unmask` runs once. Its output is
   recorded below verbatim, whatever it says.
3. Earlier in this session the 2016–2018 roster and stats files were
   downloaded to check the status vocabulary (establishing that `INA` starts
   in 2019). No event, prediction or points outcome was computed on any
   2013–2018 data. The 2013–2015 files and all snap counts were downloaded
   for this study.
