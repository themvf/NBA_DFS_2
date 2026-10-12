# Game-leader calibration study (`nfl-game-leaders-calibration-v1`)

Registered 2026-10-11, before any outcome in this study was computed. Every
choice below is fixed here; a change after outcomes are seen is a new version.

## Question

Are the game-leader probabilities on `/nfl/game-leaders` honest, and can three
specific changes make them better than a simple recent-average baseline?

1. Real gameday inactives plus the default role scenario
   (`nfl-game-leaders-roles-v1`, shipped 2026-10-11 without validation).
2. Prior-season usage in the role estimates. Week 5 2026 showed the model
   rating nearly every player below his own DraftKings line (mean gap about 7
   points of probability), concentrated on stars with thin or uneven 2026 usage.
3. A one-number-per-market recalibration (temperature).

## Why 2022 is the test season

Every 2023–2025 outcome has already been examined by some game-leader study:
2024 weeks 5–8 (development), 2025 weeks 5–12 (evaluations), 2023 and 2024 (the
ranking challenger's training and selection) and 2025 weeks 13–18 (the
challenger's holdout, recorded as examined in `nfl-game-leaders-model.md`).
Study 1 (`nfl-joint-score-state-v1`) also graded 2025 joint outcomes. So
2023–2025 are development only, and the single confirmation look is the 2022
regular season, which no game-leader study has graded. It needs a separate
frozen capture (`calibration-v1/capture-2021-2022.json.gz`); 2021 supplies
prior-season usage and early-week history.

## Population

- Regular-season games in which both teams have the three reconciled prior
  games the model requires, and whose outcome the model's own `grade()` can
  score. Skipped games are counted with reasons, never dropped silently.
- Decision time is kickoff minus one minute. Training uses only games that
  kicked off earlier.
- **Inactives**: a player listed `INA` or `RES` for that season and week in the
  nflverse weekly roster file is `out`. Gameday inactives are published about 90
  minutes before kickoff, so this is pregame information.
- **Roster**: reconstructed from the previous three team games (`roster()`);
  players with no recent usage fall to the model's newcomer slots.
- **Depth order** (used by the role rule for vacated carries and newcomer
  targets): ranked by recent usage, carries for QB/RB and targets for WR/TE.
  Historical FantasyPros/Sleeper depth snapshots do not exist; the live page
  uses them, so this is a stated difference between the study and production.

## Variants (fixed list)

| Variant | Description |
| --- | --- |
| A | No scenario, reconstructed roster, no inactives (the configuration published before 2026-10-10). |
| B | Inactives + `roles-v1`. The current production configuration. |
| C(m) | B, with each player's per-game carries and targets blended with his prior-season per-game rate as `m` pseudo-games, m ∈ {2, 4, 8}. |

All forecasts: 2,000 draws, seed 20261008, otherwise default `Settings()`.

## Fitted quantities (development only)

- **Variant choice**: the variant among B and C(m) with the lowest development
  mean log loss summed over the four markets. One choice for all markets, so a
  market cannot pick its own winner. C(m) is compared with B on 2024–2025 only,
  the seasons where a prior season exists in the development capture.
- **Temperature** per market, on the chosen variant: p_i ∝ p_i^(1/T),
  T on a 0.50–2.00 grid in steps of 0.05, minimizing development log loss.
- **Probabilistic baseline** per market: named players' share ∝
  exp(recent average / s), with a fixed unnamed-contributor share r; s on a
  grid of 2–40 (yards markets) or 0.25–4 (receptions), r ∈ {0.01, 0.03, 0.05},
  minimizing development log loss.

## Development and test windows

- Development: 2023 weeks 4–18, 2024 weeks 1–18, 2025 weeks 1–18 (eligible
  games only).
- Test, one look: 2022 weeks 1–18, eligible games only, with every fitted
  quantity frozen from development.

## Metrics and gates

Per market (rushing yards, receiving yards, receptions, total yards),
evaluated separately:

- **Primary**: mean per-game log loss of the actual leader (ties split), final
  calibrated model minus the probabilistic baseline, with a game-clustered
  bootstrap 95% interval (2,000 resamples, seed 20261011). The market **passes**
  if the interval's upper bound is below zero.
- **Reported, not gating**: Brier score; top-choice credit against the naive
  recent-average leader, with its bootstrap interval; calibration bins and
  expected calibration error before and after the temperature; and variants A,
  B and the chosen variant side by side, which measures what the roles fix bought.

## What each outcome means

- **Pass**: the page may say the market's probabilities beat a recent-average
  baseline on an untouched season. It remains a research model.
- **Fail**: no claim. No re-slicing by position, team or week; a new idea is a
  new version with its own registration.
- Either way, 2026 weeks 6 onward are graded weekly against the same baselines
  as they finish, and against the DraftKings-implied leader shares once enough
  captured weeks exist (that comparison will get its own registration; no
  historical DraftKings data exists for it).

## Known limits

- Historical inactives come from nflverse status codes, not an archived
  pregame capture. Players listed as questionable who then played are treated
  as available, which the live page cannot know in advance.
- Rosters are reconstructed from usage; a newly signed player is a newcomer slot.
- Reconciled box scores include later stat corrections.
- Player-game rows inside a game are dependent; uncertainty is resampled by game.

## Result (2026-10-11): two of four markets pass

Development (2023–2025, 689 graded games, 127 skipped for registered reasons)
chose **C2** (prior-season usage as two pseudo-games; summed log loss 8.226
against B 8.347, C4 8.325, C8 8.342 on the 481 games with a prior season).
Every fitted temperature is above 1 (rushing 1.30, receiving 1.05, receptions
1.15, total 1.25): the model is overconfident in all four markets. The fit
(`calibration-v1/fit.json`) was committed before the 2022 records were opened.

**2022, single look** (245 graded games, 26 skipped). Log loss of the calibrated
C2 model minus the fitted recent-average baseline, game-clustered 95% interval:

| Market | Model | Baseline | Difference | 95% interval | Gate |
| --- | ---: | ---: | ---: | --- | --- |
| Rushing yards | 1.729 | 2.054 | −0.325 | −0.540 to −0.070 | **Pass** |
| Total yards | 2.275 | 2.456 | −0.181 | −0.285 to −0.082 | **Pass** |
| Receiving yards | 2.356 | 2.459 | −0.103 | −0.271 to +0.113 | Fail |
| Receptions | 2.497 | 2.417 | +0.080 | −0.151 to +0.357 | Fail |

Reported, not gating:

- **What the roles fix bought** (uncalibrated log loss, 2022): rushing
  A 2.165 → B 1.893 → C2 1.803; total yards 2.512 → 2.387 → 2.275. For the
  pass-catching markets the gains do not hold: receiving yards A 2.347,
  B 2.262, C2 2.363; receptions A 2.508, B 2.625, C2 2.534.
- **Picking the leader**: the model's top choice is no better than the naive
  recent-average leader in any market; every interval includes zero
  (for example rushing +0.012, −0.045 to +0.065).
- **Calibration**: expected calibration error after the temperature is 0.004–0.009
  in every market.

Reading: for rushing and total yards the probabilities are measurably better
than a recent-average baseline on a season no game-leader study had touched.
For receiving yards and receptions they are not, and the role changes do not
reliably help those markets. In no market does the model pick the leader
better than the simple rule. Per the registration, receiving and receptions get
no claim and no re-slicing; adopting C2 and the temperatures in production is a
separate decision.
