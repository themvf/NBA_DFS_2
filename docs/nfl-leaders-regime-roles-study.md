# Game-leader roles: regime-conditioned usage with a fitted prior strength (`nfl-leaders-regime-roles-v1`)

Registered 2026-10-18, before any outcome below was computed and before week 7
kicked off. Every choice here is fixed; a change after outcomes are seen is a
new version. Roles v2 (the prior-season role gate shipped the same day) is not
part of this study and stays as shipped.

## Question

The role share behind every game-leader forecast
(`model/nfl_game_leaders_roles.py`) is an equal-weight average of a player's
usage over the team's last five games, blended with last season as two
pseudo-games, with a ruled-out back's carries handed down the depth chart
55/35/10. That estimator is the maximum-likelihood estimate for a role that
is constant over the window, and three of the four week-6 cases that prompted
this (Allen, Pierce, Tuten) are roles that changed inside it. Two questions:

1. **Conditioning on an observed change point.** When a starter is ruled out
   and has already missed games, does estimating the remaining players' shares
   from the team's games *without him*, with the depth split as a prior of
   strength K, predict next-game shares better than the depth split alone?
2. **Recency inside the window.** Does an exponential weight with half-life h
   over the window's games predict next-game shares better than equal weights?

## Estimator (`roles-v3`, research only until this study passes)

Carries and targets per game are treated as `Multinomial(N_g, θ)` with
`θ ~ Dirichlet(α)`, so the share estimate is

    share_i = (Σ_g w_g c_i,g + α_i) / (Σ_g w_g N_g + Σ_j α_j)

- **Window and weights**: the team's last five games; `w_g = 0.5^((G−1−g)/h)`
  with h in games, `h = ∞` meaning equal weights (the current rule).
- **Regime**: when a player ruled `out` was in the window, games he played are
  dropped from the sum for every teammate at his position group (RB for a
  back, WR/TE for a receiver), and `α` for those teammates is K times the
  v2 depth split applied to his shrunk rate. With no regime games the
  estimate is the depth split, which is exactly v2.
- **Everything else as v2**: the prior-season blend and its role gate, the
  newcomer targets, the reserve, departed players at zero, quarterback carries.

Both parameters are fitted; neither is stated. Grid: `K ∈ {0.5, 1, 2, 4}`,
`h ∈ {∞, 4, 3, 2, 1}`. One pair for carries and one for targets, chosen
jointly on development data.

## Data and windows

- Harness: `research/nfl_game_leaders_calibration.py`, which reconstructs
  rosters from usage and takes inactives from the nflverse weekly roster
  (`INA`/`RES`). Historical depth order is recent usage rank, as in the
  calibration study, a stated difference from the live page.
- **Development (fit)**: 2023 weeks 4–18, 2024 weeks 1–18, 2025 weeks 1–18,
  eligible games only. All three seasons have been examined by earlier
  game-leader studies; they are tuning data here and support no claim.
- **Confirmation (one look)**: 2026 regular-season weeks 7–18, prospective.
  The fitted (K, h) are frozen in `artifacts/nfl-game-leaders/regime-roles-v1/fit.json`
  before week 7's first kickoff, and roles-v3 shares are written as a shadow
  column beside the v2 scenario in each week's stored request, never into
  the published forecast.
- Optional second look, declared now: 2021 weeks 4–18 if a 2020–2021 capture
  can be built (2020 supplies prior usage). It is graded once, after the fit
  is frozen, and reported beside the forward result; it does not replace it.

## Metrics

- **Primary (fitting and confirmation)**: Dirichlet-multinomial log score of
  the next game's observed carries and targets under the predicted shares,
  per team-game, on the **affected subset**: team-games where a top-two
  back or top-three receiver by window usage is out and has already missed at
  least one window game. Paired difference roles-v3 minus roles-v2, with a
  week-clustered bootstrap 95% interval (2,000 resamples, seed 20261018).
- **Secondary, same subset**: mean absolute error of each named player's
  carries and targets.
- **Guard (all team-games, every market)**: leader log loss of the published
  model with v3 shares minus with v2 shares, same bootstrap. The guard must
  not be worse than +0.005 at the interval's upper bound; v3 may not buy
  share accuracy at the cost of the leader forecast.
- Reported, not gating: the same on the unaffected subset (where v3 reduces
  to v2 plus recency), and the fitted (K, h).

## Floors and verdict

Confirmation needs at least **60 affected team-games over 8 game weeks**.
Expected accrual is about 8–12 affected team-games a week, so the floor
should be met by week 14–15; no verdict before both floors, and no interim
efficacy look. PASS requires the primary interval's upper bound below zero
and the guard held. FAIL or an unmet floor leaves v2 in production.

## Family

Two questions, one fitted pair each, one confirmation look: a family of two
tests, reported as such. No re-slicing by position, team or market; no
re-fitting K or h on confirmation data; no third estimator bolted on if both
fail. A different model is a new registration.

## Honest prior

Question 1 should pass: the repo's absence studies already found the team's
own games without the starter beat a fixed pie once the absence is under way
(`nfl-absence-commit-v1`, `nfl-rb-first-game-blind-v1` at n=1), and the
week-6 cases are large (Allen 83% of post-Hall carries against a 44% share).
Question 2 is weaker: Tuten's rise fits a slope of +0.077 share per game with
SE 0.023 (t ≈ 3.3 on 3 df), and a single game's share at 27 carries has a
sampling SE of 0.09 before game script, so a short half-life may buy variance
and nothing else. Expect h to fit at 3–4 or ∞.
