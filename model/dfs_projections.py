"""DFS projection model for NBA players.

Computes independent DraftKings fantasy point projections using:
  - Per-game stat averages from nba_player_stats (rolling 10-game window)
  - Pace context from nba_team_stats (team + opponent)
  - Team-specific implied total from Vegas O/U + moneylines
  - Defensive difficulty from opponent nba_team_stats.def_rtg
  - Usage rate as volume multiplier for pace-driven stat uplift

DraftKings NBA scoring:
  PTS × 1.0 | REB × 1.25 | AST × 1.5 | STL × 2.0 | BLK × 2.0 | TOV × -0.5
  3PM × 0.5 (3-pointer made bonus)
  DD  × 1.5 (double-double bonus, weighted by historical dd_rate)

Model improvements vs v1:
  - Team-specific implied total (home vs away), not full game O/U split evenly
  - Usage rate scales the pace/environment benefit (high-usage players capture more)
  - Assists get partial pace adjustment in addition to def_factor
  - DD rate adjusted for game environment (more possessions = more DD chances)

Key constants calibrated to 2025-26 NBA season averages.
"""

from __future__ import annotations

import random

LEAGUE_AVG_PACE       = 100.0   # NBA possessions per 48 min
LEAGUE_AVG_DEF_RTG    = 114.5   # NBA defensive rating league average (2025-26 actual: 114.57)
LEAGUE_AVG_TOTAL      = 230.0   # NBA Vegas game total approximate average (2025-26 actual: 229.88)
LEAGUE_AVG_TEAM_TOTAL = 115.0   # Per-team share of LEAGUE_AVG_TOTAL (2025-26 actual: 114.94)
LEAGUE_AVG_USAGE      = 20.0    # NBA league average usage rate %


# ── Moneyline helpers ─────────────────────────────────────────────────────────


def _ml_to_prob(ml: int) -> float:
    """Convert American moneyline to implied probability (includes vig)."""
    if ml >= 0:
        return 100.0 / (ml + 100.0)
    return abs(ml) / (abs(ml) + 100.0)


def compute_team_implied_total(
    vegas_total: float,
    home_ml: int | None,
    away_ml: int | None,
    is_home: bool,
) -> float:
    """Compute a team's implied points total from the game O/U and moneylines.

    Key insight: in a 228-total game where one team is a heavy favorite, that
    team is expected to score more than 114 — splitting the total evenly
    understates the favorite and overstates the underdog.

    Formula:
      1. Convert moneylines to vig-removed implied probabilities.
      2. Estimate the spread: each ~2.5% deviation from 50% win probability
         ≈ 1 point of spread (empirically calibrated for NBA).
      3. home_implied = total/2 + spread/2
         away_implied = total  − home_implied

    Caps implied spread at ±15 pts to handle blowout lines.
    Falls back to total/2 when moneylines are missing.

    Examples:
      230 total, home -180 (+155) → home ≈ 118.4, away ≈ 111.6
      230 total, even game        → home = away = 115.0
    """
    if home_ml is None or away_ml is None:
        return vegas_total / 2.0

    raw_home = _ml_to_prob(home_ml)
    raw_away = _ml_to_prob(away_ml)
    vig      = raw_home + raw_away
    home_prob_clean = raw_home / vig   # vig-removed win probability

    # Each 2.5% deviation from 50% ≈ 1 point of spread in NBA
    implied_spread = (home_prob_clean - 0.5) / 0.025
    implied_spread = max(-15.0, min(15.0, implied_spread))

    home_implied = vegas_total / 2.0 + implied_spread / 2.0
    away_implied = vegas_total - home_implied
    return home_implied if is_home else away_implied


# ── Main projection ───────────────────────────────────────────────────────────


def compute_our_projection(
    player: dict,
    team: dict,
    opponent: dict,
    vegas_total: float | None = None,
    home_ml: int | None = None,
    away_ml: int | None = None,
    is_home: bool = False,
    days_rest: int | None = None,
) -> float | None:
    """Compute our DFS projection for an NBA player.

    Args:
        player:      nba_player_stats row — avg_minutes, ppg, rpg, apg, spg, bpg,
                     tovpg, threefgm_pg, usage_rate, dd_rate required.
        team:        nba_team_stats row for player's team — pace.
        opponent:    nba_team_stats row for opponent — pace, def_rtg.
        vegas_total: Vegas over/under for the game (optional).
        home_ml:     Home team American moneyline (e.g. -180). Combined with
                     away_ml to compute team-specific implied total. Falls back
                     to O/U ÷ 2 if either is missing.
        away_ml:     Away team American moneyline (e.g. +155).
        is_home:     True if this player's team is the home team.

    Returns:
        Projected DK fantasy points, or None if insufficient data.
    """
    avg_minutes = player.get("avg_minutes") or 0.0
    if avg_minutes < 10:
        return None  # deep bench / insufficient sample

    ppg         = player.get("ppg")         or 0.0
    rpg         = player.get("rpg")         or 0.0
    apg         = player.get("apg")         or 0.0
    spg         = player.get("spg")         or 0.0
    bpg         = player.get("bpg")         or 0.0
    tovpg       = player.get("tovpg")       or 0.0
    threefgm_pg = player.get("threefgm_pg") or 0.0
    dd_rate     = player.get("dd_rate")     or 0.0
    usage_rate  = player.get("usage_rate")  or LEAGUE_AVG_USAGE

    team_pace   = team.get("pace")        or LEAGUE_AVG_PACE
    opp_pace    = opponent.get("pace")    or LEAGUE_AVG_PACE
    opp_def_rtg = opponent.get("def_rtg") or LEAGUE_AVG_DEF_RTG

    # ── Environment factors ───────────────────────────────────────────────────

    game_pace   = (team_pace + opp_pace) / 2
    pace_factor = game_pace / LEAGUE_AVG_PACE

    # Team-specific implied total: use moneylines to split the O/U correctly.
    # A heavy home favorite in a 230 O/U game gets ~118 implied, not 115.
    if vegas_total:
        team_implied = compute_team_implied_total(vegas_total, home_ml, away_ml, is_home)
        total_factor = team_implied / LEAGUE_AVG_TEAM_TOTAL
    else:
        total_factor = 1.0

    # Blended environment: 40% pace, 60% team implied total
    combined_env = pace_factor * 0.4 + total_factor * 0.6

    # Defensive adjustment: higher DefRtg = worse defense = more scoring allowed
    def_factor = opp_def_rtg / LEAGUE_AVG_DEF_RTG

    # ── Usage rate as volume multiplier ───────────────────────────────────────
    # Stars (30%+ usage) capture a larger share of extra possessions in
    # high-pace/high-implied-total games. Role players (10% usage) capture less.
    # Capped at 0.5x–2.0x to prevent extreme outliers distorting projections.
    usage_factor = min(2.0, max(0.5, usage_rate / LEAGUE_AVG_USAGE))

    # Adjusted environment: scales only the deviation from baseline.
    # In a league-average game (combined_env = 1.0), usage has no effect.
    adjusted_env = 1.0 + (combined_env - 1.0) * usage_factor

    # ── Per-stat projections ──────────────────────────────────────────────────
    # Points:   primary driver is defensive quality, not pace
    proj_pts  = ppg * def_factor

    # Rebounds: pace-driven (more possessions = more missed shots to rebound)
    proj_reb  = rpg * adjusted_env

    # Assists: defense is primary (weaker D = more scoring = more assists),
    # but pace also plays a partial role (more possessions = more assist chances)
    proj_ast  = apg * def_factor * (1.0 + (combined_env - 1.0) * 0.5)

    # Steals, blocks, turnovers: all pace-driven
    proj_stl  = spg   * adjusted_env
    proj_blk  = bpg   * adjusted_env
    proj_tov  = tovpg * adjusted_env

    # 3-pointers: attempt rate and percentage don't vary meaningfully with pace/D
    proj_3pm  = threefgm_pg

    # Double-double rate: more possessions give the player more chances to hit
    # two stat categories ≥ 10; scale with usage-adjusted environment
    proj_dd   = dd_rate * adjusted_env

    # ── Rest/travel factor ────────────────────────────────────────────────────
    # B2B 2nd night: ~5% performance decline (fatigue, travel load)
    # 3-in-4 nights: ~3% decline
    # 4+ days rest: ~2% uplift (full recovery, especially for older players)
    if days_rest is None or days_rest == 3:
        rest_factor = 1.0    # normal 2-day rest or unknown
    elif days_rest <= 1:
        rest_factor = 0.95   # back-to-back
    elif days_rest == 2:
        rest_factor = 0.97   # 3-in-4 nights
    else:
        rest_factor = 1.02   # 4+ days rest

    # ── DK NBA scoring ────────────────────────────────────────────────────────
    fpts = (
        proj_pts * 1.0
        + proj_reb * 1.25
        + proj_ast * 1.5
        + proj_stl * 2.0
        + proj_blk * 2.0
        - proj_tov * 0.5
        + proj_3pm * 0.5    # 3-pointer bonus
        + proj_dd  * 1.5    # expected double-double bonus per game
    ) * rest_factor
    return round(fpts, 2)


# ── Monte Carlo ───────────────────────────────────────────────────────────────

GPP_BOOM_THRESHOLD = 50.0   # DK FPTS ≥ 50 considered a tournament-winning game
MONTE_CARLO_SIMS   = 1000


def compute_monte_carlo(
    our_proj: float,
    fpts_std: float,
    n_sims: int = MONTE_CARLO_SIMS,
    boom_threshold: float = GPP_BOOM_THRESHOLD,
) -> tuple[float, float, float]:
    """Simulate player FPTS distribution and return (floor, ceiling, boom_rate).

    Assumes FPTS ~ Normal(our_proj, fpts_std). Samples are clamped at 0.

    Args:
        our_proj:       Central projection (mean of the normal distribution).
        fpts_std:       Historical per-game FPTS standard deviation.
        n_sims:         Number of Monte Carlo draws (default 1000).
        boom_threshold: DK FPTS threshold defining a "boom" game (default 50).

    Returns:
        floor      — 10th percentile: the realistic downside scenario.
        ceiling    — 90th percentile: the realistic upside (GPP pay-off scenario).
        boom_rate  — P(FPTS ≥ boom_threshold): tournament viability score.
    """
    samples = sorted(max(0.0, random.gauss(our_proj, fpts_std)) for _ in range(n_sims))
    floor    = samples[int(0.10 * n_sims)]
    ceiling  = samples[int(0.90 * n_sims)]
    boom_rate = sum(1 for s in samples if s >= boom_threshold) / n_sims
    return round(floor, 2), round(ceiling, 2), round(boom_rate, 3)


# ── Leverage ──────────────────────────────────────────────────────────────────


def compute_baseline_ownership(
    ref_proj: float,
    pool_avg_proj: float,
    anchor: float = 15.0,
) -> float:
    """Estimate contest ownership % from a player's projection relative to the pool.

    Used when LineStar ownership data is unavailable. Proportional model:
      own = anchor × (ref_proj / pool_avg_proj)
    where anchor ≈ 15% (empirically: 800% total ownership / ~53 rostered players).

    Caps at [1%, 50%] to stay realistic.

    Args:
        ref_proj:      Player projection — avg_fpts_dk preferred, else our_proj.
        pool_avg_proj: Average ref_proj across the player pool.
        anchor:        Expected average ownership % (default 15%).
    """
    if pool_avg_proj <= 0:
        return anchor
    own = anchor * (ref_proj / pool_avg_proj)
    return round(max(1.0, min(50.0, own)), 2)


def compute_leverage(
    our_proj: float,
    proj_own_pct: float,
    field_proj: float | None = None,
    spg: float = 0.0,
    bpg: float = 0.0,
    contrarian_factor: float = 0.7,
) -> float:
    """Compute GPP leverage score for a player.

    Core idea: leverage measures how much MORE we like a player than the field
    does, amplified at low ownership.  A negative score means the model is
    below-field on this player — they should be faded in GPP.

        edge     = our_proj − field_proj
        leverage = edge × (1 − own_pct/100)^contrarian × ceiling_bonus

    Args:
        our_proj:          Our projected DK FPTS.
        proj_own_pct:      Projected ownership % (0–100).
        field_proj:        Best available estimate of what drives field ownership:
                             1st choice — avg_fpts_dk (DK's projection on the salary page,
                               which is what most contestants use when building lineups)
                             2nd choice — linestar_proj
                             Falls back to our_proj if neither is available (positive
                             leverage for all projected players; old behaviour).
        spg:               Steals per game — ceiling/boom proxy.
        bpg:               Blocks per game — ceiling/boom proxy.
        contrarian_factor: Ownership discount exponent (0.7 = moderate contrarian).

    Returns:
        Leverage score. Positive = good GPP target (we like more than field at
        low ownership). Negative = fade (below-field view or over-owned).
    """
    # Edge: how much more bullish we are than the market
    edge = (our_proj - field_proj) if field_proj is not None else our_proj

    own_fraction   = max(0.0, min(1.0, proj_own_pct / 100.0))
    ceiling_bonus  = 1.0 + spg * 0.05 + bpg * 0.04
    leverage       = edge * (1.0 - own_fraction) ** contrarian_factor * ceiling_bonus
    return round(leverage, 3)
