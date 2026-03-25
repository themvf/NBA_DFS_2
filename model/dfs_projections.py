"""DFS projection model for NBA players.

Computes independent DraftKings fantasy point projections using:
  - Per-game stat averages from nba_player_stats (rolling 10-game window)
  - Pace context from nba_team_stats (team + opponent)
  - Defensive difficulty from opponent nba_team_stats.def_rtg
  - Vegas total blended with pace for a more accurate environment signal

DraftKings NBA scoring:
  PTS × 1.0 | REB × 1.25 | AST × 1.5 | STL × 2.0 | BLK × 2.0 | TOV × -0.5
  3PM × 0.5 (3-pointer made bonus)
  DD  × 1.5 (double-double bonus, weighted by historical dd_rate)

Key difference from NCAA model:
  - No blowout curve (NBA teams rarely blow out enough to affect starter minutes)
  - avg_minutes is a direct value from the API (not min_pct × 40)
  - def_factor direction is inverted: higher def_rtg = worse defense = easier scoring
  - SPG/BPG are per-game values (not team-possession percentages)
"""

from __future__ import annotations

LEAGUE_AVG_PACE    = 100.0   # NBA possessions per 48 min, 2025-26 season average
LEAGUE_AVG_DEF_RTG = 112.0   # NBA defensive rating league average
LEAGUE_AVG_TOTAL   = 228.0   # NBA Vegas game total approximate average


def compute_our_projection(
    player: dict,
    team: dict,
    opponent: dict,
    vegas_total: float | None = None,
) -> float | None:
    """Compute our DFS projection for an NBA player.

    Args:
        player:      nba_player_stats row — avg_minutes, ppg, rpg, apg, spg, bpg,
                     tovpg, threefgm_pg, usage_rate, dd_rate required.
        team:        nba_team_stats row for player's team — pace.
        opponent:    nba_team_stats row for opponent — pace, def_rtg.
        vegas_total: Vegas over/under for the game (optional). When provided,
                     blended with pace for a more accurate environment signal.

    Returns:
        Projected DK fantasy points, or None if insufficient data.
    """
    avg_minutes = player.get("avg_minutes") or 0.0
    if avg_minutes < 10:
        return None  # deep bench / insufficient sample

    ppg         = player.get("ppg")        or 0.0
    rpg         = player.get("rpg")        or 0.0
    apg         = player.get("apg")        or 0.0
    spg         = player.get("spg")        or 0.0
    bpg         = player.get("bpg")        or 0.0
    tovpg       = player.get("tovpg")      or 0.0
    threefgm_pg = player.get("threefgm_pg") or 0.0
    dd_rate     = player.get("dd_rate")    or 0.0

    team_pace = team.get("pace") or LEAGUE_AVG_PACE
    opp_pace  = opponent.get("pace") or LEAGUE_AVG_PACE
    opp_def_rtg = opponent.get("def_rtg") or LEAGUE_AVG_DEF_RTG

    # Pace adjustment: faster games = more possessions = more stat opportunities
    game_pace   = (team_pace + opp_pace) / 2
    pace_factor = game_pace / LEAGUE_AVG_PACE

    # Vegas total: more accurate environment signal than pace alone.
    # Blend 40% pace-derived / 60% Vegas-derived.
    total_factor = (vegas_total / LEAGUE_AVG_TOTAL) if vegas_total else 1.0
    combined_env = pace_factor * 0.4 + total_factor * 0.6

    # Defensive adjustment: higher DefRtg = worse defense = more scoring allowed.
    # Direction is opposite to NCAA (where lower AdjDE = weaker defense).
    def_factor = opp_def_rtg / LEAGUE_AVG_DEF_RTG

    # Project each stat using environment multipliers.
    # NBA stats are per-game averages — no per-minute rate conversion needed.
    proj_pts  = ppg * def_factor
    proj_reb  = rpg * combined_env
    proj_ast  = apg * def_factor
    proj_stl  = spg * combined_env
    proj_blk  = bpg * combined_env
    proj_tov  = tovpg * combined_env
    proj_3pm  = threefgm_pg  # 3PT% doesn't vary materially with pace/defense

    # DK NBA scoring
    fpts = (
        proj_pts  * 1.0
        + proj_reb  * 1.25
        + proj_ast  * 1.5
        + proj_stl  * 2.0
        + proj_blk  * 2.0
        - proj_tov  * 0.5
        + proj_3pm  * 0.5    # 3-pointer bonus
        + dd_rate   * 1.5    # expected double-double bonus per game
    )
    return round(fpts, 2)


def compute_leverage(
    our_proj: float,
    proj_own_pct: float,
    our_win_prob: float | None = None,
    vegas_win_prob: float | None = None,
    contrarian_factor: float = 0.7,
    spg: float = 0.0,
    bpg: float = 0.0,
) -> float:
    """Compute GPP leverage score for a player.

    Combines projected FPTS, projected ownership (lower = more leverage),
    and a ceiling bonus for high-variance players (big steal/block upside).

    Args:
        our_proj:          Our projected DK FPTS.
        proj_own_pct:      Projected ownership % (0–100).
        our_win_prob:      Our model win probability (not used in NBA — pass None).
        vegas_win_prob:    Vegas implied win probability (not used in NBA — pass None).
        contrarian_factor: Ownership discount exponent (0.7 = moderate contrarian).
        spg:               Steals per game — boom-game proxy.
        bpg:               Blocks per game — boom-game proxy.

    Returns:
        Leverage score (higher = better GPP play).
    """
    own_fraction = max(0.0, min(1.0, proj_own_pct / 100))
    base = our_proj * (1 - own_fraction) ** contrarian_factor

    # Edge multiplier: if our model disagrees with Vegas on win prob, amplify.
    # In NBA we typically don't run a separate win-prob model, so this stays 1.0.
    if our_win_prob is not None and vegas_win_prob is not None and vegas_win_prob > 0:
        edge = max(0.0, our_win_prob - vegas_win_prob)
        base *= 1 + edge * 2

    # Ceiling bonus: players who can boom via steals/blocks get a multiplier.
    # Uses per-game values directly (spg=1.5, bpg=2.0 → ceiling_bonus ≈ 1.155).
    ceiling_bonus = 1.0 + spg * 0.05 + bpg * 0.04
    base *= ceiling_bonus

    return round(base, 3)
