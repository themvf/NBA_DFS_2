"""Fantasy-playoff strength of schedule (weeks 15-17) from prior-season defense.

WHY THIS IS SCOPED THE WAY IT IS
--------------------------------
Season-long SOS was measured and deliberately NOT built. Two independent effects
kill it, both quantified against real nflverse data:

  1. Defenses do not carry over. Year-over-year correlation of per-defense rates
     is 0.175 for pass yards allowed, 0.231 for rush yards, and 0.028 for passing
     touchdowns allowed -- essentially nothing for the metric that most directly
     drives QB/WR/TE scoring.
  2. Seventeen opponents average almost all of the remaining variation away.
     Individual defenses span 35-42% (pass) and 52-83% (rush) between best and
     worst, but a full-season slate compresses to a 12-14% spread between the
     most extreme TEAMS.

Multiply those together and a full-season adjustment is worth about 1.2-1.4% to
a typical player -- roughly three points on a 250-point receiver, against
projection error an order of magnitude larger. Shipping it would add a
confident-looking number that cannot move accuracy, which is exactly how the
v1.7 DST regression happened.

Weeks 15-17 are different, for a structural reason rather than a hopeful one:
three opponents average out far less than seventeen, so the same unstable
defensive ratings produce a 27-32% spread instead of 12-14%. Effective signal is
roughly 2.3-3.6% for a typical player -- 2-3x season-long, and it lands on the
exact weeks DraftKings Best Ball scores as separate tournament rounds.

That is still small. It is published as an INDICATOR, never folded into the
projection, so it can break a tie between similar players without implying
precision the 0.18 carryover cannot support.

Reproduce the underlying measurements with `model/ff_playoff_sos_screen.py`.
"""

from __future__ import annotations

import statistics
from typing import Any

import pandas as pd

NFLVERSE_TEAM_WEEK_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/stats_team/"
    "stats_team_week_{season}.csv"
)

FANTASY_PLAYOFF_WEEKS = (15, 16, 17)

# Which side of the ball each position is exposed to. Receiving backs obviously
# see pass defense too, but a running back's fantasy line is dominated by rushing
# volume, and splitting the difference would blur an already-small signal.
POSITION_DEFENSE_METRIC = {
    "QB": "pass_yds",
    "WR": "pass_yds",
    "TE": "pass_yds",
    "RB": "rush_yds",
}

# How many teams at each end get flagged. Eight of 32 is the top/bottom quartile:
# wide enough to be useful on a draft board, narrow enough that a flag means
# something. Deliberately NOT a continuous score -- see module docstring.
FLAG_TEAM_COUNT = 8


def _defense_allowed(season: int) -> dict[str, dict[str, float]]:
    """Per-team defensive rates: what opposing offenses did against them, per game.

    nflverse's team-week rows are OFFENSIVE lines, so each row is credited
    against `opponent_team`'s defense.
    """
    from ingest.ff_independent import normalize_team

    frame = pd.read_csv(NFLVERSE_TEAM_WEEK_URL.format(season=season))
    if "season_type" in frame.columns:
        frame = frame[frame["season_type"] == "REG"]
    frame = frame.assign(
        opponent_team=frame["opponent_team"].map(normalize_team),
    )
    grouped = frame.groupby("opponent_team").agg(
        pass_yds=("passing_yards", "sum"),
        rush_yds=("rushing_yards", "sum"),
        games=("week", "count"),
    )
    out: dict[str, dict[str, float]] = {}
    for team, row in grouped.iterrows():
        games = max(1, int(row["games"]))
        out[str(team)] = {
            "pass_yds": float(row["pass_yds"]) / games,
            "rush_yds": float(row["rush_yds"]) / games,
        }
    return out


def _playoff_opponents(schedule: pd.DataFrame, season: int) -> dict[str, list[str]]:
    from ingest.ff_independent import normalize_team

    games = schedule[
        (schedule["season"] == season)
        & (schedule["game_type"] == "REG")
        & (schedule["week"].isin(FANTASY_PLAYOFF_WEEKS))
    ]
    opponents: dict[str, list[str]] = {}
    for _, game in games.iterrows():
        home, away = normalize_team(game["home_team"]), normalize_team(game["away_team"])
        opponents.setdefault(home, []).append(away)
        opponents.setdefault(away, []).append(home)
    return opponents


def compute_playoff_sos(
    schedule: pd.DataFrame, target_season: int
) -> dict[tuple[str, str], dict[str, Any]]:
    """{(team, position): {rating, rank, flag, opponents}} for weeks 15-17.

    `rank` is 1 = softest slate. `flag` is "soft", "tough", or None. Ratings come
    from the PRIOR season's defenses, which is all that is knowable pre-draft.
    """
    defense = _defense_allowed(target_season - 1)
    opponents = _playoff_opponents(schedule, target_season)
    if not defense or not opponents:
        return {}

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for position, metric in POSITION_DEFENSE_METRIC.items():
        ratings: dict[str, float] = {}
        for team, opposing in opponents.items():
            values = [defense[o][metric] for o in opposing if o in defense]
            if values:
                ratings[team] = statistics.mean(values)
        if not ratings:
            continue
        # Higher yards allowed = softer slate = better for our player.
        ordered = sorted(ratings.items(), key=lambda item: -item[1])
        league_average = statistics.mean(ratings.values())
        for rank, (team, rating) in enumerate(ordered, start=1):
            flag = None
            if rank <= FLAG_TEAM_COUNT:
                flag = "soft"
            elif rank > len(ordered) - FLAG_TEAM_COUNT:
                flag = "tough"
            out[(team, position)] = {
                "rating": round(rating, 1),
                "rank": rank,
                "of": len(ordered),
                "flag": flag,
                "metric": metric,
                "vs_league_average_pct": round((rating / league_average - 1) * 100, 1),
                "opponents": opponents.get(team, []),
                "defense_season": target_season - 1,
                "weeks": list(FANTASY_PLAYOFF_WEEKS),
            }
    return out
