"""One spelling per NFL team, across sources that disagree about four of them.

nflverse's play-by-play says `LA`, `WAS`, `AZ`, `JAC`; our schedule, results
and DST ledger say `LAR`, `WSH`, `ARI`, `JAX`. Joining the two without
normalizing does not raise -- it silently returns nothing for those teams.

That is not hypothetical. The 2026 week-2 DST reconciliation derived zero
sacks, zero interceptions and zero fumble recoveries for the Rams and
Commanders, and the error was invisible in validation because the 13-game
slate under test contained neither team. The same class of gap has been found
twice before in this repo (`AZ`/`ARI` dropping every Cardinals player from the
fantasy board along with their bye weeks, and `_selection_prices` hardcoded to
one book).

This map is currently duplicated verbatim in seven other modules
(`model/nfl_dfs_injury_identity.py`, `model/nfl_dfs_redzone_share.py`,
`model/nfl_dfs_redzone_trips.py`, `model/nfl_dfs_target_share.py`,
`model/nfl_dfs_workload_opponent.py`, `ingest/ff_independent.py`,
`ingest/nfl_season_schedule.py`). They are not changed here -- this module is
the canonical home for new code, and migrating the rest is its own change.
"""

from __future__ import annotations

# Source spelling -> our canonical spelling. Relocations are included because
# historical rows genuinely carry the old code.
NFL_TEAM_ALIASES: dict[str, str] = {
    "LA": "LAR",    # nflverse play-by-play
    "WAS": "WSH",   # nflverse play-by-play and DraftKings
    "AZ": "ARI",    # nflverse weekly rosters
    "JAC": "JAX",   # nflverse, some seasons
    "SD": "LAC",    # relocated 2017
    "OAK": "LV",    # relocated 2020
    "STL": "LAR",   # relocated 2016
}


def normalize_team(team: str | None) -> str | None:
    """Canonical abbreviation, or None when there is nothing to normalize.

    None in, None out: a missing team is unknown, never a team.
    """
    if team is None:
        return None
    cleaned = str(team).strip().upper()
    if not cleaned:
        return None
    return NFL_TEAM_ALIASES.get(cleaned, cleaned)
