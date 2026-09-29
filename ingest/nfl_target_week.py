"""The one definition of "the current NFL game week".

Three writers used to answer this differently. The projection/context publisher
(``ingest.nfl_dfs_projections.infer_target_week``) kept a week current until
six hours after a kickoff, while the Sleeper/monitor/freeze job
(``ingest.nfl_availability_operations``) and the FantasyPros capture
(``ingest.nfl_dfs_availability``) moved on the instant the last game kicked
off (``kickoff > now``). For the ~6 hours after every Monday-night kickoff the
publisher wrote week-N contexts while the monitor audited week N+1 and reported
"Context covers 0 of 32 team-games" (2026-09-29 00:16-06:09 UTC, eleven
consecutive runs).

The rule, used by all of them:

    A game holds its week open until GAME_GRACE (6 hours) after its kickoff.
    The target week is the week of the earliest-kicking-off game still open.

Why six hours after kickoff, and not "until the game is final":
  * A game in progress still belongs to its week. Moving on at kickoff drops
    the week while its last game is being played.
  * ``nfl_season_games.completed`` is mutable current state written by the
    daily results ingest. It lags the real final by hours, and because it is
    not point-in-time an answer that consulted it could not be replayed for a
    past moment. Six hours exceeds a full game including overtime and ordinary
    weather delays, so the deterministic bound gives the same answer a working
    "final" flag would within the window that matters.
  * Ordering by earliest open kickoff (not by the smallest open week number)
    lets a postponed game return to the front only when it is actually next.

Games without a kickoff time are ignored: they cannot be placed in time.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping


GAME_GRACE = timedelta(hours=6)


class SeasonComplete(ValueError):
    """Every loaded regular-season game is past its grace window."""


def _aware(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None


def game_is_open(kickoff: datetime | None, now: datetime) -> bool:
    """True while ``now`` is before ``kickoff + GAME_GRACE``."""
    return _aware(kickoff) and now < kickoff + GAME_GRACE


def select_target_week(games: Iterable[Mapping[str, Any]], now: datetime) -> int | None:
    """The week of the earliest game still open at ``now``; None if none is."""
    if not _aware(now):
        raise ValueError("now must be timezone-aware")
    open_games = [game for game in games if game_is_open(game.get("kickoff"), now)]
    if not open_games:
        return None
    earliest = min(open_games, key=lambda game: (game["kickoff"], int(game["week"])))
    return int(earliest["week"])


def target_week(db: Any, season: int, now: datetime) -> int:
    """Resolve the current regular-season week from the canonical schedule.

    Raises ``ValueError`` when no regular-season schedule is loaded for the
    season (a real failure), and ``SeasonComplete`` when one is loaded but
    every game is past its grace window (nothing left to target).
    """
    rows = db.execute(
        """SELECT week,kickoff FROM nfl_season_games
           WHERE season=%s AND game_type='REG' AND kickoff IS NOT NULL""",
        (season,),
    )
    if not rows:
        raise ValueError(f"No regular-season schedule is loaded for {season}")
    week = select_target_week(rows, now)
    if week is None:
        raise SeasonComplete(f"Every {season} regular-season game is more than "
                             f"{GAME_GRACE} past kickoff")
    return week
