"""Shared fail-closed eligibility for live MLB game-line predictions."""

from __future__ import annotations

from db.database import DatabaseManager


def eligible_pregame_matchup_ids(
    db: DatabaseManager,
    game_date: str,
) -> set[int]:
    """Return games with a known start time that is still in the future.

    Live prediction/rating surfaces must use this shared guard. Historical
    backfills are separate workflows and deliberately do not call it.
    """
    rows = db.execute(
        """
        SELECT id
        FROM mlb_matchups
        WHERE game_date = %s
          AND commence_time IS NOT NULL
          AND commence_time > NOW()
        """,
        (game_date,),
    )
    return {int(row["id"]) for row in rows}
