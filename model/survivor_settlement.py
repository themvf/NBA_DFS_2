"""Lock and settle survivor picks and recommendations.

A survivor tool that cannot be graded is a toy. This module is what turns the
grid into something with a track record:

  lock     - a pick becomes immutable at its own game's kickoff. A pick that
             can still be edited after the game starts is not a record of a
             decision, and grading it would be grading hindsight.
  settle   - a locked pick, and the recommendation that was frozen alongside
             it, are graded against the real final score.

Tie handling follows the POOL's rule, not a modeling preference. Most pools
score a tie as elimination; some let it survive. The rule lives on
survivor_pools.tie_rule and is applied here rather than assumed.

Recommendations are settled independently of picks, on purpose. The question
"was our recommendation right" is not the same as "did the user survive" --
they only coincide when the user took the recommendation, and the whole point
of keeping both is to be able to tell those apart later.

Usage:
    python -m model.survivor_settlement
    python -m model.survivor_settlement --season 2026 --dry-run
"""

from __future__ import annotations

import argparse
import logging

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)


def lock_started_picks(db: DatabaseManager, season: int, dry_run: bool = False) -> int:
    """Freeze any pick whose game has kicked off.

    Locking is keyed to the pick's OWN game, not to the week's first kickoff:
    a Monday-night pick is legitimately editable while Sunday games are
    already final, and pretending otherwise would lock decisions that the user
    could still honestly change.
    """
    rows = db.execute(
        """
        SELECT p.id
        FROM survivor_entry_picks p
        JOIN nfl_season_games g ON g.id = p.game_id
        WHERE p.locked_at IS NULL
          AND g.season = %s
          AND g.kickoff IS NOT NULL
          AND g.kickoff <= NOW()
        """,
        (season,),
    )
    if dry_run or not rows:
        return len(rows)
    db.execute(
        "UPDATE survivor_entry_picks SET locked_at = NOW() WHERE id = ANY(%s)",
        ([row["id"] for row in rows],),
    )
    return len(rows)


def _outcome(
    *, team_id: int, home_team_id: int, home_score: int, away_score: int, tie_rule: str
) -> str:
    if home_score == away_score:
        return "push" if tie_rule == "tie_survives" else "lost"
    home_won = home_score > away_score
    is_home = team_id == home_team_id
    return "won" if home_won == is_home else "lost"


def settle_picks(db: DatabaseManager, season: int, dry_run: bool = False) -> dict[str, int]:
    """Grade every locked pick whose game is final."""
    rows = db.execute(
        """
        SELECT p.id, p.team_id, p.week, e.id AS entry_id, e.status AS entry_status,
               e.strikes_used, pool.strikes, pool.tie_rule,
               g.home_team_id, g.home_score, g.away_score
        FROM survivor_entry_picks p
        JOIN survivor_entries e ON e.id = p.entry_id
        JOIN survivor_pools pool ON pool.id = e.pool_id
        JOIN nfl_season_games g ON g.id = p.game_id
        WHERE p.result = 'pending'
          AND g.season = %s
          AND g.completed = TRUE
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
        ORDER BY p.week
        """,
        (season,),
    )

    tally = {"won": 0, "lost": 0, "push": 0, "eliminated": 0}
    for row in rows:
        result = _outcome(
            team_id=row["team_id"],
            home_team_id=row["home_team_id"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            tie_rule=row["tie_rule"],
        )
        tally[result] += 1
        if dry_run:
            continue

        db.execute(
            "UPDATE survivor_entry_picks SET result = %s, settled_at = NOW(), "
            "locked_at = COALESCE(locked_at, NOW()) WHERE id = %s",
            (result, row["id"]),
        )
        if result != "lost":
            continue

        # A loss consumes a strike; running out ends the entry. A pool with
        # strikes = 0 is the ordinary one-and-done format.
        strikes_used = row["strikes_used"] + 1
        eliminated = strikes_used > row["strikes"]
        if eliminated:
            tally["eliminated"] += 1
        db.execute(
            """
            UPDATE survivor_entries
            SET strikes_used = %s,
                status = %s,
                eliminated_week = CASE WHEN %s THEN %s ELSE eliminated_week END
            WHERE id = %s
            """,
            (strikes_used, "eliminated" if eliminated else "alive",
             eliminated, row["week"], row["entry_id"]),
        )
    return tally


def settle_recommendations(db: DatabaseManager, season: int, dry_run: bool = False) -> dict[str, int]:
    """Grade frozen recommendations against real results.

    Superseded rows are graded too. A recommendation we later replaced still
    happened, and dropping it from the record would quietly select for the
    advice that survived to the end of the week.
    """
    rows = db.execute(
        """
        SELECT r.id, r.recommended_team_id, g.home_team_id, g.home_score, g.away_score,
               COALESCE(pool.tie_rule, 'tie_loses') AS tie_rule
        FROM survivor_recommendations r
        JOIN nfl_season_games g ON g.id = r.game_id
        LEFT JOIN survivor_pools pool ON pool.id = r.pool_id
        WHERE r.result = 'pending'
          AND g.season = %s
          AND g.completed = TRUE
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
        """,
        (season,),
    )
    tally = {"won": 0, "lost": 0, "push": 0}
    for row in rows:
        result = _outcome(
            team_id=row["recommended_team_id"],
            home_team_id=row["home_team_id"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            tie_rule=row["tie_rule"],
        )
        tally[result] += 1
        if not dry_run:
            db.execute(
                "UPDATE survivor_recommendations SET result = %s, settled_at = NOW() WHERE id = %s",
                (result, row["id"]),
            )
    return tally


def report(db: DatabaseManager, season: int) -> None:
    """Realized vs expected advance rate, shown at whatever n exists.

    Deliberately printed even when the sample is tiny. Hiding a rate behind a
    sample floor makes a new tool look like it has no track record when what
    it actually has is a short one; the honest presentation is the raw record
    beside the rate.
    """
    rows = db.execute(
        """
        SELECT result, COUNT(*) AS n, AVG(p_advance) AS expected
        FROM survivor_recommendations
        WHERE season = %s AND result <> 'pending'
        GROUP BY result
        """,
        (season,),
    )
    if not rows:
        print("no settled recommendations yet")
        return

    counts = {row["result"]: row["n"] for row in rows}
    decided = counts.get("won", 0) + counts.get("lost", 0)
    expected_row = db.execute_one(
        """
        SELECT AVG(p_advance) AS expected
        FROM survivor_recommendations
        WHERE season = %s AND result IN ('won', 'lost')
        """,
        (season,),
    )
    expected = expected_row["expected"] if expected_row else None
    print(f"recommendations: {counts.get('won', 0)}W-{counts.get('lost', 0)}L-{counts.get('push', 0)}P")
    if decided:
        realized = counts.get("won", 0) / decided
        print(f"  realized advance rate {realized*100:.1f}% vs expected "
              f"{float(expected or 0)*100:.1f}% over {decided} decided")
        if decided < 30:
            print(f"  n={decided} -- descriptive only, far too few to calibrate against")


def run(db: DatabaseManager, season: int, dry_run: bool = False) -> None:
    locked = lock_started_picks(db, season, dry_run)
    picks = settle_picks(db, season, dry_run)
    recs = settle_recommendations(db, season, dry_run)
    prefix = "[dry run] " if dry_run else ""
    print(f"{prefix}locked {locked} picks whose game has started")
    print(f"{prefix}picks settled: {picks['won']}W {picks['lost']}L {picks['push']}P "
          f"({picks['eliminated']} entries eliminated)")
    print(f"{prefix}recommendations settled: {recs['won']}W {recs['lost']}L {recs['push']}P")
    report(db, season)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(DatabaseManager(load_config().database_url), args.season, args.dry_run)


if __name__ == "__main__":
    main()
