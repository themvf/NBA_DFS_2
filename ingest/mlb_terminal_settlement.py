"""Free MLB final-score refresh and signal grading, with no odds calls or notifications."""
from __future__ import annotations

import logging

import requests

from ingest.mlb_schedule import fetch_scores

logger = logging.getLogger(__name__)


def settle_results(db):
    from model.line_alerts import settle
    from model.mlb_terminal_signals import run

    dates = db.execute("""SELECT DISTINCT m.game_date FROM mlb_matchups m
        WHERE m.commence_time<=NOW()
          AND m.game_date BETWEEN (NOW() AT TIME ZONE 'America/New_York')::date-7
                              AND (NOW() AT TIME ZONE 'America/New_York')::date
          AND (COALESCE(m.game_status,'') NOT IN ('Final','Game Over','Postponed','Cancelled')
               OR EXISTS (SELECT 1 FROM line_alerts a WHERE a.sport='mlb' AND a.matchup_id=m.id
                          AND a.outcome IS NULL))
        ORDER BY m.game_date""")
    # fetch_scores raises when the MLB Stats API cannot be read (it used to
    # return 0). One unreadable date must not stop the others from settling,
    # so each date is tried, the failures are named, and the run still fails
    # after settling whatever did arrive.
    failed: list[str] = []
    for row in dates:
        try:
            fetch_scores(db, str(row["game_date"]))
        except requests.RequestException as exc:
            logger.warning("MLB scores fetch failed for %s: %s", row["game_date"], exc)
            failed.append(str(row["game_date"]))
    settled = settle(db, "mlb")
    run(db, settle_only=True)
    if failed:
        raise RuntimeError(f"MLB terminal settlement: scores could not be fetched for {', '.join(failed)}; "
                           f"other dates settled ({settled} legacy grades)")
    return {"score_dates": len(dates), "legacy_grades": settled}


if __name__ == "__main__":
    from config import load_config
    from db.database import DatabaseManager

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(settle_results(DatabaseManager(load_config().database_url, initialize_schema=False)))
