"""Full-season NFL market prices for the survivor grid.

WHY THIS EXISTS, AND WHY IT IS NOT A CHANGE TO ingest/nfl_schedule.py
---------------------------------------------------------------------
`fetch_odds` already calls the bulk `/odds` endpoint, which returns EVERY
upcoming NFL event -- all 272 regular-season games, priced by DraftKings and
William Hill from Week 1 through Week 18 -- for a flat 6 credits (markets x
regions). It then stores only the games on the target date and discards the
rest. So the survivor grid was modeling 160 games whose real prices we were
already buying and throwing away.

The obvious fix is to widen `fetch_odds`. That would be wrong right now:
`game_odds_history` feeds the line-alert detectors, and the NFL
`total_walking` fade study registered in CLAUDE.md freezes its population as
regular-season alerts from 2026-09-09 with at least two pre-commence captures.
Going from ~16 captured games per run to 272 would start generating alerts on
games ten weeks out, whose lines wander for entirely different reasons. That is
a regime change inside a live pre-registered experiment.

So this is a second, independent consumer of the same response. It writes only
to `nfl_season_games.market_*`, touches neither `game_odds_history` nor
`nfl_matchups`, and therefore cannot contaminate the study.

Cost: 6 credits per run (3 markets x 2 regions), same as the existing call.

Usage:
    python -m ingest.nfl_survivor_odds
    python -m ingest.nfl_survivor_odds --season 2026
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_schedule import ODDS_API_BASE, NFL_ODDS_REGIONS, _extract_markets, _parse_iso
from model.soccer_bet_rating import american_to_prob

logger = logging.getLogger(__name__)

SPORT_KEY = "americanfootball_nfl"

# Two-way overround above this is a quote so wide the implied probability is
# not worth trusting. Measured 2026-09-01: DraftKings holds a flat ~4.3% from
# Week 1 through Week 18, so this only catches genuine outliers rather than
# quietly filtering out lookahead lines.
MAX_OVERROUND = 1.25


def _overround(home_ml: int | None, away_ml: int | None) -> float | None:
    if home_ml is None or away_ml is None:
        return None
    total = american_to_prob(home_ml) + american_to_prob(away_ml)
    return total if total > 0 else None


def fetch_season_odds(db: DatabaseManager, api_key: str, season: int) -> dict:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required")

    response = requests.get(
        f"{ODDS_API_BASE}/sports/{SPORT_KEY}/odds/",
        params={
            "apiKey": api_key,
            "regions": NFL_ODDS_REGIONS,
            "markets": "h2h,spreads,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        },
        timeout=30,
    )
    response.raise_for_status()
    logger.info(
        "survivor odds quota: cost=%s remaining=%s",
        response.headers.get("x-requests-last", "?"),
        response.headers.get("x-requests-remaining", "?"),
    )
    events = response.json() or []

    # Resolve provider events to survivor rows through the event id that
    # nfl_matchups already owns. Identity is never re-derived from team names
    # here -- nfl_schedule.fetch_events is the single place that mapping lives.
    rows = db.execute(
        """
        SELECT g.id, m.event_id, g.kickoff
        FROM nfl_season_games g
        JOIN nfl_matchups m ON m.id = g.matchup_id
        WHERE g.season = %s AND m.event_id IS NOT NULL
        """,
        (season,),
    )
    by_event = {str(row["event_id"]): row for row in rows}

    now = datetime.now(timezone.utc)
    stored = skipped_started = skipped_unmatched = skipped_wide = 0

    for event in events:
        target = by_event.get(str(event.get("id") or ""))
        if target is None:
            skipped_unmatched += 1
            continue

        # Never store a price at or after kickoff: an in-play quote presented as
        # a pregame number is the exact failure this repo repaired for MLB.
        try:
            commence = _parse_iso(event.get("commence_time"))
        except ValueError:
            skipped_unmatched += 1
            continue
        if commence <= now:
            skipped_started += 1
            continue

        markets = _extract_markets(event)
        overround = _overround(markets["home_ml"], markets["away_ml"])
        if overround is not None and overround > MAX_OVERROUND:
            skipped_wide += 1
            continue

        db.execute(
            """
            UPDATE nfl_season_games
            SET market_home_ml = %s,
                market_away_ml = %s,
                market_spread_line = %s,
                market_total_line = %s,
                market_book_count = %s,
                market_overround = %s,
                market_captured_at = NOW()
            WHERE id = %s
            """,
            (
                markets["home_ml"],
                markets["away_ml"],
                # nfl_schedule stores the home spread book-style (favorite
                # negative); the survivor grid's convention is
                # positive-is-home-favored, matching nflverse.
                None if markets["home_spread"] is None else -float(markets["home_spread"]),
                markets["vegas_total"],
                markets["bookmaker_count"],
                overround,
                target["id"],
            ),
        )
        stored += 1

    return {
        "events": len(events),
        "stored": stored,
        "skipped_started": skipped_started,
        "skipped_unmatched": skipped_unmatched,
        "skipped_wide": skipped_wide,
    }


def coverage(db: DatabaseManager, season: int) -> list[dict]:
    return db.execute(
        """
        SELECT week,
               COUNT(*) AS games,
               COUNT(market_home_ml) AS priced,
               ROUND(AVG(market_overround)::numeric, 4) AS avg_overround,
               ROUND(AVG(market_book_count)::numeric, 1) AS avg_books
        FROM nfl_season_games
        WHERE season = %s
        GROUP BY week ORDER BY week
        """,
        (season,),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    summary = fetch_season_odds(db, config.odds_api.api_key, args.season)
    print(
        f"{summary['events']} provider events -> {summary['stored']} priced; "
        f"skipped {summary['skipped_started']} started, "
        f"{summary['skipped_unmatched']} unmatched, "
        f"{summary['skipped_wide']} too wide"
    )
    priced = 0
    total = 0
    for row in coverage(db, args.season):
        priced += row["priced"]
        total += row["games"]
    print(f"season coverage: {priced}/{total} games priced")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
