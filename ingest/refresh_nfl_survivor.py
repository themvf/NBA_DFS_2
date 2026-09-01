"""Weekly refresh for the NFL survivor grid.

Order matters and the failure policy matters more. Each step blocks only what
it actually covers -- the lesson from the MLB bullpen-gate incident, where one
miscounting health check silently disabled prop capture, a pipeline it had
nothing to do with, for weeks:

  1. schedule   -- nflverse season grid. A failure here blocks everything,
                   because there is no grid without it.
  2. odds       -- full-season Odds API prices. A failure degrades the grid to
                   nflverse's partial lines plus modeled cells, which is what
                   it ran on before this step existed; it never blocks.
  3. model      -- probabilities and ratings. A failure blocks new
                   probabilities; the previous run's grid still renders.
  4. popularity -- field pick share. A failure blocks the fade-the-field mode
                   only, and NEVER the grid. It is also expected to fail
                   early in a season: survivorgrid publishes a season shortly
                   before week 1, and "not published yet" is not an outage.
  5. settlement -- lock started picks, grade finished ones.

Usage:
    python -m ingest.refresh_nfl_survivor
    python -m ingest.refresh_nfl_survivor --season 2026 --recalibrate
"""

from __future__ import annotations

import argparse
import logging
import traceback

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument(
        "--recalibrate",
        action="store_true",
        help="re-measure the horizon error table (slow; it is a constant of the sport)",
    )
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    failures: list[str] = []

    # 1. Schedule -- hard dependency.
    from ingest.nfl_season_schedule import load_season, verify_season

    print("== schedule ==")
    written = load_season(db, args.season)
    print(f"  loaded {written} games")
    problems = verify_season(db, args.season)
    for problem in problems:
        print(f"  FAIL {problem}")
    if problems:
        raise SystemExit("schedule health gate failed -- the grid cannot be built")
    print("  PASS 32 teams x every week x exactly one bye")

    # 2. Full-season market prices. Optional: the grid falls back to nflverse
    #    lines plus modeled cells, which is what it ran on before this existed.
    print("== market prices ==")
    try:
        from ingest.nfl_survivor_odds import fetch_season_odds

        api_key = load_config().odds_api.api_key
        if not api_key:
            print("  SKIP no ODDS_API_KEY; falling back to nflverse lines")
        else:
            summary = fetch_season_odds(db, api_key, args.season)
            print(f"  {summary['stored']} of {summary['events']} events priced "
                  f"({summary['skipped_started']} started, "
                  f"{summary['skipped_unmatched']} unmatched, "
                  f"{summary['skipped_wide']} too wide)")
    except Exception:  # noqa: BLE001 - degrades the grid, never blocks it
        traceback.print_exc()
        failures.append("market prices")

    # 3. Probabilities.
    print("== probabilities ==")
    try:
        from ingest.nfl_season_schedule import fetch_schedule
        from model.nfl_survivor_model import (
            compute_and_store,
            fit_spread_prob,
            historical_games,
            horizon_calibration,
            load_sigma_table,
            store_calibration,
        )

        schedule = fetch_schedule()
        fit = fit_spread_prob(historical_games(schedule))
        if args.recalibrate or not load_sigma_table(db):
            print("  measuring horizon error (slow, once per season is plenty)")
            store_calibration(db, horizon_calibration(schedule))
        summary = compute_and_store(db, args.season, fit)
        print(f"  anchored at week {summary['anchor_week']} from "
              f"{summary['quoted_games']} quoted games")
        for label, count in summary["provenance"].items():
            if count:
                print(f"    {label:20s} {count:4d} games")
    except Exception:  # noqa: BLE001 - one step's failure must not kill the rest
        traceback.print_exc()
        failures.append("probabilities")

    # 4. Field pick share -- optional, and its absence is usually not a fault.
    print("== pick popularity ==")
    try:
        from ingest.survivor_pick_popularity import load as load_popularity

        results = load_popularity(db, args.season, list(range(1, 19)))
        published = [week for week, outcome in results.items() if isinstance(outcome, int)]
        missing = [week for week, outcome in results.items() if outcome == "not published"]
        broken = {week: outcome for week, outcome in results.items()
                  if isinstance(outcome, str) and outcome != "not published"}
        print(f"  {len(published)} weeks captured, {len(missing)} not published yet")
        for week, outcome in broken.items():
            print(f"  FAIL week {week}: {outcome}")
        if broken:
            failures.append("pick popularity (parser shape)")
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        failures.append("pick popularity")

    # 5. Settlement.
    print("== settlement ==")
    try:
        from model.survivor_settlement import run as settle

        settle(db, args.season)
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        failures.append("settlement")

    if failures:
        raise SystemExit(f"completed with failures: {', '.join(failures)}")
    print("\nall steps clean")


if __name__ == "__main__":
    main()
