"""Plan an NFL historical-odds backfill without making paid API calls."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping

import psycopg2

from config import load_config


PLAN_VERSION = "nfl-historical-odds-plan-v1"
HISTORICAL_CREDITS_PER_REGION_MARKET = 10
DEFAULT_CHECKPOINTS_MINUTES = {
    "t_minus_48h": 48 * 60,
    "t_minus_24h": 24 * 60,
    "t_minus_6h": 6 * 60,
    "t_minus_90m": 90,
    "t_minus_15m": 15,
    "close_proxy_t5m": 5,
}


def planned_snapshots(
    games: Iterable[tuple[int, int, datetime]],
    checkpoints: Mapping[str, int] = DEFAULT_CHECKPOINTS_MINUTES,
) -> dict[str, object]:
    by_checkpoint: dict[str, set[datetime]] = {name: set() for name in checkpoints}
    by_season: dict[int, set[datetime]] = {}
    all_snapshots: set[datetime] = set()
    game_count = 0
    for season, _game_id, kickoff in games:
        game_count += 1
        if kickoff.tzinfo is None:
            raise ValueError("kickoff must be timezone-aware")
        for name, minutes in checkpoints.items():
            snapshot = (kickoff - timedelta(minutes=minutes)).astimezone(timezone.utc)
            by_checkpoint[name].add(snapshot)
            by_season.setdefault(season, set()).add(snapshot)
            all_snapshots.add(snapshot)
    return {
        "games": game_count,
        "unique_snapshot_calls": len(all_snapshots),
        "calls_by_checkpoint": {name: len(values) for name, values in by_checkpoint.items()},
        "calls_by_season": {str(season): len(values) for season, values in sorted(by_season.items())},
        "snapshot_times": sorted(value.isoformat() for value in all_snapshots),
    }


def build_plan(
    games: Iterable[tuple[int, int, datetime]],
    *,
    seasons: list[int],
    regions: int = 1,
    markets: int = 3,
    checkpoints: Mapping[str, int] = DEFAULT_CHECKPOINTS_MINUTES,
) -> dict[str, object]:
    plan = planned_snapshots(games, checkpoints)
    calls = int(plan["unique_snapshot_calls"])
    credits_per_call = HISTORICAL_CREDITS_PER_REGION_MARKET * regions * markets
    result = {
        "artifact_type": "nfl-historical-odds-backfill-plan",
        "plan_version": PLAN_VERSION,
        "seasons": seasons,
        "checkpoints_minutes_before_kickoff": dict(checkpoints),
        "regions": regions,
        "markets": markets,
        "credits_per_call": credits_per_call,
        "estimated_credits": calls * credits_per_call,
        "paid_api_calls_made": 0,
        **plan,
    }
    digest_payload = json.dumps(result, sort_keys=True, separators=(",", ":"))
    result["artifact_digest"] = hashlib.sha256(digest_payload.encode()).hexdigest()
    return result


def load_games(database_url: str, seasons: list[int]) -> list[tuple[int, int, datetime]]:
    with psycopg2.connect(database_url) as connection:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT season,id,kickoff FROM nfl_season_games
                   WHERE game_type='REG' AND season=ANY(%s) AND kickoff IS NOT NULL
                   ORDER BY season,week,id""",
                (seasons,),
            )
            return [(int(season), int(game_id), kickoff) for season, game_id, kickoff in cursor.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", nargs="+", type=int, required=True)
    parser.add_argument(
        "--checkpoint",
        nargs="+",
        choices=tuple(DEFAULT_CHECKPOINTS_MINUTES),
        help="Subset to cost; defaults to all supported checkpoints",
    )
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    config = load_config()
    checkpoints = (
        {name: DEFAULT_CHECKPOINTS_MINUTES[name] for name in args.checkpoint}
        if args.checkpoint else DEFAULT_CHECKPOINTS_MINUTES
    )
    plan = build_plan(
        load_games(config.database_url, args.season),
        seasons=args.season,
        checkpoints=checkpoints,
    )
    artifact = args.artifact or Path("artifacts") / (
        "nfl_historical_odds_plan_" + "_".join(map(str, args.season)) + ".json"
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: plan[key] for key in (
        "games", "unique_snapshot_calls", "credits_per_call", "estimated_credits", "paid_api_calls_made"
    )}, indent=2))


if __name__ == "__main__":
    main()
