"""High-cadence, ADP-only snapshot capture for fantasy-football draft-market tracking.

`ingest.ff_independent` rebuilds the entire board every run -- Sleeper, three
seasons of nflverse player stats, the nflverse schedule, and FFC ADP -- which
is both slow and unnecessary just to track ADP movement, and its per-player
ADP values are overwritten on every rebuild (`ff_player_rankings.adp` has no
history). This script does one thing: call Fantasy Football Calculator's ADP
endpoint for STD/HALF/PPR and append one row per matched player per scoring
format to `ff_adp_snapshots`, so risers/fallers can be measured over time
instead of only "current vs. whatever the last full rebuild happened to see."

Requires `ff_players` to already be populated for the season (run
`ingest.ff_independent` at least once first) -- this script only matches
against the existing player universe, it does not build one.

The draft market ends with week 1 of the NFL regular season. After the last
week-1 kickoff (from `nfl_season_games`) the script stores nothing and exits 0
with a one-line reason. Before then the row-count guard still refuses a thin
response, because during draft season a short feed means a truncated one.
FFC's 2026 feed shrank from ~260 players per format on 2026-09-06 to 118/54/29
(STD/HALF/PPR, sample windows ending 09-15/09-14/09-25) by 2026-09-29, and the
guard turned every run from 2026-09-14 on red.

Usage:
    python -m ingest.ff_adp_snapshot --season 2026
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase, as_float, as_int
from ingest.ff_independent import (
    FFC_ADP_URL,
    FFC_FORMATS,
    _fetch_json,
    _snapshot,
    build_adp_lookup,
)

MIN_PLAYER_UNIVERSE = 100
MIN_ADP_ROWS = 100


def _floor_to_12h(moment: datetime) -> datetime:
    """Round down to 00:00 or 12:00 UTC.

    Makes re-runs within the same 12-hour window idempotent (an UPDATE on the
    same captured_at, not a new near-duplicate row) instead of depending on
    the scheduler firing at exactly the same wall-clock minute every time.
    """
    hour = 0 if moment.hour < 12 else 12
    return moment.astimezone(timezone.utc).replace(hour=hour, minute=0, second=0, microsecond=0)


def _player_lookup(db: RefreshDatabase, season: int) -> dict[tuple[str, str], int]:
    """Same (normalized_name, position) / (team, 'DST') keying as build_adp_lookup."""
    rows = db.execute(
        "SELECT id, normalized_name, position, team_abbrev FROM ff_players WHERE season=%s",
        (season,),
    )
    lookup: dict[tuple[str, str], int] = {}
    for row in rows:
        position = row["position"]
        key = (row["team_abbrev"] or "", "DST") if position == "DST" else (row["normalized_name"], position)
        if key[0] and key not in lookup:
            lookup[key] = int(row["id"])
    return lookup


def draft_market_close(db: RefreshDatabase, season: int) -> datetime | None:
    """Last week-1 regular-season kickoff: after it, redraft drafting is over."""
    row = db.execute_one(
        """SELECT MAX(kickoff) AS closes_at FROM nfl_season_games
           WHERE season=%s AND game_type='REG' AND week=1""",
        (season,),
    )
    return row["closes_at"] if row else None


def _run(season: int, db: RefreshDatabase, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    closes_at = draft_market_close(db, season)
    if closes_at is not None and now > closes_at:
        return {
            "season": season, "skipped": True,
            "reason": (f"the {season} draft market closed when NFL week 1 ended "
                       f"({closes_at.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC); nothing stored"),
        }
    captured_at = _floor_to_12h(now)
    player_lookup = _player_lookup(db, season)
    if len(player_lookup) < MIN_PLAYER_UNIVERSE:
        raise RuntimeError(
            f"ff_players has too few {season} rows ({len(player_lookup)}) to match ADP against -- "
            "run `python -m ingest.ff_independent` first to build the player universe."
        )

    scorings: dict[str, Any] = {}
    for scoring, source_format in FFC_FORMATS.items():
        url = FFC_ADP_URL.format(format=source_format, season=season)
        payload, digest = _fetch_json(url)
        player_rows = payload.get("players", [])
        if not isinstance(player_rows, list) or len(player_rows) < MIN_ADP_ROWS:
            raise RuntimeError(f"Fantasy Football Calculator {scoring} ADP returned suspiciously few rows")
        lookup = build_adp_lookup(payload)
        matched_keys = [key for key in lookup if key in player_lookup]

        # Uses a distinct dataset name ("adp-snapshot") from the full board
        # rebuild's "adp" dataset -- same endpoint, but this keeps the two
        # capture paths' matched/unmatched provenance independently queryable
        # rather than one process's stats overwriting the other's.
        snapshot_id = _snapshot(
            db, source="fantasy-football-calculator", dataset="adp-snapshot", season=season,
            digest=digest, row_count=len(player_rows), scoring=scoring, ranking_type="ADP",
            params={
                "url": url, "teams": 12, "format": source_format,
                "captured_at": captured_at.isoformat(), "cadence_hours": 12,
            },
            model_eligible=False,
            eligibility_reason="market context is excluded from football-performance features",
        )
        db.execute(
            "UPDATE ff_source_snapshots SET matched_count=%s,unmatched_count=%s WHERE id=%s",
            (len(matched_keys), len(player_rows) - len(matched_keys), snapshot_id),
        )

        stored = 0
        for key in matched_keys:
            raw = lookup[key]
            adp = as_float(raw.get("adp"))
            if adp is None:
                continue
            db.execute(
                """INSERT INTO ff_adp_snapshots
                   (player_id, season, scoring, captured_at, source_snapshot_id,
                    adp, adp_stdev, adp_high, adp_low, times_drafted)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(player_id, scoring, captured_at) DO UPDATE SET
                     source_snapshot_id=EXCLUDED.source_snapshot_id, adp=EXCLUDED.adp,
                     adp_stdev=EXCLUDED.adp_stdev, adp_high=EXCLUDED.adp_high,
                     adp_low=EXCLUDED.adp_low, times_drafted=EXCLUDED.times_drafted""",
                (
                    player_lookup[key], season, scoring, captured_at, snapshot_id, adp,
                    as_float(raw.get("stdev")), as_float(raw.get("high")), as_float(raw.get("low")),
                    as_int(raw.get("times_drafted")),
                ),
            )
            stored += 1
        scorings[scoring] = {
            "players": len(player_rows), "matched": len(matched_keys), "stored": stored,
            "source_snapshot_id": snapshot_id,
        }

    return {"season": season, "captured_at": captured_at.isoformat(), "scorings": scorings}


def run(season: int) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        result = _run(season, db)
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()
    result = run(args.season)
    if result.get("skipped"):
        print(f"ADP snapshot skipped: {result['reason']}.")
    print(json.dumps(result, indent=2))
