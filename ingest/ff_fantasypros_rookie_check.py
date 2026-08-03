"""Read-only diagnostic: is FantasyPros' `nfl/{season}/projections` endpoint for a
past season genuinely dated to that season, and how did it grade against real
2025 rookie outcomes?

This makes no writes to `ff_players`, `ff_ranking_sets`, or `ff_player_rankings` --
it only reads the already-persisted season=2026 player identities (for
fantasypros_player_id + rookie_year lookups) and season=2025 actual outcomes
already in `ff_player_season_features`, then calls the FantasyPros API fresh for
`nfl/2025/projections`.

Two questions, both answered from real data, not assumed:

1. Leakage test: does the "2025" payload include any player whose Sleeper
   rookie_year is 2026 (i.e., someone who did not exist in the NFL during the
   2025 season)? If so, the endpoint is not genuinely season-scoped -- it is
   serving current data regardless of the season parameter.
2. Accuracy test (only meaningful if the leakage test passes): for confirmed
   2025 rookies (rookie_year == 2025) matched by fantasypros_player_id, compare
   FantasyPros' projected points against the real 2025 season outcome already
   stored in ff_player_season_features.

Usage:
    python -m ingest.ff_fantasypros_rookie_check --season 2025 --output report.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
from typing import Any

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import FantasyProsClient, projection_stats, source_points

SCORING_TYPES = ("STD", "HALF", "PPR")


def _rookie_fpid_map(db: DatabaseManager, rookie_year: str) -> dict[int, dict[str, Any]]:
    rows = db.execute(
        """SELECT fantasypros_player_id, id AS player_id, canonical_name, position
           FROM ff_players
           WHERE fantasypros_player_id IS NOT NULL
             AND metadata->'sleeper'->'metadata'->>'rookie_year' = %s""",
        (rookie_year,),
    )
    return {int(row["fantasypros_player_id"]): dict(row) for row in rows}


def _actual_points(db: DatabaseManager, player_id: int, season: int) -> dict[str, float | None]:
    row = db.execute_one(
        "SELECT fantasy_points_std, fantasy_points_ppr, games FROM ff_player_season_features WHERE player_id=%s AND season=%s",
        (player_id, season),
    )
    if not row:
        return {"actual_std": None, "actual_ppr": None, "actual_games": None}
    std = row["fantasy_points_std"]
    ppr = row["fantasy_points_ppr"]
    return {
        "actual_std": std,
        "actual_ppr": ppr,
        "actual_half": (std + ppr) / 2 if std is not None and ppr is not None else None,
        "actual_games": row["games"],
    }


def run_check(season: int) -> dict[str, Any]:
    config = load_config()
    db = DatabaseManager(config.database_url)

    rookies_target_season = _rookie_fpid_map(db, str(season))
    rookies_current_season = _rookie_fpid_map(db, str(season + 1))

    client = FantasyProsClient(os.environ.get("FANTASYPROS_API_KEY", ""))
    payload = client.get(f"nfl/{season}/projections", {"week": 0, "positions": "QB:RB:WR:TE:K:DST"})
    rows = [row for row in payload.get("players", []) if isinstance(row, dict)]
    returned_fpids = {int(row["fpid"]) for row in rows if row.get("fpid") is not None}

    leaked_fpids = sorted(set(rookies_current_season) & returned_fpids)
    leaked_players = [rookies_current_season[fpid]["canonical_name"] for fpid in leaked_fpids]

    comparisons: list[dict[str, Any]] = []
    for row in rows:
        fpid = row.get("fpid")
        if fpid is None or int(fpid) not in rookies_target_season:
            continue
        rookie = rookies_target_season[int(fpid)]
        stats = projection_stats(row.get("stats"))
        actual = _actual_points(db, int(rookie["player_id"]), season)
        entry = {
            "fantasypros_player_id": int(fpid),
            "name": rookie["canonical_name"],
            "position": rookie["position"],
            "projected_std": source_points(stats, "STD"),
            "projected_half": source_points(stats, "HALF"),
            "projected_ppr": source_points(stats, "PPR"),
            **actual,
        }
        if entry["projected_ppr"] is not None and entry["actual_ppr"] is not None:
            entry["error_ppr"] = entry["projected_ppr"] - entry["actual_ppr"]
        comparisons.append(entry)

    errors_ppr = [c["error_ppr"] for c in comparisons if c.get("error_ppr") is not None]
    summary = {
        "n_target_season_rookies_known": len(rookies_target_season),
        "n_current_season_rookies_known": len(rookies_current_season),
        "n_projections_rows_returned": len(rows),
        "n_leaked_current_season_rookies_in_target_payload": len(leaked_fpids),
        "leaked_player_names": leaked_players,
        "leakage_verdict": (
            "STALE_OR_LIVE_DATA -- current-season rookies who did not exist during the "
            f"target season appeared in the {season} payload"
            if leaked_fpids
            else "NO_LEAKAGE_DETECTED -- no current-season-only rookie appeared in the "
            f"{season} payload (necessary, not sufficient, evidence of genuine season scoping)"
        ),
        "n_target_rookies_matched_in_payload": len(comparisons),
        "n_matched_with_actual_outcome": len(errors_ppr),
        "ppr_mae": round(statistics.mean(abs(e) for e in errors_ppr), 2) if errors_ppr else None,
        "ppr_bias": round(statistics.mean(errors_ppr), 2) if errors_ppr else None,
    }

    return {
        "season": season,
        "summary": summary,
        "comparisons": sorted(comparisons, key=lambda c: c["name"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    report = run_check(args.season)
    rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
