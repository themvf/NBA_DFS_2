"""Publish descriptive NFL context snapshots for completed PBP games."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_context_publish import prepare_publication, persist_publication
from model.nfl_context_measures import build_neutral_snap_interval_context
from model.nfl_play_facts import facts_frame


def missing_targets(db: DatabaseManager, season: int) -> list[tuple[str, list[str]]]:
    rows = db.execute(
        """
        SELECT p.game_id, MIN(p.away_team) away_team, MIN(p.home_team) home_team
        FROM nfl_pbp_archetypes p
        LEFT JOIN nfl_context_snapshots c
          ON c.target_id=p.game_id
         AND c.definition_id='neutral_offensive_snap_interval_seconds@v1'
         AND c.publication_status='current'
        WHERE p.season=%s
        GROUP BY p.game_id
        HAVING COUNT(DISTINCT c.subject_id) < 2
        ORDER BY p.game_id
        """,
        (season,),
    )
    return [
        (str(row["game_id"]), [str(row["away_team"]), str(row["home_team"])])
        for row in rows
    ]


def prepare_game_publication(
    history_pbp: Path,
    *,
    targets: list[tuple[str, list[str]]],
    observed_at: datetime,
):
    base = prepare_publication(
        history_pbp,
        target_game=None,
        teams=[],
        observed_at=observed_at,
        season_type="REG",
    )
    frame = facts_frame(base.facts)
    contexts = tuple(
        build_neutral_snap_interval_context(
            frame,
            team=team,
            target_id=target_id,
            as_of_at=observed_at,
            source_snapshot_ids=[base.source_observation_id],
            fact_release_id=base.fact_release_id,
        )
        for target_id, teams in targets
        for team in teams
    )
    return replace(base, contexts=contexts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history_pbp", type=Path)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url)
    targets = missing_targets(db, args.season)
    if not targets:
        print(json.dumps({"season": args.season, "targets": 0, "applied": args.apply}))
        return 0
    publication = prepare_game_publication(
        args.history_pbp,
        targets=targets,
        observed_at=datetime.now(timezone.utc),
    )
    result = {
        "season": args.season,
        "targets": len(targets),
        "contexts": len(publication.contexts),
        "applied": args.apply,
    }
    if args.apply:
        result["writeResult"] = persist_publication(db, publication)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
