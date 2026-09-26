"""Non-UI descriptive consumer for saved NFL context snapshots."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from typing import Any

from config import load_config
from db.database import DatabaseManager
from model.nfl_context_engine import ContextReader, ContextUsage, ResolvedContext, stable_digest
from model.nfl_context_store import (
    PostgresContextRepository,
    PostgresManifestStore,
    PostgresPolicyRegistry,
)


VERSION = "nfl-context-postgame-v1"
DEFINITION_ID = "neutral_offensive_snap_interval_seconds@v1"


def compare_contexts(contexts: list[ResolvedContext]) -> dict[str, Any]:
    if len(contexts) != 2:
        raise ValueError("postgame comparison requires exactly two team contexts")
    rows = []
    for context in contexts:
        measurement = context.measurement
        if measurement.value is None:
            raise ValueError(f"{measurement.subject_id} context is unknown")
        rows.append(
            {
                "team": measurement.subject_id,
                "seconds": measurement.value,
                "intervals": measurement.denominator,
                "snapshotId": measurement.snapshot_id,
                "factReleaseId": measurement.fact_release_id,
            }
        )
    rows.sort(key=lambda row: row["seconds"])
    return {
        "version": VERSION,
        "definitionId": DEFINITION_ID,
        "teams": rows,
        "fasterHistoricalTeam": rows[0]["team"],
        "differenceSeconds": rows[1]["seconds"] - rows[0]["seconds"],
        "interpretation": (
            "Descriptive prior-season game-clock interval only; no causal, "
            "predictive, market, DFS, or betting claim."
        ),
    }


def run(db: DatabaseManager, *, target_id: str, teams: list[str]) -> dict[str, Any]:
    if len(teams) != 2 or len(set(teams)) != 2:
        raise ValueError("exactly two distinct teams are required")
    requested_as_of = datetime.now(timezone.utc)
    reader = ContextReader(PostgresContextRepository(db), PostgresPolicyRegistry(db))
    contexts = [
        reader.read_current(
            definition_id=DEFINITION_ID,
            subject_id=team,
            target_id=target_id,
            consumer_id="nfl_postgame_evaluator",
            use_case="context_replay",
            cohort="all_teams",
            usage=ContextUsage.DESCRIPTIVE,
            requested_as_of=requested_as_of,
        )
        for team in teams
    ]
    payload = compare_contexts(contexts)
    run_id = stable_digest(
        {
            "version": VERSION,
            "targetId": target_id,
            "snapshots": sorted(context.measurement.snapshot_id for context in contexts),
        }
    )
    manifest_id = PostgresManifestStore(db).persist(
        consumer_id="nfl_postgame_evaluator",
        run_id=run_id,
        contexts=contexts,
        model_artifact_id=VERSION,
        resolved_at=requested_as_of,
    )
    payload.update({"runId": run_id, "targetId": target_id, "manifestId": manifest_id})
    db.execute(
        """
        INSERT INTO nfl_context_postgame_reports
            (run_id, target_id, manifest_id, report_version, payload)
        VALUES (%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (run_id) DO NOTHING
        """,
        (run_id, target_id, manifest_id, VERSION, json.dumps(payload)),
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-game", required=True)
    parser.add_argument("--team", action="append", required=True)
    args = parser.parse_args()
    result = run(
        DatabaseManager(load_config().database_url),
        target_id=args.target_game,
        teams=args.team,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
