"""Publish revisioned NFL play facts and descriptive context snapshots.

Dry-run is the default.  ``--apply`` writes immutable evidence, fact revisions,
context snapshots, and descriptive-only consumer qualifications.  The as-of
time defaults to the actual system observation time; callers cannot backdate a
newly observed source into a prospective model run.

Example:
    python -m ingest.nfl_context_publish PBP.parquet \
      --target-game 2026_02_NYG_LA --team NYG --team LA
    python -m ingest.nfl_context_publish PBP.parquet \
      --target-game 2026_02_NYG_LA --team NYG --team LA --apply
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg2.extras import execute_batch

from config import load_config
from db.database import DatabaseManager
from model.nfl_context_engine import ContextMeasurement, stable_digest
from model.nfl_context_measures import (
    NEUTRAL_SNAP_INTERVAL,
    build_neutral_snap_interval_context,
)
from model.nfl_play_facts import FACT_SCHEMA_VERSION, PlayFact, build_play_facts, facts_frame


POLICY_VERSION = "nfl-context-descriptive-v1"


@dataclass(frozen=True)
class Publication:
    source_observation_id: str
    fact_release_id: str
    source_digest: str
    source_metadata: dict[str, Any]
    facts: tuple[PlayFact, ...]
    contexts: tuple[ContextMeasurement, ...]
    observed_at: datetime

    def summary(self) -> dict[str, Any]:
        regimes: dict[str, int] = {}
        for fact in self.facts:
            regimes[fact.regime] = regimes.get(fact.regime, 0) + 1
        return {
            "sourceObservationId": self.source_observation_id,
            "factReleaseId": self.fact_release_id,
            "sourceDigest": self.source_digest,
            "facts": len(self.facts),
            "regimes": regimes,
            "penaltyEvents": sum(len(fact.penalties) for fact in self.facts),
            "contexts": [context.as_dict() for context in self.contexts],
            "observedAt": self.observed_at.isoformat(),
        }


def _manifest(path: Path) -> dict[str, Any]:
    latest = path.parent / "latest.json"
    if not latest.exists():
        return {"cache_path": str(path)}
    record = json.loads(latest.read_text(encoding="utf-8"))
    record["cache_path"] = str(path)
    return record


def prepare_publication(
    path: Path,
    *,
    target_game: str | None,
    teams: list[str],
    observed_at: datetime,
    season_type: str = "REG",
) -> Publication:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    raw = path.read_bytes()
    source_digest = hashlib.sha256(raw).hexdigest()
    metadata = _manifest(path)
    expected = metadata.get("response_hash")
    if expected and expected != source_digest:
        raise ValueError("source digest does not match its frozen manifest")
    source_observation_id = f"nflverse-pbp-{source_digest}"
    fact_release_id = stable_digest(
        {
            "dataset": "nfl-play-facts",
            "schemaVersion": FACT_SCHEMA_VERSION,
            "sourceObservationId": source_observation_id,
            "seasonType": season_type,
        }
    )
    pbp = pd.read_parquet(path)
    if "season_type" in pbp.columns and season_type:
        pbp = pbp[pbp["season_type"].eq(season_type)].copy()
    built = build_play_facts(
        pbp,
        source_observation_id=source_observation_id,
        fact_release_id=fact_release_id,
    )
    materialized = facts_frame(built)
    if teams and not target_game:
        raise ValueError("target_game is required when publishing contexts")
    contexts = tuple(
        build_neutral_snap_interval_context(
            materialized,
            team=team,
            target_id=str(target_game),
            as_of_at=observed_at,
            source_snapshot_ids=[source_observation_id],
            fact_release_id=fact_release_id,
        )
        for team in teams
    )
    return Publication(
        source_observation_id=source_observation_id,
        fact_release_id=fact_release_id,
        source_digest=source_digest,
        source_metadata=metadata,
        facts=tuple(built),
        contexts=contexts,
        observed_at=observed_at.astimezone(timezone.utc),
    )


def persist_publication(db: DatabaseManager, publication: Publication) -> dict[str, int]:
    facts_inserted = 0
    penalties_inserted = 0
    contexts_inserted = 0
    with db.connect() as conn:
        cursor = conn.cursor()
        source_published = publication.source_metadata.get("source_published_at")
        idempotency_key = stable_digest(
            {
                "source": "nflverse",
                "record": publication.source_observation_id,
                "payload": publication.source_digest,
            }
        )
        cursor.execute(
            """
            INSERT INTO nfl_evidence_observations
                (observation_id, source, source_record_key, source_published_at,
                 system_observed_at, raw_payload, payload_digest, idempotency_key)
            VALUES (%s, 'nflverse', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                publication.source_observation_id,
                publication.source_metadata.get("url") or publication.source_observation_id,
                source_published,
                publication.observed_at,
                json.dumps(publication.source_metadata),
                publication.source_digest,
                idempotency_key,
            ),
        )
        cursor.execute(
            """
            INSERT INTO nfl_fact_releases
                (release_id, dataset_key, fact_schema_version,
                 source_observation_ids, payload_digest)
            VALUES (%s, 'nfl-play-facts', %s, %s, %s)
            ON CONFLICT (release_id) DO NOTHING
            """,
            (
                publication.fact_release_id,
                FACT_SCHEMA_VERSION,
                json.dumps([publication.source_observation_id]),
                stable_digest([fact.fact_revision_id for fact in publication.facts]),
            ),
        )

        game_ids = sorted({fact.game_id for fact in publication.facts})
        cursor.execute(
            """
            SELECT game_id, play_id, fact_revision_id, revision_number, revision_status
            FROM nfl_play_fact_revisions
            WHERE game_id = ANY(%s)
            ORDER BY game_id, play_id, revision_number
            """,
            (game_ids,),
        )
        existing: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for row in cursor.fetchall():
            existing.setdefault((str(row["game_id"]), int(row["play_id"])), []).append(row)

        fact_values = []
        supersede_ids: list[str] = []
        penalty_values = []
        for fact in publication.facts:
            key = (fact.game_id, fact.play_id)
            prior = existing.get(key, [])
            if any(row["fact_revision_id"] == fact.fact_revision_id for row in prior):
                continue
            revision_number = max((int(row["revision_number"]) for row in prior), default=0) + 1
            supersede_ids.extend(
                str(row["fact_revision_id"])
                for row in prior
                if row.get("revision_status") == "current"
            )
            fact_values.append(
                (
                    fact.fact_revision_id,
                    fact.game_id,
                    fact.play_id,
                    revision_number,
                    fact.fact_release_id,
                    fact.source_observation_id,
                    fact.snap_execution,
                    fact.action_validity,
                    fact.description,
                    json.dumps(fact.payload),
                )
            )
            for penalty in fact.penalties:
                penalty_values.append(
                    (
                        fact.fact_revision_id,
                        penalty.occurrence,
                        penalty.adjudication,
                        penalty.penalty_type,
                        penalty.team,
                        penalty.enforced_yards,
                        json.dumps({"rawSegment": penalty.raw_segment}),
                    )
                )
        if supersede_ids:
            cursor.execute(
                """UPDATE nfl_play_fact_revisions
                   SET revision_status='superseded', revised_at=NOW()
                   WHERE fact_revision_id = ANY(%s)""",
                (supersede_ids,),
            )
        if fact_values:
            execute_batch(
                cursor,
                """
                INSERT INTO nfl_play_fact_revisions
                    (fact_revision_id, game_id, play_id, revision_number,
                     fact_release_id, source_observation_id, snap_execution,
                     action_validity, description, fact_payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                fact_values,
                page_size=500,
            )
            facts_inserted = len(fact_values)
        if penalty_values:
            execute_batch(
                cursor,
                """
                INSERT INTO nfl_play_penalty_events
                    (fact_revision_id, occurrence, adjudication, penalty_type,
                     team, enforced_yards, enforcement)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (fact_revision_id, occurrence) DO NOTHING
                """,
                penalty_values,
                page_size=500,
            )
            penalties_inserted = len(penalty_values)

        definition = NEUTRAL_SNAP_INTERVAL
        cursor.execute(
            """
            INSERT INTO nfl_context_definitions
                (definition_id, context_key, version, unit, description, definition,
                 freshness_seconds)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (definition_id) DO NOTHING
            """,
            (
                definition.definition_id,
                definition.key,
                definition.version,
                definition.unit,
                definition.description,
                json.dumps(definition.definition),
                definition.freshness_seconds,
            ),
        )
        for context in publication.contexts:
            cursor.execute(
                """
                SELECT snapshot_id FROM nfl_context_snapshots
                WHERE definition_id=%s AND subject_id=%s AND target_id=%s
                  AND fact_release_id=%s
                  AND publication_status='current'
                  AND numerator IS NOT DISTINCT FROM %s
                  AND denominator IS NOT DISTINCT FROM %s
                  AND value IS NOT DISTINCT FROM %s
                LIMIT 1
                """,
                (
                    context.definition_id,
                    context.subject_id,
                    context.target_id,
                    context.fact_release_id,
                    context.numerator,
                    context.denominator,
                    context.value,
                ),
            )
            if cursor.fetchone():
                continue
            cursor.execute(
                """
                INSERT INTO nfl_context_snapshots
                    (snapshot_id, definition_id, subject_type, subject_id,
                     target_id, as_of_at, available_at, measurement_window, numerator, denominator, value,
                     value_state, coverage, uncertainty, estimation,
                     source_snapshot_ids, fact_release_id, payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (snapshot_id) DO NOTHING
                """,
                (
                    context.snapshot_id,
                    context.definition_id,
                    context.subject_type,
                    context.subject_id,
                    context.target_id,
                    context.as_of_at,
                    context.available_at,
                    json.dumps(context.window),
                    context.numerator,
                    context.denominator,
                    context.value,
                    context.state.value,
                    json.dumps(context.coverage),
                    json.dumps(context.uncertainty) if context.uncertainty else None,
                    json.dumps(context.estimation) if context.estimation else None,
                    json.dumps(list(context.source_snapshot_ids)),
                    context.fact_release_id,
                    json.dumps(context.payload),
                ),
            )
            cursor.execute(
                """
                UPDATE nfl_context_snapshots
                SET publication_status='superseded', superseded_by=%s
                WHERE definition_id=%s AND subject_id=%s AND target_id=%s
                  AND snapshot_id<>%s AND publication_status='current'
                """,
                (
                    context.snapshot_id,
                    context.definition_id,
                    context.subject_id,
                    context.target_id,
                    context.snapshot_id,
                ),
            )
            contexts_inserted += 1

        for consumer_id, use_case in (
            ("nfl_pbp_explorer", "game_explanation"),
            ("nfl_postgame_evaluator", "context_replay"),
        ):
            cursor.execute(
                """
                INSERT INTO nfl_context_qualifications
                    (consumer_id, definition_id, use_case, cohort, usage,
                     policy_version, approved)
                VALUES (%s,%s,%s,'all_teams','descriptive',%s,TRUE)
                ON CONFLICT DO NOTHING
                """,
                (consumer_id, definition.definition_id, use_case, POLICY_VERSION),
            )
            cursor.execute(
                """
                INSERT INTO nfl_consumer_policy_pointers
                    (consumer_id, policy_version, activated_by)
                VALUES (%s,%s,'ingest.nfl_context_publish')
                ON CONFLICT (consumer_id) DO UPDATE
                SET policy_version=EXCLUDED.policy_version,
                    activated_at=NOW(), activated_by=EXCLUDED.activated_by
                """,
                (consumer_id, POLICY_VERSION),
            )
    return {
        "factsInserted": facts_inserted,
        "penaltiesInserted": penalties_inserted,
        "contextsPublished": contexts_inserted,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbp", type=Path)
    parser.add_argument("--target-game")
    parser.add_argument("--team", action="append", default=[])
    parser.add_argument("--season-type", default="REG")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    observed_at = datetime.now(timezone.utc)
    publication = prepare_publication(
        args.pbp,
        target_game=args.target_game,
        teams=args.team,
        observed_at=observed_at,
        season_type=args.season_type,
    )
    result: dict[str, Any] = publication.summary()
    result["applied"] = args.apply
    if args.apply:
        db = DatabaseManager(load_config().database_url)
        result["writeResult"] = persist_publication(db, publication)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
