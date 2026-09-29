"""Persist Phase 2 NFL availability contexts through the shared context engine."""
from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable, Mapping

from model.nfl_availability_context import (
    PLAYER_GAME_AVAILABILITY,
    POLICY_VERSION,
    TEAM_QB_STATE,
    build_availability_contexts,
)
from model.nfl_context_engine import ContextMeasurement, stable_digest
from model.nfl_game_availability import game_has_started


CONSUMERS = (
    ("nfl_availability_vercel", "availability_display"),
    ("nfl_availability_dfs", "projection_audit"),
    ("nfl_availability_market", "market_research"),
    ("nfl_availability_props", "prop_research"),
)


def persist_availability_contexts(
    cursor: Any,
    *,
    run_id: str,
    projections: Iterable[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    available_at: datetime,
) -> dict[str, Any]:
    as_of_at = datetime.fromisoformat(str(manifest["as_of_at"]))
    # A game that kicked off at or before this run's decision time keeps its
    # pregame context as the current row. Publishing for it would supersede
    # that row (the supersession below only requires an older as_of_at), which
    # is how every week-3 context became a post-kickoff UNKNOWN on 2026-09-29.
    # Its game id is also left out of the withdrawal cohort below, so nothing
    # in a started game is touched.
    rows = list(projections)
    started_games = sorted({
        _game_id(row) for row in rows if game_has_started(row.get("commence_time"), as_of_at)
    })
    projections = [row for row in rows if _game_id(row) not in started_games]
    all_decisions = manifest.get("availability_decisions") or {}
    decisions = {
        str(row["player_id"]): all_decisions[str(row["player_id"])]
        for row in projections if str(row.get("player_id")) in all_decisions
    }
    if not projections:
        return {
            "releaseId": None, "contextsPublished": 0, "snapshotCount": 0,
            "snapshotManifestDigest": stable_digest([]),
            "definitions": [PLAYER_GAME_AVAILABILITY.definition_id, TEAM_QB_STATE.definition_id],
            "coverage": {}, "policyVersion": POLICY_VERSION,
            "startedGamesSkipped": started_games,
        }
    source_observation_ids = sorted({
        str(observation_id)
        for decision in decisions.values()
        for key in ("qualifying_observation_ids", "display_only_observation_ids")
        for observation_id in (decision.get(key) or [])
    })
    release_payload = {
        "runId": run_id,
        "decisionDigest": stable_digest(decisions),
        "modelVersion": manifest.get("model_version"),
        "asOfAt": manifest.get("as_of_at"),
    }
    release_id = stable_digest({"dataset": "nfl_availability", **release_payload})
    contexts, coverage = build_availability_contexts(
        projections,
        decisions,
        season=int(manifest["season"]),
        week=manifest.get("week"),
        as_of_at=as_of_at,
        available_at=available_at,
        fact_release_id=release_id,
    )

    cursor.execute(
        """INSERT INTO nfl_fact_releases
             (release_id,dataset_key,fact_schema_version,source_observation_ids,
              payload_digest,published_at)
           VALUES (%s,'nfl_availability','nfl-availability-context-v1',%s::jsonb,%s,%s)
           ON CONFLICT (release_id) DO NOTHING""",
        (release_id, json.dumps(source_observation_ids), stable_digest(release_payload), available_at),
    )
    for definition in (PLAYER_GAME_AVAILABILITY, TEAM_QB_STATE):
        cursor.execute(
            """INSERT INTO nfl_context_definitions
                 (definition_id,context_key,version,unit,description,definition,freshness_seconds)
               VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s)
               ON CONFLICT (definition_id) DO NOTHING""",
            (
                definition.definition_id, definition.key, definition.version,
                definition.unit, definition.description, json.dumps(definition.definition),
                definition.freshness_seconds,
            ),
        )

    inserted = 0
    for context in contexts:
        cursor.execute(
            """INSERT INTO nfl_context_snapshots
                 (snapshot_id,definition_id,subject_type,subject_id,target_id,
                  as_of_at,available_at,measurement_window,numerator,denominator,value,
                  value_state,coverage,uncertainty,estimation,source_snapshot_ids,
                  fact_release_id,payload)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,
                       %s::jsonb,%s::jsonb,%s::jsonb,%s,%s::jsonb)
               ON CONFLICT (snapshot_id) DO NOTHING""",
            (
                context.snapshot_id, context.definition_id, context.subject_type,
                context.subject_id, context.target_id, context.as_of_at,
                context.available_at, json.dumps(context.window), context.numerator,
                context.denominator, context.value, context.state.value,
                json.dumps(context.coverage),
                json.dumps(context.uncertainty) if context.uncertainty else None,
                json.dumps(context.estimation) if context.estimation else None,
                json.dumps(list(context.source_snapshot_ids)), context.fact_release_id,
                json.dumps(context.payload),
            ),
        )
        inserted += int(cursor.rowcount > 0)
        cursor.execute(
            """UPDATE nfl_context_snapshots
               SET publication_status='superseded', superseded_by=%s
               WHERE definition_id=%s AND subject_id=%s AND target_id=%s
                 AND snapshot_id<>%s AND publication_status='current'
                 AND as_of_at<=%s""",
            (
                context.snapshot_id, context.definition_id, context.subject_id,
                context.target_id, context.snapshot_id, context.as_of_at,
            ),
        )

    # A correction can remove a player from a game (trade, roster cleanup,
    # identity correction). Same-subject supersession alone would leave that
    # obsolete player-game snapshot marked current forever. Withdraw every
    # older current row in the published game cohort that is absent from this
    # complete release.
    for definition in (PLAYER_GAME_AVAILABILITY, TEAM_QB_STATE):
        definition_contexts = [value for value in contexts if value.definition_id == definition.definition_id]
        target_ids = sorted({value.target_id for value in definition_contexts})
        current_ids = sorted(value.snapshot_id for value in definition_contexts)
        if target_ids and current_ids:
            cursor.execute(
                """UPDATE nfl_context_snapshots
                   SET publication_status='withdrawn', superseded_by=NULL
                   WHERE definition_id=%s AND target_id=ANY(%s)
                     AND publication_status='current' AND NOT (snapshot_id=ANY(%s))""",
                (definition.definition_id, target_ids, current_ids),
            )

    for consumer_id, use_case in CONSUMERS:
        for definition in (PLAYER_GAME_AVAILABILITY, TEAM_QB_STATE):
            cursor.execute(
                """INSERT INTO nfl_context_qualifications
                     (consumer_id,definition_id,use_case,cohort,usage,
                      policy_version,approved,max_age_seconds)
                   VALUES (%s,%s,%s,'all','descriptive',%s,TRUE,%s)
                   ON CONFLICT DO NOTHING""",
                (
                    consumer_id, definition.definition_id, use_case,
                    POLICY_VERSION, definition.freshness_seconds,
                ),
            )
        cursor.execute(
            """INSERT INTO nfl_consumer_policy_pointers
                 (consumer_id,policy_version,activated_by)
               VALUES (%s,%s,'ingest.nfl_availability_context_publish')
               ON CONFLICT (consumer_id) DO UPDATE
               SET policy_version=EXCLUDED.policy_version,
                   activated_at=NOW(),activated_by=EXCLUDED.activated_by""",
            (consumer_id, POLICY_VERSION),
        )

    return {
        "releaseId": release_id,
        "contextsPublished": inserted,
        "snapshotCount": len(contexts),
        "snapshotManifestDigest": stable_digest(
            sorted(context.snapshot_id for context in contexts)
        ),
        "definitions": [
            PLAYER_GAME_AVAILABILITY.definition_id,
            TEAM_QB_STATE.definition_id,
        ],
        "coverage": coverage,
        "policyVersion": POLICY_VERSION,
        "startedGamesSkipped": started_games,
    }


def _game_id(row: Mapping[str, Any]) -> str:
    return str(row.get("game_id") or row.get("event_id") or "")
