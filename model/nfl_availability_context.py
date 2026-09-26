"""Build immutable structured NFL availability context snapshots.

The builders consume only the already-frozen projection-run inputs.  They do
not query current rosters or injury tables, which prevents a later depth chart
from being projected backward into an earlier decision.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Iterable, Mapping

from model.nfl_context_engine import (
    ContextDefinition,
    ContextMeasurement,
    ContextState,
    stable_digest,
)


PLAYER_GAME_AVAILABILITY = ContextDefinition(
    key="player_game_availability",
    version="v1",
    unit="player_game_state",
    description="Point-in-time canonical game availability and role evidence.",
    definition={
        "states": [
            "ACTIVE_CONFIRMED", "EXPECTED_ACTIVE", "QUESTIONABLE", "DOUBTFUL",
            "OUT_CONFIRMED", "CONFLICT", "STALE", "UNKNOWN",
        ],
        "platformEligibilityIncluded": False,
        "participationProbability": "null_until_calibrated",
    },
    freshness_seconds=72 * 3600,
)

TEAM_QB_STATE = ContextDefinition(
    key="team_qb_state",
    version="v1",
    unit="team_game_state",
    description="Point-in-time quarterback starter and qualified replacement state.",
    definition={
        "starterVolumePreservation": "baseline_hypothesis_only",
        "starterQualityBaseline": "independently_versioned_or_null",
    },
    freshness_seconds=72 * 3600,
)

POLICY_VERSION = "nfl-availability-context-v1"


def expected_role(position: str | None, depth_order: Any) -> str:
    try:
        depth = int(depth_order)
    except (TypeError, ValueError):
        return "UNRESOLVED"
    if depth < 1:
        return "UNRESOLVED"
    position = str(position or "").upper()
    if position == "QB":
        return "QB1" if depth == 1 else "QB2" if depth == 2 else "QB3_PLUS"
    if position == "RB":
        return "RB1" if depth == 1 else "RB_COMMITTEE" if depth <= 3 else "RB_DEPTH"
    if position == "WR":
        return "WR_STARTER" if depth <= 3 else "WR_ROTATION" if depth <= 5 else "WR_DEPTH"
    if position == "TE":
        return "TE1" if depth == 1 else "TE_ROTATION" if depth <= 3 else "TE_DEPTH"
    return "UNRESOLVED"


def _source_ids(decision: Mapping[str, Any]) -> tuple[str, ...]:
    values = {
        str(value)
        for key in ("qualifying_source_snapshot_ids", "display_only_source_snapshot_ids")
        for value in (decision.get(key) or [])
    }
    chosen = decision.get("source_snapshot_id")
    if chosen is not None:
        values.add(str(chosen))
    return tuple(sorted(values))


def _evidence_digest(decision: Mapping[str, Any], role: str, depth_order: Any) -> str:
    return stable_digest({
        "decision": dict(decision),
        "expectedRole": role,
        "depthOrder": depth_order,
    })


def build_availability_contexts(
    projections: Iterable[Mapping[str, Any]],
    decisions: Mapping[str, Mapping[str, Any]],
    *,
    season: int,
    week: int | None,
    as_of_at: datetime,
    available_at: datetime,
    fact_release_id: str,
    resolution_policy_version: str = "player-game-availability-v1",
    starter_quality_baseline_id: str | None = None,
) -> tuple[tuple[ContextMeasurement, ...], dict[str, Any]]:
    """Create player and team-QB contexts without consulting mutable state."""
    players = [dict(row) for row in projections]
    player_contexts: list[ContextMeasurement] = []
    player_payloads: dict[int, dict[str, Any]] = {}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for player in players:
        player_id = int(player["player_id"])
        decision = dict(decisions.get(str(player_id)) or {})
        game_id = str(player.get("game_id") or player.get("event_id") or "")
        if not game_id:
            raise ValueError(f"player {player_id} has no immutable game identifier")
        team = str(player.get("team") or "")
        role = expected_role(player.get("position"), player.get("depth_order"))
        state = str(decision.get("state") or "UNKNOWN")
        qualifying = list(decision.get("qualifying_observation_ids") or [])
        display_only = list(decision.get("display_only_observation_ids") or [])
        freshness = "fresh" if qualifying else "stale" if state == "STALE" else "unresolved"
        conflict = "conflict" if state == "CONFLICT" else "none"
        confidence = (
            "high" if state in {"ACTIVE_CONFIRMED", "OUT_CONFIRMED"}
            else "medium" if state in {"EXPECTED_ACTIVE", "QUESTIONABLE", "DOUBTFUL"}
            else "low"
        )
        payload = {
            "season": season,
            "week": week,
            "game_id": game_id,
            "player_id": player_id,
            "team": team,
            "position": player.get("position"),
            "as_of_at": as_of_at.isoformat(),
            "kickoff": decision.get("kickoff"),
            "resolved_availability_state": state,
            "normalized_status": decision.get("projection_status"),
            "expected_role": role,
            "depth_order": player.get("depth_order"),
            "participation_probability": None,
            "replacement_player_id": None,
            "freshness_state": freshness,
            "conflict_state": conflict,
            "confidence_tier": confidence,
            "observation_ids": sorted(set(qualifying + display_only)),
            "source_snapshot_ids": list(_source_ids(decision)),
            "resolution_policy_version": resolution_policy_version,
            "evidence_digest": _evidence_digest(decision, role, player.get("depth_order")),
            "reason": decision.get("reason"),
        }
        player_payloads[player_id] = payload
        grouped[(game_id, team)].append(player)

    # Replacement is derived solely from the frozen, fresh depth_order carried
    # by each projection row. Missing depth stays unresolved.
    for (_, _), team_players in grouped.items():
        by_position: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for player in team_players:
            by_position[str(player.get("position") or "")].append(player)
        for position_players in by_position.values():
            eligible = sorted(
                (
                    row for row in position_players
                    if player_payloads[int(row["player_id"])]["resolved_availability_state"] != "OUT_CONFIRMED"
                    and row.get("depth_order") is not None
                ),
                key=lambda row: (int(row["depth_order"]), int(row["player_id"])),
            )
            for player in position_players:
                payload = player_payloads[int(player["player_id"])]
                if payload["resolved_availability_state"] != "OUT_CONFIRMED":
                    continue
                depth = player.get("depth_order")
                replacements = [row for row in eligible if depth is None or int(row["depth_order"]) > int(depth)]
                if replacements:
                    payload["replacement_player_id"] = int(replacements[0]["player_id"])

    for player in players:
        player_id = int(player["player_id"])
        payload = player_payloads[player_id]
        decision = decisions.get(str(player_id)) or {}
        sources = _source_ids(decision)
        player_contexts.append(ContextMeasurement(
            subject_type="player",
            subject_id=str(player_id),
            target_id=payload["game_id"],
            definition_id=PLAYER_GAME_AVAILABILITY.definition_id,
            as_of_at=as_of_at,
            available_at=available_at,
            window={"season": season, "week": week, "gameId": payload["game_id"]},
            numerator=None,
            denominator=None,
            value=None,
            state=ContextState.OBSERVED,
            coverage={
                "hasQualifiedObservation": bool(decision.get("qualifying_observation_ids")),
                "hasFreshDepth": player.get("depth_order") is not None,
                "freshnessState": payload["freshness_state"],
                "conflictState": payload["conflict_state"],
            },
            source_snapshot_ids=sources,
            fact_release_id=fact_release_id,
            payload=payload,
        ))

    qb_contexts: list[ContextMeasurement] = []
    player_snapshot_ids = {
        int(context.payload["player_id"]): context.snapshot_id
        for context in player_contexts
    }
    for (game_id, team), team_players in sorted(grouped.items()):
        qbs = sorted(
            (row for row in team_players if str(row.get("position")) == "QB"),
            key=lambda row: (
                row.get("depth_order") is None,
                int(row.get("depth_order") or 999),
                int(row["player_id"]),
            ),
        )
        if not qbs:
            continue
        baseline = next((row for row in qbs if row.get("depth_order") == 1), None)
        baseline_payload = player_payloads[int(baseline["player_id"])] if baseline else None
        active = next((
            row for row in qbs
            if row.get("depth_order") is not None
            and player_payloads[int(row["player_id"])]["resolved_availability_state"] != "OUT_CONFIRMED"
        ), None)
        replacement = (
            active if baseline is not None and active is not None
            and int(active["player_id"]) != int(baseline["player_id"]) else None
        )
        if baseline is None:
            change_state = "UNRESOLVED_DEPTH"
        elif baseline_payload["resolved_availability_state"] == "OUT_CONFIRMED" and replacement:
            change_state = "CONFIRMED_REPLACEMENT"
        elif baseline_payload["resolved_availability_state"] == "OUT_CONFIRMED":
            change_state = "OUT_NO_QUALIFIED_REPLACEMENT"
        else:
            change_state = "NO_CONFIRMED_CHANGE"
        source_ids = tuple(sorted({
            source_id for row in qbs
            for source_id in player_payloads[int(row["player_id"])]["source_snapshot_ids"]
        }))
        payload = {
            "season": season,
            "week": week,
            "game_id": game_id,
            "team": team,
            "as_of_at": as_of_at.isoformat(),
            "expected_starter_player_id": int(active["player_id"]) if active else None,
            "baseline_starter_player_id": int(baseline["player_id"]) if baseline else None,
            "starter_availability_state": (
                baseline_payload["resolved_availability_state"] if baseline_payload else "UNKNOWN"
            ),
            "replacement_player_id": int(replacement["player_id"]) if replacement else None,
            "starter_change_state": change_state,
            "days_since_change": None,
            "starter_quality_baseline_id": starter_quality_baseline_id,
            "depth_evidence": {
                "playerDepth": [
                    {"playerId": int(row["player_id"]), "depthOrder": row.get("depth_order")}
                    for row in qbs
                ],
                "pointInTimeOnly": True,
            },
            "availability_evidence": {
                "playerContextIds": [player_snapshot_ids[int(row["player_id"])] for row in qbs],
                "sourceSnapshotIds": list(source_ids),
            },
            "coverage": {
                "hasQb1": baseline is not None,
                "hasExpectedStarter": active is not None,
                "hasQualityBaseline": starter_quality_baseline_id is not None,
            },
            "confidence": "high" if baseline and active else "low",
        }
        qb_contexts.append(ContextMeasurement(
            subject_type="team",
            subject_id=team,
            target_id=game_id,
            definition_id=TEAM_QB_STATE.definition_id,
            as_of_at=as_of_at,
            available_at=available_at,
            window={"season": season, "week": week, "gameId": game_id},
            numerator=None,
            denominator=None,
            value=None,
            state=ContextState.OBSERVED,
            coverage=payload["coverage"],
            source_snapshot_ids=source_ids,
            fact_release_id=fact_release_id,
            payload=payload,
        ))

    all_contexts = tuple(player_contexts + qb_contexts)
    states = Counter(value.payload.get("resolved_availability_state") for value in player_contexts)
    by_position = Counter(str(value.payload.get("position") or "UNKNOWN") for value in player_contexts)
    sources = Counter(
        str((decisions.get(str(value.payload["player_id"])) or {}).get("source") or "none")
        for value in player_contexts
    )
    freshness = Counter(str(value.payload["freshness_state"]) for value in player_contexts)
    conflict_states = Counter(str(value.payload["conflict_state"]) for value in player_contexts)
    by_game: dict[str, Counter[str]] = defaultdict(Counter)
    by_team: dict[str, Counter[str]] = defaultdict(Counter)
    for value in player_contexts:
        state = str(value.payload["resolved_availability_state"])
        by_game[value.target_id][state] += 1
        by_team[f"{value.target_id}:{value.payload['team']}"][state] += 1
    report = {
        "version": "nfl-availability-context-coverage-v1",
        "season": season,
        "week": week,
        "asOfAt": as_of_at.isoformat(),
        "availableAt": available_at.isoformat(),
        "playerContexts": len(player_contexts),
        "teamQbContexts": len(qb_contexts),
        "games": len({value.target_id for value in player_contexts}),
        "teams": len({(value.target_id, value.payload["team"]) for value in player_contexts}),
        "byState": dict(sorted(states.items())),
        "byPosition": dict(sorted(by_position.items())),
        "byChosenSource": dict(sorted(sources.items())),
        "byFreshness": dict(sorted(freshness.items())),
        "byConflictState": dict(sorted(conflict_states.items())),
        "byGame": {key: dict(sorted(value.items())) for key, value in sorted(by_game.items())},
        "byTeam": {key: dict(sorted(value.items())) for key, value in sorted(by_team.items())},
        "conflicts": states.get("CONFLICT", 0),
        "stale": states.get("STALE", 0),
        "unknown": states.get("UNKNOWN", 0),
        "reportDigest": "",
    }
    report["reportDigest"] = stable_digest({key: value for key, value in report.items() if key != "reportDigest"})
    return all_contexts, report
