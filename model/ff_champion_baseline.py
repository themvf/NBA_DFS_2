"""Freeze a reproducible fantasy-football champion ranking artifact.

The persisted ranking set is authoritative for the model identity exposed by
redraft, Best Ball, draft sessions, and advisors. This utility records the
latest populated STD/HALF/PPR boards for one explicit champion version without
recomputing or mutating any projection.

Usage:
    python -m model.ff_champion_baseline --season 2026
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from config import load_config
from ingest.ff_independent import MODEL_VERSION


SCORING_TYPES = ("STD", "HALF", "PPR")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def build_board_evidence(board: dict[str, Any], rankings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in rankings]
    projection_rows = sorted(
        (
            {
                "playerId": int(row["player_id"]),
                "projectedPoints": row.get("our_projected_points"),
                "projectionLow": row.get("projection_low"),
                "projectionHigh": row.get("projection_high"),
                "expectedGames": row.get("expected_games"),
                "confidence": row.get("confidence"),
            }
            for row in rows
        ),
        key=lambda row: row["playerId"],
    )
    order_rows = sorted(
        (
            {
                "playerId": int(row["player_id"]),
                "ourRank": row.get("our_rank"),
                "positionRank": row.get("position_rank"),
                "tier": row.get("tier"),
            }
            for row in rows
        ),
        key=lambda row: (
            row["ourRank"] is None,
            row["ourRank"] if row["ourRank"] is not None else 10**9,
            row["playerId"],
        ),
    )
    return {
        "scoring": str(board["scoring"]),
        "rankingSetId": int(board["ranking_set_id"]),
        "rankingSetName": str(board["ranking_set_name"]),
        "sourceSnapshotId": int(board["source_snapshot_id"]),
        "sourceResponseHash": str(board["source_response_hash"]),
        "sourceRequestParams": board.get("source_request_params") or {},
        "createdAt": str(board["created_at"]),
        "playerCount": len(rows),
        "projectionDigest": _digest(projection_rows),
        "orderDigest": _digest(order_rows),
    }


def build_manifest(
    *,
    season: int,
    model_version: str,
    boards: list[dict[str, Any]],
    rankings_by_set: dict[int, list[dict[str, Any]]],
    frozen_at: str,
) -> dict[str, Any]:
    evidence = [
        build_board_evidence(board, rankings_by_set[int(board["ranking_set_id"])])
        for board in sorted(boards, key=lambda row: SCORING_TYPES.index(str(row["scoring"])))
    ]
    observed_scoring = tuple(row["scoring"] for row in evidence)
    if observed_scoring != SCORING_TYPES:
        raise ValueError(f"Expected populated boards for {SCORING_TYPES}, found {observed_scoring}")
    if any(row["playerCount"] < 100 for row in evidence):
        raise ValueError("Every frozen champion board must contain at least 100 players")
    stable_contract = {
        "season": season,
        "championModelVersion": model_version,
        "boards": evidence,
    }
    return {
        "schemaVersion": 1,
        "artifactType": "fantasy-football-champion-baseline",
        **stable_contract,
        "frozenAt": frozen_at,
        "combinedDigest": _digest(stable_contract),
        "projectionBehaviorChanged": False,
    }


def verification_model_version(
    explicit_model_version: str | None,
    frozen_manifest: dict[str, Any] | None,
) -> str:
    """Pin verification to the artifact identity unless explicitly overridden."""
    if explicit_model_version:
        return explicit_model_version
    if frozen_manifest:
        frozen_version = frozen_manifest.get("championModelVersion")
        if isinstance(frozen_version, str) and frozen_version.strip():
            return frozen_version
    return MODEL_VERSION


def load_manifest_inputs(database_url: str, season: int, model_version: str) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """WITH candidates AS (
                     SELECT rs.id AS ranking_set_id,rs.name AS ranking_set_name,
                       COALESCE(rs.scoring_profile->>'preset','PPR') AS scoring,
                       rs.source_snapshot_id,rs.created_at,
                       COALESCE(NULLIF(rs.import_summary->>'model_version',''),
                         NULLIF(ss.request_params->>'model_version','')) AS model_version,
                       ss.response_hash AS source_response_hash,
                       ss.request_params AS source_request_params,
                       COUNT(pr.id)::int AS player_count
                     FROM ff_ranking_sets rs
                     JOIN ff_source_snapshots ss ON ss.id=rs.source_snapshot_id
                     LEFT JOIN ff_player_rankings pr ON pr.ranking_set_id=rs.id
                     WHERE rs.season=%s
                     GROUP BY rs.id,ss.response_hash,ss.request_params
                   ), latest AS (
                     SELECT DISTINCT ON (scoring) * FROM candidates
                     WHERE model_version=%s AND player_count>=100
                     ORDER BY scoring,created_at DESC,ranking_set_id DESC
                   )
                   SELECT * FROM latest ORDER BY
                     CASE scoring WHEN 'STD' THEN 1 WHEN 'HALF' THEN 2 WHEN 'PPR' THEN 3 ELSE 4 END""",
                (season, model_version),
            )
            boards = [dict(row) for row in cursor.fetchall()]
            rankings_by_set: dict[int, list[dict[str, Any]]] = {}
            for board in boards:
                ranking_set_id = int(board["ranking_set_id"])
                cursor.execute(
                    """SELECT player_id,our_rank,position_rank,tier,our_projected_points,
                         projection_low,projection_high,expected_games,confidence
                       FROM ff_player_rankings WHERE ranking_set_id=%s
                       ORDER BY player_id""",
                    (ranking_set_id,),
                )
                rankings_by_set[ranking_set_id] = [dict(row) for row in cursor.fetchall()]
    return boards, rankings_by_set


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--model-version")
    parser.add_argument("--verify", type=Path, help="Verify the live boards against a frozen manifest")
    args = parser.parse_args()
    config = load_config()
    if not config.database_url:
        raise RuntimeError("DATABASE_URL is required to freeze the champion baseline")
    frozen = json.loads(args.verify.read_text(encoding="utf-8")) if args.verify else None
    model_version = verification_model_version(args.model_version, frozen)
    boards, rankings_by_set = load_manifest_inputs(config.database_url, args.season, model_version)
    manifest = build_manifest(
        season=args.season,
        model_version=model_version,
        boards=boards,
        rankings_by_set=rankings_by_set,
        frozen_at=datetime.now(timezone.utc).isoformat(),
    )
    if args.verify:
        assert frozen is not None
        expected = {
            "season": frozen.get("season"),
            "championModelVersion": frozen.get("championModelVersion"),
            "combinedDigest": frozen.get("combinedDigest"),
        }
        observed = {
            "season": manifest["season"],
            "championModelVersion": manifest["championModelVersion"],
            "combinedDigest": manifest["combinedDigest"],
        }
        if observed != expected:
            raise RuntimeError(f"Champion baseline mismatch: expected {expected}, observed {observed}")
        print(json.dumps({"status": "verified", **observed}, indent=2))
        return 0
    print(json.dumps(manifest, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
