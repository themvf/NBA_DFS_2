"""Import a human-reviewed exact-game NFL inactive list.

Input JSON must contain ``season``, ``week``, ``source_label``,
``source_published_at``, and ``records``. Each record needs ``game_id`` and one
stable player identifier (``player_id`` or ``gsis_id``). Only INACTIVE rows are
accepted in v1; this deliberately cannot infer actives from list omission.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_injuries import persist_injury_observation
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot
from model.nfl_context_engine import stable_digest


def _time(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def prepare_import(db: Any, payload: dict[str, Any], *, reviewed_by: str,
                   reviewed_at: datetime) -> dict[str, Any]:
    if not reviewed_by.strip():
        raise ValueError("reviewed_by is required")
    season = int(payload["season"]); week = int(payload["week"])
    source_label = str(payload.get("source_label") or "").strip()
    published_at = _time(payload["source_published_at"])
    records = payload.get("records")
    if not source_label or not isinstance(records, list) or not records:
        raise ValueError("source_label and at least one record are required")
    prepared = []
    for record in records:
        if str(record.get("status") or "").upper() != "INACTIVE":
            raise ValueError("v1 reviewed imports accept explicit INACTIVE rows only")
        game_id = str(record.get("game_id") or "")
        game = db.execute_one(
            """SELECT g.nflverse_game_id game_id,g.kickoff,
                      home.abbreviation home_team,away.abbreviation away_team
               FROM nfl_season_games g
               JOIN nfl_teams home ON home.team_id=g.home_team_id
               JOIN nfl_teams away ON away.team_id=g.away_team_id
               WHERE g.season=%s AND g.week=%s AND g.nflverse_game_id=%s""",
            (season, week, game_id),
        )
        if not game or published_at >= game["kickoff"] or reviewed_at >= game["kickoff"]:
            raise ValueError(f"{game_id} is missing or the review is not pre-kickoff")
        if record.get("player_id") is not None:
            player = db.execute_one(
                "SELECT id,team_abbrev FROM ff_players WHERE season=%s AND id=%s",
                (season, int(record["player_id"])),
            )
        elif record.get("gsis_id"):
            player = db.execute_one(
                "SELECT id,team_abbrev FROM ff_players WHERE season=%s AND gsis_id=%s",
                (season, str(record["gsis_id"])),
            )
        else:
            raise ValueError("each official row requires player_id or gsis_id")
        if not player or str(player["team_abbrev"]) not in {game["home_team"], game["away_team"]}:
            raise ValueError(f"player identity/team does not match {game_id}")
        prepared.append({
            "playerId": int(player["id"]), "gameId": game_id,
            "kickoff": game["kickoff"].isoformat(), "status": "INACTIVE",
            "report_type": "inactive_list", "source_label": source_label,
            "source_published_at": published_at.isoformat(),
        })
    body = {
        "season": season, "week": week, "sourceLabel": source_label,
        "sourcePublishedAt": published_at.isoformat(), "reviewedBy": reviewed_by,
        "reviewedAt": reviewed_at.isoformat(), "records": prepared,
    }
    return {**body, "payloadDigest": stable_digest(body), "importId": stable_digest({"type": "official-inactives", **body})}


def persist_import(db: Any, prepared: dict[str, Any]) -> dict[str, Any]:
    snapshot_id = persist_source_snapshot(db, SnapshotProvenance(
        source="nfl_official", dataset=f"reviewed-inactives-{prepared['season']}-{prepared['week']}-{prepared['importId'][:12]}",
        season=int(prepared["season"]), week=int(prepared["week"]),
        request_params={"sourceLabel": prepared["sourceLabel"], "reviewedBy": prepared["reviewedBy"]},
        source_published_at=_time(prepared["sourcePublishedAt"]),
        fetched_at=_time(prepared["reviewedAt"]), response_hash=prepared["payloadDigest"],
        row_count=len(prepared["records"]), matched_count=len(prepared["records"]),
        fallback_tier="A", model_eligible=True,
        eligibility_reason="human-reviewed exact-game official inactive list",
    ))
    observations = []
    for record in prepared["records"]:
        result = persist_injury_observation(
            db, player_id=int(record["playerId"]), season=int(prepared["season"]),
            source="nfl_official", source_snapshot_id=snapshot_id,
            row=record, reconcile_current=False,
        )
        observations.append(result["observation_id"])
    db.execute(
        """INSERT INTO nfl_official_inactive_imports
             (import_id,season,week,reviewed_by,reviewed_at,source_label,
              source_snapshot_id,row_count,payload_digest)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(import_id) DO NOTHING""",
        (prepared["importId"], prepared["season"], prepared["week"],
         prepared["reviewedBy"], _time(prepared["reviewedAt"]), prepared["sourceLabel"],
         snapshot_id, len(prepared["records"]), prepared["payloadDigest"]),
    )
    return {"importId": prepared["importId"], "sourceSnapshotId": snapshot_id,
            "observationIds": observations, "rows": len(observations)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--reviewed-by", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    reviewed_at = datetime.now(timezone.utc)
    db = RefreshDatabase(load_config().database_url); failed = False
    try:
        prepared = prepare_import(db, payload, reviewed_by=args.reviewed_by, reviewed_at=reviewed_at)
        result = persist_import(db, prepared) if args.apply else {"validated": True, "importId": prepared["importId"], "rows": len(prepared["records"])}
        print(json.dumps(result, indent=2))
        return 0
    except Exception:
        failed = True
        raise
    finally:
        db.close(error=failed or not args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
