"""Publish the frozen CFBD offensive-drive-volume context for upcoming games."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import UUID, uuid5

from config import load_config
from model.cfb_context_features import offensive_drive_volume


NAMESPACE = UUID("5f9c71b8-f378-445a-890c-e68f5a855358")
BOOTSTRAP_NAMESPACE = UUID("9fa3a542-a6a6-4fd6-b759-82126722e5e0")
DEFINITION_CONFIG = {
    "definition_id": "cfb_offensive_drive_volume", "definition_version": 1,
    "history_window_games": 4, "population": "completed FBS-v-FBS",
    "drive_identity": "distinct cfbd_drive_id where subject is offense",
    "missingness": "missing at zero eligible games; partial below four; no imputation",
    "availability": "drive.ingested_at <= snapshot.as_of_at",
}


def _id(kind: str, value: object):
    return uuid5(NAMESPACE, f"{kind}:{value}")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def publish(database_url: str, *, apply: bool, as_of: datetime | None = None, max_days: int = 14) -> dict:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor, execute_values, register_uuid

    register_uuid()
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-drive-context-v1",))
        if as_of is None:
            cursor.execute("SELECT date_trunc('minute',clock_timestamp()) AS now")
            as_of = cursor.fetchone()["now"]
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        cursor.execute("""SELECT id,home_team_id,away_team_id,commence_time FROM cfb_matchups
          WHERE completed=false AND commence_time>%s AND commence_time<=%s+(%s||' days')::interval
          ORDER BY commence_time,id""", (as_of, as_of, max_days))
        targets = [dict(row) for row in cursor.fetchall()]
        team_ids = sorted({int(row[key]) for row in targets for key in ("home_team_id", "away_team_id")})
        if not team_ids:
            return {"mode": "applied" if apply else "plan", "as_of": as_of.isoformat(), "targets": 0, "snapshots": 0}
        cursor.execute("""SELECT d.cfbd_drive_id,d.game_id,d.offense_team_id,d.ingested_at,d.source_payload_hash,
          m.commence_time,m.completed,ht.classification AS home_classification,at.classification AS away_classification
          FROM cfb_drives d JOIN cfb_matchups m ON m.id=d.game_id
          JOIN cfb_teams ht ON ht.team_id=m.home_team_id JOIN cfb_teams at ON at.team_id=m.away_team_id
          WHERE d.offense_team_id=ANY(%s) AND m.commence_time<%s ORDER BY d.offense_team_id,m.commence_time,d.cfbd_drive_id""",
          (team_ids, as_of))
        by_team: dict[int, list[dict]] = defaultdict(list)
        for row in cursor.fetchall():
            by_team[int(row["offense_team_id"])].append(dict(row))
        planned = []
        for target in targets:
            for team_key in ("home_team_id", "away_team_id"):
                team_id = int(target[team_key])
                result = offensive_drive_volume(by_team.get(team_id, ()), team_id=team_id,
                                                  target_event_id=int(target["id"]), as_of_at=as_of)
                selected_games = set(result["included_game_ids"])
                selected_drives = [row for row in by_team.get(team_id, ())
                                   if int(row["game_id"]) in selected_games and row["ingested_at"] <= as_of]
                identity = {"definition": ["cfb_offensive_drive_volume", 1], "subject_key": result["subject_key"],
                            "target_event_key": result["target_event_key"], "as_of_at": result["as_of_at"],
                            "drive_ids": sorted(int(row["cfbd_drive_id"]) for row in selected_drives)}
                result["input_digest"] = sha256(_canonical(identity)).hexdigest()
                result["scenario_key"] = "main"
                planned.append((result, selected_drives))
        if not apply:
            connection.rollback()
            return {"mode": "plan", "as_of": as_of.isoformat(), "targets": len(targets),
                    "snapshots": len(planned), "complete": sum(row[0]["coverage_state"] == "complete" for row in planned),
                    "partial": sum(row[0]["coverage_state"] == "partial" for row in planned),
                    "missing": sum(row[0]["coverage_state"] == "missing" for row in planned),
                    "distinct_input_drives": len({drive["cfbd_drive_id"] for _, drives in planned for drive in drives})}

        policy_id = uuid5(BOOTSTRAP_NAMESPACE, "evidence-policy:collegefootballdata:1")
        projection_payload = {"fields": ["cfbd_drive_id", "game_id", "offense_team_id", "ingested_at",
                                                 "source_payload_hash", "game.commence_time", "team.classification"],
                              "definition_support": "cfb_offensive_drive_volume:1"}
        projection_digest = sha256(_canonical(projection_payload)).hexdigest()
        projection_artifact = _id("projection-schema", projection_digest)
        config_digest = sha256(_canonical(DEFINITION_CONFIG)).hexdigest()
        config_artifact = _id("definition-config", config_digest)
        cursor.execute("""INSERT INTO cfb_engine_artifacts
          (artifact_id,kind,digest,representation,byte_count,metadata,evidence_policy_id)
          VALUES (%s,'cfbd-drive-volume-source-projection',%s,'schema',%s,%s,%s),
                 (%s,'cfb-drive-volume-definition-config',%s,'configuration',%s,%s,NULL)
          ON CONFLICT DO NOTHING""",
          (projection_artifact, projection_digest, len(_canonical(projection_payload)), Json(projection_payload), policy_id,
           config_artifact, config_digest, len(_canonical(DEFINITION_CONFIG)), Json(DEFINITION_CONFIG)))
        unique_drives = {int(drive["cfbd_drive_id"]): drive for _, drives in planned for drive in drives}
        artifact_rows, source_rows, source_ids = [], [], {}
        for drive_id, drive in unique_drives.items():
            projection = {key: drive.get(key) for key in ("cfbd_drive_id", "game_id", "offense_team_id", "ingested_at",
                                                               "source_payload_hash", "commence_time", "home_classification", "away_classification")}
            digest = sha256(_canonical(projection)).hexdigest()
            artifact_id = _id("drive-artifact", digest)
            source_id = _id("drive-source", f"{drive_id}:{digest}")
            source_ids[drive_id] = source_id
            artifact_rows.append((artifact_id, "legacy-normalized-cfbd-drive", digest, None, "normalized",
                                  len(_canonical(projection)), Json(projection, dumps=lambda value: json.dumps(value, default=str)), policy_id))
            source_rows.append((source_id, "collegefootballdata", str(drive_id), digest, artifact_id,
                                "legacy_unverified", drive["commence_time"], None, drive["ingested_at"], "historical", projection_artifact))
        if artifact_rows:
            execute_values(cursor, """INSERT INTO cfb_engine_artifacts
              (artifact_id,kind,digest,uri,representation,byte_count,metadata,evidence_policy_id) VALUES %s
              ON CONFLICT DO NOTHING""", artifact_rows, page_size=500)
            execute_values(cursor, """INSERT INTO cfb_engine_sources
              (source_id,provider,provider_record_key,revision_key,artifact_id,representation,event_at,published_at,observed_at,origin,projection_schema_id)
              VALUES %s ON CONFLICT(provider,provider_record_key,revision_key) DO NOTHING""", source_rows, page_size=500)
        manifests, snapshots, items = [], [], []
        for result, drives in planned:
            key = sha256(_canonical({"definition": [result["definition_id"], result["definition_version"]],
                                     "subject": result["subject_key"], "target": result["target_event_key"],
                                     "as_of": result["as_of_at"], "input_digest": result["input_digest"]})).hexdigest()
            manifest_digest = sha256(_canonical({"kind": "drive-volume-inputs", "key": key,
                                                  "sources": sorted(str(source_ids[int(row["cfbd_drive_id"])]) for row in drives)})).hexdigest()
            manifest_id, snapshot_id = _id("manifest", manifest_digest), _id("snapshot", key)
            manifests.append((manifest_id, "inputs", f"{result['subject_key']}:{result['target_event_key']}", as_of,
                              manifest_digest, policy_id))
            snapshots.append((snapshot_id, result["definition_id"], result["definition_version"], result["subject_key"],
                              result["target_event_key"], as_of, None, as_of, Json(result), result["scalar_value"],
                              result["coverage_state"], "observed", result["availability_basis"], "prospective",
                              manifest_id, config_artifact, "main", key))
            items.extend((manifest_id, "drive_source", index, source_ids[int(row["cfbd_drive_id"])], None, None, None, None, None, None)
                         for index, row in enumerate(sorted(drives, key=lambda item: int(item["cfbd_drive_id"]))))
        execute_values(cursor, """INSERT INTO cfb_context_manifests(manifest_id,kind,scope_key,as_of_at,manifest_digest,evidence_policy_id)
          VALUES %s ON CONFLICT(manifest_digest) DO NOTHING""", manifests, page_size=500)
        execute_values(cursor, """INSERT INTO cfb_context_snapshots
          (snapshot_id,definition_id,definition_version,subject_key,target_event_key,as_of_at,window_start,window_end,payload,
           scalar_value,coverage_state,measurement_kind,availability_basis,origin,input_manifest_id,configuration_artifact_id,scenario_key,idempotency_key)
          VALUES %s ON CONFLICT(idempotency_key) DO NOTHING""", snapshots, page_size=500)
        if items:
            execute_values(cursor, """INSERT INTO cfb_context_manifest_items
              (manifest_id,slot,ordinal,source_id,capture_id,quote_id,snapshot_id,artifact_id,child_manifest_id,economic_resolution_id)
              VALUES %s ON CONFLICT DO NOTHING""", items, page_size=1000)
    return {"mode": "applied", "as_of": as_of.isoformat(), "targets": len(targets), "snapshots": len(planned),
            "sources": len(unique_drives), "manifest_items": len(items)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--max-days", type=int, default=14)
    args = parser.parse_args()
    print(json.dumps(publish(load_config().database_url or "", apply=args.apply, as_of=args.as_of,
                             max_days=args.max_days), indent=2))


if __name__ == "__main__":
    main()
