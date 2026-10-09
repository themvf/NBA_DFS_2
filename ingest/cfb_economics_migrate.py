"""Append canonical CFB economic resolutions with pinned legacy evidence."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from uuid import UUID, uuid5

from config import load_config
from model.cfb_context_economics import resolve_legacy_economics


NAMESPACE = UUID("102c3604-398a-48a0-954a-712a5966cdda")


def _id(kind: str, value: object):
    return uuid5(NAMESPACE, f"{kind}:{value}")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def migrate(database_url: str) -> dict:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor, register_uuid

    register_uuid()
    inserted = unchanged = corrected = 0
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-economics-resolver-v1",))
        cursor.execute("""SELECT id,alert_type,signal_version,origin,game_date,matchup_id,created_at,outcome,
          pnl_units,clv_pp,grading_json,details_json,trigger_history_id FROM line_alerts WHERE sport='cfb' ORDER BY id""")
        alerts = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""SELECT g.* FROM alert_grades g JOIN line_alerts a ON a.id=g.alert_id
          WHERE g.is_current AND a.sport='cfb' ORDER BY g.alert_id,g.id""")
        grades: dict[int, list[dict]] = {}
        for grade in cursor.fetchall():
            grades.setdefault(int(grade["alert_id"]), []).append(dict(grade))
        for alert in alerts:
            current_grades = grades.get(int(alert["id"]), [])
            resolution = resolve_legacy_economics(alert, current_grades)
            evidence = {"alert": alert, "current_grades": current_grades,
                        "migration_limitation": "unique current grade at migration cutoff; not historical reconstruction"}
            evidence_bytes = _canonical(evidence)
            evidence_digest = sha256(evidence_bytes).hexdigest()
            artifact_id = _id("grade-evidence", evidence_digest)
            manifest_digest = sha256(_canonical({"kind": "grade-evidence", "artifact": evidence_digest})).hexdigest()
            manifest_id = _id("grade-manifest", manifest_digest)
            resolution_payload = resolution.to_dict()
            selected_grade_json = (current_grades[0].get("grading_json") or {}) if len(current_grades) == 1 else {}
            metrics = {"reason_codes": resolution_payload["reason_codes"], "pnl_source": resolution_payload["pnl_source"],
                       "legacy_grade_ids": [grade["id"] for grade in current_grades],
                       "probability_clv_pp": alert.get("clv_pp"), "line_clv": next(
                           (grade.get("line_clv") for grade in current_grades if grade.get("line_clv") is not None), None),
                       "decimal_price_ratio_pct": selected_grade_json.get("price_clv_pct"),
                       "close_decimal": selected_grade_json.get("close_decimal"),
                       "close_history_id": selected_grade_json.get("close_history_id")}
            idempotency_key = sha256(_canonical({"alert_id": alert["id"], "resolution": resolution_payload,
                                                  "metrics": metrics, "evidence_digest": evidence_digest})).hexdigest()
            resolution_id = _id("economic-resolution", idempotency_key)
            cursor.execute("SELECT resolution_id FROM cfb_economic_resolutions WHERE idempotency_key=%s", (idempotency_key,))
            if cursor.fetchone():
                unchanged += 1
                continue
            cursor.execute("""SELECT r.resolution_id FROM cfb_economic_resolutions r
              WHERE r.alert_id=%s AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)
              ORDER BY r.resolution_id""", (alert["id"],))
            heads = cursor.fetchall()
            supersedes = heads[0]["resolution_id"] if len(heads) == 1 else None
            if heads:
                corrected += 1
            cursor.execute("""INSERT INTO cfb_engine_artifacts
              (artifact_id,kind,digest,uri,representation,byte_count,metadata)
              VALUES (%s,'legacy-cfb-grade-evidence',%s,NULL,'normalized',%s,%s) ON CONFLICT DO NOTHING""",
              (artifact_id, evidence_digest, len(evidence_bytes),
               Json(evidence, dumps=lambda value: json.dumps(value, default=str))))
            cursor.execute("""INSERT INTO cfb_context_manifests
              (manifest_id,kind,scope_key,as_of_at,manifest_digest,policy_id)
              VALUES (%s,'inputs',%s,%s,%s,NULL) ON CONFLICT DO NOTHING""",
              (manifest_id, f"cfb:alert:{alert['id']}", alert["created_at"], manifest_digest))
            cursor.execute("""INSERT INTO cfb_context_manifest_items
              (manifest_id,slot,ordinal,artifact_id) VALUES (%s,'grade_evidence',0,%s) ON CONFLICT DO NOTHING""",
              (manifest_id, artifact_id))
            clv_state = "conflict" if resolution.result_state == "conflict" else "available" if (
                metrics["probability_clv_pp"] is not None or metrics["line_clv"] is not None) else "missing"
            cursor.execute("""INSERT INTO cfb_economic_resolutions
              (resolution_id,alert_id,resolver_version,grade_evidence_manifest_id,result_state,outcome,entry_decimal,
               stake_units,pnl_units,roi_stake_units,clv_state,metrics,supersedes_resolution_id,idempotency_key)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (resolution_id, alert["id"], resolution.resolver_version, manifest_id, resolution.result_state,
               resolution.outcome, resolution.entry_decimal, resolution.stake_units, resolution.pnl_units,
               resolution.roi_stake_units, clv_state, Json(metrics), supersedes, idempotency_key))
            inserted += 1
    return {"inserted": inserted, "unchanged": unchanged, "corrected": corrected, "alerts": len(alerts)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(json.dumps(migrate(load_config().database_url or ""), indent=2))


if __name__ == "__main__":
    main()
