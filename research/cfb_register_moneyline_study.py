"""Persist the frozen CFB moneyline study registration from its audit artifact."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
from uuid import UUID, uuid5

from config import PROJECT_DIR, load_config


NAMESPACE = UUID("dd4a431a-4171-4a82-b664-4ff43f383bf5")
BOOTSTRAP_NAMESPACE = UUID("9fa3a542-a6a6-4fd6-b759-82126722e5e0")


def _id(kind: str, value: object):
    return uuid5(NAMESPACE, f"{kind}:{value}")


def register(database_url: str, audit_path: Path) -> dict:
    import psycopg2
    from psycopg2.extras import Json, register_uuid

    register_uuid()
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    config = audit["study_registration"]
    digest = config["configuration_digest"]
    encoded = json.dumps(config, indent=2, sort_keys=True).encode()
    computed = sha256(json.dumps({key: value for key, value in config.items() if key != "configuration_digest"},
                                  sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
    if computed != digest:
        raise ValueError("study configuration digest does not match the audit artifact")
    frozen_path = PROJECT_DIR / "artifacts" / f"cfb_moneyline_study_{digest}.json"
    if frozen_path.exists() and frozen_path.read_bytes() != encoded:
        raise ValueError("frozen study artifact path contains different bytes")
    frozen_path.write_bytes(encoded)
    study_version = int(config["study_version"])
    artifact_id = _id("study-config", digest)
    detector_digest = sha256((PROJECT_DIR / "model" / "line_alerts.py").read_bytes()).hexdigest()
    detector_artifact_id = _id("detector-code", detector_digest)
    study_id = _id("study", config["study_id"])
    payload_schema = uuid5(BOOTSTRAP_NAMESPACE, "artifact:cfb-context-payload-schema-v1")
    shadow_policy = uuid5(BOOTSTRAP_NAMESPACE, "consumer-policy:cfb-shadow-study:1")
    with psycopg2.connect(database_url) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-moneyline-study-v1",))
        cursor.execute("""INSERT INTO cfb_engine_artifacts
          (artifact_id,kind,digest,uri,representation,byte_count,metadata)
          VALUES (%s,'cfb-moneyline-study-configuration',%s,%s,'configuration',%s,%s)
          ON CONFLICT DO NOTHING""", (artifact_id, digest, str(frozen_path), len(encoded),
                                      Json({"study_id": config["study_id"], "study_version": study_version})))
        cursor.execute("""INSERT INTO cfb_engine_artifacts
          (artifact_id,kind,digest,uri,representation,byte_count,metadata)
          VALUES (%s,'cfb-moneyline-detector-code',%s,%s,'code',%s,%s)
          ON CONFLICT DO NOTHING""", (detector_artifact_id, detector_digest, str(PROJECT_DIR / "model" / "line_alerts.py"),
                                      (PROJECT_DIR / "model" / "line_alerts.py").stat().st_size,
                                      Json({"candidate_versions": config["candidate_versions"]})))
        cursor.execute("""INSERT INTO cfb_engine_studies
          (study_id,study_version,configuration_artifact_id,configuration_digest,cohort_schema_id,cohort_rules,
           primary_metric,primary_unit,clustering_method,multiple_testing_family,frozen_at,minimum_effect,minimum_clusters,
           power_precision_plan,comparison_plan,execution_plan,health_floors)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
          (study_id, study_version, artifact_id, digest, payload_schema,
           Json({"population": config["population"], "segments": config["segments"],
                 "supersedes_study_version": config.get("supersedes_study_version"),
                 "candidate_definitions": config["candidate_definitions"],
                 "window_decision_rule": config["window_decision_rule"],
                 "overall_qualification_rule": config["overall_qualification_rule"]}),
           config["primary_metric"], config["primary_unit"], config["clustering_method"], config["multiple_testing_family"],
           config["frozen_at"], config["minimum_effect"], config["minimum_independent_game_dates"],
           Json({"precision_plan": config["precision_plan"]}), Json({"plan": config["comparison_plan"]}),
           Json({"paper_decision_policy": config["paper_decision_policy"], "secondary_metrics": config["secondary_metrics"],
                 "review_schedule": config["review_schedule"], "multiplicity_application": config["multiplicity_application"]}),
           Json(config["health_floors"])))
        for window in config["windows"]:
            cursor.execute("""INSERT INTO cfb_engine_study_windows
              (study_id,study_version,window_key,purpose,start_at,end_at) VALUES (%s,%s,%s,%s,%s,%s)
              ON CONFLICT DO NOTHING""", (study_id, study_version, window["window_key"], window["purpose"], window["start_at"], window["end_at"]))
        cursor.execute("""INSERT INTO cfb_engine_study_dependencies
          (study_id,study_version,role,ordinal,artifact_id) VALUES (%s,%s,'detector_code',0,%s)
          ON CONFLICT DO NOTHING""", (study_id, study_version, detector_artifact_id))
        cursor.execute("""INSERT INTO cfb_engine_study_dependencies
          (study_id,study_version,role,ordinal,policy_id) VALUES (%s,%s,'consumer_policy',0,%s)
          ON CONFLICT DO NOTHING""", (study_id, study_version, shadow_policy))
    return {"study_id": str(study_id), "study_version": study_version, "configuration_digest": digest,
            "artifact": str(frozen_path), "windows": len(config["windows"]), "consumer_permission": "decision-denied"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, default=PROJECT_DIR / "artifacts" / "cfb_moneyline_audit_rev3.json")
    args = parser.parse_args()
    print(json.dumps(register(load_config().database_url or "", args.audit), indent=2))


if __name__ == "__main__":
    main()
