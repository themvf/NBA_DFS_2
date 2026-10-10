"""Run non-destructive revision-3 CFB engine acceptance checks."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from config import PROJECT_DIR, load_config


def _expect_failure(cursor, name: str, statement, checks: dict) -> None:
    cursor.execute("SAVEPOINT acceptance_case")
    try:
        statement()
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as exc:
        cursor.execute("ROLLBACK TO SAVEPOINT acceptance_case")
        checks[name] = {"status": "pass", "observed": exc.__class__.__name__}
    else:
        cursor.execute("ROLLBACK TO SAVEPOINT acceptance_case")
        checks[name] = {"status": "fail", "observed": "invalid operation was accepted"}


def run(database_url: str) -> dict:
    import psycopg2
    from psycopg2.extras import RealDictCursor, register_uuid

    register_uuid()
    checks: dict = {}
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
        cursor.execute("""SELECT entity_type,count(*) n FROM cfb_engine_subjects GROUP BY entity_type""")
        subjects = {row["entity_type"]: row["n"] for row in cursor.fetchall()}
        cursor.execute("SELECT count(*) n FROM cfb_engine_events")
        events = cursor.fetchone()["n"]
        cursor.execute("SELECT count(*) n FROM cfb_engine_teams")
        teams = cursor.fetchone()["n"]
        checks["typed_identity_bridges"] = {"status": "pass" if subjects == {"event": events, "team": teams} else "fail",
                                             "subjects": subjects, "events": events, "teams": teams}
        _expect_failure(cursor, "unbridged_subject_rejected", lambda: cursor.execute(
            "INSERT INTO cfb_engine_subjects(subject_key,namespace,entity_type) VALUES (%s,'cfb','event')",
            (f"cfb:event:acceptance-{uuid4()}",)), checks)

        cursor.execute("""SELECT count(*) n FROM (SELECT capture_id,count(DISTINCT bookmaker_updated_at) updates
          FROM cfb_engine_quote_observations GROUP BY capture_id HAVING count(DISTINCT bookmaker_updated_at)>1) x""")
        differing = cursor.fetchone()["n"]
        checks["per_book_timestamps_preserved"] = {"status": "pass" if differing > 0 else "fail", "captures": differing}

        cursor.execute("""SELECT count(*) n FROM cfb_context_snapshots s
          JOIN cfb_engine_events e ON e.event_key=s.target_event_key JOIN cfb_matchups m ON m.id=e.matchup_id
          WHERE s.origin='prospective' AND s.as_of_at>=m.commence_time""")
        after_kickoff = cursor.fetchone()["n"]
        cursor.execute("""SELECT count(*) n FROM cfb_context_snapshots s
          JOIN cfb_context_manifest_items i ON i.manifest_id=s.input_manifest_id
          JOIN cfb_engine_sources src ON src.source_id=i.source_id
          WHERE s.origin='prospective' AND src.observed_at>s.as_of_at""")
        late_sources = cursor.fetchone()["n"]
        checks["prospective_temporal_boundaries"] = {
            "status": "pass" if after_kickoff == 0 and late_sources == 0 else "fail",
            "after_kickoff": after_kickoff, "sources_observed_after_snapshot": late_sources,
        }

        cursor.execute("""SELECT count(*) n FROM cfb_economic_resolutions r
          WHERE NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)""")
        economic_heads = cursor.fetchone()["n"]
        cursor.execute("""SELECT count(*) n FROM cfb_economic_resolutions r WHERE r.result_state='conflict'
          AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)""")
        economic_conflicts = cursor.fetchone()["n"]
        checks["canonical_economic_heads"] = {"status": "pass" if economic_heads > 0 and economic_conflicts == 0 else "fail",
                                               "heads": economic_heads, "conflicts": economic_conflicts}

        cursor.execute("SELECT study_id,study_version FROM cfb_engine_studies LIMIT 1")
        study = cursor.fetchone()
        _expect_failure(cursor, "frozen_study_update_rejected", lambda: cursor.execute(
            "UPDATE cfb_engine_studies SET minimum_effect=minimum_effect WHERE study_id=%s AND study_version=%s",
            (study["study_id"], study["study_version"])), checks)

        cursor.execute("SELECT manifest_id FROM cfb_context_manifests LIMIT 1")
        manifest = cursor.fetchone()["manifest_id"]
        _expect_failure(cursor, "manifest_cycle_rejected", lambda: cursor.execute(
            "INSERT INTO cfb_context_manifest_items(manifest_id,slot,ordinal,child_manifest_id) VALUES (%s,'cycle-test',999999,%s)",
            (manifest, manifest)), checks)

        cursor.execute("SELECT policy_id,step_index,slot FROM cfb_policy_dependency_slots LIMIT 1")
        slot = cursor.fetchone()
        cursor.execute("SELECT artifact_id FROM cfb_engine_artifacts WHERE representation<>'schema' LIMIT 1")
        wrong_schema = cursor.fetchone()["artifact_id"]
        _expect_failure(cursor, "policy_schema_mismatch_rejected", lambda: cursor.execute(
            """INSERT INTO cfb_policy_slot_versions(policy_id,step_index,slot,preference,definition_id,definition_version,payload_schema_id)
               VALUES (%s,%s,%s,999,'cfb_market_movement_context',1,%s)""",
            (slot["policy_id"], slot["step_index"], slot["slot"], wrong_schema)), checks)

        cursor.execute("SELECT policy_id FROM cfb_context_consumer_policies WHERE consumer_id='pickem'")
        wrong_policy = cursor.fetchone()["policy_id"]
        _expect_failure(cursor, "decision_binding_mismatch_rejected", lambda: cursor.execute(
            """INSERT INTO cfb_context_policy_decisions
              (decision_id,request_id,consumer_id,policy_generation,policy_id,mode,requested_as_of,evaluation_at,request_digest,result,reason_codes)
              VALUES (%s,%s,'cfb-terminal',1,%s,'current',now(),now(),'acceptance','deny','[]'::jsonb)""",
            (uuid4(), uuid4(), wrong_policy)), checks)

        cursor.execute("SELECT policy_id FROM cfb_engine_evidence_policies LIMIT 1")
        evidence_policy = cursor.fetchone()["policy_id"]
        erasure_id = uuid4()
        def incomplete_erasure():
            cursor.execute("""INSERT INTO cfb_engine_erasures
              (erasure_id,evidence_policy_id,reason_code,authorized_by,requested_at,idempotency_key)
              VALUES (%s,%s,'acceptance','test',now(),%s)""", (erasure_id, evidence_policy, str(uuid4())))
            cursor.execute("""INSERT INTO cfb_engine_erasure_events
              (erasure_id,event_sequence,state,occurred_at,details) VALUES (%s,0,'completed',now(),'{}')""", (erasure_id,))
        _expect_failure(cursor, "incomplete_erasure_rejected", incomplete_erasure, checks)

        cursor.execute("SELECT consumer_id,availability,manifest_id FROM cfb_context_release_pointers ORDER BY consumer_id")
        pointers = [dict(row) for row in cursor.fetchall()]
        checks["consumer_specific_unavailable_pointers"] = {
            "status": "pass" if pointers and all(row["availability"] == "unavailable" and row["manifest_id"] is None for row in pointers) else "fail",
            "pointers": pointers,
        }
        cursor.execute("SELECT provider,retention_mode FROM cfb_engine_evidence_policies ORDER BY provider")
        policies = [dict(row) for row in cursor.fetchall()]
        checks["provider_entitlement"] = {"status": "blocked", "policies": policies,
                                           "reason": "Human review of account-specific terms is not recorded."}
        checks["production_raw_privilege_denial"] = {"status": "blocked",
                                                       "reason": "No dedicated production decision-service database role has been activated."}
        cursor.execute("SELECT count(*) n FROM cfb_engine_evaluations")
        evaluations = cursor.fetchone()["n"]
        checks["future_confirmation_windows"] = {"status": "waiting", "evaluations": evaluations,
                                                  "reason": "Frozen future windows cannot be evaluated before observations accrue."}
        connection.rollback()
    failing = [name for name, value in checks.items() if value["status"] == "fail"]
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "contract_revision": 3,
            "implementation_status": "fail" if failing else "implemented_with_external_gates",
            "implemented_checks_status": "fail" if failing else "pass",
            "end_to_end_acceptance": "incomplete",
            "status_summary": "Implemented checks pass; end-to-end acceptance remains incomplete."
                              if not failing else "Implemented checks failed; end-to-end acceptance remains incomplete.",
            "open_gates": [
                {"item": "pilot_operational_evidence", "owner_role": "data/platform engineering",
                 "required_evidence": "scheduled run ledger, all detector funnels and receipts, reconciled real-data trace",
                 "gate": "collection_operational_verification"},
                {"item": "shared_consumers", "owner_role": "application/data engineering",
                 "required_evidence": "terminal, shadow study and postgame export pinned reconciliation fixture and real-data sample",
                 "gate": "phase_3_engineering_acceptance"},
                {"item": "football_context_experiment", "owner_role": "research/modeling",
                 "required_evidence": "separate frozen market-only versus drive-volume registration before untouched enrollment",
                 "gate": "incremental_context_qualification"},
                {"item": "provider_permissions", "owner_role": "product/legal/account owner",
                 "required_evidence": "reviewed account-specific terms and versioned evidence policies",
                 "gate": "consumer_activation"},
                {"item": "privilege_isolation_and_erasure", "owner_role": "platform/security and data/platform engineering",
                 "required_evidence": "negative database access tests and executable erasure receipts",
                 "gate": "consumer_activation"},
                {"item": "untouched_confirmation_windows", "owner_role": "research/modeling",
                 "required_evidence": "independent frozen evaluations after the scheduled review dates",
                 "gate": "consumer_activation"},
            ],
            "failing_checks": failing, "checks": checks}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROJECT_DIR / "artifacts" / "cfb_context_acceptance_rev3.json")
    args = parser.parse_args()
    report = run(load_config().database_url or "")
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "status": report["implementation_status"],
                      "failing_checks": report["failing_checks"]}, indent=2))
    raise SystemExit(1 if report["failing_checks"] else 0)


if __name__ == "__main__":
    main()
