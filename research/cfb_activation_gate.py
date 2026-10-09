"""Phase 6 CFB consumer-activation assessment; never activates automatically."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json

from config import load_config


def assess_activation(*, confirmation_results: dict[str, str], provider_modes: dict[str, str],
                      current_permission: str, raw_evidence_isolated: bool,
                      reviewed_acceptance_record: bool) -> dict:
    reasons = []
    for window in ("confirmation_1", "confirmation_2"):
        if confirmation_results.get(window) != "pass":
            reasons.append(f"{window}_not_passed")
    unresolved = sorted(provider for provider, mode in provider_modes.items() if mode == "unknown")
    if unresolved:
        reasons.append("provider_entitlement_unresolved:" + ",".join(unresolved))
    if not raw_evidence_isolated:
        reasons.append("production_raw_evidence_privilege_not_denied")
    if not reviewed_acceptance_record:
        reasons.append("consumer_acceptance_record_missing")
    if current_permission == "decision-denied":
        reasons.append("current_policy_decision_denied")
    return {
        "eligible_for_activation_proposal": not reasons,
        "activation_performed": False,
        "reason_codes": reasons,
        "required_action": "separate reviewed consumer-specific proposal" if not reasons else "remain denied",
    }


def assess_database(database_url: str, consumer_id: str, *, study_version: int | None = None) -> dict:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        cursor = connection.cursor()
        if study_version is None:
            cursor.execute("SELECT max(study_version) version FROM cfb_engine_studies")
            study_version = cursor.fetchone()["version"]
        cursor.execute("""SELECT DISTINCT ON (window_key) window_key,result,report_revision
          FROM cfb_engine_evaluations WHERE study_version=%s AND window_key IN ('confirmation_1','confirmation_2')
          ORDER BY window_key,report_revision DESC""", (study_version,))
        confirmations = {row["window_key"]: row["result"] for row in cursor.fetchall()}
        cursor.execute("SELECT provider,retention_mode FROM cfb_engine_evidence_policies")
        provider_modes = {row["provider"]: row["retention_mode"] for row in cursor.fetchall()}
        cursor.execute("""SELECT p.usage FROM cfb_context_policy_bindings b
          JOIN cfb_context_binding_revisions r ON r.consumer_id=b.consumer_id AND r.policy_generation=b.generation
          JOIN cfb_context_consumer_policies p ON p.policy_id=r.policy_id
          WHERE b.consumer_id=%s""", (consumer_id,))
        policy = cursor.fetchone()
        permission = policy["usage"] if policy else "decision-denied"
        # This remains false until a dedicated decision-service role and denied
        # raw-table privilege test are recorded in an acceptance artifact.
        raw_isolated = False
        cursor.execute("""SELECT count(*) n FROM cfb_context_audit_events
          WHERE event_type='administrative_access' AND scope_key=%s
            AND details->>'record_type'='consumer_activation_acceptance'""", (consumer_id,))
        reviewed = cursor.fetchone()["n"] > 0
    assessment = assess_activation(confirmation_results=confirmations, provider_modes=provider_modes,
                                   current_permission=permission, raw_evidence_isolated=raw_isolated,
                                   reviewed_acceptance_record=reviewed)
    return {"consumer_id": consumer_id, "study_version": study_version,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "confirmation_results": confirmations, "provider_modes": provider_modes,
            "current_permission": permission, **assessment}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("consumer_id", choices=("cfb-terminal", "cfb-shadow-study", "cfb-postgame-export", "pickem", "survivor"))
    parser.add_argument("--study-version", type=int)
    args = parser.parse_args()
    print(json.dumps(assess_database(load_config().database_url or "", args.consumer_id,
                                     study_version=args.study_version), indent=2))


if __name__ == "__main__":
    main()
