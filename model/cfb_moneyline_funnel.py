"""Persist the frozen CFB moneyline detectors' actual scan opportunities.

The collector is populated by ``line_alerts.scan`` while it evaluates each
latest pregame capture. It never creates alerts or changes detector thresholds.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from uuid import UUID, uuid5

from model.cfb_detector_funnel import summarize_opportunities


NAMESPACE = UUID("bcd7a57b-5cc1-46cb-a0b1-13e629cc52bf")
FROZEN_STUDY_DIGEST = "364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c"
DETECTORS = {
    "dk_value": "cfb-lines-v1",
    "pinnacle_divergence": "cfb-lines-v1",
    "steam": "cfb-lines-v1",
    "walking": "cfb-lines-v1",
    "late_move": "market-structure-v1",
}


def _id(kind: str, key: str) -> UUID:
    return uuid5(NAMESPACE, f"{kind}:{key}")


class MoneylineFunnelCollector:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.scope_key = (f"cfb:workflow:{os.environ['GITHUB_RUN_ID']}"
                          if os.getenv("GITHUB_ACTIONS") == "true" and os.getenv("GITHUB_RUN_ID")
                          else "cfb:manual")
        self.run_key_suffix = (os.getenv("GITHUB_RUN_ID") if self.scope_key.startswith("cfb:workflow:")
                               else datetime.now(timezone.utc).isoformat())
        self.opportunities: dict[tuple[str, str], dict[str, dict]] = {}
        self.capture_ids: dict[str, int] = {}

    def begin_event(self, row: dict) -> None:
        event_key = f"cfb:event:{row['matchup_id']}"
        self.capture_ids[event_key] = int(row["history_id"])
        for detector in DETECTORS:
            partition = self.opportunities.setdefault((detector, event_key), {})
            for side in ("home", "away"):
                partition[side] = {
                    "opportunity_key": f"{row['history_id']}:{detector}:{side}",
                    "rejection_reasons": [], "threshold_match": False,
                    "persistence_state": None,
                }

    def reject(self, row: dict, detector: str, side: str, reason: str) -> None:
        candidate = self.opportunities[(detector, f"cfb:event:{row['matchup_id']}")][side]
        if not candidate["threshold_match"]:
            candidate["rejection_reasons"] = [reason]

    def match(self, row: dict, detector: str, side: str, *, inserted: bool) -> None:
        if detector not in DETECTORS:
            return
        candidate = self.opportunities[(detector, f"cfb:event:{row['matchup_id']}")][side]
        candidate["rejection_reasons"] = []
        candidate["threshold_match"] = True
        candidate["persistence_state"] = "persisted" if inserted else "deduped"

    def persist(self) -> dict:
        import psycopg2
        from psycopg2.extras import Json, register_uuid

        register_uuid()
        completed_at = datetime.now(timezone.utc)
        totals = {"detector_runs": 0, "funnels": 0, "candidates": 0, "matched": 0}
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout='30s'")
                cursor.execute("""SELECT 1 FROM cfb_engine_studies
                    WHERE study_version=4 AND configuration_digest=%s""", (FROZEN_STUDY_DIGEST,))
                if cursor.fetchone() is None:
                    raise RuntimeError("frozen CFB moneyline study version 4 is not registered")
                for detector, version in DETECTORS.items():
                    run_key = f"cfb-moneyline:{detector}:{version}:{self.run_key_suffix}"
                    run_id = _id("run", run_key)
                    manifest_id = _id("manifest", run_key)
                    policy = {"detector_id": detector, "detector_version": version,
                              "study_configuration_digest": FROZEN_STUDY_DIGEST}
                    encoded_policy = json.dumps(policy, sort_keys=True).encode()
                    digest = sha256(encoded_policy).hexdigest()
                    artifact_id = _id("comparison-policy", digest)
                    cursor.execute("""INSERT INTO cfb_engine_artifacts
                        (artifact_id,kind,digest,representation,byte_count,metadata)
                        VALUES (%s,'detector-comparison-policy',%s,'configuration',%s,%s)
                        ON CONFLICT DO NOTHING""",
                        (artifact_id, digest, len(encoded_policy), Json(policy)))
                    capture_rows = sorted(self.capture_ids.items())
                    manifest_digest = sha256(json.dumps({"run_key": run_key, "captures": capture_rows},
                                                       sort_keys=True).encode()).hexdigest()
                    cursor.execute("""INSERT INTO cfb_context_manifests
                        (manifest_id,kind,scope_key,as_of_at,manifest_digest)
                        VALUES (%s,'inputs',%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (manifest_id, run_key, completed_at, manifest_digest))
                    for ordinal, (event_key, history_id) in enumerate(capture_rows):
                        cursor.execute("""INSERT INTO cfb_context_manifest_items
                            (manifest_id,slot,ordinal,capture_id)
                            SELECT %s,'endpoint_capture',%s,c.capture_id
                            FROM cfb_engine_captures c WHERE c.history_id=%s
                            ON CONFLICT DO NOTHING""", (manifest_id, ordinal, history_id))
                        cursor.execute("""SELECT 1 FROM cfb_engine_captures c
                            WHERE c.history_id=%s""", (history_id,))
                        if cursor.fetchone() is None:
                            raise RuntimeError(f"CFB detector input was not normalized: {event_key} history {history_id}")
                    cursor.execute("""INSERT INTO cfb_detector_runs
                        (run_id,detector_id,detector_version,input_manifest_id,scope_key,
                         comparison_policy_artifact_id,run_key,completed_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_key) DO NOTHING""",
                        (run_id, detector, version, manifest_id, self.scope_key,
                         artifact_id, run_key, completed_at))
                    totals["detector_runs"] += 1
                    for (candidate_detector, event_key), sides in sorted(self.opportunities.items()):
                        if candidate_detector != detector:
                            continue
                        funnel = summarize_opportunities(sides.values())
                        sample_artifact = None
                        if funnel.rejection_samples:
                            sample = {"run_key": run_key, "event_key": event_key,
                                      "rejection_samples": funnel.rejection_samples}
                            encoded = json.dumps(sample, sort_keys=True).encode()
                            sample_digest = sha256(encoded).hexdigest()
                            sample_artifact = _id("rejection-sample", sample_digest)
                            cursor.execute("""INSERT INTO cfb_engine_artifacts
                                (artifact_id,kind,digest,representation,byte_count,metadata)
                                VALUES (%s,'detector-funnel-rejection-samples',%s,'report',%s,%s)
                                ON CONFLICT DO NOTHING""",
                                (sample_artifact, sample_digest, len(encoded), Json(sample)))
                        cursor.execute("""INSERT INTO cfb_detector_funnels
                            (run_id,event_key,market,candidate_count,eligible_count,matched_count,
                             deduped_count,persisted_count,failed_persistence_count,rejection_counts,sample_artifact_id)
                            VALUES (%s,%s,'moneyline',%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING""",
                            (run_id, event_key, funnel.candidate_count, funnel.eligible_count,
                             funnel.matched_count, funnel.deduped_count, funnel.persisted_count,
                             funnel.failed_persistence_count, Json(funnel.rejection_counts), sample_artifact))
                        totals["funnels"] += 1
                        totals["candidates"] += funnel.candidate_count
                        totals["matched"] += funnel.matched_count
        return totals
