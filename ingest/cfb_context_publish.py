"""Publish CFB movement-context snapshots from normalized quote rows.

Only pregame captures with qualifying, timestamped paired quotes can publish.
The output inherits the endpoint capture's origin. An endpoint cutoff avoids
recomputing older cohorts during ongoing prospective collection.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from uuid import UUID, uuid4, uuid5

from config import load_config
from model.cfb_detector_funnel import summarize_opportunities
from model.cfb_market_context import measure_movement


NAMESPACE = UUID("3234236c-a0ae-4b1a-99d9-ab079fe1fa30")
ALLOWLIST = ("draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers", "pinnacle")
CONFIG_DIGEST = sha256(json.dumps({"allowlist": ALLOWLIST, "definition_version": 1}, sort_keys=True).encode()).hexdigest()


def _id(kind: str, value: object) -> UUID:
    return uuid5(NAMESPACE, f"{kind}:{value}")


def _captures(cursor, earliest: datetime | None = None) -> dict[str, list[dict]]:
    cursor.execute("""SELECT c.capture_id,c.event_key,c.schedule_revision_id,c.observed_at,c.origin,c.pregame_state,
        r.scheduled_kickoff,q.quote_id,q.book,q.market,q.selection,q.line,q.decimal_price,q.bookmaker_updated_at
      FROM cfb_engine_captures c JOIN cfb_engine_schedule_revisions r ON r.schedule_revision_id=c.schedule_revision_id
      LEFT JOIN cfb_engine_quote_observations q ON q.capture_id=c.capture_id AND q.line_role='main'
      WHERE c.pregame_state='pregame' AND (%s::timestamptz IS NULL OR c.observed_at >= %s)
      ORDER BY c.event_key,c.observed_at,c.capture_id,q.book,q.market,q.selection""",
      (earliest, earliest))
    grouped: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in cursor.fetchall():
        capture = grouped[row["event_key"]].setdefault(str(row["capture_id"]), {
            "capture_id": row["capture_id"], "observed_at": row["observed_at"],
            "schedule_revision_id": row["schedule_revision_id"], "pregame_state": row["pregame_state"], "origin": row["origin"],
            "scheduled_kickoff": row["scheduled_kickoff"], "books": {},
        })
        if row["quote_id"] is None:
            continue
        quote = capture["books"].setdefault(row["book"], {})
        market, selection = row["market"], row["selection"]
        if market == "spread":
            quote[f"spread_{selection}"] = float(row["line"])
            quote[f"spread_{selection}_decimal"] = float(row["decimal_price"])
            quote[f"spread_{selection}_updated_at"] = row["bookmaker_updated_at"]
        elif market == "total":
            quote["total_line"] = float(row["line"])
            quote[f"{selection}_decimal"] = float(row["decimal_price"])
            quote[f"{selection}_updated_at"] = row["bookmaker_updated_at"]
        quote.setdefault(f"{market}_quote_ids", []).append(str(row["quote_id"]))
    return {event: list(captures.values()) for event, captures in grouped.items()}


def _rejection_reasons(result) -> list[str]:
    if result.status == "accepted":
        return []
    reason = result.reason or "unknown"
    reasons = {"temporal_origin_violation" if reason == "temporal_or_origin_violation" else reason}
    for key in ("start_rejections", "endpoint_rejections"):
        for book_reason in (result.payload.get(key) or {}).values():
            if book_reason == "future_bookmaker_timestamp":
                reasons.add("temporal_origin_violation")
            elif book_reason == "stale_quote":
                reasons.add("stale_quote")
    return sorted(reasons)


def _persist_run_funnels(cursor, opportunities: dict, *, existing_keys: set[str], config_artifact: UUID) -> dict:
    """Save one exact publication-run partition, including rejected endpoints."""
    from psycopg2.extras import Json, execute_values

    run_id = uuid4()
    completed_at = datetime.now(timezone.utc)
    scope_key = (f"cfb:workflow:{os.getenv('GITHUB_RUN_ID', 'unknown')}"
                 if os.getenv("GITHUB_ACTIONS") == "true" else "cfb:manual")
    endpoint_ids = sorted({row["capture_id"] for rows in opportunities.values() for row in rows})
    manifest_id = _id("detector-run-manifest", run_id)
    manifest_digest = sha256(json.dumps({"run_id": str(run_id), "endpoint_capture_ids": endpoint_ids}, sort_keys=True).encode()).hexdigest()
    cursor.execute("""INSERT INTO cfb_context_manifests(manifest_id,kind,scope_key,as_of_at,manifest_digest)
        VALUES (%s,'inputs',%s,%s,%s)""",
        (manifest_id, f"cfb-market-context-detector:{run_id}", completed_at, manifest_digest))
    if endpoint_ids:
        execute_values(cursor, """INSERT INTO cfb_context_manifest_items(manifest_id,slot,ordinal,capture_id)
            VALUES %s""", [(manifest_id, "endpoint_capture", index, UUID(value))
                          for index, value in enumerate(endpoint_ids)])
    cursor.execute("""INSERT INTO cfb_detector_runs
        (run_id,detector_id,detector_version,input_manifest_id,scope_key,comparison_policy_artifact_id,run_key,completed_at)
        VALUES (%s,'cfb_market_movement_context','1',%s,%s,%s,%s,%s)""",
        (run_id, manifest_id, scope_key, config_artifact, f"cfb-market-context-v1:{run_id}", completed_at))
    totals = {"runs": 1, "scope": scope_key, "funnels": 0, "candidates": 0,
              "matched": 0, "persisted": 0, "deduped": 0}
    for (event_key, market), rows in sorted(opportunities.items()):
        partition = []
        for row in rows:
            result = row["result"]
            key = result.payload.get("idempotency_key")
            partition.append({"opportunity_key": f"{row['capture_id']}:{market}",
                              "rejection_reasons": _rejection_reasons(result),
                              "threshold_match": result.status == "accepted",
                              "persistence_state": ("deduped" if key in existing_keys else "persisted")
                              if result.status == "accepted" else None})
        funnel = summarize_opportunities(partition)
        samples = {reason: list(keys) for reason, keys in funnel.rejection_samples.items()}
        sample_artifact = None
        if samples:
            sample_payload = {"run_id": str(run_id), "event_key": event_key, "market": market,
                              "rejection_samples": samples}
            encoded = json.dumps(sample_payload, sort_keys=True, separators=(",", ":")).encode()
            digest = sha256(encoded).hexdigest()
            sample_artifact = _id("detector-funnel-samples", digest)
            cursor.execute("""INSERT INTO cfb_engine_artifacts
                (artifact_id,kind,digest,representation,byte_count,metadata)
                VALUES (%s,'detector-funnel-rejection-samples',%s,'report',%s,%s) ON CONFLICT DO NOTHING""",
                (sample_artifact, digest, len(encoded), Json(sample_payload)))
        cursor.execute("""INSERT INTO cfb_detector_funnels
            (run_id,event_key,market,candidate_count,eligible_count,matched_count,deduped_count,
             persisted_count,failed_persistence_count,rejection_counts,sample_artifact_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (run_id, event_key, market, funnel.candidate_count, funnel.eligible_count,
             funnel.matched_count, funnel.deduped_count, funnel.persisted_count,
             funnel.failed_persistence_count, Json(funnel.rejection_counts), sample_artifact))
        totals["funnels"] += 1
        totals["candidates"] += funnel.candidate_count
        totals["matched"] += funnel.matched_count
        totals["persisted"] += funnel.persisted_count
        totals["deduped"] += funnel.deduped_count
    return totals


def publish(database_url: str, *, apply: bool, max_events: int | None = None, endpoint_after=None) -> dict:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor, execute_values, register_uuid

    register_uuid()
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("SET LOCAL lock_timeout='30s'")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cfb-context-publish-v1",))
        workflow_run = os.getenv("GITHUB_ACTIONS") == "true"
        earliest = endpoint_after - timedelta(minutes=40) if endpoint_after else None
        if workflow_run and apply and endpoint_after:
            cursor.execute("""SELECT min(c.observed_at) FROM cfb_engine_captures c
                WHERE c.pregame_state='pregame' AND c.observed_at >= %s
                  AND NOT EXISTS (
                    SELECT 1 FROM cfb_context_manifest_items i
                    JOIN cfb_detector_runs r ON r.input_manifest_id=i.manifest_id
                    WHERE i.capture_id=c.capture_id AND i.slot='endpoint_capture'
                      AND r.detector_id='cfb_market_movement_context'
                      AND r.scope_key LIKE 'cfb:workflow:%%')""", (endpoint_after,))
            first_unprocessed = cursor.fetchone()["min"]
            earliest = (first_unprocessed or datetime.now(timezone.utc)) - timedelta(minutes=40)
        grouped = _captures(cursor, earliest)
        if max_events is not None:
            grouped = dict(list(sorted(grouped.items()))[:max_events])
        processed_capture_ids: set[str] = set()
        if workflow_run and apply:
            candidate_ids = [str(endpoint["capture_id"]) for captures in grouped.values() for endpoint in captures
                             if endpoint_after is None or endpoint["observed_at"] >= endpoint_after]
            if candidate_ids:
                cursor.execute("""SELECT DISTINCT i.capture_id FROM cfb_context_manifest_items i
                    JOIN cfb_detector_runs r ON r.input_manifest_id=i.manifest_id
                    WHERE r.scope_key LIKE 'cfb:workflow:%%' AND r.detector_id='cfb_market_movement_context'
                      AND i.slot='endpoint_capture' AND i.capture_id=ANY(%s)""",
                    ([UUID(value) for value in candidate_ids],))
                processed_capture_ids = {str(row["capture_id"]) for row in cursor.fetchall()}
        accepted, rejected = [], defaultdict(int)
        opportunities: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for event_key, captures in grouped.items():
            for endpoint in captures:
                if endpoint_after is not None and endpoint["observed_at"] < endpoint_after:
                    continue
                if str(endpoint["capture_id"]) in processed_capture_ids:
                    continue
                for market in ("spread", "total"):
                    result = measure_movement(
                        captures, endpoint_capture_id=endpoint["capture_id"], market=market,
                        allowlist=ALLOWLIST, event_key=event_key, config_digest=CONFIG_DIGEST,
                        scheduled_kickoff=endpoint["scheduled_kickoff"],
                    )
                    opportunities[(event_key, market)].append({"capture_id": str(endpoint["capture_id"]),
                                                                  "result": result})
                    if result.status == "accepted":
                        accepted.append((result.payload, endpoint["origin"]))
                    else:
                        rejected[result.reason or "unknown"] += 1
        if not apply:
            connection.rollback()
            return {"mode": "plan", "events": len(grouped), "accepted_snapshots": len(accepted), "rejections": dict(rejected)}

        manifest_rows, snapshot_rows, item_rows = [], [], []
        config_artifact = uuid5(UUID("9fa3a542-a6a6-4fd6-b759-82126722e5e0"), "artifact:cfb-market-allowlist-v1")
        keys = [payload["idempotency_key"] for payload, _ in accepted]
        existing_keys: set[str] = set()
        if keys:
            cursor.execute("SELECT idempotency_key FROM cfb_context_snapshots WHERE idempotency_key=ANY(%s)", (keys,))
            existing_keys = {row["idempotency_key"] for row in cursor.fetchall()}
        for payload, origin in accepted:
            key = payload["idempotency_key"]
            manifest_id = _id("manifest", key)
            snapshot_id = _id("snapshot", key)
            manifest_digest = sha256(json.dumps({"payload": payload, "kind": "movement-inputs"}, sort_keys=True).encode()).hexdigest()
            manifest_rows.append((manifest_id, "inputs", f"{payload['event_key']}:{payload['market']}", payload["as_of_at"], manifest_digest, None))
            snapshot_rows.append((snapshot_id, "cfb_market_movement_context", 1, payload["event_key"], payload["event_key"],
                                  payload["as_of_at"], payload["window_start"], payload["window_end"], Json(payload),
                                  payload["scalar_value"], "complete", "observed", "legacy normalized quote observations",
                                  origin, manifest_id, config_artifact, "main", key))
            capture_ids = [UUID(payload["start_capture_id"]), UUID(payload["endpoint_capture_id"])]
            item_rows.extend((manifest_id, "capture", index, None, capture_id, None, None, None, None, None)
                             for index, capture_id in enumerate(capture_ids))
            quote_ids = sorted({quote_id for book in payload["book_deltas"].values()
                                for quote_id in book["start_quote_ids"] + book["endpoint_quote_ids"]})
            item_rows.extend((manifest_id, "quote", index, None, None, UUID(quote_id), None, None, None, None)
                             for index, quote_id in enumerate(quote_ids))
        if manifest_rows:
            execute_values(cursor, """INSERT INTO cfb_context_manifests(manifest_id,kind,scope_key,as_of_at,manifest_digest,policy_id)
              VALUES %s ON CONFLICT(manifest_digest) DO NOTHING""", manifest_rows, page_size=500)
            execute_values(cursor, """INSERT INTO cfb_context_snapshots
              (snapshot_id,definition_id,definition_version,subject_key,target_event_key,as_of_at,window_start,window_end,payload,
               scalar_value,coverage_state,measurement_kind,availability_basis,origin,input_manifest_id,configuration_artifact_id,scenario_key,idempotency_key)
              VALUES %s ON CONFLICT(idempotency_key) DO NOTHING""", snapshot_rows, page_size=500)
            execute_values(cursor, """INSERT INTO cfb_context_manifest_items
              (manifest_id,slot,ordinal,source_id,capture_id,quote_id,snapshot_id,artifact_id,child_manifest_id,economic_resolution_id)
              VALUES %s ON CONFLICT DO NOTHING""", item_rows, page_size=1000)
        funnel_totals = _persist_run_funnels(cursor, opportunities, existing_keys=existing_keys,
                                             config_artifact=config_artifact)
        connection.commit()
        return {"mode": "applied", "events": len(grouped), "accepted_snapshots": len(accepted),
                "manifest_items": len(item_rows), "rejections": dict(rejected),
                "skipped_prior_workflow_captures": len(processed_capture_ids), "funnel": funnel_totals}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--endpoint-after", type=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")))
    args = parser.parse_args()
    print(json.dumps(publish(load_config().database_url or "", apply=args.apply,
                             max_events=args.max_events, endpoint_after=args.endpoint_after), indent=2))


if __name__ == "__main__":
    main()
