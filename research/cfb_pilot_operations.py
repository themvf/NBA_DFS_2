"""Produce a read-only, pinned operations snapshot for the CFB pilot.

GitHub run history and the database are sampled independently. The report keeps
their cutoffs explicit and never treats a successful workflow as proof that all
downstream evidence was persisted.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import time

from config import PROJECT_DIR, load_config
from research.cfb_study_evaluation import status as study_status


PILOT_START = datetime(2026, 9, 25, tzinfo=timezone.utc)
PILOT_END = datetime(2026, 10, 14, tzinfo=timezone.utc)
FROZEN_DIGEST = "364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c"
WORKFLOWS = ("capture_event_closes.yml", "refresh_cfb_terminal.yml")


def _runs(workflow: str, start: datetime, cutoff: datetime) -> list[dict]:
    command = ["gh", "run", "list", "--workflow", workflow, "--limit", "1000", "--json",
               "databaseId,headSha,createdAt,updatedAt,conclusion,status,url,event"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    rows = json.loads(result.stdout)
    selected = []
    for row in rows:
        created = datetime.fromisoformat(row["createdAt"].replace("Z", "+00:00"))
        if not start <= created <= cutoff:
            continue
        item = dict(row)
        updated = datetime.fromisoformat(row["updatedAt"].replace("Z", "+00:00"))
        if updated > cutoff:
            item["status"] = "outcome_after_cutoff"
            item["conclusion"] = ""
        selected.append(item)
    return selected


def _run_steps(run_id: int) -> list[dict]:
    result = subprocess.run(["gh", "run", "view", str(run_id), "--json", "jobs"],
                            check=True, capture_output=True, text=True)
    return [step for job in json.loads(result.stdout)["jobs"] for step in job["steps"]]


def _count(cursor, sql: str, start: datetime, cutoff: datetime) -> dict:
    cursor.execute(sql, (start, cutoff))
    row = cursor.fetchone()
    return {"count": row[0], "sample_id": str(row[1]) if row[1] is not None else None}


def _moneyline_settlement(rows: list[dict], candidate_definitions: list[dict]) -> dict:
    frozen = {(item["alert_type"], item["signal_version"]) for item in candidate_definitions}
    eligible = [
        row for row in rows
        if (row["alert_type"], row["signal_version"]) in frozen
        and (row["details_json"] or {}).get("market") in (None, "moneyline")
    ]
    settled = [row for row in eligible if row["settled_at"] is not None]
    metric_rows = [row for row in settled if (row["metrics"] or {}).get("decimal_price_ratio_pct") is not None]
    return {
        "frozen_candidate_signals": len(eligible),
        "settled_signals": len(settled),
        "settled_independent_game_dates": len({row["game_date"] for row in settled}),
        "settled_with_primary_metric": len(metric_rows),
        "settled_missing_primary_metric": len(settled) - len(metric_rows),
        "settled_with_verified_close_id": sum(row["close_history_id"] is not None for row in settled),
        "settled_with_unverified_rule": sum(
            (row["grading_json"] or {}).get("settlement_rule_status") == "UNVERIFIED_LEGACY_QUOTES"
            for row in settled
        ),
        "settled_conflicts": sum(row["result_state"] == "conflict" for row in settled),
    }


def _expected_slots(workflow: str, start: datetime, cutoff: datetime) -> int:
    slot = start.replace(second=0, microsecond=0)
    if slot < start:
        slot += timedelta(minutes=1)
    count = 0
    while slot < cutoff:
        if workflow == WORKFLOWS[0]:
            due = slot.minute % 5 == 0
        else:
            # Four cron slots per hour in refresh_cfb_terminal.yml.
            due = slot.minute in (7, 22, 37, 52)
        count += int(due)
        slot += timedelta(minutes=1)
    return count


def _build_once(database_url: str, start: datetime = PILOT_START) -> dict:
    import psycopg2

    with psycopg2.connect(database_url) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SELECT now()")
            cutoff = min(cursor.fetchone()[0], PILOT_END)
            monitor_start = max(start, cutoff - timedelta(hours=24))
            cursor.execute("""SELECT study_version,configuration_digest,primary_metric,primary_unit,cohort_rules
                FROM cfb_engine_studies WHERE study_version=4 AND configuration_digest=%s""", (FROZEN_DIGEST,))
            study = cursor.fetchone()
            if not study:
                raise RuntimeError("frozen study version 4 is absent or its digest changed")
            stages = {
                "stored_provider_history": _count(cursor, """SELECT count(*),min(id::text)
                    FROM game_odds_history WHERE sport='cfb' AND captured_at >= %s AND captured_at < %s""", start, cutoff),
                "provider_captures": _count(cursor, """SELECT count(*),min(capture_id::text)
                    FROM cfb_engine_captures WHERE origin='prospective' AND observed_at >= %s AND observed_at < %s""", start, cutoff),
                "normalized_quotes": _count(cursor, """SELECT count(*),min(q.quote_id::text)
                    FROM cfb_engine_quote_observations q JOIN cfb_engine_captures c USING(capture_id)
                    WHERE c.origin='prospective' AND c.observed_at >= %s AND c.observed_at < %s""", start, cutoff),
                "detector_runs": _count(cursor, """SELECT count(*),min(run_id::text)
                    FROM cfb_detector_runs WHERE scope_key LIKE 'cfb:workflow:%%'
                    AND completed_at >= %s AND completed_at < %s""", start, cutoff),
                "detector_funnels": _count(cursor, """SELECT count(*),min(f.run_id::text)
                    FROM cfb_detector_funnels f JOIN cfb_detector_runs r USING(run_id)
                    WHERE r.scope_key LIKE 'cfb:workflow:%%' AND r.completed_at >= %s AND r.completed_at < %s""", start, cutoff),
                "persisted_signals": _count(cursor, """SELECT count(*),min(id::text)
                    FROM line_alerts WHERE sport='cfb' AND origin='prospective'
                    AND created_at >= %s AND created_at < %s""", start, cutoff),
                "economic_heads": _count(cursor, """SELECT count(*),min(r.resolution_id::text)
                    FROM cfb_economic_resolutions r JOIN line_alerts a ON a.id=r.alert_id
                    WHERE a.sport='cfb' AND a.origin='prospective' AND a.created_at >= %s AND a.created_at < %s
                    AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)""", start, cutoff),
            }
            cursor.execute("""SELECT a.id,a.alert_type,a.signal_version,a.game_date,a.settled_at,
                a.details_json,a.grading_json,a.close_history_id,r.result_state,r.metrics
                FROM line_alerts a LEFT JOIN cfb_economic_resolutions r ON r.alert_id=a.id
                  AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n
                                  WHERE n.supersedes_resolution_id=r.resolution_id)
                WHERE a.sport='cfb' AND a.origin='prospective'
                  AND a.created_at >= %s AND a.created_at < %s""", (start, cutoff))
            columns = [column[0] for column in cursor.description]
            settlement_evidence = _moneyline_settlement(
                [dict(zip(columns, row)) for row in cursor.fetchall()],
                study[4].get("candidate_definitions", []),
            )
            cursor.execute("SELECT count(*) FROM cfb_engine_settlement_rules WHERE market='moneyline'")
            settlement_evidence["registered_moneyline_rule_versions"] = cursor.fetchone()[0]
            cursor.execute("""SELECT count(*),count(*) FILTER (WHERE q.settlement_rule_id IS NOT NULL)
                FROM cfb_engine_quote_observations q JOIN cfb_engine_captures c USING(capture_id)
                WHERE q.market='moneyline' AND c.origin='prospective'
                  AND c.observed_at >= %s AND c.observed_at < %s""", (start, cutoff))
            quote_total, quote_with_rule = cursor.fetchone()
            settlement_evidence["prospective_moneyline_quotes"] = quote_total
            settlement_evidence["quotes_with_rule_version"] = quote_with_rule
            cursor.execute("""SELECT count(*) FROM cfb_detector_publication_receipts""")
            receipts = cursor.fetchone()[0]
            cursor.execute("""SELECT count(*) FROM cfb_engine_captures
                WHERE origin='prospective' AND pregame_state='pregame'
                  AND observed_at >= %s AND observed_at < %s""", (monitor_start, cutoff))
            recent_pregame_captures = cursor.fetchone()[0]
            cursor.execute("""SELECT f.candidate_count,f.eligible_count,f.failed_persistence_count,f.rejection_counts
                FROM cfb_detector_funnels f JOIN cfb_detector_runs r USING(run_id)
                WHERE r.scope_key LIKE 'cfb:workflow:%%' AND r.completed_at >= %s AND r.completed_at < %s""",
                (monitor_start, cutoff))
            funnel_rows = cursor.fetchall()
            cursor.execute("""SELECT DISTINCT detector_id,detector_version FROM cfb_detector_runs
                WHERE scope_key LIKE 'cfb:workflow:%%' AND completed_at >= %s AND completed_at < %s""", (start, cutoff))
            observed_detectors = {(row[0], row[1]) for row in cursor.fetchall()}
            cursor.execute("""SELECT scope_key,detector_id,detector_version FROM cfb_detector_runs
                WHERE scope_key LIKE 'cfb:workflow:%%' AND completed_at >= %s AND completed_at < %s""", (start, cutoff))
            detector_runs_by_workflow: dict[str, set[tuple[str, str]]] = {}
            for scope_key, detector_id, detector_version in cursor.fetchall():
                detector_runs_by_workflow.setdefault(scope_key.rsplit(":", 1)[-1], set()).add(
                    (detector_id, detector_version))
            cursor.execute("""SELECT count(*) FROM (
                SELECT DISTINCT ON (observation_key,consumer_id) status
                FROM cfb_detector_publication_receipts
                ORDER BY observation_key,consumer_id,receipt_version DESC
                ) latest WHERE status='failed'""")
            failed_receipts = cursor.fetchone()[0]
            cursor.execute("""SELECT count(*) FROM cfb_context_snapshots
                WHERE origin='prospective' AND created_at >= %s AND created_at < %s""", (start, cutoff))
            context_snapshots = cursor.fetchone()[0]
            cursor.execute("""SELECT count(*) FROM cfb_context_policy_decisions
                WHERE evaluation_at >= %s AND evaluation_at < %s""", (start, cutoff))
            policy_reads = cursor.fetchone()[0]
            cursor.execute("""SELECT a.id,a.trigger_history_id,c.capture_id,c.observed_at,
                (SELECT count(*) FROM cfb_engine_quote_observations q WHERE q.capture_id=c.capture_id) quote_count,
                s.snapshot_id,s.input_manifest_id,s.definition_version,s.as_of_at,
                r.resolution_id,r.result_state
                FROM line_alerts a JOIN cfb_engine_captures c ON c.history_id=a.trigger_history_id
                JOIN cfb_context_snapshots s ON s.payload->>'endpoint_capture_id'=c.capture_id::text
                  AND s.definition_id='cfb_market_movement_context' AND s.origin='prospective'
                LEFT JOIN cfb_economic_resolutions r ON r.alert_id=a.id
                  AND NOT EXISTS(SELECT 1 FROM cfb_economic_resolutions n WHERE n.supersedes_resolution_id=r.resolution_id)
                WHERE a.sport='cfb' AND a.origin='prospective' AND a.created_at >= %s AND a.created_at < %s
                ORDER BY a.created_at,s.as_of_at,s.snapshot_id LIMIT 1""", (start, cutoff))
            trace_row = cursor.fetchone()
            real_data_trace = dict(zip(("signal_id", "trigger_history_id", "capture_id", "capture_observed_at",
                                        "normalized_quote_count", "snapshot_id", "source_manifest_id",
                                        "definition_version", "snapshot_as_of_at", "economic_resolution_id",
                                        "economic_state"), trace_row)) if trace_row else None
    frozen_status = study_status(database_url, study_version=4, now=cutoff)
    run_history = {}
    for workflow in WORKFLOWS:
        runs = sorted(_runs(workflow, start, cutoff), key=lambda row: row["createdAt"])
        first_by_commit = {}
        for row in runs:
            if row["conclusion"] == "success":
                first_by_commit.setdefault(row["headSha"], row)
        run_history[workflow] = {
            "expected_scheduled_slots": _expected_slots(workflow, start, cutoff),
            "observed_runs": len(runs),
            "scheduled_runs": sum(row["event"] == "schedule" for row in runs),
            "dispatch_runs": sum(row["event"] == "workflow_dispatch" for row in runs),
            "failed_or_incomplete": [row for row in runs if row["status"] != "completed" or row["conclusion"] != "success"],
            "first_success": next((row for row in runs if row["conclusion"] == "success"), None),
            "first_success_by_deployed_commit": first_by_commit,
            "latest_run": runs[-1] if runs else None,
            "history_limit_reached": len(runs) >= 1000,
            "runs": runs,
        }
    capture_successes = [row for row in run_history[WORKFLOWS[0]]["runs"] if row["conclusion"] == "success"]
    latest_capture_success = capture_successes[-1] if capture_successes else None
    capture_steps = _run_steps(latest_capture_success["databaseId"]) if latest_capture_success else []
    required_steps = ("Normalize stored CFB captures for the frozen pilot",
                      "Publish CFB market context from stored captures",
                      "Detect and grade prospective CFB market signals",
                      "Reconcile canonical CFB economics")
    step_status = {name: next((step["conclusion"] for step in capture_steps if step["name"] == name), "absent")
                   for name in required_steps}
    findings = []
    if stages["detector_runs"]["count"] == 0 or stages["detector_funnels"]["count"] == 0:
        findings.append("No persisted detector-run/funnel evidence in the pilot interval; zero signals cannot be explained by a funnel.")
    required_detectors = {(item["alert_type"], item["signal_version"])
                          for item in study[4].get("candidate_definitions", [])}
    missing_detectors = sorted(required_detectors - observed_detectors)
    if missing_detectors:
        findings.append("Frozen moneyline detector/version funnels are missing from scheduled workflow evidence.")
    successful_workflow_runs = [row for row in run_history[WORKFLOWS[0]]["runs"]
                                if row["conclusion"] == "success"]
    missing_per_run = [{"run_id": row["databaseId"], "url": row["url"],
                        "missing": sorted(required_detectors - detector_runs_by_workflow.get(str(row["databaseId"]), set()))}
                       for row in successful_workflow_runs]
    missing_per_run = [row for row in missing_per_run if row["missing"]]
    if missing_per_run:
        findings.append("One or more successful CFB workflows lack a frozen detector/version funnel for that exact run.")
    if receipts == 0:
        findings.append("No detector publication receipts are persisted.")
    if policy_reads == 0:
        findings.append("No shared policy-reader decisions are persisted in the pilot interval.")
    if settlement_evidence["settled_missing_primary_metric"]:
        findings.append("Settled frozen moneyline candidates are missing the primary price-ratio metric.")
    if settlement_evidence["settled_with_unverified_rule"] or (
        settlement_evidence["prospective_moneyline_quotes"] > settlement_evidence["quotes_with_rule_version"]
    ):
        findings.append("Moneyline quote settlement-rule versions are not verified for the active pilot.")
    if stages["persisted_signals"]["count"] == 0:
        findings.append("No prospective CFB signal was persisted; downstream study observations must remain empty.")
    if stages["persisted_signals"]["count"] and not stages["economic_heads"]["count"]:
        findings.append("Prospective CFB signals exist without canonical economic heads; the study cannot enroll them yet.")
    if stages["provider_captures"]["count"] == 0:
        findings.append("No pilot-period provider history has been normalized as a prospective engine capture.")
    if not real_data_trace:
        findings.append("No exact provider-to-economics real-data trace is available.")
    if any(item["history_limit_reached"] for item in run_history.values()):
        findings.append("GitHub run history reached the 1000-run query limit; the full reporting interval is not covered.")
    if any(value != "success" for value in step_status.values()):
        findings.append("The latest successful capture workflow has not executed every new normalization, publication, and detection step.")
    checks = {}
    for workflow, maximum_gap_minutes in ((WORKFLOWS[0], 20), (WORKFLOWS[1], 120)):
        completed = [row for row in run_history[workflow]["runs"] if row["conclusion"] == "success"]
        last = max((datetime.fromisoformat(row["updatedAt"].replace("Z", "+00:00")) for row in completed), default=None)
        age_minutes = (cutoff - last).total_seconds() / 60 if last else None
        checks[workflow] = {"status": "pass" if age_minutes is not None and age_minutes <= maximum_gap_minutes else "alert",
                            "last_success_utc": last.isoformat() if last else None,
                            "age_minutes": round(age_minutes, 1) if age_minutes is not None else None,
                            "threshold_minutes": maximum_gap_minutes}
    candidate_total = sum(row[0] for row in funnel_rows)
    eligible_total = sum(row[1] for row in funnel_rows)
    failed_persistence = sum(row[2] for row in funnel_rows)
    temporal_rejections = sum((row[3] or {}).get("temporal_origin_violation", 0) for row in funnel_rows)
    stale_rejections = sum((row[3] or {}).get("stale_quote", 0) for row in funnel_rows)
    funnel_monitoring = {
        "window_start_utc": monitor_start.isoformat(),
        "zero_eligible_opportunities": "not_evaluable" if not funnel_rows or candidate_total < 20
                                       else "alert" if eligible_total == 0 and recent_pregame_captures else "pass",
        "temporal_rejection_spike": "not_evaluable" if candidate_total < 20 else "alert" if temporal_rejections / candidate_total > .05 else "pass",
        "stale_book_spike": "not_evaluable" if candidate_total < 20 else "alert" if stale_rejections / candidate_total > .20 else "pass",
        "persistence_or_publication_failure": "alert" if failed_persistence or failed_receipts else "pass" if funnel_rows and receipts else "not_evaluable",
        "candidate_count": candidate_total, "eligible_count": eligible_total,
        "recent_pregame_captures": recent_pregame_captures,
        "temporal_rejections": temporal_rejections, "stale_quote_rejections": stale_rejections,
        "failed_persistence": failed_persistence, "failed_publication_receipts": failed_receipts,
    }
    latest_id = latest_capture_success["databaseId"] if latest_capture_success else None
    current_missing = next((item["missing"] for item in missing_per_run if item["run_id"] == latest_id), [])
    funnel_monitoring["missing_frozen_detector_funnels"] = (
        "alert" if all(value == "success" for value in step_status.values()) and current_missing
        else "not_evaluable" if any(value != "success" for value in step_status.values())
        else "pass")
    return {
        "status": "operationally_verified" if not findings and all(check["status"] == "pass" for check in checks.values())
                  and all(value == "pass" for key, value in funnel_monitoring.items() if key in (
                      "zero_eligible_opportunities", "temporal_rejection_spike", "stale_book_spike",
                      "persistence_or_publication_failure"))
                  and all(not item["failed_or_incomplete"] for item in run_history.values()) else "incomplete",
        "reporting_interval_utc": {"start_inclusive": start.isoformat(), "end_exclusive": cutoff.isoformat()},
        "evidence_cutoff_utc": cutoff.isoformat(),
        "pilot_window_end_utc": PILOT_END.isoformat(),
        "monitor_active": datetime.now(timezone.utc) < PILOT_END,
        "environment": "GitHub Actions workflows plus configured PostgreSQL database",
        "study": {"version": study[0], "configuration_digest": study[1], "primary_metric": study[2], "primary_unit": study[3]},
        "detector_versions": {"required_frozen": sorted(required_detectors),
                              "observed_workflow": sorted(observed_detectors), "missing_frozen": missing_detectors,
                              "successful_workflow_runs": len(successful_workflow_runs),
                              "runs_missing_frozen_funnels": len(missing_per_run),
                              "missing_run_samples": missing_per_run[:10]},
        "workflows": run_history,
        "latest_deployed_capture_path": {"run": latest_capture_success, "required_step_conclusions": step_status},
        "stages": stages,
        "settlement_evidence": settlement_evidence,
        "context_snapshots": context_snapshots,
        "frozen_study_window_status": frozen_status["windows"],
        "real_data_trace": real_data_trace,
        "run_gap_checks": checks,
        "funnel_monitoring": funnel_monitoring,
        "policy_reader_decisions": policy_reads,
        "publication_receipts": receipts,
        "findings": findings,
        "limitations": ["Expected scheduled slots follow the committed cron cadence; GitHub triggers can be delayed or skipped, so observed run counts do not identify exact missing slots. Dispatch runs are listed separately.",
                        "Stage counts have different grains and are not expected to match.",
                        "A database aggregate cannot substitute for a linked real-data trace or per-detector zero-match funnel."],
    }


def build(database_url: str, start: datetime = PILOT_START) -> dict:
    """Retry transient schema-lock deadlocks with fresh read transactions."""
    import psycopg2

    retryable = (psycopg2.errors.DeadlockDetected, psycopg2.errors.LockNotAvailable)
    for attempt in range(3):
        try:
            return _build_once(database_url, start)
        except retryable:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROJECT_DIR / "artifacts" / "cfb_pilot_operations.json")
    parser.add_argument("--fail-on-alert", action="store_true",
                        help="Exit nonzero for actionable run-gap or funnel alerts; incomplete engineering gates remain reported")
    args = parser.parse_args()
    report = build(load_config().database_url or "")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    alerts = [name for name, check in report["run_gap_checks"].items() if check["status"] == "alert"]
    alerts += [name for name, value in report["funnel_monitoring"].items() if value == "alert"]
    print(json.dumps({"output": str(args.output), "status": report["status"],
                      "alerts": alerts, "findings": report["findings"]}, indent=2))
    if args.fail_on_alert and report["monitor_active"] and alerts:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
