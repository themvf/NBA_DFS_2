"""Publish compact immutable research reports; no production or entry authority."""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID
from model.nfl_context_engine import stable_digest
from model.nfl_matchup_features import stamp

VERSION = "nfl-matchup-published-report-v1"
DDL = """CREATE TABLE IF NOT EXISTS nfl_matchup_research_reports (
 report_id TEXT PRIMARY KEY, upload_id UUID NOT NULL REFERENCES nfl_dfs_slate_uploads(upload_id),
 baseline_run_id UUID NOT NULL REFERENCES nfl_dfs_projection_runs(run_id),
 captured_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 comparison_digest TEXT NOT NULL, source_digests JSONB NOT NULL, payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS nfl_matchup_research_report_lookup
 ON nfl_matchup_research_reports(upload_id,captured_at DESC,published_at DESC);"""

def build_report(comparison, *, comparison_bytes_digest, coherent=None, portfolios=None, archived=None, source_digests=None):
    baseline, upload = str(comparison["baseline_run_id"]), str(comparison["upload_id"])
    UUID(baseline); UUID(upload)
    if comparison.get("production_changed") is not False:
        raise ValueError("Only explicitly shadow comparisons can be published here")
    digest = stable_digest(comparison)
    if coherent is not None:
        audit = coherent.get("audit", {})
        if audit.get("productionRunId") != baseline or audit.get("uploadId") != upload or coherent.get("manifest", {}).get("sources", {}).get("comparison_digest") != digest:
            raise ValueError("Coherent report does not match the exact comparison, baseline and salary upload")
        byte_hash = coherent.get("manifest", {}).get("sources", {}).get("comparison_file_sha256")
        if byte_hash is not None and byte_hash != comparison_bytes_digest:
            raise ValueError("Coherent source file hash differs from the supplied comparison")
        if coherent.get("productionChanged") is not False:
            raise ValueError("Coherent report must remain shadow-only")
    if portfolios is not None:
        if portfolios.get("inputAudit", {}).get("productionRunId") != baseline or portfolios.get("uploadId") != upload or portfolios.get("comparisonDigest") != comparison_bytes_digest:
            raise ValueError("Portfolio report does not match the exact comparison, baseline and salary upload")
        if portfolios.get("productionChanged") is not False:
            raise ValueError("Portfolio report must remain shadow-only")
        if coherent is not None:
            if portfolios.get("evaluationModel") != coherent.get("manifest", {}).get("version"):
                raise ValueError("Portfolio and coherent report model versions differ")
            if portfolios.get("scenarioManifest") is not None and stable_digest(portfolios["scenarioManifest"]) != stable_digest(coherent["manifest"]):
                raise ValueError("Portfolio and coherent source/seed manifests differ")
    archive_summary = None
    if archived is not None:
        if archived.get("authority") != "postlock_evaluation_only" or archived.get("forecast_inputs_allowed") is not False:
            raise ValueError("Archived outcomes must be explicitly evaluation-only")
        archive_summary = {k: v for k, v in archived.items() if k != "contests"}
        archive_summary["contests"] = [{k: v for k, v in c.items() if k not in ("score_curve", "slot_actuals")}
                                       for c in archived.get("contests", [])]
    players = []
    for p in comparison.get("players", []):
        shadow, base = p.get("shadow") or {}, p.get("baseline") or {}
        if not shadow or not base:
            continue
        players.append({"name": p["name"], "position": p["position"], "team": p["team"], "salary": p["salary"],
            "baseline": base.get("model_proj_fpts"), "candidate": (shadow.get("candidate") or {}).get("mean"),
            "delta": shadow.get("delta", 0), "status": shadow.get("status"), "reason": shadow.get("reason", ""),
            "ledger": shadow.get("ledger", [])})
    report = {"version": VERSION, "authority": "research_display_only", "productionChanged": False,
        "uploadId": upload, "baselineRunId": baseline, "comparisonDigest": digest,
        "comparisonBytesDigest": comparison_bytes_digest, "date": comparison["as_of_at"][:10],
        "capturedAt": comparison["as_of_at"], "baselineVersion": comparison["baseline_version"],
        "salaryRows": comparison["salary_rows"], "players": players,
        "firstKickoff": min((stamp(p["kickoff"]).isoformat() for p in comparison.get("players", []) if p.get("kickoff")), default=None),
        "coherent": coherent, "portfolios": portfolios, "archived": archive_summary,
        "sourceDigests": source_digests or {},
        "missing": (["A matching coherent scenario report is not published."] if coherent is None else []) +
                   (["A matching portfolio comparison is not published."] if portfolios is None else [])}
    if len(json.dumps(report, allow_nan=False).encode()) > 8 * 1024 * 1024:
        raise ValueError("Compact report exceeds the 8 MB publication limit")
    report["reportId"] = stable_digest(report)
    return report

def persist_report(db, report):
    from psycopg2.extras import Json
    if not report.get("firstKickoff") or datetime.now(timezone.utc) >= stamp(report["firstKickoff"]):
        raise ValueError("Pregame publication cutoff passed or target kickoff is missing; no report was written")
    if stable_digest({k:v for k,v in report.items() if k != "reportId"}) != report.get("reportId"):
        raise ValueError("Report content no longer matches its immutable identity")
    db.execute(DDL)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO nfl_matchup_research_reports
            (report_id,upload_id,baseline_run_id,captured_at,comparison_digest,source_digests,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(report_id) DO NOTHING""",
            (report["reportId"], report["uploadId"], report["baselineRunId"], report["capturedAt"],
             report["comparisonDigest"], Json(report["sourceDigests"]), Json(report)))
        inserted = cur.rowcount
    return {"report_id": report["reportId"], "inserted": inserted, "upload_id": report["uploadId"]}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--coherent", type=Path)
    parser.add_argument("--portfolios", type=Path)
    parser.add_argument("--archived", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    values, digests = {}, {}
    for key in ("comparison", "coherent", "portfolios", "archived"):
        path = getattr(args, key)
        if path is not None:
            raw = path.read_bytes(); values[key] = json.loads(raw)
            digests[key] = {"sha256": hashlib.sha256(raw).hexdigest(), "file_name": path.name}
    report = build_report(**values, comparison_bytes_digest=digests["comparison"]["sha256"], source_digests=digests)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    result = {"report_id": report["reportId"], "upload_id": report["uploadId"], "players": len(report["players"]),
              "coherent": report["coherent"] is not None, "portfolios": report["portfolios"] is not None, "applied": args.apply}
    if args.apply:
        from config import load_config
        from db.database import DatabaseManager
        result.update(persist_report(DatabaseManager(load_config().database_url, initialize_schema=False), report))
    print(json.dumps(result))

if __name__ == "__main__":
    main()
