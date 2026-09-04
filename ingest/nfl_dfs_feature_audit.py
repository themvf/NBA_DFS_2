"""Save a reproducible audit of stored NFL inputs; no external data calls."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import Json
from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_feature_audit import build_audit, digest, normalize


def collect(db, study_id):
    # Snapshot both populations in one repeatable-read transaction.
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("SELECT row_key,payload FROM nfl_dfs_research_history WHERE run_id=%s ORDER BY row_key", (study_id,))
            frozen = cursor.fetchall()
            cursor.execute("""SELECT w.*,p.position,p.gsis_id FROM ff_player_week_stats w
                LEFT JOIN ff_players p ON p.id=w.player_id
                WHERE w.season_type='REG' ORDER BY w.id""")
            working = cursor.fetchall()
    if not frozen:
        raise ValueError("Pinned research history is missing; refusing a misleading partial audit")
    return {"frozen_history": [normalize(r, "frozen_history") for r in frozen],
            "working_source": [normalize(r, "working_source") for r in working]}


def persist(db, report, evidence):
    report_digest = digest(report)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO nfl_dfs_feature_audits
                (audit_digest,version,payload,input_evidence) VALUES (%s,%s,%s,%s)
                ON CONFLICT (audit_digest) DO NOTHING""",
                (report_digest, report["version"], Json(report), Json(evidence)))
    return report_digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="artifacts/nfl_dfs_shadow_config.json")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    db = PipelineDatabase(load_config().database_url)
    evidence = collect(db, config["study_run_id"])
    report = build_audit(evidence, datetime.now(timezone.utc).isoformat(), config["study_run_id"])
    report["implementation"] = {p: digest(Path(p).read_text()) for p in (
        "model/nfl_dfs_feature_audit.py", "ingest/nfl_dfs_feature_audit.py")}
    report_digest = persist(db, report, evidence)
    print(json.dumps({"audit_digest": report_digest, "datasets": [
        {k: d[k] for k in ("dataset", "scanned", "eligible", "excluded", "cohorts")} for d in report["datasets"]]}))


if __name__ == "__main__":
    main()
