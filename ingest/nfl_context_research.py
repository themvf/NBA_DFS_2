"""Run and optionally persist the NFL context-to-opportunity study."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from model.nfl_context_research import run_study


def persist(db: DatabaseManager, report: dict[str, object]) -> None:
    db.execute(
        """INSERT INTO nfl_context_research_runs(run_id, definition_id, study_version, status, report)
           VALUES (%s,%s,%s,%s,%s)
           ON CONFLICT(run_id) DO NOTHING""",
        (
            report["runId"], report["definitionId"], report["studyVersion"],
            report["status"], Json(report),
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbp", nargs="+", type=Path, help="frozen season PBP parquet files")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--apply", action="store_true", help="persist immutable report to Postgres")
    args = parser.parse_args()
    report, _samples = run_study(args.pbp)
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    if args.apply:
        persist(DatabaseManager(load_config().database_url), report)
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
