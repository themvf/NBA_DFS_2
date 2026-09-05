"""Freeze research-only conditional efficiency and exact DK scoring forecasts."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from psycopg2.extras import Json

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_efficiency import CONFIG, RARE_FIELDS, RATE_DEFS, VERSION, backtest, build, metrics
from model.nfl_dfs_feature_audit import digest


ROOT = Path(__file__).resolve().parents[1]
DATASET_VERSION = "nfl-dfs-efficiency-components-v2"


def file_digest(relative_path: str) -> str:
    return sha256((ROOT / relative_path).read_bytes()).hexdigest()


def efficiency_fields() -> tuple[str, ...]:
    fields = set(RARE_FIELDS)
    for spec in RATE_DEFS.values():
        fields.add(str(spec["numerator"]))
        definition = spec["denominator"]
        fields.update((definition,) if isinstance(definition, str) else definition or ())
    return tuple(sorted(fields))


def raw_history(db: PipelineDatabase) -> list[dict]:
    rows = db.execute(
        """SELECT w.id,p.gsis_id,p.position,w.season,w.week,w.team,w.opponent,
                  w.source,w.fetched_at,w.source_row
           FROM ff_player_week_stats w
           JOIN ff_players p ON p.id=w.player_id
           WHERE w.season_type='REG' AND w.source='nflverse'
             AND p.position=ANY(%s)
           ORDER BY w.season,w.week,w.id""",
        (["QB", "RB", "WR", "TE"],),
    )
    fields = efficiency_fields()
    history = []
    for row in rows:
        raw = row["source_row"] if isinstance(row["source_row"], dict) else {}
        stats = {key: raw[key] for key in fields if key in raw}
        history.append({
            "record_id": row["id"], "identity": row["gsis_id"], "position": row["position"],
            "season": row["season"], "week": row["week"], "team": row["team"],
            "opponent": row["opponent"], "source": row["source"],
            "source_hash": digest(raw), "fetched_at": row["fetched_at"].isoformat(), "stats": stats,
        })
    dst_rows = db.execute(
        """SELECT w.id,w.season,w.week,w.team,w.opponent,r.id result_id,r.computed_at,
                  r.input_digest,r.scoring_version,r.actual_dk_fpts,r.scoring_evidence
           FROM ff_player_week_stats w
           JOIN ff_players p ON p.id=w.player_id
           JOIN LATERAL (
             SELECT result.* FROM nfl_dfs_player_week_results result
             WHERE result.player_week_stat_id=w.id AND result.scoring_status='exact'
             ORDER BY result.computed_at DESC,result.id DESC LIMIT 1
           ) r ON TRUE
           WHERE w.season_type='REG' AND w.source='nflverse' AND p.position='DST'
           ORDER BY w.season,w.week,w.id"""
    )
    for row in dst_rows:
        evidence = row["scoring_evidence"] if isinstance(row["scoring_evidence"], dict) else {}
        components = evidence.get("scoring_components") or {}
        stats = {key: value for key, value in components.items() if isinstance(value, (int, float))}
        stats["fantasy_points"] = float(row["actual_dk_fpts"])
        history.append({
            "record_id": f"dst:{row['result_id']}", "identity": f"DST:{row['team']}", "position": "DST",
            "season": row["season"], "week": row["week"], "team": row["team"], "opponent": row["opponent"],
            "source": f"nflverse+{row['scoring_version']}", "source_hash": row["input_digest"],
            "fetched_at": row["computed_at"].isoformat(), "stats": stats,
        })
    history.sort(key=lambda row: (row["season"], row["week"], str(row["record_id"])))
    return history


def workload_run(db: PipelineDatabase, season: int | None, week: int | None) -> dict:
    where = []
    params: list[object] = []
    if season is not None:
        where.append("season=%s")
        params.append(season)
    if week is not None:
        where.append("week=%s")
        params.append(week)
    clause = "WHERE " + " AND ".join(where) if where else ""
    rows = db.execute(
        f"SELECT run_digest,dataset_digest,season,week,as_of_at,payload FROM nfl_dfs_workload_runs {clause} ORDER BY as_of_at DESC,run_digest DESC LIMIT 1",
        tuple(params),
    )
    if not rows:
        raise RuntimeError("No saved workload run matches the requested season/week")
    return dict(rows[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    db = PipelineDatabase(load_config().database_url)
    workload = workload_run(db, args.season, args.week)
    history = raw_history(db)
    dataset = {
        "version": DATASET_VERSION,
        "missing_policy": "missing remains absent; zero denominators are undefined rates",
        "players": history,
        "source_rows": len(history),
    }
    dataset_digest = digest(dataset)
    forecasts = build(workload["payload"], history)
    evaluated = backtest(history)
    report = {
        "version": VERSION,
        "config": CONFIG,
        "dataset_digest": dataset_digest,
        "workload_run_digest": workload["run_digest"],
        "workload_dataset_digest": workload["dataset_digest"],
        "season": workload["season"],
        "week": workload["week"],
        "as_of_at": now.isoformat(),
        "forecasts": forecasts,
        "backtest": {
            "status": "retrospective_2024_2025_previously_inspected_oracle_denominators",
            "metrics": metrics(evaluated),
            "rows": len(evaluated),
        },
        "implementation": {
            "model_sha256": file_digest("model/nfl_dfs_efficiency.py"),
            "ingest_sha256": file_digest("ingest/nfl_dfs_efficiency.py"),
            "scorer_sha256": file_digest("model/nfl_dfs_historical.py"),
        },
        "production_changed": False,
        "coherence_scope": "team_coupled_offense_plus_separate_dst",
        "limits": [
            "Efficiency draws condition on the research workload allocation and unresolved active-season roster.",
            "The retrospective rate check uses realized opportunity denominators to isolate efficiency; it is not a full fantasy projection backtest.",
            "Offensive draws share one team state; opportunity budgets and passing/receiving completions, yards, and touchdowns reconcile exactly, including visible unallocated-role buckets.",
            "DST is modeled separately by whole-game exact-component resampling; opponent context is not yet an explicit conditional feature.",
        ],
    }
    run_digest = digest(report)
    dump = lambda value: json.dumps(value, default=str, allow_nan=False)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO nfl_dfs_component_datasets VALUES (%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
                (dataset_digest, DATASET_VERSION, Json(dataset, dumps=dump)),
            )
            cursor.execute(
                """INSERT INTO nfl_dfs_efficiency_runs
                   (run_digest,workload_run_digest,dataset_digest,season,week,as_of_at,payload)
                   VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (run_digest, workload["run_digest"], dataset_digest, workload["season"], workload["week"], now, Json(report, dumps=dump)),
            )
    player_count = sum(len(forecast["players"]) for forecast in forecasts)
    print(json.dumps({
        "dataset_digest": dataset_digest, "run_digest": run_digest,
        "workload_run_digest": workload["run_digest"], "players": player_count,
        "backtest": report["backtest"],
    }))


if __name__ == "__main__":
    main()
