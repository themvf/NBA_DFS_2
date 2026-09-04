"""Run and save the NFL model study without paid API calls or optimizer writes.

python -m ingest.nfl_dfs_research --source-root <historical-cache-root> --persist
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from config import load_config
from ingest.nfl_season_schedule import TEAM_ABBREV_OVERRIDES
from model.nfl_dfs_backtest import source_paths, load_source_rows
from model.nfl_dfs_historical import (
    HistoricalWeek, MODEL_CONFIG, MODEL_VERSION, OFFENSE_FIELDS, BOOM_THRESHOLDS,
    ProjectionContext, artifact_digest, project_player,
)
from model.nfl_dfs_research import VERSION, SEED, POSITIONS, evaluate, implied_totals


def normalize_team(team: str) -> str:
    return TEAM_ABBREV_OVERRIDES.get(team, team)


def load_inputs(root: Path, connection) -> tuple[list[HistoricalWeek], dict, list[dict]]:
    rows, sources = load_source_rows(source_paths(root, range(2020, 2026)))
    # Only primitive, relevant inputs enter the frozen research snapshot.
    cleaned = []
    for row in rows:
        stats = {}
        for key in (*OFFENSE_FIELDS, "attempts", "carries", "targets"):
            value = row.stats.get(key)
            stats[key] = float(value) if value is not None and math.isfinite(float(value)) else 0.0
        cleaned.append(HistoricalWeek(
            row.player_id, row.player_gsis_id, row.player_name, row.position,
            row.season, row.week, normalize_team(row.team or ""), normalize_team(row.opponent or ""), stats,
        ))
    metadata = json.loads((root / "artifacts/ff_v2_historical_context_2020_2025.json").read_text())
    source = metadata["sources"]["schedule:all"]
    path = root / (source.get("cachePath") or source["cache_path"])
    frame = pd.read_csv(path, low_memory=False)
    games = {}
    for r in frame.to_dict("records"):
        if r["game_type"] != "REG":
            continue
        total, spread = r.get("total_line"), r.get("spread_line")
        valid_market = pd.notna(total) and pd.notna(spread)
        home_implied, away_implied = implied_totals(float(total), float(spread)) if valid_market else (None, None)
        for home in (True, False):
            team = normalize_team(r["home_team"] if home else r["away_team"])
            games[(int(r["season"]), int(r["week"]), team)] = {
                "game_id": r["game_id"], "team_implied": home_implied if home else away_implied,
                "opponent_implied": away_implied if home else home_implied,
                "team_spread": (-float(spread) if home else float(spread)) if valid_market else None,
                "market_source": "nflverse historical closing reference; availability timestamp absent",
            }
    sources.append({"path": str(path.relative_to(root)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""SELECT DISTINCT ON (r.player_week_stat_id) r.*
          FROM nfl_dfs_player_week_results r
          WHERE r.position='DST' AND r.scoring_status='exact' AND r.scoring_version='nfl-dk-realized-v2'
          ORDER BY r.player_week_stat_id,r.computed_at DESC,r.id DESC""")
        dst = [dict(r) for r in cursor.fetchall()]
    for r in dst:
        team = normalize_team(r["team"])
        cleaned.append(HistoricalWeek(
            int(artifact_digest({"dst": team})[:15], 16), f"DST:{team}", team + " DST", "DST",
            r["season"], r["week"], team, normalize_team(r["opponent"]),
            {"fantasy_points": r["actual_dk_fpts"]},
        ))
    sources.append({"source": "versioned DST ledger", "rows": len(dst), "sha256": artifact_digest(dst),
                    "source_ids": [r["id"] for r in dst], "scoring_reconciled_to_contest_csv": False})
    return cleaned, games, sources


def build_samples(rows: list[HistoricalWeek], games: dict, draws: int = 200) -> tuple[list[dict], dict]:
    samples = []
    exclusions: Counter = Counter()
    by_week: dict[tuple[int, int], list[HistoricalWeek]] = defaultdict(list)
    for row in rows:
        by_week[row.chronological_key].append(row)
    prior: dict[str, list[HistoricalWeek]] = defaultdict(list)
    own_history: dict[int, list[HistoricalWeek]] = defaultdict(list)
    for (season, week), targets in sorted(by_week.items()):
        if season >= 2023:
            for target in targets:
                if target.position not in POSITIONS:
                    continue
                own = own_history[target.player_id][-34:]
                if len(own) < 2:
                    exclusions[f"{season}:{target.position}:fewer_than_two_prior_games"] += 1
                    continue
                game = games.get((season, week, target.team))
                if not game:
                    exclusions[f"{season}:{target.position}:unmapped_game"] += 1
                    continue
                projection = project_player(
                    player_id=target.player_id, player_gsis_id=target.player_gsis_id,
                    player_name=target.player_name, position=target.position,
                    historical_rows=prior[target.position], cutoff_season=season, cutoff_week=week,
                    context=ProjectionContext(), seed=SEED, config={**MODEL_CONFIG, "draws": draws},
                )
                opportunity_key = "attempts" if target.position == "QB" else "targets"
                opportunity = np.mean([
                    float(r.stats.get(opportunity_key, 0)) + (float(r.stats.get("carries", 0)) if target.position == "RB" else 0)
                    for r in own[-4:]
                ])
                samples.append({
                    "sample_key": f"{season}:{week}:{target.player_gsis_id}",
                    "player_id": target.player_id, "player_name": target.player_name,
                    "season": season, "week": week, "position": target.position,
                    "team": target.team, "opponent": target.opponent, **game,
                    "actual": target.dk_points, "baseline": projection.model_proj_fpts,
                    "own_history_baseline": projection.baseline_fpts,
                    "history_games": len(own), "prior_opportunity": float(opportunity),
                    "history_cutoff": list(max(r.chronological_key for r in prior[target.position])),
                    "p10": projection.floor_fpts, "p90": projection.ceiling_fpts,
                    "boom_probability": projection.boom_rate, "boom_threshold": BOOM_THRESHOLDS[target.position],
                })
            print(f"Built {season} week {week}: {len(samples)} samples", flush=True)
        # Add outcomes only AFTER every forecast for the week is frozen.
        for row in targets:
            prior[row.position].append(row)
            own_history[row.player_id].append(row)
    return samples, dict(exclusions)


def save_json(path: Path, value, compressed: bool = False) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()
    if compressed:
        encoded = gzip.compress(encoded, mtime=0)
    if path.exists() and path.read_bytes() != encoded:
        raise ValueError(f"Refusing to overwrite different immutable artifact: {path}")
    path.write_bytes(encoded)


def persist(connection, report: dict, predictions: list[dict], history: list[HistoricalWeek]) -> None:
    from db.schema import TABLES
    with connection.cursor() as cursor:
        for ddl in TABLES:
            if "CREATE TABLE IF NOT EXISTS nfl_dfs_research_" in ddl:
                cursor.execute(ddl)
        cursor.execute("""INSERT INTO nfl_dfs_research_runs(run_id,report)
           VALUES (%s,%s) ON CONFLICT DO NOTHING""", (report["run_id"], Json(report)))
        execute_values(cursor, """INSERT INTO nfl_dfs_research_samples(run_id,sample_key,variant,payload)
          VALUES %s ON CONFLICT DO NOTHING""",
          [(report["run_id"], r["sample_key"], r["model"], Json(r)) for r in predictions], page_size=500)
        execute_values(cursor, """INSERT INTO nfl_dfs_research_history(run_id,row_key,payload)
          VALUES %s ON CONFLICT DO NOTHING""",
          [(report["run_id"], f"{r.season}:{r.week}:{r.player_gsis_id}", Json(r.__dict__)) for r in history], page_size=500)
    connection.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--draws", type=int, default=200)
    parser.add_argument("--persist", action="store_true")
    args = parser.parse_args()
    conn = psycopg2.connect(load_config().database_url)
    try:
        rows, games, sources = load_inputs(args.source_root, conn)
    finally:
        conn.close()
    # No database connection is held during the long pure calculation.
    if rows:
        input_digest = artifact_digest({"rows": [r.__dict__ for r in rows], "games": sorted(games.items()), "draws": args.draws, "version": VERSION})
        directory = Path("artifacts") / f"nfl_dfs_research_{input_digest[:16]}"
        directory.mkdir(exist_ok=True, parents=True)
        cache = directory / "samples.json.gz"
        if cache.exists():
            cached = json.loads(gzip.decompress(cache.read_bytes()))
            samples, exclusions = cached["samples"], cached["exclusions"]
        else:
            samples, exclusions = build_samples(rows, games, args.draws)
            save_json(cache, {"samples": samples, "exclusions": exclusions}, True)
        report, predictions = evaluate(samples)
        implementation = {str(p).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in (
            Path("model/nfl_dfs_research.py"), Path("ingest/nfl_dfs_research.py"), Path("model/nfl_dfs_historical.py"),
        )}
        code_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        study_id = artifact_digest({"input_digest": input_digest, "implementation": implementation, "code_commit": code_commit})
        report.update({
            "run_id": study_id, "input_digest": input_digest, "implementation": implementation,
            "model_version": MODEL_VERSION, "draws": args.draws,
            "source_evidence": sources, "exclusions": exclusions,
            "cohort": "full-source recorded player stats, not current-player membership; DST 32-team ledger; >=2 prior weeks; DNPs not inferred",
            "limitations": ["Not a salary-slate cohort", "2025 previously inspected", "closing references are not verified checkpoint observations", "DST derived scoring not reconciled to DK contest exports"],
            "code_commit": code_commit,
        })
        report["output_digest"] = artifact_digest({"report": report, "predictions": predictions})
        results_dir = directory / study_id[:16]
        results_dir.mkdir(exist_ok=True)
        save_json(results_dir / "report.json", report)
        save_json(results_dir / "predictions.json.gz", predictions, True)
        save_json(directory / "history.json.gz", [r.__dict__ for r in rows], True)
        if args.persist:
            with psycopg2.connect(load_config().database_url) as output_connection:
                persist(output_connection, report, predictions, rows)
        print(json.dumps({"directory": str(results_dir), "samples": len(samples),
            "shadow_eligible": [k for k,v in report["candidates"].items() if v["status"] == "eligible_for_shadow_only"]}, indent=2))


if __name__ == "__main__":
    main()
