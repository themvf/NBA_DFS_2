"""Freeze forward-only NFL research forecasts; never write optimizer inputs.

Recipes are pinned by study ID and output digest. Candidate eligibility comes
from the saved study, not a human-friendly model name. Settlements append new
evidence without changing the pregame payload.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from config import load_config
from db.schema import TABLES
from ingest.nfl_dfs_projections import _history, _players, _slate_environment, infer_target_week
from model.nfl_dfs_historical import HistoricalWeek, MODEL_CONFIG, ProjectionContext, artifact_digest, project_player, BOOM_THRESHOLDS
from model.nfl_dfs_research import SEED, POSITIONS, predict, metrics, clustered_mae_delta


class Reader:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, params=()):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            return [dict(r) for r in cursor.fetchall()]


def candidate_allowed(report: dict, position: str) -> bool:
    return report.get("candidates", {}).get(f"{position}:opportunity", {}).get("status") == "eligible_for_shadow_only"


def require_pregame(now: datetime, kickoff: datetime) -> None:
    if now.tzinfo is None or kickoff.tzinfo is None or now >= kickoff:
        raise ValueError("shadow forecasts must be frozen strictly before kickoff")


def matches_source_digest(content: bytes, expected: str) -> bool:
    """Honor existing byte-level study pins across Git LF/CRLF checkouts only."""
    lf = content.replace(b"\r\n", b"\n")
    return expected in {hashlib.sha256(value).hexdigest()
                        for value in (content, lf, lf.replace(b"\n", b"\r\n"))}


def freeze(connection, report: dict, season: int, week: int, now: datetime) -> dict:
    baseline_source = Path("model/nfl_dfs_historical.py").read_bytes()
    baseline_hash = hashlib.sha256(baseline_source).hexdigest()
    if not matches_source_digest(baseline_source, report["implementation"]["model/nfl_dfs_historical.py"]):
        raise ValueError("Baseline implementation drifted from the pinned study; rerun research before shadow")
    shadow_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    reader = Reader(connection)
    historical = reader.execute("SELECT payload FROM nfl_dfs_research_history WHERE run_id=%s ORDER BY row_key", (report["run_id"],))
    if not historical:
        raise ValueError("Study's full-cohort frozen history is missing")
    history = [HistoricalWeek(**r["payload"]) for r in historical]
    last_study_season = max(r.season for r in history)
    # Append only new seasons to the full source cohort, without replacing
    # retired players with the current-player convenience universe.
    for row in _history(reader, season, week):
        if row.season <= last_study_season:
            continue
        key = f"DST:{row.team}" if row.position == "DST" else row.player_gsis_id
        if not key:
            continue
        stable = int(artifact_digest({"dst": row.team} if row.position == "DST" else {"gsis_id": key})[:15], 16)
        history.append(HistoricalWeek(stable, key, row.player_name, row.position, row.season, row.week, row.team, row.opponent, row.stats))
    history_digest = artifact_digest([r.__dict__ for r in history])
    # Release the read transaction before the potentially lengthy simulation.
    connection.commit()
    environment = _slate_environment(reader, season, week)
    players = _players(reader, season, sorted(environment))
    eligible = [r for r in players if r["position"] in POSITIONS]
    inserts = []
    skipped = 0
    for player in eligible:
        env = environment[player["team_abbrev"]]
        kickoff = env["commence_time"]
        if kickoff is None or now >= kickoff:
            skipped += 1
            continue
        require_pregame(now, kickoff)
        position = player["position"]
        gsis = f"DST:{player['team_abbrev']}" if position == "DST" else player["gsis_id"]
        if not gsis:
            skipped += 1
            continue
        stable = int(artifact_digest({"dst": player["team_abbrev"]} if position == "DST" else {"gsis_id": gsis})[:15], 16)
        priors = [r for r in history if r.position == position and r.chronological_key < (season, week)]
        own = sorted([r for r in priors if r.player_id == stable], key=lambda r:r.chronological_key)[-34:]
        if len(own) < 2:
            skipped += 1
            continue
        projection = project_player(player_id=stable, player_gsis_id=gsis, player_name=player["canonical_name"], position=position,
            historical_rows=priors, cutoff_season=season, cutoff_week=week, context=ProjectionContext(),
            seed=SEED, config={**MODEL_CONFIG, "draws": report["draws"]})
        opportunity_key = "attempts" if position == "QB" else "targets"
        row = {
            "season": season, "week": week,
            "shadow_version": "nfl-dfs-shadow-v1", "shadow_implementation_digest": shadow_hash,
            "baseline_implementation_digest": baseline_hash,
            "baseline_config": {**MODEL_CONFIG, "draws": report["draws"]}, "seed": SEED,
            "baseline": projection.model_proj_fpts, "history_games": len(own),
            "prior_opportunity": float(np.mean([float(r.stats.get(opportunity_key, 0) or 0) +
                (float(r.stats.get("carries", 0) or 0) if position == "RB" else 0) for r in own[-4:]])),
            "position": position, "player_name": player["canonical_name"], "gsis_id": gsis,
            "p10": projection.floor_fpts, "p90": projection.ceiling_fpts,
            "boom_probability": projection.boom_rate, "boom_threshold": BOOM_THRESHOLDS[position],
            "history_digest": history_digest, "history_cutoff": list(max(r.chronological_key for r in priors)),
            "source_study_digest": report["output_digest"], "candidate": None,
            "optimizer_effect": "none", "population": "canonical active weekly team roster, not a DK salary slate",
        }
        if candidate_allowed(report, position):
            recipe = report["candidates"][f"{position}:opportunity"]["recipe"]
            residuals = np.array(recipe["residuals"])
            p = predict(recipe, row)
            row["candidate"] = {"prediction": p, "p10": p+float(np.quantile(residuals,.1)),
                "p90": p+float(np.quantile(residuals,.9)),
                "boom_probability": float(np.mean(p+residuals >= row["boom_threshold"])),
                "recipe_digest": artifact_digest(recipe)}
        inserts.append((report["run_id"], player["id"], season, week, now, kickoff, Json(row), artifact_digest(row)))
    # Computation may take minutes. Stamp availability after it finishes,
    # dropping games that began while forecasts were being calculated.
    completed_at = datetime.now(timezone.utc)
    inserts = [(study, player, s, w, completed_at, kick, payload, digest)
               for study, player, s, w, _start, kick, payload, digest in inserts if completed_at < kick]
    with connection.cursor() as cursor:
        if inserts:
            execute_values(cursor, """INSERT INTO nfl_dfs_shadow_predictions
              (study_run_id,player_id,season,week,captured_at,kickoff,payload,input_digest)
              VALUES %s ON CONFLICT DO NOTHING""", inserts, page_size=500)
    connection.commit()
    return {"frozen": len(inserts), "skipped": skipped, "optimizer_effect": "none"}


def settle(connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute("""INSERT INTO nfl_dfs_shadow_outcomes(prediction_id,result_id,payload)
          SELECT p.id,r.id,jsonb_build_object('actual',r.actual_dk_fpts,'game_id',r.game_id,
            'scoring_version',r.scoring_version,'source_digest',r.input_digest,
            'scoring_status',r.scoring_status,'exclusion_reason',r.exclusion_reason)
          FROM nfl_dfs_shadow_predictions p
          JOIN LATERAL (
            SELECT r.* FROM nfl_dfs_player_week_results r
            WHERE r.player_id=p.player_id AND r.season=p.season AND r.week=p.week
            ORDER BY r.computed_at DESC,r.id DESC LIMIT 1
          ) r ON TRUE
          JOIN nfl_season_games g ON g.id=r.game_id
          WHERE g.completed AND p.kickoff < NOW()
          ON CONFLICT DO NOTHING""")
        count = cursor.rowcount
    connection.commit()
    return count


def evaluation(connection, study_id: str) -> dict:
    # One (last accepted) pregame forecast per player-week, one latest outcome;
    # daily freezes never inflate the effective sample size.
    reader = Reader(connection)
    records = reader.execute("""SELECT DISTINCT ON(p.player_id,p.season,p.week) p.payload,o.payload outcome
      FROM nfl_dfs_shadow_predictions p
      LEFT JOIN LATERAL (SELECT o.payload FROM nfl_dfs_shadow_outcomes o
        JOIN nfl_dfs_player_week_results r ON r.id=o.result_id WHERE o.prediction_id=p.id
        ORDER BY r.computed_at DESC,r.id DESC LIMIT 1) o ON TRUE
      WHERE p.study_run_id=%s
      ORDER BY p.player_id,p.season,p.week,p.captured_at DESC,p.id DESC""", (study_id,))
    rows = [r for r in records if r["outcome"] and r["outcome"].get("actual") is not None
            and r["outcome"].get("scoring_status", "exact") == "exact"]
    cohorts = {}
    for position in POSITIONS:
        paired = []
        baselines = []
        for record in rows:
            p, outcome = record["payload"], record["outcome"]
            if p["position"] == position and p["candidate"] is not None:
                paired.append({**p, **p["candidate"], **outcome})
                baselines.append({**p, **outcome})
        cohorts[position] = {"n": len(paired), "candidate": metrics(paired),
            "baseline": metrics(baselines, "baseline"),
            "mae_delta": clustered_mae_delta(paired),
            "status": "awaiting_forward_results" if not paired else "forward_shadow_only"}
    counts = reader.execute("""SELECT COUNT(DISTINCT (player_id,season,week))::int n
      FROM nfl_dfs_shadow_predictions WHERE study_run_id=%s""", (study_id,))[0]
    return {"study_run_id": study_id, "cohorts": cohorts, "production_promotion": False,
            "weekly": weekly_metrics(rows),
            "frozen_player_weeks": counts["n"], "scored_player_weeks": len(rows),
            "unscored_player_weeks": counts["n"]-len(rows),
            "unscored_policy": "pending or no matching stat row; not assumed DNP or zero"}


def weekly_metrics(records: list[dict]) -> list[dict]:
    """One already-deduplicated forecast per player-week, grouped explicitly."""
    groups = {}
    for record in records:
        p, outcome = record["payload"], record["outcome"]
        key = (p["season"], p["week"], p["position"])
        groups.setdefault(key, []).append((p, outcome))
    return [{"season": key[0], "week": key[1], "position": key[2],
             "baseline": metrics([{**p, **o} for p, o in values], "baseline"),
             "candidate": metrics([{**p, **p["candidate"], **o}
                                   for p, o in values if p["candidate"] is not None])}
            for key, values in sorted(groups.items())]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("artifacts/nfl_dfs_shadow_config.json"))
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--settle-only", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    now = datetime.now(timezone.utc).replace(second=0,microsecond=0)
    season = args.season or (now.year-1 if now.month <= 3 else now.year)
    with psycopg2.connect(load_config().database_url) as connection:
        with connection.cursor() as cursor:
            for ddl in TABLES:
                if "CREATE TABLE IF NOT EXISTS nfl_dfs_shadow_" in ddl:
                    cursor.execute(ddl)
        connection.commit()
        reader = Reader(connection)
        reports = reader.execute("SELECT report FROM nfl_dfs_research_runs WHERE run_id=%s", (config["study_run_id"],))
        if not reports or reports[0]["report"]["output_digest"] != config["output_digest"]:
            raise ValueError("Pinned study missing or digest does not match")
        report = reports[0]["report"]
        settled = settle(connection)
        result = {"settled": settled}
        if not args.settle_only:
            upcoming = reader.execute("""SELECT COUNT(*)::int n FROM nfl_season_games
                WHERE season=%s AND game_type='REG' AND kickoff > NOW()""", (season,))[0]["n"]
            if season < config["fresh_evaluation_season"]:
                raise ValueError("Cannot freeze a retrospective season as forward shadow")
            if upcoming:
                week = args.week or infer_target_week(reader, season)
                result.update(freeze(connection, report, season, week, now))
            else:
                result["freeze_status"] = "no_upcoming_regular_season_games"
        result["evaluation"] = evaluation(connection, report["run_id"])
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO nfl_dfs_shadow_evaluations(evaluation_digest,study_run_id,payload)
              VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
              (artifact_digest(result["evaluation"]), report["run_id"], Json(result["evaluation"])))
        connection.commit()
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
