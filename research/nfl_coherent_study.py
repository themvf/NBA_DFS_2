"""Read immutable compact reports and exact outcome revisions for WIS grading."""
from __future__ import annotations

import json
from pathlib import Path

from model.nfl_coherent_study import grade_coherent
from model.nfl_matchup_study import digest
from model.nfl_dfs_context_variant_study import timestamp


def registered_manifests():
    paths = sorted(Path("research").glob("nfl_coherent_scenario_study_v*.json"))
    current = Path("research/nfl_coherent_scenario_study.json")
    if current.exists():
        paths.append(current)
    manifests = {}
    for path in paths:
        value = json.loads(path.read_text(encoding="utf-8"))
        if value.get("registered_at"):
            manifests[digest(value)] = value
    return list(manifests.values())


def normalize_distribution(value):
    if not value:
        return None
    return {**value,"median":value.get("p50")}


def frozen_rows(records, manifest, outcomes):
    rows=[]
    for record in records:
        payload=record["payload"]
        coherent=payload.get("coherent") or {}
        source=(coherent.get("manifest") or {}).get("sources") or {}
        for row in coherent.get("paired_grading_rows",[]):
            outcome=outcomes.get((str(row["player_id"]),row["game_id"]),{})
            available=max(timestamp(source["captured_at"]),timestamp(record["baseline_created_at"]))
            rows.append({"forecast_id":f"{record['report_id']}:{row['player_id']}","player_id":row["player_id"],
                "game_id":row["game_id"],"season":record["season"],"week":record["week"],"format":record["format"],
                "position":row["position"],"kickoff":row["kickoff"],"captured_at":row["decision_at"],
                "available_at":available,"published_at":record["published_at"],"model_version":coherent.get("version"),
                "registration_hash":digest(source.get("registration",{})),"implementation_hashes":source.get("implementation_hashes"),
                "draws":coherent.get("manifest",{}).get("draws"),"baseline_run_id":str(record["baseline_run_id"]),
                "baseline_identity_valid":str(row.get("baseline_run_id"))==str(record["baseline_run_id"])==str(source.get("baseline_run_id")),
                "baseline_config_hash":digest({"model_version":record["baseline_version"],"model_config":record["baseline_config"]}),
                "input_manifest_hash":digest({"sources":source,"row":row,"report_id":record["report_id"]}),
                "retrospective":source.get("retrospective") is not False or coherent.get("status")!="coherent_research_unqualified",
                "baseline_reproduced":row.get("status")=="paired" and bool(source.get("baseline_marginal_file_sha256")),
                "baseline":normalize_distribution(row.get("baseline")),"candidate":normalize_distribution(row.get("candidate")),
                "scoring_version":outcome.get("scoring_version") or manifest.get("outcome_scoring_version",manifest.get("scoring_version")),
                "scoring_status":outcome.get("scoring_status"),"actual":outcome.get("actual_dk_fpts"),
                "result_id":outcome.get("id"),"result_digest":outcome.get("input_digest"),"result_at":outcome.get("computed_at")})
    return rows


def coherent_reports(db,season,now):
    manifests=registered_manifests()
    if not manifests:
        return []
    exists=db.execute_one("SELECT to_regclass('nfl_matchup_research_reports') relation")["relation"]
    complete=[(r["season"],r["week"]) for r in db.execute("""SELECT season,week FROM nfl_season_games
        WHERE season=%s AND game_type='REG' GROUP BY season,week HAVING bool_and(completed) AND max(kickoff)<%s""",(season,now))]
    outcomes={}
    if exists:
        for row in db.execute("""SELECT DISTINCT ON(r.player_id,r.game_id) r.id,r.player_id,g.nflverse_game_id,
            r.actual_dk_fpts,r.scoring_status,r.scoring_version,r.input_digest,r.computed_at
            FROM nfl_dfs_player_week_results r JOIN nfl_season_games g ON g.id=r.game_id
            WHERE r.season=%s AND g.completed AND r.computed_at<=%s
            ORDER BY r.player_id,r.game_id,r.computed_at DESC,r.id DESC""",(season,now)):
            outcomes[(str(row["player_id"]),row["nflverse_game_id"])]=row
    reports=[]
    for manifest in manifests:
        records=db.execute("""SELECT r.report_id,r.published_at,r.baseline_run_id,r.payload,
            b.season,b.week,b.model_version baseline_version,b.model_config baseline_config,b.created_at baseline_created_at,u.format
            FROM nfl_matchup_research_reports r JOIN nfl_dfs_projection_runs b ON b.run_id=r.baseline_run_id
            JOIN nfl_dfs_slate_uploads u ON u.upload_id=r.upload_id
            WHERE b.season=%s AND r.published_at>=%s AND r.published_at<=%s AND r.payload->'coherent'->>'version'=%s""",
            (season,manifest["registered_at"],now,manifest["model_version"])) if exists else []
        rows=frozen_rows(records,manifest,outcomes)
        cohorts=list(manifest.get("cohorts",{})) or [None]
        for cohort in cohorts:
            report=grade_coherent(manifest,rows,now=now,complete_weeks=complete,cohort=cohort)
            report.update(published_reports=len(records),published_forecast_rows=len(rows),
                          source="immutable compact reports and exact scoring revisions")
            reports.append(report)
    return reports
