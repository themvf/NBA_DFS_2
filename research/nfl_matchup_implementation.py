"""Fit retrospective challengers and freeze a paired real-slate comparison.

python -m research.nfl_matchup_implementation --season 2026 --week 3 --fit --persist
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_projections import _history, infer_target_week
from ingest.nfl_matchup_context import load_matchups, persist_matchups, persist_forecasts
from model.nfl_context_engine import stable_digest
from model.nfl_matchup_projection import VERSION, fit_family, sample_baseline_draws, shadow_projection
from model.nfl_pfr_supplement import team_code

MODEL_PATH = Path("artifacts/nfl_matchup_models_v1.json")


def fit_models(db, development_dir):
    crosswalk = {r["external_id"]: r["gsis_id"] for r in db.execute(
        "SELECT external_id,gsis_id FROM nfl_player_identity_crosswalk WHERE namespace='pfr' AND status='resolved'")}
    hist = db.execute("""SELECT p.gsis_id,p.position,w.season,w.week,w.team,w.opponent,w.source_row
        FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id
        WHERE w.season BETWEEN 2023 AND 2025 AND w.season_type='REG' AND p.position IN ('QB','RB')
        ORDER BY w.season,w.week,p.gsis_id""")
    positions = {r["gsis_id"]:r["position"] for r in hist}
    stats = {(r["season"],r["week"],r["gsis_id"]):r for r in hist}
    rows = []
    game_inputs = []
    for season in (2023,2024,2025):
        passing = pd.read_csv(development_dir / f"advstats_week_pass_{season}.csv")
        rushing = pd.read_csv(development_dir / f"advstats_week_rush_{season}.csv")
        # Training is explicitly retrospective; today's crosswalk isn't
        # claimed to have been known before these historical games.
        for f in (passing,rushing):
            f["team"] = f.team.map(team_code); f["opponent"] = f.opponent.map(team_code)
            f["gsis_id"] = f.pfr_player_id.map(crosswalk)
        rushing = rushing[rushing.gsis_id.map(positions) == "RB"]
        game_features = {}
        for (game_id,team), group in passing.groupby(["game_id","team"]):
            game_features[(game_id,team)] = {"season":season,"week":int(group.week.iloc[0]),"opponent":group.opponent.iloc[0],
                "pressure":float(group.times_pressured_pct.iloc[0])*100 if len(group)==1 and pd.notna(group.times_pressured_pct.iloc[0]) else None}
        for (game_id,team), group in rushing.groupby(["game_id","team"]):
            complete=group.dropna(subset=["carries","rushing_yards_before_contact","rushing_yards_after_contact"])
            n=float(complete.carries.sum())
            feature=game_features.setdefault((game_id,team),{"season":season,"week":int(group.week.iloc[0]),"opponent":group.opponent.iloc[0],"pressure":None})
            feature.update(carries=n,ybc=float(complete.rushing_yards_before_contact.sum())/n if n else None,
                           yac=float(complete.rushing_yards_after_contact.sum())/n if n else None)
        for (game_id,team), cur in sorted(game_features.items(),key=lambda kv:kv[1]["week"]):
            week=cur["week"]; opponent=cur["opponent"]
            own=sorted([v for (g,t),v in game_features.items() if t==team and v["week"]<week],key=lambda v:v["week"])[-4:]
            opp=sorted([v for (g,t),v in game_features.items() if v["opponent"]==opponent and v["week"]<week],key=lambda v:v["week"])[-4:]
            def avg(items,key):
                values=[r[key] for r in items if r.get(key) is not None]
                return float(np.mean(values)) if len(values)>=2 else None
            def rate(items,key):
                valid=[r for r in items if r.get(key) is not None and r.get("carries",0)>0]
                n=sum(r["carries"] for r in valid)
                return sum(r[key]*r["carries"] for r in valid)/n if n>=20 else None
            game_inputs.append({"game_id":game_id,"season":season,"week":week,"team":team,
                "own_pressure":avg(own,"pressure"),"opp_pressure":avg(opp,"pressure"),
                "own_ybc":rate(own,"ybc"),"opp_ybc":rate(opp,"ybc"),"own_yac":rate(own,"yac"),"opp_yac":rate(opp,"yac")})
            for family, frame, position, units, yards in (("pressure",passing,"QB","attempts","passing_yards"),
                                                        ("contact",rushing,"RB","carries","rushing_yards")):
                features={"own_pressure":avg(own,"pressure"),"opp_pressure":avg(opp,"pressure")} if family=="pressure" else {
                    "own_ybc":rate(own,"ybc"),"opp_ybc":rate(opp,"ybc"),"own_yac":rate(own,"yac"),"opp_yac":rate(opp,"yac")}
                if any(v is None for v in features.values()): continue
                for p in frame[(frame.game_id==game_id)&(frame.team==team)].to_dict("records"):
                    current=stats.get((season,week,p["gsis_id"]))
                    if not current or current["position"]!=position: continue
                    actual=current["source_row"] or {}; n=float(actual.get(units) or 0)
                    if n < (15 if family=="pressure" else 5): continue
                    prior=[r for r in hist if r["gsis_id"]==p["gsis_id"] and (r["season"],r["week"])<(season,week)][-8:]
                    denom=sum(float((r["source_row"] or {}).get(units) or 0) for r in prior)
                    if denom<=0: continue
                    baseline=sum(float((r["source_row"] or {}).get(yards) or 0) for r in prior)/denom
                    rows.append({"family":family,"game_id":game_id,"player_id":p["gsis_id"],"season":season,"week":week,
                                 **features,"residual":float(actual.get(yards) or 0)/n-baseline})
    source_manifest={"files":json.loads((development_dir/"source-manifest.json").read_text()),
                     "history_digest":stable_digest(hist),"crosswalk_digest":stable_digest(crosswalk),
                     "training_window":"2023-2025 regular season", "availability":"retrospective_development_only",
                     "baseline":"previous eight player games opportunity-weighted efficiency; prospective gate compares exact v5",
                     "test_outcomes_used":False}
    fitted={family:fit_family(family,rows,source_manifest) for family in ("pressure","contact")}
    Path("artifacts/nfl_matchup_development_games.json").write_text(json.dumps(game_inputs,indent=2),encoding="utf-8")
    if MODEL_PATH.exists():
        previous=json.loads(MODEL_PATH.read_text())
        if {k:v.get("artifact_hash") for k,v in previous.items()}!={k:v.get("artifact_hash") for k,v in fitted.items()}:
            raise ValueError("fitted model changed: register a new version rather than overwrite the frozen artifact")
    else:
        MODEL_PATH.write_text(json.dumps(fitted,indent=2),encoding="utf-8")
    return fitted


def compare_slate(db, *, season, week, upload_id, fitted, as_of, baseline_run_id=None):
    upload=db.execute_one("""SELECT u.* FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
        WHERE u.upload_id=%s AND r.season=%s AND r.week=%s""",(upload_id,season,week)) if upload_id else db.execute_one(
        """SELECT u.* FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
        WHERE u.format='classic' AND r.season=%s AND r.week=%s ORDER BY u.created_at DESC LIMIT 1""",(season,week))
    if not upload: raise ValueError("No saved salary slate")
    run=db.execute_one("""SELECT * FROM nfl_dfs_projection_runs WHERE season=%s AND week=%s
        AND as_of_at<=%s AND created_at<=%s AND model_version='nfl-dfs-historical-v5'
        AND (%s::uuid IS NULL OR run_id=%s::uuid) ORDER BY as_of_at DESC LIMIT 1""",(season,week,as_of,as_of,baseline_run_id,baseline_run_id))
    if not run: raise ValueError("No eligible v5 baseline")
    salary=[dict(r) for r in db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY id",(upload["upload_id"],))]
    projections={int(r["player_id"]):dict(r) for r in db.execute("SELECT * FROM nfl_dfs_player_projections WHERE run_id=%s",(run["run_id"],))}
    history=_history(db,season,week)
    matchups=load_matchups(db,season,week,as_of)
    team_game={team_code(t):m for m in matchups.values() for t in (m["home"],m["away"])}
    players=[]; skipped=defaultdict(int)
    for s in salary:
        p=projections.get(s["ff_player_id"])
        if not p: skipped["unmatched_projection"]+=1; continue
        m=team_game.get(team_code(s["team"]))
        if not m: skipped["not_upcoming_this_week"]+=1; continue
        source=p.get("source_evidence") or {}
        if source.get("game_id") and source["game_id"]!=m["game_id"]:
            skipped["game_identity_mismatch"]+=1; continue
        p.update(is_out=s["is_out"],season=season,week=week)
        draws=sample_baseline_draws(p,history,config=run["model_config"],seed=run["seed"])
        shadow=shadow_projection(p,draws,m,fitted)
        players.append({"player_id":p["player_id"],"name":s["name"],"position":s["position"],"team":s["team"],
                        "salary":s["salary"],"slate_player_id":s["id"],"dk_player_id":s["dk_player_id"],
                        "game_id":m["game_id"],"kickoff":m["kickoff"],"baseline":p,"shadow":shadow,
                        "sources":[{**s,"identity_manifest":{k:v for k,v in (s.get("identity_manifest") or {}).items() if k!="mappings"}}
                                   for s in m["sources"]],"matchup_manifest_hash":m["manifest_hash"]})
    artifact={"version":VERSION,"season":season,"week":week,"as_of_at":as_of.isoformat(),"upload_id":str(upload["upload_id"]),
              "file_name":upload["file_name"],"baseline_run_id":str(run["run_id"]),"baseline_version":run["model_version"],
              "baseline_as_of_at":run["as_of_at"].isoformat(),"salary_rows":len(salary),"skipped":dict(skipped),
              "baseline_config_hash":stable_digest({"model_version":run["model_version"],"model_config":run["model_config"]}),
              "model_hashes":{k:v.get("artifact_hash") for k,v in fitted.items()},"players":players,
              "matchups":matchups,
              "implementation_hash_algorithm":"sha256_lf_normalized",
              "implementation_hashes":{path:hashlib.sha256(Path(path).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for path in
                  ("model/nfl_matchup_features.py","model/nfl_matchup_projection.py","model/nfl_dfs_historical.py",
                   "ingest/nfl_matchup_context.py","research/nfl_matchup_implementation.py","db/nfl_pfr_schema.py")},
              "production_changed":False,"forecast_state":"shadow_only; forward outcomes not yet available"}
    # Normalize database decimals/UUID/time values once, before hashing/storage.
    return json.loads(json.dumps(artifact,default=str)),matchups


def render(artifact):
    applied=[p for p in artifact["players"] if p["shadow"]["status"]=="under_evaluation"]
    lines=["# Today's DFS matchup comparison", "",f"Slate: {artifact['file_name']} | {artifact['salary_rows']} salary entries",
           f"Frozen at: {artifact['as_of_at']}",f"Baseline: {artifact['baseline_version']} ({artifact['baseline_run_id']})",
           "", "Active production projections are unchanged. The new numerical results below are separately frozen shadow forecasts.",
           f"{len(artifact['players'])} matched upcoming player entries; {len(applied)} have eligible numerical matchup challengers.",
           "", "| Player | Team | Pos | Salary | Baseline DK | Shadow DK | Change | P90 change | Component |",
           "|---|---|---|---:|---:|---:|---:|---:|---|"]
    for p in sorted(applied,key=lambda p:abs(p["shadow"]["delta"]),reverse=True):
        s=p["shadow"]
        lines.append(f"| {p['name']} | {p['team']} | {p['position']} | ${p['salary']:,} | {s['baseline']['mean']:.2f} | {s['candidate']['mean']:.2f} | {s['delta']:+.2f} | {s['candidate']['p90']-s['baseline']['p90']:+.2f} | {s['ledger'][0]['component']} |")
    counts=defaultdict(int)
    for p in artifact["players"]: counts[p["shadow"].get("reason","unknown")]+=1
    lines += ["", "## Coverage and interpretation", "", "- Pressure changes QB passing-yard efficiency only; contact changes RB rushing-yard efficiency only.",
              "- Opportunity, touchdowns, receivers, and DST retain their baseline. No generic matchup bonus is added.",
              "- Models were fitted on 2023-2025 retrospective development data; this is a prospective experiment, not proof of improved accuracy.",
              "- Every candidate is scored draw by draw with the original platform bonuses; changed player percentiles are not lineup percentiles.",
              "- Saved baseline reproduction is required. Availability-adjusted or unreproduced baselines are withheld.",
              "", "Fallback reasons: " + json.dumps(dict(counts),sort_keys=True),"", "The JSON companion freezes source IDs, units, mappings, coefficients, and component ledgers for replay and later grading."]
    return "\n".join(lines)+"\n"


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    today=datetime.now(timezone.utc)
    parser.add_argument("--season",type=int,default=today.year-(today.month<=3));parser.add_argument("--week",type=int)
    parser.add_argument("--upload-id");parser.add_argument("--fit",action="store_true");parser.add_argument("--persist",action="store_true")
    parser.add_argument("--baseline-run-id",help="Pin an existing eligible v5 baseline for a reproducible comparison")
    parser.add_argument("--upcoming",action="store_true",help="Use the current/upcoming week and latest matching saved slate")
    parser.add_argument("--development-dir",type=Path,default=Path("data/pfr/matchup-development"))
    parser.add_argument("--output-dir",type=Path,default=Path("artifacts/nfl-matchup-implementation")/today.date().isoformat())
    args=parser.parse_args();db=DatabaseManager(load_config().database_url,initialize_schema=False)
    with db.reuse_connection():
        args.week=args.week if args.week is not None else infer_target_week(db,args.season)
        fitted=fit_models(db,args.development_dir) if args.fit else json.loads(MODEL_PATH.read_text())
        print(json.dumps({"fitted":{k:{"n":v.get("n"),"status":v.get("status")} for k,v in fitted.items()}}),flush=True)
        try:
            artifact,matchups=compare_slate(db,season=args.season,week=args.week,upload_id=args.upload_id,fitted=fitted,as_of=datetime.now(timezone.utc),baseline_run_id=args.baseline_run_id)
        except ValueError as exc:
            if str(exc)!="No saved salary slate": raise
            matchups=load_matchups(db,args.season,args.week,datetime.now(timezone.utc))
            print(json.dumps({"status":"awaiting_current_salary_slate","season":args.season,"week":args.week,
                              "matchup_games":len(matchups),"context_rows":persist_matchups(db,matchups) if args.persist else 0}))
            return
        args.output_dir.mkdir(parents=True,exist_ok=True)
        (args.output_dir/"slate-comparison.json").write_text(json.dumps(artifact,indent=2),encoding="utf-8")
        (args.output_dir/"slate-comparison.md").write_text(render(artifact),encoding="utf-8")
        persisted={"context_rows":persist_matchups(db,matchups),**persist_forecasts(db,artifact)} if args.persist else None
        print(json.dumps({"players":len(artifact["players"]),"applied":sum(p["shadow"]["status"]=="under_evaluation" for p in artifact["players"]),
                          "production_changed":False,"output":str(args.output_dir),"persisted":persisted}))


if __name__=="__main__":main()
