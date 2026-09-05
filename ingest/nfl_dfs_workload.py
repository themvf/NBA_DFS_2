"""Freeze raw stored component history and save research-only workload forecasts."""
import argparse, json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from psycopg2.extras import Json
from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase, target_season
from model.nfl_dfs_feature_audit import digest
from model.nfl_dfs_workload import VERSION, CONFIG, backtest, build, metrics

ROOT = Path(__file__).resolve().parents[1]
COMPONENT_FIELDS = ("attempts", "carries", "targets")


def file_digest(relative_path):
    return sha256((ROOT / relative_path).read_bytes()).hexdigest()


def raw_history(db):
    rows = db.execute("""SELECT w.id,p.gsis_id,p.position,w.season,w.week,w.team,w.opponent,w.source,w.fetched_at,w.source_row
      FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id WHERE w.season_type='REG' AND w.source='nflverse' ORDER BY w.id""")
    players, teams = [], []
    for r in rows:
        raw = r["source_row"] if isinstance(r["source_row"], dict) else {}
        stats = raw.get("raw_team_stats") if r["position"] == "DST" else raw
        stats = stats if isinstance(stats, dict) else {}
        component_stats = {key: stats[key] for key in COMPONENT_FIELDS if key in stats}
        base = {"record_id": r["id"], "identity": "DST:"+r["team"] if r["position"] == "DST" else r["gsis_id"],
                "position": r["position"], "season": r["season"], "week": r["week"], "team": r["team"],
                "opponent": r["opponent"], "source": r["source"], "source_hash": digest(raw),
                "fetched_at": r["fetched_at"].isoformat(), "stats": component_stats}
        (teams if r["position"] == "DST" else players).append(base)
    return players, teams


def inputs(db, season, week, as_of):
    games = db.execute("""SELECT g.id game_id,g.season,g.week,g.kickoff,h.abbreviation home_team,a.abbreviation away_team
      FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
      WHERE g.season=%s AND g.week=%s AND g.game_type='REG' AND g.kickoff>%s ORDER BY g.kickoff,g.id""", (season, week, as_of))
    teams = sorted({t for g in games for t in (g["home_team"], g["away_team"])})
    roster = db.execute("""SELECT gsis_id identity,canonical_name name,position,team_abbrev team FROM ff_players
      WHERE season=%s AND active AND team_abbrev=ANY(%s) AND position=ANY(%s) ORDER BY team_abbrev,position,canonical_name""",
      (season, teams, ["QB","RB","WR","TE"])) if teams else []
    for game in games:
        game["kickoff"] = game["kickoff"].isoformat()
    return games, roster


def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("--season",type=int); parser.add_argument("--week",type=int); args=parser.parse_args()
    now=datetime.now(timezone.utc); db=PipelineDatabase(load_config().database_url); season=target_season(args.season,now)
    if not args.week: args.week=db.execute("SELECT min(week) week FROM nfl_season_games WHERE season=%s AND game_type='REG' AND kickoff>%s",(season,now))[0]["week"]
    players,teams=raw_history(db)
    immutable_dataset={"version":"nfl-dfs-raw-components-v1","missing_policy":"missing remains absent; never defaulted to zero",
             "players":players,"teams":teams,"source_rows":len(players)+len(teams)}
    dataset_digest=digest(immutable_dataset)
    dataset=immutable_dataset
    games,roster=inputs(db,season,args.week,now)
    forecasts=build(teams,players,games,roster,now.isoformat())
    test=backtest(teams); report={"version":VERSION,"config":CONFIG,"dataset_digest":dataset_digest,"season":season,"week":args.week,
      "as_of_at":now.isoformat(),"forecasts":forecasts,"backtest":{"status":"retrospective_2024_2025_previously_inspected","metrics":metrics(test),"rows":len(test)},
      "production_changed":False,"population":"canonical active season roster, not a DK salary slate",
      "implementation":{"model_sha256":file_digest("model/nfl_dfs_workload.py"),"ingest_sha256":file_digest("ingest/nfl_dfs_workload.py")},
      "limits":["Historical weekly roster membership is unavailable; player allocations are not backtested as a lineup-known cohort.","No injury/depth-chart evidence is used.","Unallocated work is retained rather than assigned to unknown players."]}
    run_digest=digest(report)
    with db.connect() as c:
      with c.cursor() as q:
       dump=lambda value: json.dumps(value,default=str,allow_nan=False)
       q.execute("INSERT INTO nfl_dfs_component_datasets VALUES (%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",(dataset_digest,dataset["version"],Json(dataset,dumps=dump)))
       q.execute("INSERT INTO nfl_dfs_workload_runs(run_digest,dataset_digest,season,week,as_of_at,payload) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",(run_digest,dataset_digest,season,args.week,now,Json(report,dumps=dump)))
    print(json.dumps({"dataset_digest":dataset_digest,"run_digest":run_digest,"games":len(games),"roster":len(roster),"backtest":report["backtest"]}))
if __name__=="__main__": main()
