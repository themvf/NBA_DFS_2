"""Capture a strictly pregame Showdown baseline for coherent research; never fit or promote."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nfl_context_engine import stable_digest
from model.nfl_matchup_features import stamp
from model.nfl_pfr_supplement import team_code
from research.nfl_slate_baseline_comparison import capture_comparison


def validate_capture(result, *, as_of):
    """Validate retained identities without consulting a mutable player registry."""
    if result.get("format") != "showdown":
        raise ValueError("Showdown salary upload required")
    if result.get("saved_optimizer_run_id") or result.get("forecast_state") == "retrospective_development":
        raise ValueError("Retrospective inputs cannot be presented as a live capture")
    if result.get("baseline_version") != "nfl-dfs-historical-v5":
        raise ValueError("The registered v5 baseline is required")
    cutoff=stamp(as_of)
    if stamp(result["baseline_as_of_at"]) > cutoff or stamp(result["as_of_at"]) > cutoff:
        raise ValueError("Forecast source is later than the decision cutoff")
    rows=result.get("players",[]); salaries=result.get("salary_snapshot",[])
    if not rows or len({p["game_id"] for p in rows}) != 1 or len({s.get("game_key") for s in salaries}) != 1:
        raise ValueError("Showdown capture requires exactly one identified game")
    teams={team_code(t) for t in salaries[0]["game_key"].split("@")}
    if len(teams)!=2 or {team_code(p["team"]) for p in rows} != teams:
        raise ValueError("Both Showdown teams require identified forecast rows")
    if any(stamp(p["kickoff"]) <= cutoff for p in rows):
        raise ValueError("Showdown kickoff has already occurred")
    if any(n>1 for n in Counter(p["dk_player_id"] for p in rows).values()) or any(n>1 for n in Counter(p["player_id"] for p in rows).values()):
        raise ValueError("Duplicate salary or forecast player identity")
    salary_by_id={s["dk_player_id"]:s for s in salaries}
    if any(stamp(t)>cutoff for s in salaries for t in (s.get("updated_at"),s.get("created_at")) if t):
        raise ValueError("Salary source became available after cutoff")
    captain_ids=[s.get("captain_dk_player_id") for s in salaries]
    if any(x is None for x in captain_ids) or len(set(captain_ids))!=len(captain_ids) or any(not s.get("captain_salary") for s in salaries):
        raise ValueError("Showdown Captain salary identities are missing or duplicated")
    for row in rows:
        s=salary_by_id.get(row["dk_player_id"]); p=row["baseline"]
        if not s or s.get("ff_player_id") != p.get("player_id") or row["player_id"] != p.get("player_id"):
            raise ValueError("Salary and forecast player identities differ")
        if s.get("identity_method") in ("unmatched","ambiguous","team_conflict","position_conflict","identifier_conflict","missing_team"):
            raise ValueError("Salary identity is unresolved")
        if p.get("position") != s.get("position") or team_code(p.get("team") or "") != team_code(s.get("team") or ""):
            raise ValueError("Salary and forecast position/team differ")
        opponent=teams-{team_code(s["team"])}
        if team_code(s.get("opponent") or "") not in opponent or (p.get("opponent") and team_code(p["opponent"]) not in opponent):
            raise ValueError("Salary or forecast targets a different opponent")
        gsis=(s.get("identity_evidence") or {}).get("gsisId")
        if gsis and gsis != p.get("player_gsis_id"):
            raise ValueError("Frozen salary and forecast GSIS identities differ")
        if s.get("position") != "DST" and not p.get("player_gsis_id"):
            raise ValueError("Forecast GSIS identity is unresolved")
        if str(p.get("run_id")) != str(result["baseline_run_id"]):
            raise ValueError("Forecast belongs to a different saved baseline run")
        for source in (s.get("updated_at"),s.get("created_at"),p.get("created_at")):
            if source and stamp(source)>cutoff:
                raise ValueError("Salary or forecast row became available after cutoff")
        target=(p.get("source_evidence") or {}).get("game_id")
        if target and target != row["game_id"]:
            raise ValueError("Forecast targets a different game")
        if row["shadow"].get("ledger") or row["shadow"].get("delta") != 0:
            raise ValueError("Baseline adapter cannot invent a matchup adjustment")
    return result


def select_upcoming_upload(db, *, season, week=None, as_of):
    """Latest complete Showdown upload whose sole canonical game is still ahead."""
    rows=db.execute("""SELECT u.upload_id,u.projection_run_id FROM nfl_dfs_slate_uploads u
        JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
        WHERE u.format='showdown' AND r.season=%s AND (%s::int IS NULL OR r.week=%s)
          AND r.model_version='nfl-dfs-historical-v5' AND r.created_at<=%s AND r.as_of_at<=%s AND u.created_at<=%s
          AND (SELECT COUNT(*) FROM nfl_dfs_slate_players s WHERE s.upload_id=u.upload_id)=u.player_count
          AND (SELECT COUNT(DISTINCT s.game_key) FROM nfl_dfs_slate_players s WHERE s.upload_id=u.upload_id)=1
          AND NOT EXISTS(SELECT 1 FROM nfl_dfs_slate_players s LEFT JOIN nfl_season_games g ON g.season=r.season AND g.week=r.week
            AND g.nflverse_game_id IS NOT NULL AND EXISTS(SELECT 1 FROM nfl_teams h,nfl_teams a
              WHERE h.team_id=g.home_team_id AND a.team_id=g.away_team_id AND
                REPLACE(REPLACE(REPLACE(s.game_key,'LAR','LA'),'WSH','WAS'),'JAC','JAX')=
                REPLACE(REPLACE(REPLACE(a.abbreviation||'@'||h.abbreviation,'LAR','LA'),'WSH','WAS'),'JAC','JAX'))
            WHERE s.upload_id=u.upload_id AND (g.id IS NULL OR g.kickoff IS NULL OR g.kickoff<=%s OR g.completed))
        ORDER BY u.created_at DESC,u.upload_id DESC LIMIT 1""",(season,week,week,as_of,as_of,as_of,as_of))
    return dict(rows[0]) if rows else None


def capture(db, *, upload_id, baseline_run_id=None, as_of=None):
    now=stamp(as_of or datetime.now(timezone.utc))
    u=db.execute_one("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s",(upload_id,))
    if not u or u["format"] != "showdown": raise ValueError("Showdown upload not found")
    run_id=baseline_run_id or u["projection_run_id"]
    r=db.execute_one("SELECT * FROM nfl_dfs_projection_runs WHERE run_id=%s",(run_id,))
    if not r or max(stamp(r["created_at"]),stamp(r["as_of_at"]),stamp(u["created_at"]))>now:
        raise ValueError("Saved sources were not available at capture")
    all_salary=db.execute("SELECT * FROM nfl_dfs_slate_players WHERE upload_id=%s ORDER BY dk_player_id",(upload_id,))
    keys={"@".join(team_code(t) for t in (s.get("game_key") or "").split("@")) for s in all_salary}
    if len(all_salary)!=u["player_count"] or len(keys)!=1:
        raise ValueError("Showdown requires a complete one-game salary upload")
    games=db.execute("""SELECT g.nflverse_game_id game_id,g.kickoff,g.completed,h.abbreviation home,a.abbreviation away
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s""",(r["season"],r["week"]))
    target=[g for g in games if f"{team_code(g['away'])}@{team_code(g['home'])}" in keys]
    if len(target)!=1 or target[0].get("completed") or not target[0].get("kickoff") or stamp(target[0]["kickoff"])<=now:
        raise ValueError("No wholly pregame canonical Showdown slate")
    result=capture_comparison(db,upload_id=upload_id,run_id=run_id)
    # capture_comparison freezes after its database reads; the completed capture
    # timestamp is the actual cutoff, and all retained rows are checked against it.
    cutoff=stamp(result["as_of_at"])
    if len(result.get("salary_snapshot",[]))+len(result.get("skipped",[])) != u["player_count"]:
        raise ValueError("Salary upload is incomplete")
    normalize=lambda value:json.loads(json.dumps(value,default=str))
    original_by_id={s["dk_player_id"]:s for s in all_salary}
    if any(stable_digest(normalize(s)) != stable_digest(normalize(original_by_id.get(s["dk_player_id"]))) for s in result["salary_snapshot"]):
        raise ValueError("Salary sources changed while the capture was being built; retry")
    # Preserve unmatched rows too: the scenario export must keep the actual
    # salary population as its denominator, not silently count only matches.
    result["salary_snapshot"]=all_salary
    validate_capture(result,as_of=cutoff)
    result["capture_contract"]={"version":"nfl-showdown-pregame-baseline-v1","authority":"shadow_only",
        "numerical_matchup_adjustment":False,"identity_source":"retained_salary_and_forecast_rows",
        "salary_digest":stable_digest(json.loads(json.dumps(result["salary_snapshot"],default=str))),
        "full_salary_source_digest":stable_digest(json.loads(json.dumps(all_salary,default=str))),
        "baseline_run_digest":stable_digest(json.loads(json.dumps(dict(r),default=str)))}
    return result


def main():
    now=datetime.now(timezone.utc)
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--upload-id");ap.add_argument("--baseline-run-id");ap.add_argument("--upcoming",action="store_true")
    ap.add_argument("--season",type=int,default=now.year-(now.month<=3));ap.add_argument("--week",type=int)
    ap.add_argument("--output",type=Path,required=True)
    args=ap.parse_args()
    if not args.upload_id and not args.upcoming:ap.error("Pass --upload-id or --upcoming")
    db=DatabaseManager(load_config().database_url,initialize_schema=False)
    if not args.upload_id:
        selected=select_upcoming_upload(db,season=args.season,week=args.week,as_of=now)
        if selected is None:
            print(json.dumps({"status":"awaiting_current_showdown_slate","season":args.season,"week":args.week,"production_changed":False}));return
        args.upload_id=str(selected["upload_id"])
        args.baseline_run_id=args.baseline_run_id or str(selected["projection_run_id"])
    result=capture(db,upload_id=args.upload_id,baseline_run_id=args.baseline_run_id)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,default=str,indent=2),encoding="utf-8")
    print(json.dumps({"status":"captured","format":"showdown","upload_id":result["upload_id"],"players":len(result["players"]),"output":str(args.output),"production_changed":False}))

if __name__ == "__main__":main()
