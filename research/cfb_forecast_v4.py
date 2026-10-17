"""Registered CFB drive/market challenger. All outputs remain research-only."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import gzip
import json
from pathlib import Path
import subprocess
import numpy as np
from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from research.cfb_v4_diagnostics import legacy_audit
from research.cfb_v4_context_model import fit_context,vector
from research.cfb_v4_features import audit_games,replay,FEATURES
from research.cfb_v4_calibration import fit_football,football_score,fit_market,predict,proposition
from research.cfb_v4_store import VERSION,PROTOCOL,digest,ensure_schema,registration
from ingest.cfb_v4_context import context_status


def load_data(db):
    games=[dict(r) for r in db.execute("""SELECT m.id,m.season,m.commence_time,m.completed,m.home_score,m.away_score,
        m.home_team_id,m.away_team_id,m.neutral_site,m.start_time_tbd,m.final_at,m.game_date,
        h.name home_name,a.name away_name FROM cfb_matchups m
        JOIN cfb_teams h ON h.team_id=m.home_team_id JOIN cfb_teams a ON a.team_id=m.away_team_id
        WHERE m.season>=2022 AND lower(h.classification)='fbs' AND lower(a.classification)='fbs'
        AND m.commence_time IS NOT NULL ORDER BY m.commence_time,m.id""")]
    drives=[dict(r) for r in db.execute("SELECT * FROM cfb_drives WHERE season>=2022")]
    plays=[dict(r) for r in db.execute("""SELECT game_id,cfbd_drive_id,play_type,
        CASE WHEN play_text ~* 'kneel|takes? a knee' OR play_type IN ('Rushing Touchdown','Passing Touchdown','Fumble Recovery (Own)') THEN LEFT(play_text,2000) ELSE '' END play_text,
        period,offense_score,defense_score,ppa,yards_gained FROM cfb_plays WHERE season>=2022""")]
    stats,audits=audit_games(games,drives,plays)
    manifest={"games":len(games),"drives":len(drives),"plays":len(plays),
              "stats_digest":digest({str(k):v for k,v in stats.items()}),
              "captured_at":datetime.now(timezone.utc).isoformat(),
              "historical_status":"reconstructed_from_current_source_rows_not_certified_historical_capture_times"}
    return games,stats,audits,manifest


def market_quote(history,at,book="draftkings",max_age=15):
    rows=[h for h in history if h["captured_at"]<=at and (at-h["captured_at"]).total_seconds()<=max_age*60]
    if not rows:return None
    # Latest source observation owns the decision; do not cherry-pick a superseded old quote.
    h=max(rows,key=lambda h:(h["captured_at"],h["id"]))
    q=(h.get("books") or {}).get(book)
    if not isinstance(q,dict):return None
    try:
        update=datetime.fromisoformat(q["last_update"].replace("Z","+00:00"))
        if not -90<=(h["captured_at"]-update).total_seconds()<=300:return None
        for key in ("spread_home","spread_away","spread_home_price","spread_away_price","total_line","over","under"):
            if q.get(key) is None or not np.isfinite(q[key]):return None
        if q["spread_home"]!=-q["spread_away"]:return None
        if any(not q[k] or abs(q[k])<100 for k in ("spread_home_price","spread_away_price","over","under")):return None
        def prob(price):return 100/(price+100) if price>0 else abs(price)/(abs(price)+100)
        ml=[q.get("ml_home"),q.get("ml_away")]
        hp,ap=[prob(p) for p in ml] if all(p and abs(p)>=100 for p in ml) else (None,None)
        return {"history_id":h["id"],"captured_at":h["captured_at"],"book":book,"quote":q,
                "margin":-q["spread_home"],"total":q["total_line"],
                "home_prob":hp/(hp+ap) if hp is not None else None}
    except (KeyError,ValueError,TypeError):return None


def select_football(games,stats):
    candidates=[];chosen=None
    for prior in (0,1,2,4):
        for half_life in (3,6,1000):
            rows=replay(games,stats,prior,half_life)
            train=[r for r in rows if 2022<=r["season"]<=2024 and r.get("actual")]
            dev=[r for r in rows if r["season"]==2025 and r.get("actual")]
            if len(train)<300 or len(dev)<100:continue
            model=fit_football(train)
            error=float(np.mean([abs(football_score(r,model)["margin"]-(r["home_score"]-r["away_score"])) for r in dev]))
            result={"prior_games":prior,"half_life_games":half_life,"training_n":len(train),"development_n":len(dev),"development_margin_mae":error}
            candidates.append(result)
            if chosen is None or error<chosen[0]:chosen=(error,result,rows)
    if chosen is None:raise ValueError("Insufficient audited 2022-2025 games for prior selection")
    _,selection,rows=chosen
    fitted=fit_football([r for r in rows if r["season"]<=2025 and r.get("actual")])
    scaler,ridge=fitted.steps[0][1],fitted.steps[1][1]
    return rows,fitted,{"selected":selection,"candidates":candidates,"feature_order":FEATURES,
                       "scaler_mean":scaler.mean_.tolist(),"scaler_scale":scaler.scale_.tolist(),
                       "coefficients":ridge.coef_.tolist(),"intercept":float(ridge.intercept_),
                       "pace_model":{"feature_order":["own_drives","opponent_drives_allowed","own_seconds_per_snap","opponent_seconds_per_snap","own_success","opponent_success"],
                           "scaler_mean":fitted.cfb_pace_.steps[0][1].mean_.tolist(),
                           "scaler_scale":fitted.cfb_pace_.steps[0][1].scale_.tolist(),
                           "coefficients":fitted.cfb_pace_.steps[1][1].coef_.tolist(),
                           "intercept":float(fitted.cfb_pace_.steps[1][1].intercept_)}}


def run(db,registered=None,now=None):
    now=now or datetime.now(timezone.utc)
    spec=json.loads(PROTOCOL.read_text(encoding="utf-8"))
    cutoff=datetime.fromisoformat(spec["development_cutoff"].replace("Z","+00:00"))
    games,stats,audits,manifest=load_data(db)
    print("Audited source games:",len(audits),"usable:",sum(a["usable"] for a in audits),flush=True)
    rows,model,football_fit=select_football(games,stats)
    history=defaultdict(list)
    for h in db.execute("""SELECT id,matchup_id,captured_at,books FROM game_odds_history
        WHERE sport='cfb' AND captured_at>='2026-01-01' ORDER BY matchup_id,captured_at,id"""):
        history[h["matchup_id"]].append(dict(h))
    development=[];excluded=defaultdict(int)
    for r in rows:
        if r["season"]!=2026 or not r.get("actual") or r["commence_time"]+timedelta(hours=6)>=cutoff:continue
        target=r["commence_time"]-timedelta(hours=3)
        quote=market_quote(history[r["id"]],target)
        if not quote:excluded["missing_fresh_T180_quote"]+=1;continue
        development.append({"game_id":r["id"],"date":str(r["game_date"]),
            "raw":football_score(r,model),"market":quote,
            "actual":{"margin":r["home_score"]-r["away_score"],"total":r["home_score"]+r["away_score"]}})
    fit=fit_market(development)
    contexts=[dict(r) for r in db.execute("""SELECT c.* FROM cfb_v4_context c JOIN cfb_matchups m ON m.id=c.game_id
        WHERE c.captured_at<=%s AND c.captured_at>=%s AND m.commence_time>%s""",(now,now-timedelta(days=7),now))] if registered else []
    roster_refs={int(r["team_id"]):dict(r) for r in db.execute("""SELECT DISTINCT ON(team_id)
        id,team_id,source,captured_at,available_at,is_complete,confidence,summary_json
        FROM cfb_roster_snapshots WHERE season=2026 AND point_in_time_eligible=TRUE
        AND captured_at<=%s AND available_at<=%s ORDER BY team_id,captured_at DESC,id DESC""",(now,now))}
    context_rows=db.execute("""SELECT DISTINCT ON(s.game_id) s.evidence_json,m.game_date,m.home_score,m.away_score
        FROM cfb_v4_snapshots s JOIN cfb_matchups m ON m.id=s.game_id
        WHERE m.completed=TRUE AND m.final_at<=%s AND s.frozen_at<m.commence_time
        AND s.kickoff=m.commence_time AND s.frozen_at BETWEEN s.kickoff-INTERVAL '195 minutes' AND s.kickoff-INTERVAL '60 minutes'
        ORDER BY s.game_id,s.frozen_at,s.id""",(now,)) if registered else []
    context_fit,context_model=fit_context(context_rows)
    upcoming=[]
    for r in rows:
        if r["completed"] or not now<r["commence_time"]<=now+timedelta(days=2):continue
        context=context_status(contexts,r,now)
        boundary=r["commence_time"].replace(hour=0,minute=0,second=0,microsecond=0)
        missing_history={side:[g["id"] for g in games if g["season"]==r["season"] and g["completed"]
            and g["commence_time"]+timedelta(hours=6)<boundary and r[side+"_team_id"] in (g["home_team_id"],g["away_team_id"])
            and not stats.get((g["id"],r[side+"_team_id"]),{}).get("usable")] for side in ("home","away")}
        raw=football_score(r,model);quote=market_quote(history[r["id"]],now)
        item={"game_id":r["id"],"kickoff":r["commence_time"],"home":r["home_name"],"away":r["away_name"],
              "raw":raw,"context":context,
              "roster_reference":{side:roster_refs.get(r[side+"_team_id"]) for side in ("home","away")},
              "roster_reference_limit":"Annual rosters do not confirm availability; continuity needs verified prior-roster coverage.",
              "input_coverage":{"home_games":r["home_current_games"],"away_games":r["away_current_games"],
              "home_ppa_plays":r["home_ppa_plays"],"away_ppa_plays":r["away_ppa_plays"],
              "home_source_games":r["profiles"][r["home_team_id"]]["source_games"],"away_source_games":r["profiles"][r["away_team_id"]]["source_games"]},
              "missing_history_games":missing_history,"model_inputs":{"home":r["x_home"],"away":r["x_away"],"pace":r["pace_features"]},"market":quote,"status":"RESEARCH_ONLY",
              "betting_permission":False,"reasons":["not_prospectively_validated","context_numeric_effects_unfitted"]+context["reasons"],
              "registration_digest":registered["digest"] if registered else None,"generated_at":now}
        if any(missing_history.values()):item["reasons"].append("incomplete_current_season_history")
        if quote:
            forecast=predict({"raw":raw,"market":quote},fit)
            item["forecast"]=forecast;item["propositions"]={}
            for market,sides in (("spread",("home","away")),("total",("over","under"))):
                field="margin" if market=="spread" else "total"
                for side in sides:
                    line=quote["quote"]["spread_home"] if market=="spread" else quote["total"]
                    price=quote["quote"]["spread_"+side+"_price"] if market=="spread" else quote["quote"][side]
                    item["propositions"][market+"_"+side]={"line":line,"price":price,
                        **proposition(forecast[field],fit[field]["residuals"],line,price,side,market)}
            if forecast["home_win_probability"] is not None:
                for side,p in (("home",forecast["home_win_probability"]),("away",1-forecast["home_win_probability"])):
                    price=quote["quote"].get("ml_"+side)
                    if price and abs(price)>=100:
                        decimal=1+price/100 if price>0 else 1+100/abs(price)
                        item["propositions"]["moneyline_"+side]={"price":price,"p_win":p,"p_push":0.,"p_loss":1-p,"ev":p*decimal-1,"status":"exploratory"}
        else:item["reasons"].append("fresh_exact_book_quote_unavailable")
        context_vector=vector(item)
        item["context_shadow_adjustment"]=context_model.predict([context_vector])[0].tolist() if context_model is not None and context_vector is not None else None
        # Use the first T195..T60 eligible publication, never choose entry after seeing a result.
        lead=(r["commence_time"]-now).total_seconds()/60
        item["primary_eligible"]=bool(registered and registered["registered_at"]<=now<r["commence_time"]
                                       and now<datetime(2027,1,1,tzinfo=timezone.utc) and quote and 60<=lead<=195 and not r["start_time_tbd"] and not any(missing_history.values()))
        upcoming.append(item)
    return {"version":VERSION,"status":"RESEARCH_ONLY","generated_at":now,"registered":registered,
            "source_git_sha":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
            "source_manifest":manifest,"football_fit":football_fit,"market_fit":fit,"context_fit":context_fit,
            "development_coverage":{"eligible":len(development),"excluded":dict(excluded)},
            "source_feature_facts":[{"key":list(key),"value":value} for key,value in stats.items()],
            "prior_miss_audit":legacy_audit(db,audits),
            "source_audit":{"games":len(audits),"usable":sum(a["usable"] for a in audits),"games_detail":audits},
            "upcoming":upcoming,"limitations":["Historical inputs reconstructed from revised source tables, not certified archived decision-time inputs.",
            "Context captured separately; QB/injury/weather numerical effects need their own historical evidence.",
            "Current-season development was previously inspected; no result here is untouched confirmation."]}


def publish(db,report):
    reg=registration(db)
    if not reg:raise ValueError("Register protocol before publishing")
    if report["registered"] is None or report["registered"]["digest"]!=reg["digest"]:raise ValueError("Run did not use registered protocol")
    with db.connect() as conn:
        cur=conn.cursor()
        cur.execute("INSERT INTO cfb_v4_runs(protocol_version,report_json) VALUES(%s,%s) RETURNING id",(VERSION,Json({**{k:v for k,v in jsonable(report).items() if k not in ("upcoming","source_feature_facts","source_audit")},
                     "source_audit":{k:v for k,v in report["source_audit"].items() if k!="games_detail"}})))
        run_id=cur.fetchone()["id"]
        for item in report["upcoming"]:
            # Transaction-time guard: a slow run must not enroll a game that started meanwhile.
            cur.execute("""INSERT INTO cfb_v4_snapshots(run_id,game_id,kickoff,odds_history_id,evidence_json)
                SELECT %s,%s,%s,%s,%s WHERE clock_timestamp()<%s RETURNING id""",
                (run_id,item["game_id"],item["kickoff"],item["market"]["history_id"] if item["market"] else None,Json(jsonable(item)),item["kickoff"]))
            row=cur.fetchone()
            if row and item["primary_eligible"]:
                cur.execute("""INSERT INTO cfb_v4_decisions(protocol_version,game_id,kickoff,snapshot_id,odds_history_id,evidence_json)
                    SELECT %s,%s,%s,%s,%s,%s WHERE clock_timestamp()<=%s::timestamptz-INTERVAL '60 minutes'
                    AND clock_timestamp()-%s::timestamptz<=INTERVAL '15 minutes'
                    AND EXISTS(SELECT 1 FROM cfb_matchups WHERE id=%s AND commence_time=%s AND NOT completed AND NOT start_time_tbd)
                    ON CONFLICT(protocol_version,game_id) DO NOTHING""",
                    (VERSION,item["game_id"],item["kickoff"],row["id"],item["market"]["history_id"],Json(jsonable(item)),item["kickoff"],item["market"]["captured_at"],item["game_id"],item["kickoff"]))
    return run_id


def jsonable(value):
    return json.loads(json.dumps(value,default=str))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register",action="store_true");parser.add_argument("--publish",action="store_true")
    parser.add_argument("--output",type=Path,default=Path("artifacts/cfb/v4-report.json"))
    args=parser.parse_args();db=DatabaseManager(load_config().database_url or "",initialize_schema=False)
    ensure_schema(db)
    if args.register:
        sha=subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip()
        print(json.dumps(registration(db,sha),default=str));return
    reg=registration(db)
    if args.publish and not reg:raise ValueError("Protocol is not registered")
    report=run(db,reg)
    if args.publish:report["published_run_id"]=publish(db,report)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,default=str,indent=2),encoding="utf-8")
    # Archive usable source feature facts for later reproduction without changing old source rows.
    with gzip.open(str(args.output)+".audits.json.gz","wt",encoding="utf-8") as out:
        json.dump({"audit":report["source_audit"],"source_features":report["source_feature_facts"],"manifest":report["source_manifest"]},out,default=str)
    print(json.dumps({"status":report["status"],"source":{k:v for k,v in report["source_audit"].items() if k!="games_detail"},
        "football_selection":report["football_fit"]["selected"],"market_fit":report["market_fit"],
        "upcoming":len(report["upcoming"]),"primary":sum(i["primary_eligible"] for i in report["upcoming"]),
        "published_run_id":report.get("published_run_id")},default=str))


if __name__=="__main__":main()
