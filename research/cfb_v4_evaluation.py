"""Grade first registered v4 paper decisions; preserve exclusions and market benchmarks."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime,timezone
import json
from pathlib import Path
import numpy as np
from psycopg2.extras import Json

from config import load_config
from ingest.cfb_plays import football_season_year
from db.database import DatabaseManager
from research.cfb_v4_store import VERSION,ensure_schema,registration


def reliability(pairs):
    if not pairs:return {"n":0,"brier":None,"log_loss":None,"intercept":None,"slope":None,"bins":[]}
    from scipy.optimize import minimize
    from scipy.special import expit,logit
    p=np.clip(np.asarray([p for p,y in pairs]),1e-6,1-1e-6);y=np.asarray([y for p,y in pairs]);x=logit(p)
    slope,intercept=None,None
    if len(p)>=50 and len(set(y))==2 and np.std(x)>1e-6:
        def objective(b):
            eta=b[0]+b[1]*x
            return float(np.sum(np.logaddexp(0,eta)-y*eta))
        fit=minimize(objective,[0.,1.],method="BFGS")
        if fit.success:intercept,slope=map(float,fit.x)
    return {"n":len(p),"brier":float(np.mean((p-y)**2)),"log_loss":float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p)))),
            "intercept":intercept,"slope":slope,
            "bins":[{"lower":lo,"upper":lo+.2,"n":int(np.sum((p>=lo)&(p<lo+.2))),
                     "predicted":float(np.mean(p[(p>=lo)&(p<lo+.2)])) if any((p>=lo)&(p<lo+.2)) else None,
                     "observed":float(np.mean(y[(p>=lo)&(p<lo+.2)])) if any((p>=lo)&(p<lo+.2)) else None} for lo in (0.,.2,.4,.6,.8)]}


def interval(items,field):
    dates=sorted({i["date"] for i in items if i.get(field) is not None})
    if len(dates)<30:return None
    groups={d:[i[field] for i in items if i["date"]==d and i.get(field) is not None] for d in dates}
    rng=np.random.default_rng(20261017)
    draws=[float(np.mean([v for d in rng.choice(dates,len(dates),replace=True) for v in groups[d]])) for _ in range(2000)]
    return [float(np.quantile(draws,.025)),float(np.quantile(draws,.975))]


def grade_market(item,home_margin,total):
    graded={}
    for market in ("spread","total","moneyline"):
        candidates=[(key,p) for key,p in item.get("propositions",{}).items() if key.startswith(market+"_")
                    and p.get("p_win") is not None and p.get("ev") is not None and p["ev"]>0]
        if not candidates:continue
        key,p=max(candidates,key=lambda pair:(pair[1]["ev"],pair[0].endswith("_home") or pair[0].endswith("_over")))
        side=key[len(market)+1:]
        difference=home_margin+p["line"] if market=="spread" else total-p["line"] if market=="total" else home_margin
        if side in ("away","under"):difference=-difference
        status="won" if difference>0 else "lost" if difference<0 else "push"
        price=p["price"];decimal=1+price/100 if price>0 else 1+100/abs(price)
        profit=decimal-1 if status=="won" else -1. if status=="lost" else 0.
        graded[market]={"side":side,"status":status,"price":price,"line":p.get("line"),"profit":profit,
                        "adverse_price_profit":decimal*.99-1 if status=="won" else profit,
                        "p_win":p["p_win"],"p_push":p.get("p_push",0),"p_loss":p.get("p_loss"),"ev":p["ev"]}
    return graded


def evaluate(rows,registered):
    settled=[];pending=[];excluded=[];selected=defaultdict(list);probabilities=defaultdict(list);legacy=defaultdict(list)
    for row in rows:
        item=row["evidence_json"]
        if (row["frozen_at"]<registered["registered_at"] or row["frozen_at"]>=row["kickoff"]
                or row["kickoff"]!=row["commence_time"]):
            excluded.append({"game_id":row["game_id"],"reason":"registration_or_kickoff_boundary"});continue
        if not row["completed"] or row["home_score"] is None or row["away_score"] is None:
            pending.append(row["game_id"]);continue
        margin=row["home_score"]-row["away_score"];total=row["home_score"]+row["away_score"]
        prediction=item.get("forecast")
        if not prediction:
            excluded.append({"game_id":row["game_id"],"reason":"missing_forecast"});continue
        record={"game_id":row["game_id"],"date":str(row["game_date"]),"actual_margin":margin,"actual_total":total,
                "context_complete":item["context"]["complete"],"graded":grade_market(item,margin,total)}
        for field,actual in (("margin",margin),("total",total)):
            baseline=item["market"][field]
            record[field+"_model_error"]=abs(prediction[field]-actual)
            record[field+"_market_error"]=abs(baseline-actual)
            record[field+"_raw_error"]=abs(item["raw"][field]-actual)
            record[field+"_fixed25_error"]=abs(baseline+.25*(item["raw"][field]-baseline)-actual)
            record[field+"_delta"]=record[field+"_model_error"]-record[field+"_market_error"]
        hp=prediction.get("home_win_probability")
        if hp is not None:
            probabilities["moneyline"].append((hp,float(margin>0)))
        for version,evidence in (row.get("matched_legacy") or {}).items():
            prior=evidence.get("forecast",{})
            if prior.get("home_points") is None or prior.get("away_points") is None:continue
            legacy[version].append({"margin_error":abs(prior["home_points"]-prior["away_points"]-margin),
                "total_error":abs(prior["home_points"]+prior["away_points"]-total),
                "v4_margin_error":record["margin_model_error"],"v4_total_error":record["total_model_error"]})
        # Use only one side per game for probability calibration, independent of EV selection.
        for name,actual in (("spread_home",margin),("total_over",total)):
            p=item.get("propositions",{}).get(name,{})
            if p.get("p_win") is None:continue
            d=actual+p["line"] if name=="spread_home" else actual-p["line"]
            if d!=0 and 1-p.get("p_push",0)>0:
                probabilities[name].append((p["p_win"]/(1-p.get("p_push",0)),float(d>0)))
        for market,g in record["graded"].items():
            g.update(game_id=row["game_id"],date=str(row["game_date"]))
            close=(row.get("close_books") or {}).get(item["market"]["book"],{})
            close_price=close.get("ml_"+g["side"]) if market=="moneyline" else close.get("spread_"+g["side"]+"_price") if market=="spread" else close.get(g["side"])
            g["line_clv"]=None;g["price_clv_pct"]=None
            close_fresh=False
            try:
                updated=datetime.fromisoformat(close["last_update"].replace("Z","+00:00"))
                captured=row["close_captured_at"]
                close_fresh=-90<=(captured-updated).total_seconds()<=300 and captured<row["kickoff"]
            except (KeyError,ValueError,TypeError):pass
            if row.get("close_history_id") and close_fresh and close_price and abs(close_price)>=100:
                entry=item["propositions"][market+"_"+g["side"]]
                if market=="spread":
                    cl=close.get("spread_"+g["side"])
                    en=entry["line"]*(1 if g["side"]=="home" else -1)
                    if cl is not None:g["line_clv"]=en-cl
                    same=cl==en
                elif market=="total":
                    cl=close.get("total_line");en=entry["line"]
                    if cl is not None:g["line_clv"]=(cl-en)*(1 if g["side"]=="over" else -1)
                    same=cl==en
                else:same=True
                if same:
                    d=lambda o:1+o/100 if o>0 else 1+100/abs(o)
                    g["price_clv_pct"]=(d(g["price"])/d(close_price)-1)*100
            selected[market].append(g)
        settled.append(record)
    metrics={}
    for field in ("margin","total"):
        metrics[field]={k:float(np.mean([r[field+"_"+k] for r in settled])) if settled else None
                        for k in ("model_error","market_error","raw_error","fixed25_error","delta")}
        metrics[field]["delta_ci95"]=interval(settled,field+"_delta")
    return {"version":VERSION,"status":"RESEARCH_ONLY","betting_permission":False,"registered_at":registered["registered_at"],
            "settled_games":len(settled),"game_dates":len({r["date"] for r in settled}),"pending_games":pending,"excluded":excluded,
            "matched_metrics":metrics,"calibration":{k:reliability(v) for k,v in probabilities.items()},
            "matched_legacy_benchmarks":{version:{"n":len(v),**{k:float(np.mean([r[k] for r in v]))
                for k in ("margin_error","total_error","v4_margin_error","v4_total_error")}} for version,v in legacy.items()},
            "legacy_match_rule":"Same exact odds-history ID, matching kickoff, earlier frozen comparison within 15 minutes of v4 decision; missing matches stay absent.",
            "paper_selection":{k:{"n":len(v),"unique_games":len({i["game_id"] for i in v}),
                  "wins":sum(i["status"]=="won" for i in v),"losses":sum(i["status"]=="lost" for i in v),"pushes":sum(i["status"]=="push" for i in v),
                  "units":sum(i["profit"] for i in v),"roi":float(np.mean([i["profit"] for i in v])),
                  "roi_ci95":interval(v,"profit"),"adverse_price_roi":float(np.mean([i["adverse_price_profit"] for i in v])),
                  "close_line_n":sum(i["line_clv"] is not None for i in v),"close_price_n":sum(i["price_clv_pct"] is not None for i in v),
                  "mean_line_clv":float(np.mean([i["line_clv"] for i in v if i["line_clv"] is not None])) if any(i["line_clv"] is not None for i in v) else None,
                  "mean_price_clv_pct":float(np.mean([i["price_clv_pct"] for i in v if i["price_clv_pct"] is not None])) if any(i["price_clv_pct"] is not None for i in v) else None} for k,v in selected.items()},
            "games":settled,"review_floor_met":len(settled)>=200 and len({r["date"] for r in settled})>=30,
            "confirmation":"2027 untouched season required; no automatic activation","uncertainty":"Intervals withheld below 30 game dates; markets on the same game are correlated."}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--output",type=Path,default=Path("artifacts/cfb/v4-forward.json"));parser.add_argument("--publish",action="store_true");args=parser.parse_args()
    db=DatabaseManager(load_config().database_url or "",initialize_schema=False);ensure_schema(db);reg=registration(db)
    if not reg:raise ValueError("Protocol not registered")
    rows=db.execute("""SELECT d.*,m.completed,m.home_score,m.away_score,m.game_date,m.commence_time,
        c.history_id close_history_id,c.captured_at close_captured_at,h.books close_books,
        prior.matched_legacy FROM cfb_v4_decisions d
        JOIN cfb_matchups m ON m.id=d.game_id
        LEFT JOIN verified_clv_closes c ON c.sport='cfb' AND c.matchup_id=m.id AND c.scheduled_start_at=d.kickoff
        LEFT JOIN game_odds_history h ON h.id=c.history_id
        LEFT JOIN LATERAL(SELECT jsonb_object_agg(q.version,q.evidence_json) matched_legacy FROM (
            SELECT DISTINCT ON(r.version) r.version,p.evidence_json
            FROM cfb_market_comparisons p JOIN cfb_forecast_runs r ON r.id=p.run_id
            JOIN cfb_game_forecasts f ON f.id=p.forecast_id
            WHERE p.game_id=d.game_id AND p.odds_history_id=d.odds_history_id AND f.kickoff=d.kickoff
              AND p.forecast_at<=d.frozen_at AND p.forecast_at>=d.frozen_at-INTERVAL '15 minutes'
              AND r.version IN ('cfb-score-context-v1','cfb-score-possession-v2','cfb-score-opponent-ppa-v3')
            ORDER BY r.version,p.forecast_at DESC,p.id DESC) q) prior ON TRUE
        WHERE d.protocol_version=%s ORDER BY d.frozen_at,d.id""",(VERSION,))
    report=evaluate(rows,reg);report["generated_at"]=datetime.now(timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,default=str,indent=2),encoding="utf-8")
    if args.publish:
        with db.connect() as conn:
            cur=conn.cursor();cur.execute("INSERT INTO cfb_prospective_evaluation_runs(version,season,generated_at,report_json) VALUES(%s,%s,%s,%s)",
                (VERSION,football_season_year(),report["generated_at"],Json(json.loads(json.dumps(report,default=str)))))
    print(json.dumps({k:v for k,v in report.items() if k!="games"},default=str))


if __name__=="__main__":main()
