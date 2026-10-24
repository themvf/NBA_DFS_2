"""Chronological basketball-points prediction study; reads frozen warehouse JSON.

See docs/nba-points-trends-study.md for the protocol and limitations.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import itertools
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HISTORY = ["p5", "p10", "p20", "ewma10", "p_std10", "p_last", "scoring_trend"]
OPPORTUNITY = ["minutes3", "minutes10", "minutes20", "fga3", "fga10", "fg3a3", "fg3a10",
               "fta3", "fta10", "points_per_min10", "fga_per_min10", "shot_share10"]
HOME = ["home"]
REST = ["team_b2b", "opponent_b2b", "team_rest", "opponent_rest", "player_rest", "three_in_four",
        "four_in_six", "previous_minutes", "b2b_workload", "b2b_road"]
ALLOWED = ["opp_points_allowed10", "opp_points_allowed20", "allowed_x_scoring"]
DEFENSE = ["team_pace10", "opp_pace10", "opp_def_rating10", "opp_def_rating20",
           "pace_x_scoring", "defense_x_scoring"]
STYLE = ["three_share10", "ft_rate10", "opp_three_rate10", "opp_three_pct10", "opp_ft_rate10",
         "three_style_match", "ft_style_match"]
H2H = ["h2h_residual", "h2h_count"]
FAMILIES = {"opportunity":OPPORTUNITY, "home":HOME, "rest":REST,
            "points_allowed":ALLOWED, "pace_defense":DEFENSE, "style":STYLE, "head_to_head":H2H}


def roll(frame, group, col, window, *, minimum=3, method="mean"):
    return frame.groupby(group, sort=False)[col].transform(
        lambda s:getattr(s.shift().rolling(window, min_periods=minimum), method)())


def build_features(raw: dict) -> tuple[pd.DataFrame, dict]:
    p, t = pd.DataFrame(raw["players"]), pd.DataFrame(raw["teams"])
    p["date"] = pd.to_datetime(p["game_date"])
    t["date"] = pd.to_datetime(t["game_date"])
    assert not p.duplicated(["game_id","player_id"]).any(), "duplicate player-game identity"
    assert not t.duplicated(["game_id","team_id"]).any(), "duplicate team-game identity"
    assert not t.duplicated(["date","team_id"]).any(), "team has multiple games on target date"
    assert t.groupby("game_id").size().eq(2).all(), "game must have two team rows"
    sums = p.groupby(["game_id","team_id"],as_index=False).agg(
        player_points=("points","sum"), player_minutes=("minutes","sum"), player_fgm=("fgm","sum"))
    t = t.merge(sums,on=["game_id","team_id"],how="left",validate="one_to_one")
    assert t.player_points.notna().all() and np.allclose(t.player_points,t.pts), "player/team scoring mismatch"
    other = t.add_prefix("other_")
    t = t.merge(other,left_on=["game_id","opponent_team_id"],right_on=["other_game_id","other_team_id"],validate="one_to_one")
    assert t.team_id.eq(t.other_opponent_team_id).all(), "non-reciprocal opponent join"
    assert np.allclose(t.opp_pts,t.other_pts), "opponent scoring mismatch"
    possessions = (t.fga+.44*t.fta-t.oreb+t.tov+t.other_fga+.44*t.other_fta-t.other_oreb+t.other_tov)/2
    duration = (t.player_minutes+t.other_player_minutes)/10
    assert possessions.gt(0).all() and duration.between(47,80).all(), "invalid game duration/possessions"
    t["pace"] = possessions*48/duration
    t["def_rating"] = 100*t.opp_pts/possessions
    t["three_rate"] = t.other_fg3a/t.other_fga
    t["three_pct"] = t.other_fg3m/t.other_fg3a.replace(0,np.nan)
    t["ft_rate"] = t.other_fta/t.other_fga
    t = t.sort_values(["date","game_id","team_id"]).reset_index(drop=True)
    t["team_history"] = t.groupby("team_id").cumcount()
    t["team_rest"] = (t.date-t.groupby("team_id").date.shift()).dt.days.sub(1).clip(0,7)
    t["team_b2b"] = t.team_rest.eq(0).astype(float)
    for name,days in [("three_in_four",3),("four_in_six",5)]:
        counts = pd.Series(0,index=t.index,dtype=float)
        for _, group in t.groupby("team_id"):
            dates = group.date.to_numpy()
            counts.loc[group.index] = [i-np.searchsorted(dates,d-np.timedelta64(days,"D"),side="left")+1 for i,d in enumerate(dates)]
        t[name] = counts
    lagged = []
    for col in ["pace","def_rating","opp_pts","three_rate","three_pct","ft_rate"]:
        for window in ([10,20] if col in ["def_rating","opp_pts"] else [10]):
            name=f"prior_{col}{window}"
            t[name]=roll(t,"team_id",col,window,minimum=10)
            lagged.append(name)
    team_cols=["game_id","team_id","opponent_team_id","pts","fga","team_history","team_rest","team_b2b","three_in_four","four_in_six",*lagged]
    own=t[team_cols].drop(columns="opponent_team_id").rename(columns={"fga":"team_fga","pts":"team_points"})
    opp=t[["game_id","team_id","team_history","team_rest","team_b2b",*lagged]].rename(
        columns={c:f"opponent_{c}" for c in ["team_id","team_history","team_rest","team_b2b",*lagged]})
    p=p.merge(own,on=["game_id","team_id"],how="left",validate="many_to_one")
    p=p.merge(opp,on=["game_id","opponent_team_id"],how="left",validate="many_to_one")
    assert p.team_history.notna().all() and p.opponent_team_history.notna().all(), "missing team history join"
    source_count=len(p)
    p=p.loc[p.minutes.gt(0)&p.points.notna()].sort_values(["date","game_id","player_id"]).reset_index(drop=True)
    p["player_history"]=p.groupby("player_id").cumcount()
    p["player_rest"]=(p.date-p.groupby("player_id").date.shift()).dt.days.sub(1).clip(0,14)
    assert not p.groupby("player_id").date.diff().dt.days.le(0).any(), "same-date player games"
    for window in [5,10,20]:p[f"p{window}"]=roll(p,"player_id","points",window,minimum=min(window,10))
    weights=np.array([.75**9]+[.25*.75**k for k in range(8,-1,-1)])
    p["ewma10"]=p.groupby("player_id").points.transform(lambda s:s.shift().rolling(10).apply(lambda a:float(a@weights),raw=True))
    p["p_std10"]=roll(p,"player_id","points",10,minimum=10,method="std")
    p["p_last"]=p.groupby("player_id").points.shift()
    p["scoring_trend"]=p.p5-p.p20
    for col in ["minutes","fga","fg3a","fta"]:
        for window in ([3,10,20] if col=="minutes" else [3,10]):
            p[f"{col}{window}"]=roll(p,"player_id",col,window)
    p["points_per_min10"]=p.p10/p.minutes10.replace(0,np.nan)
    p["fga_per_min10"]=p.fga10/p.minutes10.replace(0,np.nan)
    p["three_share10"]=p.fg3a10/p.fga10.replace(0,np.nan)
    p["ft_rate10"]=p.fta10/p.fga10.replace(0,np.nan)
    p["shot_share"]=p.fga/p.team_fga
    p["shot_share10"]=roll(p,"player_id","shot_share",10)
    p["home"]=p.is_home.astype(float)
    p["opponent_rest"]=p.opponent_team_rest
    p["opponent_b2b"]=p.opponent_team_b2b
    p["previous_minutes"]=p.groupby("player_id").minutes.shift()
    p["b2b_workload"]=p.team_b2b*p.previous_minutes
    p["b2b_road"]=p.team_b2b*(1-p.home)
    p["opp_points_allowed10"]=p.opponent_prior_opp_pts10
    p["opp_points_allowed20"]=p.opponent_prior_opp_pts20
    p["allowed_x_scoring"]=p.opp_points_allowed10*p.p10/110
    p["team_pace10"]=p.prior_pace10
    p["opp_pace10"]=p.opponent_prior_pace10
    p["opp_def_rating10"]=p.opponent_prior_def_rating10
    p["opp_def_rating20"]=p.opponent_prior_def_rating20
    p["pace_x_scoring"]=(p.team_pace10+p.opp_pace10)*p.p10/200
    p["defense_x_scoring"]=p.opp_def_rating10*p.p10/110
    p["opp_three_rate10"]=p.opponent_prior_three_rate10
    p["opp_three_pct10"]=p.opponent_prior_three_pct10
    p["opp_ft_rate10"]=p.opponent_prior_ft_rate10
    p["three_style_match"]=p.three_share10*p.opp_three_rate10*p.p10
    p["ft_style_match"]=p.ft_rate10*p.opp_ft_rate10*p.p10
    p["historical_residual"]=p.points-p.p10
    p["h2h_count"]=p.groupby(["player_id","opponent_team_id"]).historical_residual.transform(lambda s:s.shift().expanding().count()).fillna(0)
    p["h2h_residual"]=p.groupby(["player_id","opponent_team_id"]).historical_residual.transform(lambda s:s.shift().expanding(min_periods=2).mean()).fillna(0)
    eligible=p.player_history.ge(10)&p.team_history.ge(10)&p.opponent_team_history.ge(10)
    out=p.loc[eligible].copy()
    out["primary"]=out.p10.ge(20)
    audit={"source_player_rows":source_count,"nonappearances":source_count-len(p),"excluded_insufficient_history":int((~eligible).sum()),
           "eligible_rows":len(out),"eligible_games":out.game_id.nunique(),"primary_rows":int(out.primary.sum()),
           "game_duration_min_max":[float(duration.min()),float(duration.max())],
           "raw_opponent_fga_missing":int(pd.DataFrame(raw["teams"]).opp_fga.isna().sum()),
           "joins":"all unique, reciprocal, scoring totals reconciled"}
    return out,audit


def metrics(y,p):
    delta=np.asarray(p)-np.asarray(y)
    return {"n":len(delta),"mae":float(np.abs(delta).mean()),"rmse":float(np.sqrt((delta**2).mean())),"bias":float(delta.mean())}


def estimate(family,features,params):
    if family=="ridge":
        return make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),Ridge(alpha=params["alpha"]))
    return HistGradientBoostingRegressor(max_iter=150,learning_rate=.05,max_leaf_nodes=params["leaves"],
                                        l2_regularization=params["l2"],early_stopping=False,random_state=20261024)


def paired_ci(frame,left,right,repetitions=4000):
    delta=np.abs(frame[left]-frame.points)-np.abs(frame[right]-frame.points)
    ag=pd.DataFrame({"date":frame.date,"delta":delta}).groupby("date").delta.agg(["sum","count"]).to_numpy()
    rng=np.random.default_rng(20261024)
    samples=rng.integers(0,len(ag),size=(repetitions,len(ag)))
    differences=ag[samples,0].sum(axis=1)/ag[samples,1].sum(axis=1)
    return {"mae_delta":float(delta.mean()),"date_cluster_ci95":np.quantile(differences,[.025,.975]).tolist(),"dates":len(ag)}


def run(input_path:Path,output:Path):
    output.mkdir(parents=True,exist_ok=False)
    protocol=Path(__file__).resolve().parents[1]/"docs/nba-points-trends-study.md"
    (output/"registration.json").write_text(json.dumps({
        "started_at":datetime.now(timezone.utc).isoformat(),
        "source_sha256":hashlib.sha256(input_path.read_bytes()).hexdigest(),
        "protocol_sha256":hashlib.sha256(protocol.read_bytes()).hexdigest(),
        "code_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "validation_end":"2026-01-31", "test_start":"2026-02-01",
    },indent=2),encoding="utf-8")
    raw=json.loads(input_path.read_text(encoding="utf-8"))
    frame,audit=build_features(raw)
    dev=frame.date.lt("2026-01-01");val=frame.date.ge("2026-01-01")&frame.date.lt("2026-02-01");test=frame.date.ge("2026-02-01")
    result={"source_sha256":hashlib.sha256(input_path.read_bytes()).hexdigest(),"audit":audit,"splits":{},"validation":{},"test":{},"contrasts":{}}
    for label,mask in [("development",dev),("validation",val),("test",test)]:
        part=frame.loc[mask];result["splits"][label]={"rows":len(part),"primary_rows":int(part.primary.sum()),"dates":part.date.nunique(),"games":part.game_id.nunique(),"first":str(part.date.min().date()),"last":str(part.date.max().date())}
    naive={k:metrics(frame.loc[val&frame.primary,"points"],frame.loc[val&frame.primary,k]) for k in ["p5","p10","p20","ewma10"]}
    best=min(naive,key=lambda k:naive[k]["mae"])
    result["naive_validation"]=naive;result["selected_naive"]=best
    held=frame.loc[test].copy();held["naive"]=held[best]
    sets={"history":HISTORY,**{k:HISTORY+v for k,v in FAMILIES.items()},"full":HISTORY+sum(FAMILIES.values(),[])}
    sets.update({f"without_{k}":HISTORY+sum([v for name,v in FAMILIES.items() if name!=k],[]) for k in FAMILIES})
    fitmask=dev|val
    coefficients={}
    for model_family,choices in [("ridge",[{"alpha":a} for a in [10,100,1000]]),("hgb",[{"leaves":n,"l2":l} for n in [7,15] for l in [10,100]])]:
        for label,features in sets.items():
            # Nonlinear sensitivity for history, incremental families and full;
            # the leave-one-family-out fits use the primary Ridge estimator.
            if model_family=="hgb" and label.startswith("without_"):continue
            name=f"{model_family}_{label}"
            scores=[]
            for params in choices:
                model=estimate(model_family,features,params)
                model.fit(frame.loc[dev,features],frame.loc[dev,"points"])
                pred=np.maximum(0,model.predict(frame.loc[val,features]))
                primary=frame.loc[val,"primary"].to_numpy()
                scores.append((metrics(frame.loc[val&frame.primary,"points"],pred[primary])["mae"],params))
            loss,params=min(scores,key=lambda x:x[0])
            result["validation"][name]={"mae":loss,"params":params}
            model=estimate(model_family,features,params);model.fit(frame.loc[fitmask,features],frame.loc[fitmask,"points"])
            held[name]=np.maximum(0,model.predict(held[features]))
            if name=="ridge_full":coefficients=dict(zip(features,model[-1].coef_.tolist()))
            print(name,"validation",round(loss,4),flush=True)
    # Selection of a candidate is frozen by validation, before reading test results.
    candidate_names=[k for k in result["validation"] if not k.split("_",1)[1].startswith("without_")]
    selected=min(candidate_names,key=lambda k:result["validation"][k]["mae"])
    result["validation_selected_model"]=selected
    predcols=["naive",*result["validation"]]
    for name in predcols:
        result["test"][name]={}
        for cohort,mask in [("primary",held.primary),("all",pd.Series(True,index=held.index)),
                            ("primary_before_april",held.primary&held.date.lt("2026-04-01"))]:
            part=held.loc[mask]
            result["test"][name][cohort]=metrics(part.points,part[name])
            if name!="naive":result["test"][name][cohort].update(paired_ci(part,name,"naive"))
    for family in ["ridge","hgb"]:
        for label in FAMILIES:
            result["contrasts"][f"{family}_{label}_vs_history"]=paired_ci(held.loc[held.primary],f"{family}_{label}",f"{family}_history")
    for label in FAMILIES:
        result["contrasts"][f"ridge_full_vs_without_{label}"]=paired_ci(held.loc[held.primary],"ridge_full",f"ridge_without_{label}")
    result["standardized_ridge_coefficients"]=coefficients
    result["trends"]=trend_summaries(held,selected,raw)
    result["correlations"]=correlations(held,selected)
    held.to_csv(output/"heldout-predictions.csv",index=False)
    (output/"results.json").write_text(json.dumps(result,indent=2,default=lambda o:int(o) if isinstance(o,np.integer) else str(o)),encoding="utf-8")
    print(json.dumps({"audit":audit,"splits":result["splits"],"naive":best,"selected":selected,"test":{k:v["primary"] for k,v in result["test"].items()}},indent=2,default=str))


def trend_summaries(held,selected,raw):
    out={}
    primary=held.loc[held.primary].copy()
    def summarize(group):
        return {"n":len(group),"players":group.player_id.nunique(),"points":float(group.points.mean()),
                "history_residual":float((group.points-group.p10).mean()),
                "model_residual":float((group.points-group[selected]).mean()),
                "minutes":float(group.minutes.mean()),"fga":float(group.fga.mean())}
    for name,col in [("back_to_back","team_b2b"),("home","home"),("opponent_b2b","opponent_b2b")]:
        out[name]={str(k):summarize(g) for k,g in primary.groupby(col)}
    primary["defense_band"]=pd.cut(primary.opp_def_rating10,[-np.inf,110,115,np.inf],labels=["under110","110to115","over115"])
    primary["pace_band"]=pd.cut((primary.team_pace10+primary.opp_pace10)/2,[-np.inf,98,102,np.inf],labels=["under98","98to102","over102"])
    for col in ["defense_band","pace_band","three_in_four","four_in_six"]:
        out[col]={str(k):summarize(g) for k,g in primary.groupby(col,observed=True)}
    names={int(t["team_id"]):t["name"] for t in raw["team_names"]}
    out["opponents"]=[{"team":names[int(k)],**summarize(g)} for k,g in primary.groupby("opponent_team_id")]
    out["opponents"].sort(key=lambda r:r["history_residual"])
    out["h2h_coverage"]={"primary_rows":len(primary),"two_prior_meetings":int(primary.h2h_count.ge(2).sum())}
    # Player-balanced B2B contrast so stars with different schedules don't
    # silently drive the raw difference. Descriptive, not a causal estimate.
    diffs=[]
    for player,g in primary.groupby("player_id"):
        a=g.loc[g.team_b2b.eq(1)];b=g.loc[g.team_b2b.eq(0)]
        if len(a)>=3 and len(b)>=3:
            diffs.append({"player":int(player),"n_b2b":len(a),"n_other":len(b),
                          "residual_delta":float((a.points-a.p10).mean()-(b.points-b.p10).mean())})
    out["within_player_b2b"]={"players":len(diffs),"equal_player_mean_residual_delta":float(np.mean([r["residual_delta"] for r in diffs])),"details":diffs}
    return out


def correlations(held,selected):
    pairs=[]
    for game,g in held.loc[held.p10.ge(15)].groupby("game_id"):
        for (_,a),(_,b) in itertools.combinations(g.iterrows(),2):
            if a.player_id>b.player_id:a,b=b,a
            pairs.append({"game":game,"date":a.date,"a":int(a.player_id),"b":int(b.player_id),
                          "name_a":a["name"],"name_b":b["name"],"same_team":a.team_id==b.team_id,
                          "residual_a":a.points-a[selected],"residual_b":b.points-b[selected]})
    pairs=pd.DataFrame(pairs)
    out={"scope":"held-out appearances with pregame ten-game mean >=15 points; contemporaneous descriptive correlations", "aggregate":{},"pairs":[]}
    for same,g in pairs.groupby("same_team"):
        out["aggregate"]["teammates" if same else "opponents"]={"pair_games":len(g),"games":g.game.nunique(),
            "residual_correlation":float(g.residual_a.corr(g.residual_b))}
    for (_,_,same),g in pairs.groupby(["a","b","same_team"]):
        if len(g)<12:continue
        out["pairs"].append({"a":g.name_a.iloc[0],"b":g.name_b.iloc[0],"same_team":bool(same),"n":len(g),
                             "correlation":float(g.residual_a.corr(g.residual_b))})
    out["pairs"].sort(key=lambda r:r["correlation"])
    return out


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args=parser.parse_args()
    run(args.input,args.output)
