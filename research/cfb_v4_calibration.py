"""Time-ordered market residual fitting and discrete, held-out error distributions."""
from __future__ import annotations

import math
import numpy as np
from scipy.optimize import minimize
from scipy.special import expit, logit
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


def fit_football(rows):
    rows=[r for r in rows if r.get("actual")]
    if len(rows)<300:
        raise ValueError("need 300 usable training games")
    x=[r["x_home"] for r in rows]+[r["x_away"] for r in rows]
    y=[r["actual"][i]["off"]["ppd"] for i in (0,1) for r in rows]
    model=make_pipeline(StandardScaler(),Ridge(alpha=100.))
    model.fit(x,y)
    pace=make_pipeline(StandardScaler(),Ridge(alpha=100.))
    px=[r["pace_features"][i] for i in (0,1) for r in rows]
    py=[r["actual"][i]["off"]["drives"] for i in (0,1) for r in rows]
    pace.fit(px,py)
    model.cfb_pace_=pace
    return model


def football_score(row,model):
    rates=np.clip(model.predict([row["x_home"],row["x_away"]]),0.,8.)
    profiles=row["profiles"]
    other=[profiles[row[k]]["off"]["other"] for k in ("home_team_id","away_team_id")]
    drives=np.clip(model.cfb_pace_.predict(row["pace_features"]),6.,18.)
    scores=[max(0.,float(rates[i])*float(drives[i])+other[i]) for i in (0,1)]
    return {"home_points":scores[0],"away_points":scores[1],"margin":scores[0]-scores[1],
            "total":sum(scores),"rates":rates.tolist(),"expected_drives":drives.tolist(),
            "other_points":other}


def date_split(rows):
    dates=sorted({r["date"] for r in rows})
    if len(dates)<4:
        return [],[],[],{"reason":"fewer_than_four_development_dates"}
    a=max(1,len(dates)//2);b=max(a+1,3*len(dates)//4)
    return ([r for r in rows if r["date"] in dates[:a]],
            [r for r in rows if r["date"] in dates[a:b]],
            [r for r in rows if r["date"] in dates[b:]],
            {"training_dates":dates[:a],"calibration_dates":dates[a:b],"evaluation_dates":dates[b:]})


def fit_market(rows):
    train,cal,test,boundaries=date_split(rows)
    result={"split":boundaries,"training_n":len(train),"calibration_n":len(cal),"evaluation_n":len(test),
            "status":"RESEARCH_ONLY","betting_permission":False}
    # Shrink toward the market; zero is a legitimate fitted solution.
    for field in ("margin","total"):
        x=np.asarray([r["raw"][field]-r["market"][field] for r in train])
        y=np.asarray([r["actual"][field]-r["market"][field] for r in train])
        weight=float(np.clip(np.dot(x,y)/(np.dot(x,x)+100.),0.,1.)) if len(train)>=20 else 0.
        result[field]={"weight":weight}
    for field in ("margin","total"):
        errors=[int(r["actual"][field])-round(centers(r,result)[field]) for r in cal]
        result[field].update(residuals=errors if len(cal)>=20 else [],
                             probability_status="exploratory" if len(cal)>=20 else "insufficient_calibration_games")
    # Separate binary moneyline calibration, with an offset at contemporaneous no-vig market probability.
    mlcal=[r for r in cal if r["market"].get("home_prob") is not None]
    if len(mlcal)>=20 and len({r["actual"]["margin"]>0 for r in mlcal})==2:
        p=np.asarray([np.clip(r["market"]["home_prob"],.001,.999) for r in mlcal])
        z=np.asarray([(r["raw"]["margin"]-r["market"]["margin"])/14. for r in mlcal])
        y=np.asarray([float(r["actual"]["margin"]>0) for r in mlcal])
        offset=logit(p)
        def objective(theta):
            eta=offset+theta[0]+theta[1]*z
            return float(np.sum(np.logaddexp(0,eta)-y*eta)+10.*np.dot(theta,theta))
        opt=minimize(objective,[0.,0.],bounds=[(-2.,2.),(0.,2.)],method="L-BFGS-B")
        result["moneyline"]={"intercept":float(opt.x[0]),"football_coefficient":float(opt.x[1]),"n":len(mlcal),"fitted":bool(opt.success)}
    else:
        result["moneyline"]={"intercept":0.,"football_coefficient":0.,"n":len(mlcal),"fitted":False}
    result["evaluation"]=evaluate(test,result)
    return result


def centers(row,fit):
    center={field:row["market"][field]+fit[field]["weight"]*(row["raw"][field]-row["market"][field]) for field in ("margin","total")}
    center["total"]=max(0.,abs(center["margin"]),center["total"])
    return center


def predict(row,fit):
    center=centers(row,fit)
    p=row["market"].get("home_prob")
    ml=fit["moneyline"]
    probability=float(expit(logit(np.clip(p,.001,.999))+ml["intercept"]+ml["football_coefficient"]*(row["raw"]["margin"]-row["market"]["margin"])/14.)) if p is not None and ml["fitted"] else None
    return {**center,"home_win_probability":probability,
            "home_points":max(0.,(center["total"]+center["margin"])/2),
            "away_points":max(0.,(center["total"]-center["margin"])/2)}


def proposition(center,residuals,line,price,side,market):
    if len(residuals)<20:
        return {"p_win":None,"p_push":None,"p_loss":None,"ev":None,"status":"insufficient_calibration_games"}
    samples=round(center)+np.asarray(residuals)
    if market=="total":samples=np.maximum(0,samples)
    difference=samples+line if market=="spread" else samples-line
    if side in ("away","under"):
        difference=-difference
    win,push,loss=[float(np.mean(test(difference))) for test in (lambda x:x>0,lambda x:x==0,lambda x:x<0)]
    decimal=1+price/100. if price>0 else 1+100./abs(price)
    return {"p_win":win,"p_push":push,"p_loss":loss,"ev":win*decimal+push-1.,"status":"exploratory",
            "interval80":[float(np.quantile(samples,.1)),float(np.quantile(samples,.9))],"calibration_n":len(residuals)}


def evaluate(rows,fit):
    if not rows:
        return {"n":0,"game_dates":0,"status":"no_evaluation_sample"}
    metrics={"n":len(rows),"game_dates":len({r["date"] for r in rows}),"status":"development_diagnostic_not_confirmation"}
    for field in ("margin","total"):
        metrics[field]={label:float(np.mean([abs(pred(r)-r["actual"][field]) for r in rows])) for label,pred in {
            "market_mae":lambda r:r["market"][field],"raw_mae":lambda r:r["raw"][field],
            "fixed25_mae":lambda r:r["market"][field]+.25*(r["raw"][field]-r["market"][field]),
            "fitted_mae":lambda r:predict(r,fit)[field]}.items()}
    ml=[(r,predict(r,fit)["home_win_probability"]) for r in rows]
    ml=[(r,p) for r,p in ml if p is not None]
    if ml:
        y=np.asarray([float(r["actual"]["margin"]>0) for r,p in ml]);ps=np.asarray([p for r,p in ml])
        metrics["moneyline"]={"n":len(ml),"brier":float(np.mean((ps-y)**2)),
                              "market_brier":float(np.mean([(r["market"]["home_prob"]-float(r["actual"]["margin"]>0))**2 for r,p in ml])),
                              "log_loss":float(np.mean(-(y*np.log(np.clip(ps,1e-6,1-1e-6))+(1-y)*np.log(np.clip(1-ps,1e-6,1-1e-6)))))}
        metrics["moneyline"]["bins"]=[{"lower":lo/10,"n":int(sum((ps>=lo/10)&(ps<(lo+2)/10))),
             "predicted":float(np.mean(ps[(ps>=lo/10)&(ps<(lo+2)/10)])) if any((ps>=lo/10)&(ps<(lo+2)/10)) else None,
             "observed":float(np.mean(y[(ps>=lo/10)&(ps<(lo+2)/10)])) if any((ps>=lo/10)&(ps<(lo+2)/10)) else None} for lo in (0,2,4,6,8)]
    metrics["uncertainty"]="Withheld below 30 distinct game dates; no predictive clearance from this diagnostic."
    return metrics
