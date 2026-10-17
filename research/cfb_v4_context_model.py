"""Separate context-effect shadow study. Never changes the registered base decisions.

No numerical context adjustment is produced before sufficient timestamped source
coverage exists. Effect sizes are learned from earlier observed outcomes, never
hand-assigned from an injury label or a depth-chart rank.
"""
from __future__ import annotations

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

NAMES=["home_qb_changed","away_qb_changed","home_ol_unavailable","away_ol_unavailable",
       "home_unavailable","away_unavailable","home_continuity","away_continuity",
       "home_coach_changed","away_coach_changed","temperature_c","wind_kmh","precipitation_mm"]


def vector(evidence):
    captures=evidence.get("context",{}).get("captures",{})
    team_values={}
    for side in ("home","away"):
        p=captures.get(side,{}).get("payload_json",{})
        qb=p.get("qb") or {};players=p.get("unavailable_players")
        if (qb.get("status")!="confirmed" or not isinstance(qb.get("changed"),bool)
                or not isinstance(p.get("coach_changed"),bool) or p.get("roster_continuity") is None
                or not isinstance(players,list) or p.get("ol_reviewed") is not True):return None
        # Counts are descriptive covariates; no snap or point impact is inferred from them.
        ol=sum(str(r.get("position","")).upper() in {"OL","OT","OG","C","G","T"} for r in players)
        team_values[side]=[float(qb["changed"]),float(ol),float(len(players)),float(p["roster_continuity"]),float(p["coach_changed"])]
    weather=captures.get("weather",{}).get("payload_json",{})
    samples=weather.get("samples") or []
    units=weather.get("hourly_units") or {}
    if weather.get("status")!="captured" or not samples or units.get("wind_speed_10m")!="km/h" or units.get("precipitation")!="mm":return None
    try:
        values=[team_values[s][i] for i in range(5) for s in ("home","away")]
        values.extend([float(np.mean([r["temperature_2m"] for r in samples])),
                       float(np.mean([r["wind_speed_10m"] for r in samples])),
                       float(np.mean([r["precipitation"] for r in samples]))])
        return values if all(np.isfinite(v) for v in values) else None
    except (KeyError,TypeError,ValueError):return None


def fit_context(rows):
    eligible=[{**r,"x":vector(r["evidence_json"])} for r in rows]
    eligible=[r for r in eligible if r["x"] is not None and r["evidence_json"].get("forecast")]
    dates=sorted({str(r["game_date"]) for r in eligible})
    report={"eligible_games":len(eligible),"game_dates":len(dates),"status":"unfitted_context_effects",
            "feature_order":NAMES,"betting_permission":False,"applied_to_primary":False,
            "interpretation":"Context study is separate development evidence; counts do not estimate individual player opportunity or causal impact."}
    if len(eligible)<200 or len(dates)<30:return report,None
    split=dates[max(1,int(len(dates)*.7))]
    train=[r for r in eligible if str(r["game_date"])<split];test=[r for r in eligible if str(r["game_date"])>=split]
    if len(train)<100 or len(test)<30:return report,None
    x=[r["x"] for r in train]
    y=[[r["home_score"]-r["away_score"]-r["evidence_json"]["forecast"]["margin"],
        r["home_score"]+r["away_score"]-r["evidence_json"]["forecast"]["total"]] for r in train]
    model=make_pipeline(StandardScaler(),Ridge(alpha=100.));model.fit(x,y)
    correction=model.predict([r["x"] for r in test]);errors=[]
    for i,r in enumerate(test):
        actual=np.array([r["home_score"]-r["away_score"],r["home_score"]+r["away_score"]])
        base=np.array([r["evidence_json"]["forecast"]["margin"],r["evidence_json"]["forecast"]["total"]])
        errors.append([np.abs(base-actual),np.abs(base+correction[i]-actual)])
    scaler,ridge=model.steps[0][1],model.steps[1][1]
    report.update(status="exploratory_context_shadow",training_n=len(train),evaluation_n=len(test),
                  training_before=split,baseline_mae=np.mean([e[0] for e in errors],axis=0).tolist(),
                  context_mae=np.mean([e[1] for e in errors],axis=0).tolist(),
                  scaler_mean=scaler.mean_.tolist(),scaler_scale=scaler.scale_.tolist(),
                  coefficients=ridge.coef_.tolist(),intercept=ridge.intercept_.tolist())
    return report,model
