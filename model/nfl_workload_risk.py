"""Learn conditional inactive/limited/normal workload states from earlier games.

This is a shadow candidate, not a medical or production participation forecast.
No history means no probabilities. A listed active player can still have a
limited workload. New labels cannot enter an earlier decision's fitted model.
"""
import math
import re
import numpy as np
from model.nfl_ownership import digest, stamp

VERSION = "nfl-workload-risk-v1"
POSITIONS = ("QB", "RB", "WR", "TE")
FEATURES = ("doubtful", "log_baseline_opportunities", "depth_order", "depth_missing",
            "days_since_last_active", "recency_missing", *POSITIONS)
STATES = ("inactive", "limited", "normal")
LIMITED_RATIO = .75


def features(row):
    if not row.get('snapshot_id') or not row.get('observation_ids') or not re.fullmatch(r'[a-f0-9]{64}', row.get('baseline_source_digest','')):
        raise ValueError('Frozen designation and baseline provenance are required')
    if row["position"] not in POSITIONS or row["designation"] not in ("QUESTIONABLE", "DOUBTFUL"):
        raise ValueError("A skill-player Q/D pregame designation is required")
    baseline=row["baseline_opportunities"]
    if not isinstance(baseline,(int,float)) or isinstance(baseline,bool) or not math.isfinite(baseline) or baseline<=0:
        raise ValueError("An observed positive baseline workload is required")
    if not stamp(row["features_available_at"])<=stamp(row["decision_at"])<stamp(row["kickoff"]):
        raise ValueError("Workload features violate the pregame time boundary")
    optional=[]
    for key in ("depth_order","days_since_last_active"):
        value=row.get(key)
        if value is not None and (isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<0):
            raise ValueError("Invalid role/recency feature")
        optional.extend([value or 0,float(value is None)])
    return [float(row["designation"]=="DOUBTFUL"),math.log1p(baseline),*optional,
            *[float(row["position"]==pos) for pos in POSITIONS]]


def outcome(row):
    if not isinstance(row.get("played"),bool) or not isinstance(row.get("actual_opportunities"),(int,float)):
        raise ValueError("Settled participation and opportunity labels are required")
    actual=row["actual_opportunities"]
    if isinstance(actual,bool) or not math.isfinite(actual) or actual<0 or not row["played"] and actual!=0:
        raise ValueError("Participation/opportunity labels disagree")
    if not stamp(row['kickoff'])<stamp(row['settled_at'])<=stamp(row['labels_available_at']):
        raise ValueError("Outcomes require settled-game timestamps")
    return 0 if not row["played"] else 1 if actual/row["baseline_opportunities"]<LIMITED_RATIO else 2


def softmax(values):
    exp=np.exp(values-values.max(axis=-1,keepdims=True))
    return exp/exp.sum(axis=-1,keepdims=True)


def fit(cases,as_of):
    keys=[(r["player_id"],r["game_id"]) for r in cases]
    if len(keys)!=len(set(keys)):raise ValueError("Duplicate player-game risk case")
    accepted=[]
    for row in cases:
        features(row);outcome(row)
        if stamp(row["labels_available_at"])<=stamp(as_of) and stamp(row["kickoff"])<stamp(as_of):accepted.append(row)
    if len(accepted)<30:
        return {"version":VERSION,"status":"insufficient_training","as_of":as_of,"probabilities":None,
                "training_cases":len(accepted),"authority":"shadow_only"}
    x=np.array([features(r) for r in accepted]);y=np.eye(3)[[outcome(r) for r in accepted]]
    mean=x.mean(axis=0);scale=x.std(axis=0);scale[scale<1e-8]=1
    z=np.column_stack([np.ones(len(x)),(x-mean)/scale])
    coefficients=np.zeros((z.shape[1],3))
    for _ in range(400):
        penalty=.05*coefficients;penalty[0]=0
        coefficients-=.08*(z.T@(softmax(z@coefficients)-y)/len(z)+penalty)
    limited=[min(LIMITED_RATIO,r["actual_opportunities"]/r["baseline_opportunities"]) for r in accepted if outcome(r)==1]
    return {"version":VERSION,"status":"shadow_fitted","as_of":as_of,"authority":"shadow_only",
            "training_cases":len(accepted),"training_digest":digest(accepted),
            "training_games":sorted({r["game_id"] for r in accepted}),"features":FEATURES,
            "mean":mean.tolist(),"scale":scale.tolist(),"coefficients":coefficients.tolist(),
            "limited_workload_factor":float(np.mean(limited)) if limited else None,
            "qualification":"withheld_pending_participation_and_workload_gates"}


def predict(model,row):
    values=features(row)
    if stamp(model["as_of"])>stamp(row["decision_at"]):raise ValueError("Risk model was not available at this decision")
    if model["status"]!="shadow_fitted":return {"version":VERSION,"probabilities":None,"authority":"shadow_only","optimizer_enabled":False}
    if row["game_id"] in model["training_games"]:raise ValueError("Target game occurred in risk-model training")
    z=np.r_[1,(np.array(values)-model["mean"])/model["scale"]]
    probabilities=softmax(z@np.array(model["coefficients"])).tolist()
    return {"version":VERSION,"probabilities":dict(zip(STATES,probabilities)),
            "participation_probability":1-probabilities[0],"limited_workload_factor":model["limited_workload_factor"],
            "authority":"shadow_only","optimizer_enabled":False,"training_digest":model["training_digest"],
            "limitations":["Pregame designation/role/workload association, not a medical prediction.",
                           "Probability/workload calibration and the registered prospective sample are still required."]}
