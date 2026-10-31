"""Exploratory opponent/player identity extension AFTER the v1 test was seen.

This cannot be reported as a newly untouched validation period. Its purpose
is to check whether numeric defensive proxies missed team-specific effects.
"""
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from research.nba_points_trends import build_features, HISTORY, FAMILIES, metrics, paired_ci


def run(input_path,output):
    output.mkdir(parents=True,exist_ok=False)
    (output/"registration.json").write_text(json.dumps({
        "registered_at":datetime.now(timezone.utc).isoformat(),
        "status":"exploratory_posthoc_extension_after_v1_test_inspection",
        "source_sha256":hashlib.sha256(input_path.read_bytes()).hexdigest(),
        "code_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "hypotheses":["opponent team identity beyond numeric defensive proxies", "player identity beyond rolling scoring history"],
        "warning":"The February-April period is already inspected. No new confirmatory holdout claim.",
    },indent=2))
    f,audit=build_features(json.loads(input_path.read_text(encoding="utf-8")))
    f["player_category"]=f.player_id.astype(str)
    f["opponent_category"]=f.opponent_team_id.astype(str)
    dev=f.date.lt("2026-01-01");val=f.date.ge("2026-01-01")&f.date.lt("2026-02-01");test=f.date.ge("2026-02-01")
    sets={"history_player":(HISTORY,["player_category"]),
          "history_opponent":(HISTORY,["opponent_category"]),
          "history_player_opponent":(HISTORY,["player_category","opponent_category"]),
          "full_player_opponent":(HISTORY+sum(FAMILIES.values(),[]),["player_category","opponent_category"])}
    held=f.loc[test].copy()
    original=pd.read_csv(input_path.parent/"run-v1"/"heldout-predictions.csv",dtype={"game_id":str})
    # Match by explicit identity; CSV game ids can carry leading zeroes.
    original.game_id=original.game_id.astype(str).str.zfill(10)
    held.game_id=held.game_id.astype(str).str.zfill(10)
    held=held.merge(original[["game_id","player_id","ridge_history","naive"]],on=["game_id","player_id"],validate="one_to_one")
    def fit_model(numeric,categories,alpha):
        transform=ColumnTransformer([
            ("numeric",make_pipeline(SimpleImputer(strategy="median"),StandardScaler()),numeric),
            ("identity",OneHotEncoder(handle_unknown="ignore"),categories),
        ])
        return make_pipeline(transform,Ridge(alpha=alpha,solver="lsqr",tol=1e-6,max_iter=2000))
    out={"status":"exploratory_posthoc_extension", "models":{}}
    for label,(numeric,categories) in sets.items():
        candidates=[]
        for alpha in [10,100,1000]:
            m=fit_model(numeric,categories,alpha).fit(f.loc[dev],f.loc[dev,"points"])
            pred=np.maximum(0,m.predict(f.loc[val&f.primary]))
            candidates.append((metrics(f.loc[val&f.primary,"points"],pred)["mae"],alpha))
        loss,alpha=min(candidates)
        m=fit_model(numeric,categories,alpha).fit(f.loc[dev|val],f.loc[dev|val,"points"])
        held[label]=np.maximum(0,m.predict(held))
        part=held.loc[held.primary]
        out["models"][label]={"alpha":alpha,"validation_mae":loss,"test":metrics(part.points,part[label]),
                               "vs_history":paired_ci(part,label,"ridge_history"),"vs_naive":paired_ci(part,label,"naive")}
    part=held.loc[held.primary]
    out["opponent_added_to_player_history"]=paired_ci(part,"history_player_opponent","history_player")
    held[["game_id","player_id","date","name","points","primary",*sets]].to_csv(output/"predictions.csv",index=False)
    (output/"results.json").write_text(json.dumps(out,indent=2),encoding="utf-8")
    print(json.dumps(out,indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args=parser.parse_args()
    run(args.input,args.output)
