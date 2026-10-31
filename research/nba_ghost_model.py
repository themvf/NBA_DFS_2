"""Build a transparent, fixed-parameter research artifact for ghost v2.

Chronological replay results are saved with the artifact. Its coefficients are
JSON, so inference does not deserialize executable sklearn/pickle objects.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from research.nba_points_ghost_v2 import ARTIFACT_VERSION, history_features, linear_predict, valid_stats
from research.nba_points_forward import digest, write_new

HISTORY=['p5','p10','p20','ewma10','p_std10','p_last','scoring_trend','reference_q90']
OPPORTUNITY=['minutes3','minutes10','minutes20','fga3','fga10','fg3a3','fg3a10','fta3','fta10',
             'points_per_min10','fga_per_min10','shot_share10','minutes_growth','shots_growth',
             'ft_growth','fga_last','fta_last','volume_points3','conversion_gap3']
FEATURES=HISTORY+OPPORTUNITY


def frame_from_logs(raw):
    import pandas as pd
    if any(r['season_type']!='Regular Season' for r in raw['players']):raise ValueError('training source must contain only regular-season logs')
    teams={(str(t['game_id']),t['team_id']):t for t in raw['teams']}
    team_counts={};counts={}
    for t in sorted(raw['teams'],key=lambda t:(t['game_date'],t['game_id'],t['team_id'])):
        team_counts[(str(t['game_id']),t['team_id'])]=counts.get(t['team_id'],0)
        counts[t['team_id']]=counts.get(t['team_id'],0)+1
    history={};out=[];seen=set()
    for r in sorted(raw['players'],key=lambda r:(r['game_date'],r['game_id'],r['player_id'])):
        if not r.get('minutes') or r['minutes']<=0 or r.get('points') is None:continue
        if not valid_stats(r):raise ValueError('invalid training scoring or shot accounting')
        identity=(str(r['game_id']),r['player_id'])
        if identity in seen:raise ValueError('duplicate training player-game')
        seen.add(identity)
        prior=history.setdefault(r['player_id'],[])
        f=history_features(prior)
        if f and team_counts[(str(r['game_id']),r['team_id'])]>=10 and team_counts[(str(r['game_id']),r['opponent_team_id'])]>=10:
            out.append({**f,'date':str(r['game_date']),'player_id':r['player_id'],'game_id':str(r['game_id']),
                        'points':r['points'],'minutes':r['minutes'],'fga':r['fga'],'fg3a':r['fg3a'],'fta':r['fta'],
                        'primary':f['p10']>=20,'tail_hit':int(r['points']>f['reference_q90'])})
        team=teams[(str(r['game_id']),r['team_id'])]
        prior.append({**r,'team_fga':team['fga']})
    return pd.DataFrame(out)


def pack(pipeline):
    imputer,scaler,learner=pipeline
    return {'features':FEATURES,'impute':imputer.statistics_.tolist(),'mean':scaler.mean_.tolist(),
            'scale':scaler.scale_.tolist(),'coef':learner.coef_.reshape(-1).tolist(),
            'intercept':float(learner.intercept_.reshape(-1)[0]) if hasattr(learner.intercept_,'reshape') else float(learner.intercept_)}


def build(raw, source_hash, created):
    import numpy as np
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import make_pipeline
    from sklearn.linear_model import LogisticRegression, QuantileRegressor, Ridge
    from sklearn.metrics import mean_absolute_error, mean_pinball_loss
    frame=frame_from_logs(raw)
    dev=frame[frame.date<'2026-01-01'];cal=frame[(frame.date>='2026-01-01')&(frame.date<'2026-02-01')];evaluation=frame[frame.date>='2026-02-01'].copy()
    if min(len(dev),len(cal),len(evaluation))<100:raise ValueError('insufficient chronological training/calibration/evaluation coverage')
    def fit(learner,target):
        pipeline=make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),StandardScaler(),learner)
        pipeline.fit(dev[FEATURES],dev[target])
        return pipeline
    watch=fit(LogisticRegression(C=1,max_iter=1500,random_state=20261024),'tail_hit')
    p90=fit(QuantileRegressor(quantile=.9,alpha=.05,solver='highs'),'points')
    models={k:fit(Ridge(alpha=100),k) for k in ['minutes','fga','fg3a','fta']}
    thresholds={};offsets={}
    for primary in [False,True]:
        part=cal[cal.primary] if primary else cal
        thresholds[str(primary)]=float(np.quantile(watch.decision_function(part[FEATURES]),.8))
        calibration_group=cal[cal.primary.eq(primary)]
        offsets[str(primary)]=float(np.quantile(calibration_group.points-p90.predict(calibration_group[FEATURES]),.9))
    development_rows=[r for r in raw['players'] if str(r['game_date'])<'2026-01-01' and r.get('minutes') and r['minutes']>0]
    total=lambda k:sum(float(r[k]) for r in development_rows)
    league={'two':(total('fgm')-total('fg3m'))/(total('fga')-total('fg3a')),'three':total('fg3m')/total('fg3a'),'free':total('ftm')/total('fta')}
    artifact={'version':ARTIFACT_VERSION,'created_at':created.isoformat(),'source_sha256':source_hash,
              'development_end':'2025-12-31','calibration_end':'2026-01-31','watch_model':pack(watch),
              'p90_model':pack(p90),'p90_offsets':offsets,'watch_score_thresholds':thresholds,
              'opportunity_models':{k:pack(v) for k,v in models.items()},'league_rates':league,
              'method':'Fixed research parameters; .05 penalized linear P90, Ridge alpha100 opportunities, logistic C1 watch.',
              'limitations':['Exploratory historical reuse, not pristine holdout. No pregame injuries or minute restrictions.',
                             'No calibrated watch probabilities published; no evidence of improvement over sportsbook lines.',
                             'Minimum ten prior appearances; current-season/offseason transfer unvalidated.']}
    # Verify exported JSON inference agrees with the fitted sklearn estimators.
    sample=evaluation.iloc[:50]
    for spec,pipeline in [(artifact['watch_model'],watch),(artifact['p90_model'],p90),*[(artifact['opportunity_models'][k],v) for k,v in models.items()]]:
        actual=np.array([linear_predict(spec,r) for r in sample.to_dict('records')])
        expected=pipeline.decision_function(sample[FEATURES]) if pipeline is watch else pipeline.predict(sample[FEATURES])
        assert np.allclose(actual,expected,atol=1e-8),'portable inference drift'
    evaluation['model_q90']=p90.predict(evaluation[FEATURES])+evaluation.primary.map(lambda x:offsets[str(bool(x))])
    evaluation['watch_score']=watch.decision_function(evaluation[FEATURES])
    evaluation['alert']=evaluation.watch_score>=evaluation.primary.map(lambda x:thresholds[str(bool(x))])
    results={'splits':{'development':len(dev),'calibration':len(cal),'evaluation':len(evaluation)},'portable_inference_verified':True,'cohorts':{}}
    for label,part in [('all',evaluation),('prior10_points_ge20',evaluation[evaluation.primary])]:
        results['cohorts'][label]={'n':len(part),'p90_exceedance':float((part.points>part.model_q90).mean()),
            'p90_quantile_loss':float(mean_pinball_loss(part.points,part.model_q90,alpha=.9)),
            'historical_q90_loss':float(mean_pinball_loss(part.points,part.reference_q90,alpha=.9)),
            'watch':{str(flag):{'n':int(part.alert.eq(flag).sum()),'hits':int(part.loc[part.alert.eq(flag),'tail_hit'].sum())} for flag in [False,True]},
            'opportunity_mae':{k:float(mean_absolute_error(part[k],np.maximum(0,models[k].predict(part[FEATURES])))) for k in models}}
    artifact['evaluation']=results;artifact['sha256']=digest(artifact)
    return artifact


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True,type=Path);parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();content=args.input.read_bytes()
    result=build(json.loads(content),hashlib.sha256(content).hexdigest(),datetime.now(timezone.utc))
    write_new(args.output,result)
    print(json.dumps({'output':str(args.output),'sha256':result['sha256'],'evaluation':result['evaluation']},indent=2))


if __name__=='__main__':main()
