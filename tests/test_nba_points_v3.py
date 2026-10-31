from copy import deepcopy
from datetime import datetime, timezone, timedelta

import numpy as np
import pytest

from research.nba_points_forward import digest
from research.nba_points_v3_data import prepare, examples, features, context_features, absence_scenarios, FEATURES
from research.nba_points_v3_model import predict, probability, fair_odds, crps, fit, evaluate_joint, MODEL_VERSION
from research.nba_points_v3_distribution import joint_draws, fractional_top_slots
from research.nba_points_v3 import forecast, grade, diagnose, record_day, VERSION


def raw_fixture():
    raw = {'players':[],'teams':[],'team_names':[{'team_id':1,'name':'Boston Celtics'},{'team_id':2,'name':'Miami Heat'}]}
    for game in range(20):
        day = str((datetime(2026,3,1)+timedelta(days=game)).date())
        for side in [1,2]:
            raw['teams'].append(dict(game_id=str(game),team_id=side,opponent_team_id=3-side,
                game_date=day,season_type='Regular Season',is_home=side==1,fga=60,fg3a=20,fta=10,
                pts=65,oreb=5,tov=10,fetched_at='2026-10-24T12:00:00+00:00'))
            for n in range(5):
                raw['players'].append(dict(game_id=str(game),player_id=side*10+n,name=f'Player {side} {n}',
                    game_date=day,team_id=side,opponent_team_id=3-side,is_home=side==1,
                    season_type='Regular Season',minutes=48.,points=13.,fgm=5.,fga=12.,fg3m=1.,fg3a=4.,ftm=2.,fta=2.,
                    fetched_at='2026-10-24T12:00:00+00:00'))
    return raw


def model_fixture():
    def spec(v):
        return dict(features=['p20'],impute=[13.],mean=[0.],scale=[1.],coef=[0.],intercept=v)
    variant = {k:spec(v) for k,v in [('minutes',36),('fga_per_min',.5),('fg3_fraction',.4),('fta_per_min',.15)]}
    calibration = []
    for event,shift in [('low',-5),('high',5)]:
        for home in [True,False]:
            for n in range(5):
                calibration.append({'date':'2026-01-05','event':event,'home':home,'player_id':str(n),
                    'forecast':{'points':25,'minutes':36,'fga':18,'fg3a':7,'fta':5},
                    'residuals':{'points':shift,'minutes':0,'fga':0,'fg3a':0,'fta':0}})
    model = dict(version=MODEL_VERSION,created_at='2026-10-28T10:00:00+00:00',evaluation_end='2026-06-30',
        variants={'context':variant,'history_only':variant},league_rates={'two':.5,'three':.35,'free':.75},
        calibration_rows=calibration,limitations=[])
    model['sha256'] = digest(model)
    return model


def snapshot_fixture():
    raw = raw_fixture(); players,_,names,_ = prepare(raw)
    candidates = []
    for ident in [10,11,20,21]:
        h = [{**r,'team':names[r['team_id']],'source':'nba_log','source_game_id':r['game_id'],
              'provenance':[{'available_at':r['fetched_at']}]} for r in players if r['player_id']==ident]
        from model.nba_points_ladder import player_key
        candidates.append(dict(event_id='today',player=h[-1]['name'],player_key=player_key(h[-1]['name']),
            home_team='Boston Celtics',away_team='Miami Heat',history=h,availability={'status':'unverified'},
            predictions={'market_line':20.5,'blend90_10':20.,'opportunity_points':21.}))
    snapshot = dict(version='nba-points-ghost-v2',mode='prospective',created_at='2026-10-28T11:00:00+00:00',
        as_of='2026-10-28T11:00:00+00:00',lock_at='2026-10-28T23:00:00+00:00',game_date='2026-10-28',
        slate_schedule=[{'start_time':'2026-10-28T23:00:00+00:00'}],candidates=candidates)
    snapshot['sha256'] = digest(snapshot)
    return snapshot,raw


def test_audit_reconciles_and_rejects_duplicate_and_ambiguous_identity():
    raw = raw_fixture(); p,ctx,names,audit = prepare(raw)
    assert audit['reconciled_games']==20 and len(p)==200
    assert audit['ingestion_after_game_rows']==200
    assert examples(p,ctx,names)
    broken = deepcopy(raw);broken['players'].append(broken['players'][0])
    with pytest.raises(ValueError,match='duplicate player'):prepare(broken)
    broken = deepcopy(raw);broken['players'][0]['player_id']=999
    with pytest.raises(ValueError,match='ambiguous'):prepare(broken)
    broken = deepcopy(raw);broken['teams'][0]['pts']=66
    assert prepare(broken)[3]['exclusions']['unreconciled_games']==1


def test_feature_boundaries_and_stale_context():
    p,ctx,names,_ = prepare(raw_fixture());h=[r for r in p if r['player_id']==10]
    f,_=features(h[:10],'2026-03-11',True,'Boston Celtics','Miami Heat',ctx)
    poisoned = deepcopy(ctx)
    for rs in poisoned.values():
        for r in rs:
            if r['game_date']>='2026-03-11':
                r.update(team_possessions48=1000000,opponent_allowed_fga48=1000000)
    assert f==features(h[:10],'2026-03-11',True,'Boston Celtics','Miami Heat',poisoned)[0]
    with pytest.raises(ValueError,match='future or same-day'):
        features(h[:11],'2026-03-11',True,'Boston Celtics','Miami Heat',ctx)
    missing,ages=context_features(ctx,'Boston Celtics','Miami Heat','2026-10-28')
    assert all(v is None for v in missing.values())
    assert all(v is None for v in ages.values())
    # Later-ingested source cannot appear in a supposed earlier decision.
    missing,_=context_features(ctx,'Boston Celtics','Miami Heat','2026-03-11','2026-03-10T12:00:00+00:00')
    assert all(v is None for v in missing.values())


def test_opportunity_physical_bounds_and_scoring_identity():
    snapshot,_=snapshot_fixture();m=model_fixture();h=snapshot['candidates'][0]['history']
    p=predict(m,{'p20':13},h)
    assert 0<=p['fg3a']<=p['fga'] and 0<=p['minutes']<=48
    assert p['points']==pytest.approx(sum(p['scoring_components'].values()))
    m['variants']['context']['fg3_fraction']['intercept']=5
    assert predict(m,{'p20':13},h)['fg3a']==predict(m,{'p20':13},h)['fga']


def test_recent_box_context_requires_classification_reconciliation_and_observation_time():
    raw=raw_fixture();g={'espn_event_id':'espn1','game_date':'2026-10-26','completed':True,
        'home_team':'Boston Celtics','away_team':'Miami Heat','home_score':65,'away_score':65,
        'fetched_at':'2026-10-27T09:00:00+00:00'}
    raw['recent_games']=[g];raw['recent_players']=[]
    for r in raw['players'][:10]:
        raw['recent_players'].append({**r,'espn_event_id':'espn1','espn_athlete_id':str(r['player_id']),
            'game_date':'2026-10-26',
            'team': 'Boston Celtics' if r['is_home'] else 'Miami Heat','played':True,
            'fetched_at':'2026-10-27T09:00:00+00:00'})
    raw['classifications']=[{'espn_event_id':'espn1','season_type':2,'game_date':'2026-10-26',
        'home_team':'Boston Celtics','away_team':'Miami Heat','captured_at':'2026-10-27T10:00:00+00:00'}]
    _,ctx,_,audit=prepare(raw)
    assert audit['recent_box_context_team_games']==2
    f,_=context_features(ctx,'Boston Celtics','Miami Heat','2026-10-28','2026-10-27T11:00:00+00:00')
    assert f['opponent_allowed_fga48']==60 and f['team_possessions48'] is None
    early,_=context_features(ctx,'Boston Celtics','Miami Heat','2026-10-28','2026-10-27T09:30:00+00:00')
    assert early['opponent_allowed_fga48'] is None
    raw['classifications'][0]['season_type']=1
    assert prepare(raw)[3]['recent_box_context_team_games']==0


def test_integer_thresholds_fair_odds_and_proper_score():
    a=[17,18,18,19]
    assert probability(a,18)==.75 and probability(a,18.1)==.25
    assert probability(a,18,'over')==.25 and probability(a,18,'push')==.5
    assert fair_odds(.4)==150 and fair_odds(.6)==-150 and fair_odds(.5)==100
    assert fair_odds(0) is None and fair_odds(1) is None
    assert crps([20,20],20)==0 and crps([20,20],25)==5
    assert all(probability(a,t)>=probability(a,t+1) for t in range(15,21))


def test_joint_blocks_preserve_shared_errors_and_reproducibility():
    m=model_fixture();cs=[{'event_id':'x','home':s,'prediction':{'points':25}} for s in [True,True,False]]
    a,audit=joint_draws(m,cs,1000,42);b,_=joint_draws(m,cs,1000,42)
    assert np.array_equal(a,b) and np.array_equal(a[:,0],a[:,2])
    assert audit['independent_fallback_cells']==0
    assert set(a[:,0])=={20,30}
    assert np.allclose(fractional_top_slots(a,1).sum(axis=1),1)
    assert np.allclose(fractional_top_slots(a,3).sum(axis=1),3)
    shares=fractional_top_slots([[40,30,30,30]],3)[0]
    assert shares.tolist()==pytest.approx([1,2/3,2/3,2/3])


def test_forecast_preserves_source_and_rejects_backdating_and_future_inputs():
    s,raw=snapshot_fixture();original=deepcopy(s);m=model_fixture()
    f=forecast(s,m,raw,'2026-10-28T12:00:00+00:00',draws=100)
    assert s==original and f['mode']=='prospective' and len(f['rows'])==4
    assert all(r['availability']['status']=='unverified' for r in f['rows'])
    assert sum(r['distribution']['leader_share'] for r in f['rows'])==pytest.approx(1)
    assert forecast(s,m,raw,'2026-10-29T12:00:00+00:00',draws=100)['mode']=='retrospective_reconstruction'
    with pytest.raises(ValueError,match='precedes'):forecast(s,m,raw,'2026-10-28T09:00:00+00:00',draws=100)
    s['candidates'][0]['history'][0]['provenance'][0]['available_at']=s['as_of'];s['sha256']=digest({k:v for k,v in s.items() if k!='sha256'})
    with pytest.raises(ValueError,match='observed too late'):forecast(s,m,raw,'2026-10-28T12:00:00+00:00',draws=100)


def test_reported_out_excluded_unknown_not_active_and_grade_missing_not_zero():
    s,raw=snapshot_fixture();s['candidates'][0]['availability']={'status':'Out'}
    s['sha256']=digest({k:v for k,v in s.items() if k!='sha256'})
    f=forecast(s,model_fixture(),raw,'2026-10-28T12:00:00+00:00',draws=100)
    assert f['modeled_pool_n']==3 and f['exclusions'][0]['reason']=='reported_out'
    d={'snapshot_sha256':s['sha256'],'game_date':s['game_date'],'rows':[]}
    for i,r in enumerate(f['rows']):
        d['rows'].append({'event_id':r['event_id'],'player_key':r['player_key'],
                         'status':'verified' if i else 'dnp','actual':{'points':25}})
    g=grade(f,d)
    assert g['resolved_n']==2 and g['paired_n']==2 and len(g['unresolved'])==1
    d['snapshot_sha256']='wrong'
    with pytest.raises(ValueError,match='do not match'):grade(f,d)


def test_postgame_minutes_volume_conversion_reconcile():
    s,_=snapshot_fixture();c=s['candidates'][0]
    c.update(opportunity_forecasts={'minutes':30},opportunity_conversion_rates={'two':.5,'three':.3,'free':.8})
    d={'snapshot_sha256':s['sha256'],'rows':[{'event_id':c['event_id'],'player_key':c['player_key'],
        'status':'verified','actual':{'minutes':35,'fga':20,'fg3a':6,'fta':5,'points':30}}]}
    result=diagnose(s,d);r=result['rows'][0]
    assert sum(r['components'].values())==pytest.approx(30-c['predictions']['opportunity_points'])


def test_recorder_preserves_first_prospective_per_model_and_skips_after_tip(tmp_path,monkeypatch):
    import json
    from types import SimpleNamespace
    m=model_fixture();model_path=tmp_path/'model.json';model_path.write_text(json.dumps(m))
    out=tmp_path/'attempt';out.mkdir();index=tmp_path/'prospective-index';index.mkdir()
    marker=index/('2026-10-28-'+m['sha256']+'.json');marker.write_text(json.dumps({'forecast_sha256':'original'}))
    args=SimpleNamespace(model=model_path,date=datetime(2026,10,28).date(),output_dir=out)
    monkeypatch.setattr('research.nba_ghost_runner.fetch_scoreboard',lambda d:(_ for _ in []).throw(AssertionError('must not fetch')))
    r=record_day(args,{},'sha')
    assert r['reason']=='original_v3_snapshot_preserved'
    marker.unlink()
    monkeypatch.setattr('research.nba_ghost_runner.fetch_scoreboard',lambda d:({'events':[{'date':'2000-01-01T00:00:00+00:00'}]},datetime.now(timezone.utc)))
    r=record_day(args,{},'sha')
    assert r['reason']=='no_slate_or_first_tip_passed' and not (out/'forecast.json').exists()


def test_absence_analogues_are_descriptive_provider_scoped_and_source_timed():
    s,raw=snapshot_fixture();players,_,names,_=prepare(raw);c=s['candidates'][0]
    mate={'player':'Player 1 1','player_key':s['candidates'][1]['player_key'],'team':'Boston Celtics',
          'source_url':'https://official.nba.com/report','observed_at':'2026-10-28T10:00:00+00:00'}
    altered=[r for r in players if not (r['player_id']==11 and int(r['game_id'])<4)]
    a=absence_scenarios(c,altered,names,[mate],'2026-10-28T11:00:00+00:00')
    assert a[0]['absent_n']==4 and a[0]['present_n']==16 and 'scenario weight' in a[0]['interpretation']
    assert not absence_scenarios(c,altered,names,[mate],'2026-03-01T11:00:00+00:00')


def test_joint_evaluation_uses_the_real_sampler_and_explicit_conditional_pool():
    es=[]
    for n in range(10):
        es.append({'date':'2026-02-02','event':'game','home':n<5,'player':str(n),
            'points':20+n,'features':{'p20':20,'p10':20,'reference_q90':30},'points_history':[20]*20,
            'rates_history':[{'fgm':8,'fga':18,'fg3m':2,'fg3a':6,'ftm':3,'fta':4}]*10})
    m=model_fixture();m['calibration_end']='2026-01-31'
    r=evaluate_joint(m,es,100,1)
    assert r['all']['n']==10 and r['date_n']==1
    assert r['joint_sampling_total_cells']==1000 and len(r['daily_selection_audit'])==1
    assert 'appearance-conditioned' in r['evaluation_kind'] and r['all']['history20_mae']==4.5


def test_evaluation_poison_cannot_change_fitted_models_or_calibration():
    es=[]
    for month in [12,1,2]:
        year=2025 if month==12 else 2026
        for i in range(100):
            es.append({'date':f'{year}-{month:02d}-15','event':f'{month}-{i//10}', 'home':i%10<5,
                'player_id':str(i),'player_key':str(i),'points':20+i%8,'minutes':30,'fga':18,'fg3a':6,'fta':4,
                'features':{**{f:float(i%3+1) for f in FEATURES},'reference_q90':30},
                'points_history':[20]*20,
                'rates_history':[{'fgm':8,'fga':18,'fg3m':2,'fg3a':6,'ftm':3,'fta':4}]*10,
                'targets':{'minutes':30,'fga_per_min':.6,'fg3_fraction':1/3,'fta_per_min':4/30}})
    ps=[{'game_date':'2025-12-01','fgm':8,'fga':18,'fg3m':2,'fg3a':6,'ftm':3,'fta':4}]
    m=fit(es,ps,'sha','2026-10-28T10:00:00+00:00',{})
    bad=deepcopy(es)
    for e in bad:
        if e['date']>='2026-02-01':
            e['points']=1000;e['targets']['minutes']=1000
    poisoned=fit(bad,ps,'sha','2026-10-28T10:00:00+00:00',{})
    assert m['variants']==poisoned['variants'] and m['calibration_rows']==poisoned['calibration_rows']
    assert m['evaluation']!=poisoned['evaluation']
