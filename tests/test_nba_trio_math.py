from copy import deepcopy
from datetime import date,timedelta
import numpy as np
import pytest
from research.nba_trio_math import regime,compound,checked_values,quantile_adjustment,select_portfolios,grade_portfolio,fit_dependence
from research.nba_trio_math_runner import signed,checked,grade

def history():
 return [dict(game_date=str(date(2026,1,1)+timedelta(days=i)),game_id=str(i),minutes=32.,fga=18.,fg3a=8.,fgm=8.,fg3m=3.,fta=6.,ftm=5.,points=24.) for i in range(20)]
def artifact():
 return {'priors':{'minutes':[25.,1.,3.,150.],'fga_rate':[.5,1.,3.,.1],'fta_rate':[.15,1.,3.,.05]},'count_shapes':{'fga':20.,'fta':10.},'league_rates':{'two':.55,'three':.36,'free':.8}}

def test_change_filter_adapts_and_keeps_parameter_uncertainty():
 stable=regime([10.]*20,[10.,1.,3.,2.]);shift=regime([10.]*15+[25.]*5,[10.,1.,3.,2.])
 assert shift['mean']>20 and stable['mean']==pytest.approx(10)
 assert shift['run_length_mean']<stable['run_length_mean']
 assert sum(shift['weights'])==pytest.approx(1)
 assert np.isfinite(shift['states']).all()

def test_compound_integer_support_reproducibility_and_played_zero():
 h=history();h[0].update(fgm=0,fg3m=0,ftm=0,points=0)
 a,info=compound(h,'2026-02-01',artifact(),np.random.default_rng(42),5000)
 b,_=compound(h,'2026-02-01',artifact(),np.random.default_rng(42),5000)
 np.testing.assert_equal(a,b);assert (a>=0).all() and np.issubdtype(a.dtype,np.integer)
 assert a.std()>0 and 10<a.mean()<45 and info['fga']>0

@pytest.mark.parametrize('mutation',['future','duplicate','bad_shots','bad_points','missing'])
def test_history_failures(mutation):
 h=history()
 if mutation=='future':h[-1]['game_date']='2026-02-01'
 if mutation=='duplicate':h.append(deepcopy(h[-1]))
 if mutation=='bad_shots':h[-1]['fg3m']=19
 if mutation=='bad_points':h[-1]['points']=100
 if mutation=='missing':h[-1]['fga']=float('nan')
 with pytest.raises(ValueError):checked_values(h,'2026-02-01')

def test_p90_finite_sample_rank_and_unavailable_small_sample():
 assert quantile_adjustment([]) is None and quantile_adjustment([1]) is None
 assert quantile_adjustment(list(range(100)))==90

def test_portfolio_optimizes_different_outcome_and_never_degrades_own_objective():
 rng=np.random.default_rng(1);n=500
 v=rng.poisson([30,29,28,27,26,20],(n,6)).astype(float)
 v[:100,5]+=35;ids=list('abcdef');lines=[30.,29.,28.,27.,26.,20.]
 r=select_portfolios({'v3':v,'compound_joint':v[:,::-1],'compound_independent':v},lines,ids)
 base=r['market_baseline'];joint=r['joint_best_of_five'];robust=r['robust_best_of_five']
 assert joint['scenario_expected_best']['v3']>=base['scenario_expected_best']['v3']-1e-8
 assert min(robust['scenario_expected_best'][k]-base['scenario_expected_best'][k] for k in ['v3','compound_joint','compound_independent'])>=-1e-8
 for key in ['market_baseline','joint_best_of_five','robust_best_of_five']:
  trios=r[key]['trios'];assert len(trios)==5 and len({tuple(t) for t in trios})==5
  assert all(len(set(t))==3 for t in trios) and trios[0]==['a','b','c']
 assert r==select_portfolios({'v3':v,'compound_joint':v[:,::-1],'compound_independent':v},lines,ids)
 tied=select_portfolios({'v3':np.ones((10,5))},[20.]*5,list('zyxwv'))
 assert tied['market_baseline']['trios'][0]==list('zyx')

def test_five_entry_metric_differs_from_sum_and_missing_dnp_not_zero():
 trios=[list(x) for x in ['abc','abd','abe','acd','ace']]
 out={i:{'status':'verified','points':n} for i,n in zip('abcde',[10,20,30,40,50])}
 r=grade_portfolio(trios,out);assert r['best_of_five']==90 and r['all_five_total']==380
 out['e']={'status':'dnp','points':None};r=grade_portfolio(trios,out)
 assert r['status']=='incomplete' and r['best_of_five'] is None and r['all_five_total'] is None
 assert r['unresolved_ids']==['e']
 with pytest.raises(ValueError,match='five unique'):grade_portfolio(trios[:4],out)
 with pytest.raises(ValueError,match='five unique'):grade_portfolio([trios[0],*trios[:4]],out)

def test_dependence_is_psd_shrunk_and_future_calibration_excluded():
 rng=np.random.default_rng(3);rows=[]
 for game in range(45):
  for side in [False,True]:
   for rank in range(5):
    rows.append({'event':str(game),'date':'2026-01-10','home':side,'forecast':{'points':30-rank},'residuals':{k:float(rng.normal()) for k in ['minutes','fga','fta']}})
 rows.append({'date':'2026-01-20','event':'future'})
 fitted=fit_dependence(rows);assert fitted['games']==45
 for c in fitted['channels'].values():
  cov=np.array(c['correlation']);assert np.linalg.eigvalsh(cov).min()>-1e-8
  np.testing.assert_allclose(np.diag(cov),1)
  assert 0<=c['shrinkage']<=1
 assert fit_dependence([])['channels']['fga']['status']=='insufficient_history_independent'
 for r in rows[:-1]:r['residuals']={k:0. for k in ['minutes','fga','fta']}
 assert fit_dependence(rows)['channels']['fga']['status']=='degenerate_history_independent'

def test_grade_digest_and_exact_source_game():
 players=[{'id':i,'player':i,'espn_event_id':'1','band':'high','raw_p90':35,'adjusted_p90':36,'compound_mean':25,'v3_mean':24,'market_line':23.5} for i in 'abcde']
 rec=signed({'game_date':'2026-11-04','mode':'prospective','source_snapshot_sha256':'source','protocol_sha256':'protocol','model_sha256':'model','players':players,'portfolios':{'market_baseline':{'trios':[list(x) for x in ['abc','abd','abe','acd','ace']]}}})
 diag={'snapshot_sha256':'source','game_date':'2026-11-04','rows':[{'event_id':i,'player_key':'','espn_event_id':'1','status':'verified','actual':{'points':20}} for i in 'abcde']}
 # Production IDs include both market event and canonical player key.
 for p in rec['players']:p['id']+=':'
 rec['portfolios']['market_baseline']['trios']=[[i+':' for i in t] for t in rec['portfolios']['market_baseline']['trios']]
 rec=signed({k:v for k,v in rec.items() if k!='sha256'})
 assert grade(rec,diag)['portfolios']['market_baseline']['best_of_five']==60
 diag['rows'][0]['espn_event_id']='other'
 with pytest.raises(ValueError,match='game mismatch'):grade(rec,diag)
 rec['players'][0]['raw_p90']=999
 with pytest.raises(ValueError,match='digest'):checked(rec)

def test_prospective_registration_source_timing_and_reconstruction_isolation(tmp_path,monkeypatch):
 import hashlib
 from pathlib import Path
 import research.nba_trio_math as math
 import research.nba_trio_math_runner as runner
 monkeypatch.setitem(math.SETTINGS,'draws',100)
 art=artifact();art.update(version=math.VERSION+'-model',created_at='2026-11-03T15:00:00+00:00',settings=deepcopy(math.SETTINGS),
   dependence={'channels':{k:{'correlation':np.eye(10).tolist()} for k in ['minutes','fga','fta']}},calibration_scores={k:[0.]*200 for k in ['all','high','mid','low']},limitations=[])
 art=signed(art)
 proto=signed({'version':math.VERSION+'-protocol','model_sha256':art['sha256'],'settings':deepcopy(math.SETTINGS),'base_model_sha256':'base','registered_at':'2026-11-03T16:00:00+00:00','enrollment_start':'2026-11-04','code_sha256':{name:hashlib.sha256((Path(runner.__file__).parent/name).read_text(encoding='utf-8').replace('\r\n','\n').encode()).hexdigest() for name in ['nba_trio_math.py','nba_trio_math_runner.py']}})
 cs=[];rs=[]
 for j in range(5):
  h=history()
  for x in h:x['provenance']=[{'available_at':'2026-10-28T12:00:00+00:00'}]
  cs.append({'event_id':'book','player_key':str(j),'player':str(j),'home_team':'Boston Celtics','away_team':'Miami Heat','history':h})
  rs.append({'event_id':'book','player_key':str(j),'player':str(j),'market_line':30.-j,'home':j<3,'prediction':{'points':30.-j},'distribution':{'mean':30.-j},'samples':[30.-j]*100,'availability':{'status':'unknown'}})
 source=signed({'as_of':'2026-11-04T16:00:00+00:00','created_at':'2026-11-04T16:00:01+00:00','lock_at':'2026-11-04T23:00:00+00:00','game_date':'2026-11-04','mode':'prospective','candidates':cs,'slate_schedule':[{'event_id':'espn','home_team':'Boston Celtics','away_team':'Miami Heat','start_time':'2026-11-04T23:00:00+00:00'}]})
 forecast=signed({'source_snapshot_sha256':source['sha256'],'game_date':'2026-11-04','created_at':'2026-11-04T16:01:00+00:00','model_sha256':'base','mode':'prospective','rows':rs,'quoted_pool_n':5,'modeled_pool_n':5})
 result=runner.build(forecast,source,art,proto,tmp_path,'2026-11-04T16:02:00+00:00')
 assert result['mode']=='prospective' and len(result['players'])==5 and not (tmp_path/'trio-math-index').exists()
 with pytest.raises(ValueError,match='prospective protocol'):runner.build(forecast,source,art,proto,tmp_path,'2026-11-04T23:01:00+00:00')
 retro=runner.build(forecast,source,art,proto,tmp_path,'2026-11-04T23:01:00+00:00',True)
 assert retro['mode']=='retrospective_reconstruction'
 with pytest.raises(ValueError,match='publish reconstruction'):runner.publish(retro)
 bad=deepcopy(source);bad['candidates'][0]['history'][-1]['provenance'][0]['available_at']='2026-11-04T17:00:00+00:00';bad=signed({k:v for k,v in bad.items() if k!='sha256'})
 f=deepcopy(forecast);f['source_snapshot_sha256']=bad['sha256'];f=signed({k:v for k,v in f.items() if k!='sha256'})
 with pytest.raises(ValueError,match='late/unverified history'):runner.build(f,bad,art,proto,tmp_path,'2026-11-04T16:02:00+00:00')
 late=deepcopy(proto);late['registered_at']='2026-11-04T16:01:01+00:00';late=signed({k:v for k,v in late.items() if k!='sha256'})
 with pytest.raises(ValueError,match='prospective protocol'):runner.build(forecast,source,art,late,tmp_path,'2026-11-04T16:02:00+00:00')
