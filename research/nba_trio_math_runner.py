"""Versioned, prospective-only research attachment, fitting, grading and publication."""
from __future__ import annotations
from collections import defaultdict,Counter
from datetime import datetime,timezone
from pathlib import Path
import json
import numpy as np
from model.nba_points_ladder import player_key,team_key
from research.nba_points_forward import aware,digest,write_new
from research.nba_trio_math import VERSION,SETTINGS,compound,compound_slate,fit_dependence,grade_portfolio,quantile_adjustment,select_portfolios

MODEL_PATH=Path(__file__).parent/'artifacts/nba_trio_math_model_v1.json'
PROTOCOL_PATH=Path(__file__).parent/'artifacts/nba_trio_math_protocol_v2.json'
TABLES=["""CREATE TABLE IF NOT EXISTS nba_trio_math_snapshots (
 record_sha256 TEXT PRIMARY KEY,game_date DATE NOT NULL,protocol_sha256 TEXT NOT NULL,
 source_snapshot_sha256 TEXT NOT NULL,forecast_sha256 TEXT NOT NULL,lock_at TIMESTAMPTZ NOT NULL,
 record JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),UNIQUE(game_date,protocol_sha256))""",
 """CREATE TABLE IF NOT EXISTS nba_trio_math_grades (
 id BIGSERIAL PRIMARY KEY,record_sha256 TEXT NOT NULL REFERENCES nba_trio_math_snapshots(record_sha256),
 grade JSONB NOT NULL,graded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"""]

def signed(v):return {**v,'sha256':digest(v)}
def checked(v):
 if v.get('sha256')!=digest({k:x for k,x in v.items() if k!='sha256'}):raise ValueError('invalid digest')
 return v
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def scoring_band(x):return 'high' if x>=20 else 'mid' if x>=10 else 'low'

def fit(raw,base_model):
 from research.nba_points_v3_data import prepare
 players,_,_,audit=prepare(raw)
 dev=[r for r in players if r['game_date']<=SETTINGS['development_end']]
 if len(dev)<1000:raise ValueError('insufficient development data')
 priors={}
 for key,values in [('minutes',[r['minutes'] for r in dev]),('fga_rate',[r['fga']/r['minutes'] for r in dev]),('fta_rate',[r['fta']/r['minutes'] for r in dev])]:
  x=np.array(values);priors[key]=[float(x.mean()),SETTINGS['prior_strength'],3.,float(max(x.var(),1e-6)*2)]
 history=defaultdict(list);shapes={k:[] for k in ['fga','fta']}
 for r in dev:
  h=history[str(r['player_id'])][-10:]
  if len(h)>=3:
   for k in shapes:
    mu=np.mean([x[k] for x in h])
    if mu>0:shapes[k].append(((r[k]-mu)**2-mu,mu**2))
  history[str(r['player_id'])].append(r)
 count_shapes={k:float(np.clip(sum(v for _,v in rs)/max(sum(e for e,_ in rs),1e-6),1,1000)) for k,rs in shapes.items()}
 artifact={'version':VERSION+'-model','created_at':datetime.now(timezone.utc).isoformat(),'settings':SETTINGS,
           'source_sha256':digest(raw),'base_model_sha256':base_model['sha256'],'league_rates':base_model['league_rates'],
           'priors':priors,'count_shapes':count_shapes,'dependence':fit_dependence(base_model['calibration_rows']),
           'calibration_scores':{'all':[],'low':[],'mid':[],'high':[]},'calibration_n':0,
           'training_n':len(dev),'source_audit':audit,
           'limitations':['Historical data downloaded after games; development/calibration replay is retrospective.',
             'No current-season calibration guarantee, injury effect or participation probability.',
             'Rank-based opportunity residual dependence is approximate; conversion draws remain independent.',
             'Compound box-score model, not a full rotation, shot-location or possession simulator.',
             'Residual interval calibration under serial dependence is empirical, not a distribution-free conditional guarantee.']}
 histories=defaultdict(list);rng=np.random.default_rng(SETTINGS['seed'])
 for r in players:
  if r['game_date']>SETTINGS['calibration_end']:break
  h=histories[str(r['player_id'])][-20:]
  if SETTINGS['calibration_start']<=r['game_date']<=SETTINGS['calibration_end'] and len(h)>=10:
   samples,_=compound(h,r['game_date'],artifact,rng,SETTINGS['calibration_draws'])
   score=float(r['points']-np.quantile(samples,.9,method='higher'));band=scoring_band(float(samples.mean()))
   artifact['calibration_scores']['all'].append(score);artifact['calibration_scores'][band].append(score);artifact['calibration_n']+=1
  histories[str(r['player_id'])].append(r)
 if artifact['calibration_n']<SETTINGS['minimum_calibration']:raise ValueError('insufficient held-out calibration')
 return signed(artifact)

def validate_protocol(protocol,artifact):
 checked(protocol);checked(artifact)
 if protocol.get('version')!=VERSION+'-protocol' or artifact.get('version')!=VERSION+'-model' or protocol['model_sha256']!=artifact['sha256'] or protocol['settings']!=SETTINGS or artifact['settings']!=SETTINGS:raise ValueError('model/protocol mismatch')
 if set(protocol['code_sha256'])!={'nba_trio_math.py','nba_trio_math_runner.py'}:raise ValueError('incomplete code identity')
 import hashlib
 for name,sha in protocol['code_sha256'].items():
  data=(Path(__file__).parent/name).read_text(encoding='utf-8').replace('\r\n','\n').encode()
  if hashlib.sha256(data).hexdigest()!=sha:raise ValueError('registered code changed')

def calibration(artifact,root,protocol,cutoff,target):
 # Never use a grade observed after the source cutoff, even if its game was earlier.
 latest={}
 for p in (root/'trio-math-grades').glob('*.json'):
  g=checked(read(p))
  if g['protocol_sha256']!=protocol['sha256'] or g['game_date']>=target or aware(g['graded_at'])>=aware(cutoff):continue
  key=g['record_sha256'];marker=root/'trio-math-index'/(g['game_date']+'-'+protocol['sha256']+'.json')
  if not marker.exists() or read(marker)['record_sha256']!=key:continue
  original=checked(read(read(marker)['record_path']))
  if any(g[k]!=original[k] for k in ['game_date','model_sha256','protocol_sha256','source_snapshot_sha256','mode']) or original['mode']!='prospective':raise ValueError('calibration grade identity mismatch')
  if key not in latest or aware(g['graded_at'])>aware(latest[key]['graded_at']):latest[key]=g
 grades=sorted(latest.values(),key=lambda g:g['game_date'])[-30:]
 scores={k:[] for k in ['all','low','mid','high']}
 for g in grades:
  for r in g['players']:
   if r['status']!='verified':continue
   s=r['actual']-r['raw_p90'];scores['all'].append(s);scores[r['band']].append(s)
 use_current=len(scores['all'])>=SETTINGS['minimum_calibration']
 return (scores if use_current else artifact['calibration_scores']),{'source':'prior_completed_trial_grades' if use_current else 'fixed_January_holdout','grade_sha256':[g['sha256'] for g in grades] if use_current else [],'rolling_dates':len(grades) if use_current else 0}

def build(forecast,source,artifact,protocol,root,created,retrospective=False):
 checked(forecast);checked(source);validate_protocol(protocol,artifact)
 if forecast['source_snapshot_sha256']!=source['sha256'] or forecast['game_date']!=source['game_date']:raise ValueError('source/forecast mismatch')
 if forecast['model_sha256']!=protocol['base_model_sha256']:raise ValueError('base model changed')
 if not aware(source['as_of'])<=aware(source['created_at'])<=aware(forecast['created_at'])<=aware(created):raise ValueError('invalid source timeline')
 starts=[aware(g['start_time']) for g in source.get('slate_schedule',[])]
 if not starts or aware(source['lock_at'])>min(starts):raise ValueError('unverified full-slate cutoff')
 prospective=(forecast['mode']=='prospective' and source['mode']=='prospective' and
              forecast['game_date']>=protocol['enrollment_start'] and
              aware(artifact['created_at'])<=aware(protocol['registered_at'])<aware(forecast['created_at'])<=aware(created)<min(starts))
 if not prospective and not retrospective:raise ValueError('not eligible for new prospective protocol')
 byid={(str(c['event_id']),c['player_key']):c for c in source['candidates']}
 selected=sorted(forecast['rows'],key=lambda r:(-r['market_line'],r['player_key'],r['event_id']))[:SETTINGS['pool']]
 if len(selected)<5:raise ValueError('insufficient modeled pool for five unique trios')
 prepared=[]
 for r in selected:
  c=byid[(r['event_id'],r['player_key'])]
  if str(r['availability'].get('status','')).lower()=='out':raise ValueError('Out player in eligible pool')
  for h in c['history']:
   if not h.get('provenance') or any(aware(p['available_at'])>=aware(source['as_of']) for p in h['provenance']):raise ValueError('late/unverified history')
  pair=[g for g in source['slate_schedule'] if team_key(g['home_team'])==team_key(c['home_team']) and team_key(g['away_team'])==team_key(c['away_team'])]
  if len(pair)!=1:raise ValueError('ambiguous game join')
  prepared.append({**r,'history':c['history'],'target':forecast['game_date'],'espn_event_id':str(pair[0]['event_id']),
                   'home_team':c['home_team'],'away_team':c['away_team']})
 n=SETTINGS['draws'];seed=SETTINGS['seed']
 correlated,details=compound_slate(prepared,artifact,n,seed,True)
 independent,_=compound_slate(prepared,artifact,n,seed,False)
 # Preserve aligned original joint rows. Never independently shuffle v3 samples.
 original=np.array([r['samples'][:n] for r in prepared],dtype=float).T
 if original.shape!=(n,len(prepared)):raise ValueError('insufficient aligned original draws')
 ids=[r['event_id']+':'+r['player_key'] for r in prepared]
 portfolios=select_portfolios({'v3':original,'compound_joint':correlated,'compound_independent':independent},[r['market_line'] for r in prepared],ids)
 scores,cal_audit=calibration(artifact,root,protocol,source['as_of'],forecast['game_date'])
 rows=[]
 for j,r in enumerate(prepared):
  values=correlated[:,j];avg=float(values.mean());band=scoring_band(avg)
  pool=scores[band] if len(scores[band])>=SETTINGS['minimum_calibration'] else scores['all']
  adjustment=quantile_adjustment(pool);raw=float(np.quantile(values,.9,method='higher'))
  rows.append({'id':ids[j],'player':r['player'],'player_key':r['player_key'],'event_id':r['event_id'],
               'espn_event_id':r['espn_event_id'],'home_team':r['home_team'],'away_team':r['away_team'],'market_line':r['market_line'],
               'availability':r['availability'],'v3_mean':r['distribution']['mean'],'compound_mean':avg,'band':band,
               'raw_p90':raw,'adjusted_p90':max(0.,raw+adjustment) if adjustment is not None else None,
               'calibration_n':len(pool),'opportunities':details[j]})
 return signed({'version':VERSION,'game_date':forecast['game_date'],'created_at':aware(created).isoformat(),
  'lock_at':source['lock_at'],'mode':'prospective' if prospective else 'retrospective_reconstruction',
  'model_sha256':artifact['sha256'],'protocol_sha256':protocol['sha256'],'forecast_sha256':forecast['sha256'],
  'source_snapshot_sha256':source['sha256'],'base_model_sha256':forecast['model_sha256'],
  'source_frozen_at':forecast['created_at'],'source_cutoff':source['as_of'],'quoted_pool_n':forecast['quoted_pool_n'],
  'slate_schedule':source['slate_schedule'],
  'modeled_pool_n':forecast['modeled_pool_n'],'pool_n':len(rows),'players':rows,'portfolios':portfolios,
  'calibration':cal_audit,'settings':SETTINGS,'limitations':artifact['limitations']+
  ['Selection uses only the top 12 frozen market-line players in the original modeled eligible pool.',
   'Mean and tail estimates remain conditional on appearing. No arbitrary Questionable discounts.',
   'Scenario scores are simulated model outputs, not demonstrated gains or betting probabilities.']})

def publish(record):
 checked(record)
 if record['mode']!='prospective':raise ValueError('cannot publish reconstruction as prospective')
 import psycopg2
 from psycopg2.extras import Json
 from config import load_config
 with psycopg2.connect(load_config().database_url) as conn:
  with conn.cursor() as c:
   for ddl in TABLES:c.execute(ddl)
   c.execute('''INSERT INTO nba_trio_math_snapshots(record_sha256,game_date,protocol_sha256,source_snapshot_sha256,forecast_sha256,lock_at,record)
      SELECT %s,%s,%s,%s,%s,%s,%s WHERE clock_timestamp()<%s::timestamptz AND %s::timestamptz<=clock_timestamp() ON CONFLICT DO NOTHING RETURNING record_sha256''',
      (record['sha256'],record['game_date'],record['protocol_sha256'],record['source_snapshot_sha256'],record['forecast_sha256'],record['lock_at'],Json(record),record['lock_at'],record['created_at']))
   if not c.fetchone():raise ValueError('cutoff passed or original already published; no replacement')

def attach(forecast,source,directory,root,created):
 protocol=checked(read(PROTOCOL_PATH));artifact=checked(read(MODEL_PATH))
 marker=root/'trio-math-index'/(forecast['game_date']+'-'+protocol['sha256']+'.json')
 if marker.exists():return {'status':'skipped_existing_original','index':str(marker)}
 result=build(forecast,source,artifact,protocol,root,created)
 completed=datetime.now(timezone.utc)
 if completed>=aware(result['lock_at']):return {'status':'skipped_cutoff_passed_during_math','completed_at':completed.isoformat()}
 result=signed({**{k:v for k,v in result.items() if k!='sha256'},'completed_at':completed.isoformat()})
 path=directory/'trio-math.json';write_new(path,result)
 try:publish(result)
 except Exception as e:return {'status':'unindexed_publication_failed','record_path':str(path.resolve()),'record_sha256':result['sha256'],'reason':str(e)}
 marker.parent.mkdir(exist_ok=True)
 write_new(marker,{'record_path':str(path.resolve()),'record_sha256':result['sha256'],'protocol_sha256':protocol['sha256'],'model_sha256':artifact['sha256'],'game_date':result['game_date']})
 return {'status':'recorded_and_published','record_sha256':result['sha256'],'pool_n':result['pool_n'],'protocol_sha256':protocol['sha256']}

def grade(record,diagnostics,scoreboard=None):
 checked(record)
 if record['source_snapshot_sha256']!=diagnostics['snapshot_sha256'] or record['game_date']!=diagnostics['game_date']:raise ValueError('diagnostics mismatch')
 mapping={str(r['event_id'])+':'+r['player_key']:r for r in diagnostics['rows']}
 if len(mapping)!=len(diagnostics['rows']):raise ValueError('ambiguous diagnostics')
 outcomes={};players=[]
 for r in record['players']:
  d=mapping.get(r['id']);status=d['status'] if d else 'missing';points=d['actual']['points'] if d and status=='verified' else None
  if d and str(d.get('espn_event_id'))!=str(r['espn_event_id']):raise ValueError('diagnostic game mismatch')
  if status=='verified' and (not isinstance(points,(int,float)) or not np.isfinite(points) or points<0):raise ValueError('invalid verified points')
  outcomes[r['id']]={'status':status,'points':points}
  players.append({'id':r['id'],'player':r['player'],'status':status,'actual':points,'band':r['band'],'raw_p90':r['raw_p90'],
                  'adjusted_p90':r['adjusted_p90'],'compound_mean':r['compound_mean'],'v3_mean':r['v3_mean'],'market_line':r['market_line'],
                  'expected_opportunities':r.get('opportunities'),
                  'actual_opportunities':{k:d['actual'].get(k) for k in ['minutes','fga','fg3a','fta']} if d and status=='verified' else None})
 portfolios={k:grade_portfolio(v['trios'],outcomes) for k,v in record['portfolios'].items() if isinstance(v,dict) and 'trios' in v}
 resolved=[p for p in players if p['status']=='verified'];best=sum(sorted([r['actual'] for r in resolved],reverse=True)[:3]) if len(resolved)>=3 else None
 boundary=sorted([r['actual'] for r in resolved],reverse=True)[2] if len(resolved)>=3 else None
 for name,v in portfolios.items():
  selected=record['portfolios'][name]['trios'][int(np.argmax(v['trio_totals']))] if v['status']=='resolved' else []
  points={r['id']:r['actual'] for r in resolved};above=sum(x>boundary for x in points.values()) if boundary is not None else 0;ties=sum(x==boundary for x in points.values()) if boundary is not None else 0
  v['captured_top3_slots']=sum(1 if points[i]>boundary else (3-above)/ties if points[i]==boundary else 0 for i in selected) if selected and boundary is not None else None
  v['gap_to_best_verified_trio']=best-v['best_of_five'] if best is not None and v['best_of_five'] is not None else None
 expected={str(g['event_id']) for g in record.get('slate_schedule',[])}
 board={str(e['id']):e for e in (scoreboard or {}).get('events',[])}
 full_final=bool(expected) and expected==set(board) and all(e['status']['type']['completed'] for e in board.values())
 return signed({'version':VERSION+'-grade','record_sha256':record['sha256'],'protocol_sha256':record['protocol_sha256'],
  'model_sha256':record['model_sha256'],'source_snapshot_sha256':record['source_snapshot_sha256'],'game_date':record['game_date'],
  'mode':record['mode'],'graded_at':datetime.now(timezone.utc).isoformat(),'players':players,'portfolios':portfolios,
  'best_verified_trio':best,'benchmark_provisional':len(resolved)!=len(players),'full_slate_final':full_final})

def report(root):
 protocol=checked(read(PROTOCOL_PATH));records={};latest={}
 for p in (root/'trio-math-index').glob('*.json'):
  i=read(p);r=checked(read(i['record_path']))
  if r['protocol_sha256']!=protocol['sha256']:continue
  if r['sha256']!=i['record_sha256'] or r['mode']!='prospective' or r['model_sha256']!=protocol['model_sha256']:raise ValueError('invalid indexed original')
  if r['game_date'] in [x['game_date'] for x in records.values()]:raise ValueError('duplicate original date')
  records[r['sha256']]=r
 for p in (root/'trio-math-grades').glob('*.json'):
  g=checked(read(p));key=g['record_sha256']
  if key not in records:continue
  if any(g[k]!=records[key][k] for k in ['game_date','model_sha256','protocol_sha256','source_snapshot_sha256','mode']):raise ValueError('grade identity mismatch')
  if key not in latest or aware(g['graded_at'])>aware(latest[key]['graded_at']):latest[key]=g
 ready=[g for g in latest.values() if g.get('full_slate_final') and all(p['status']=='resolved' for p in g['portfolios'].values())]
 deltas={}
 for name in ['joint_best_of_five','robust_best_of_five']:
  ds=[g['portfolios'][name]['best_of_five']-g['portfolios']['market_baseline']['best_of_five'] for g in ready]
  sums=[g['portfolios'][name]['all_five_total']-g['portfolios']['market_baseline']['all_five_total'] for g in ready]
  interval=None
  if len(ds)>=5:
   rng=np.random.default_rng(SETTINGS['seed']);x=np.array(ds);means=x[rng.integers(len(x),size=(2000,len(x)))].mean(axis=1);interval=np.quantile(means,[.025,.975]).tolist()
  deltas[name]={'paired_dates':len(ds),'mean_best_difference':float(np.mean(ds)) if ds else None,'mean_all_five_difference':float(np.mean(sums)) if sums else None,'helped':sum(x>0 for x in ds),'hurt':sum(x<0 for x in ds),'tied':sum(x==0 for x in ds),'descriptive_date_bootstrap_interval95':interval}
 resolved=[p for g in latest.values() for p in g['players'] if p['status']=='verified'];ceilings={}
 for key in ['raw_p90','adjusted_p90']:
  rs=[r for r in resolved if r[key] is not None]
  ceilings[key]={'n':len(rs),'hits':sum(r['actual']>r[key] for r in rs),'loss':float(np.mean([max(.9*(r['actual']-r[key]),-.1*(r['actual']-r[key])) for r in rs])) if rs else None}
 errors={key:float(np.mean([abs(r['actual']-r[key]) for r in resolved])) if resolved else None for key in ['market_line','v3_mean','compound_mean']}
 opportunities={}
 for key in ['minutes','fga','fg3a','fta']:
  rs=[r for r in resolved if r.get('actual_opportunities') and r.get('expected_opportunities') and r['actual_opportunities'].get(key) is not None and r['expected_opportunities'].get(key) is not None]
  opportunities[key]={'n':len(rs),'mae':float(np.mean([abs(r['actual_opportunities'][key]-r['expected_opportunities'][key]) for r in rs])) if rs else None}
 return signed({'version':VERSION+'-report','created_at':datetime.now(timezone.utc).isoformat(),'protocol_sha256':protocol['sha256'],'model_sha256':protocol['model_sha256'],'recordings':len(records),'graded_dates':len(latest),'complete_paired_dates':len(ready),'comparisons':deltas,'point_errors':{'n':len(resolved),'mae':errors},'opportunity_errors':opportunities,'by_status':dict(Counter(r['status'] for g in latest.values() for r in g['players'])),'ceilings':ceilings,'status':'collecting' if records else 'pending_no_prospective_records','review_checkpoint_reached':len(ready)>=30,'confirmation':'separate_untouched_10_date_period_after_manual_review','decision':'Review only; no automatic promotion or parameter changes.'})

def main():
 import argparse
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True)
 q=sub.add_parser('fit');q.add_argument('--input',type=Path,required=True);q.add_argument('--base-model',type=Path,required=True);q.add_argument('--output',type=Path,required=True)
 q=sub.add_parser('evaluate');q.add_argument('--forecast',type=Path,required=True);q.add_argument('--source',type=Path,required=True);q.add_argument('--root',type=Path,required=True);q.add_argument('--output',type=Path,required=True);q.add_argument('--allow-retrospective',action='store_true')
 q=sub.add_parser('grade');q.add_argument('--record',type=Path,required=True);q.add_argument('--diagnostics',type=Path,required=True);q.add_argument('--output',type=Path,required=True);q.add_argument('--apply',action='store_true')
 q=sub.add_parser('report');q.add_argument('--root',type=Path,required=True);q.add_argument('--output',type=Path,required=True)
 a=p.parse_args()
 if a.action=='fit':out=fit(read(a.input),checked(read(a.base_model)))
 elif a.action=='evaluate':out=build(checked(read(a.forecast)),checked(read(a.source)),checked(read(MODEL_PATH)),checked(read(PROTOCOL_PATH)),a.root,datetime.now(timezone.utc).isoformat(),a.allow_retrospective)
 elif a.action=='report':out=report(a.root)
 else:
  record=checked(read(a.record));board=None
  if a.apply:
   from research.nba_ghost_runner import fetch_scoreboard
   from datetime import date
   board,captured=fetch_scoreboard(date.fromisoformat(record['game_date']))
   evidence=a.output.parent/(a.output.stem+'-evidence');evidence.mkdir(parents=True,exist_ok=False)
   write_new(evidence/'scoreboard.json',{'captured_at':captured,'payload':board})
  out=grade(record,read(a.diagnostics),board)
  if a.apply:
   if record['mode']!='prospective':raise ValueError('no retrospective trial writes')
   import psycopg2
   from psycopg2.extras import Json
   from config import load_config
   with psycopg2.connect(load_config().database_url) as conn:
    with conn.cursor() as c:c.execute('INSERT INTO nba_trio_math_grades(record_sha256,grade) VALUES(%s,%s)',(record['sha256'],Json(out)))
 write_new(a.output,out);print(json.dumps({'output':str(a.output),'version':out['version'],'mode':out.get('mode'),'recordings':out.get('recordings'),'calibration_n':out.get('calibration_n')}))
if __name__=='__main__':main()
