"""Isolated point-only workload adaptation. No original forecast or probability changes."""
from __future__ import annotations
import argparse,json,math,hashlib
from datetime import datetime,timezone,timedelta
from pathlib import Path
from collections import Counter
from model.nba_points_ladder import team_key
from research.nba_points_forward import digest,aware,write_new
from research.nba_points_workload_conflicts import evaluate_candidate,season_of
from research.nba_points_role_formula import base_points,recent_window,challenge,challenge_snapshot,ACTIVATION_BLOCKERS,WEIGHT,ARITHMETIC_TOLERANCE,REGISTERED_BASE
VERSION='nba-role-opportunity-challenger-v1'
PROTOCOL=Path('research/artifacts/nba_role_opportunity_v2.json')
ROSTER_VERSION='nba-rosters-v2-completed-observation'
def finite(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
def checked(d):
 if d.get('sha256')!=digest({k:v for k,v in d.items() if k!='sha256'}):raise ValueError('digest mismatch')
 return d

def adapt(c,p,slate,cutoff,roster):
 base={'event_id':str(c['event_id']),'player_key':c['player_key'],'player':c['player'],'eligible':False}
 skip=lambda why:{**base,'reason':why}
 hist=sorted([h for h in c.get('history',[]) if h.get('played',True) and str(h['game_date'])<slate and season_of(h['game_date'])==season_of(slate)],key=lambda h:(str(h['game_date']),str(h['source_game_id'])))
 if len(hist)<3:return skip('insufficient_current_season_history')
 if len({(str(h['game_date']),str(h['source_game_id'])) for h in hist})!=len(hist):return skip('ambiguous_history')
 window=hist[-5:]
 for h in window:
  if not all(finite(h.get(k)) and h[k]>=0 for k in ['minutes','fga','fg3a','fta']) or h['fg3a']>h['fga'] or (h['minutes']==0 and h['fga']+h['fta']>0):return skip('invalid_recent_window')
  if not h.get('available_at') or aware(h['available_at'])>aware(cutoff):return skip('history_not_observed_before_forecast')
 if not roster or roster.get('provider')!='ESPN' or roster.get('version')!=ROSTER_VERSION:return skip('missing_timestamped_team_evidence')
 observed=aware(roster['observed_at']);cut=aware(cutoff)
 if not cut-timedelta(hours=24)<=observed<=cut:return skip('late_or_stale_team_evidence')
 if not all(str(h.get('espn_athlete_id',h.get('source_player_id')))==str(roster.get('espn_athlete_id')) for h in window):return skip('roster_player_id_mismatch')
 if {team_key(h.get('team')) for h in window}!={team_key(roster.get('team'))}:return skip('team_change_or_unknown_continuity')
 if team_key(roster['team']) not in {team_key(c['home_team']),team_key(c['away_team'])}:return skip('roster_team_outside_frozen_game')
 if (c.get('availability') or {}).get('status','').lower()=='out':return skip('reported_out')
 rates=p.get('conversion_rates',{});required=[p.get(k) for k in ['minutes','fga','fg3a','fta','points']]+[rates.get(k) for k in ['two','three','free']]
 if not all(finite(x) for x in required):return skip('invalid_original_forecast')
 m,a,t,l=p['minutes'],p['fga'],p['fg3a'],p['fta'];M=sum(h['minutes'] for h in window);A=sum(h['fga'] for h in window);T=sum(h['fg3a'] for h in window);L=sum(h['fta'] for h in window)
 if min(m,a,M,A)<=0 or not 0<=t<=a or l<0 or not 0<m<=48 or not all(0<=rates[k]<=1 for k in ['two','three','free']):return skip('invalid_original_or_aggregate_attempts')
 points=lambda a,t,l:2*(a-t)*rates['two']+3*t*rates['three']+l*rates['free']
 if abs(points(a,t,l)-p['points'])>0.01:return skip('base_arithmetic_mismatch')
 newA=.5*a+.5*m*A/M;newL=.5*l+.5*m*L/M;newT=newA*(.5*t/a+.5*T/A)
 recent={'minutes':m,'fga':m*A/M,'fg3a':m*T/M,'fta':m*L/M};recent['points']=points(recent['fga'],recent['fg3a'],recent['fta'])
 flag=evaluate_candidate(c,slate,'v3',p)
 return {**base,'eligible':True,'conflict':flag['status']=='review_required','window_games':[h['source_game_id'] for h in window],'roster':roster,
  'original':p,'candidate':{'minutes':m,'fga':newA,'fg3a':newT,'fta':newL,'points':points(newA,newT,newL)},'recent_rate_baseline':recent,
  'market':c['predictions'].get('market_line'),'source_v2_blend':c['predictions'].get('blend90_10')}

def load_roster(cutoff):
 import psycopg2
 from psycopg2.extras import RealDictCursor
 from config import load_config
 with psycopg2.connect(load_config().database_url,cursor_factory=RealDictCursor) as conn:
  conn.set_session(readonly=True)
  with conn.cursor() as c:
   c.execute("SELECT to_regclass('nba_roster_capture_runs') t")
   if c.fetchone()['t'] is None:return {}
   c.execute("SELECT id,run_at FROM nba_roster_capture_runs WHERE status='complete' AND version=%s AND run_at<=%s AND run_at>=%s ORDER BY run_at DESC LIMIT 1",(ROSTER_VERSION,aware(cutoff),aware(cutoff)-timedelta(hours=24)))
   run=c.fetchone()
   if not run:return {}
   c.execute('SELECT DISTINCT ON (espn_athlete_id) espn_athlete_id,player_key,team,status,espn_team_id FROM nba_roster_observations WHERE run_id<=%s ORDER BY espn_athlete_id,observed_at DESC,id DESC',(run['id'],));rows=c.fetchall()
 counts=Counter(r['player_key'] for r in rows)
 return {r['player_key']:{'team':r['team'],'observed_at':run['run_at'].isoformat(),'espn_athlete_id':r['espn_athlete_id'],'provider':'ESPN','version':ROSTER_VERSION,'source_url':f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{r['espn_team_id']}/roster",'run_id':run['id']} for r in rows if r['team'] and counts[r['player_key']]==1}

def record(forecast,snapshot,protocol,roster,now,schedule=None):
 checked(forecast);checked(snapshot);checked(protocol)
 if forecast.get('mode')!='prospective' or snapshot.get('mode')!='prospective':raise ValueError('source is not a genuine prospective original')
 if protocol.get('enrollment_start') and forecast['game_date']<protocol['enrollment_start']:raise ValueError('before registered enrollment date')
 if protocol.get('code_sha256')!=hashlib.sha256(Path(__file__).read_bytes().replace(b'\r\n',b'\n')).hexdigest():raise ValueError('challenger code differs from registration')
 if len({(str(c['event_id']),c['player_key']) for c in snapshot['candidates']})!=len(snapshot['candidates']) or len({(str(c['event_id']),c['player_key']) for c in forecast['rows']})!=len(forecast['rows']):raise ValueError('ambiguous identity')
 if forecast['source_snapshot_sha256']!=snapshot['sha256'] or forecast['model_sha256']!=protocol['model_sha256']:raise ValueError('source/model mismatch')
 if aware(now)>aware(protocol['registered_at'])+timedelta(days=90):raise ValueError('enrollment window closed; review insufficient evidence')
 if not aware(protocol['registered_at'])<aware(forecast['created_at'])<=aware(now)<aware(forecast['lock_at']):raise ValueError('registration or first-tip cutoff failed')
 sources={(str(c['event_id']),c['player_key']):c for c in snapshot['candidates']};rows=[]
 for f in forecast['rows']:
  key=(str(f['event_id']),f['player_key']);rows.append(adapt(sources[key],f['prediction'],forecast['game_date'],forecast['created_at'],roster.get(f['player_key'])))
 eligible=[r for r in rows if r['eligible']]
 def trio(which):return [r['event_id']+':'+r['player_key'] for r in sorted(eligible,key=lambda r:(-r[which]['points'],r['player_key'],r['event_id']))[:3]] if len(eligible)>=3 else None
 out={'version':VERSION,'mode':'prospective','game_date':forecast['game_date'],'created_at':now,'forecast_created_at':forecast['created_at'],'lock_at':forecast['lock_at'],'forecast_sha256':forecast['sha256'],'source_snapshot_sha256':snapshot['sha256'],'protocol_sha256':protocol['sha256'],'model_sha256':forecast['model_sha256'],'rows':rows,'scheduled_events':[{'id':str(e['id']),'start':e['date']} for e in (schedule or {}).get('events',[])],'trios':{'original':trio('original'),'candidate':trio('candidate')},'probabilities':None}
 out['sha256']=digest(out);return out

def attach(forecast,snapshot,outdir,root,now,schedule=None,roster_success=True):
 protocol=checked(json.loads(PROTOCOL.read_text(encoding='utf-8')))
 r=record(forecast,snapshot,protocol,load_roster(forecast['created_at']) if roster_success else {},now,schedule)
 registry=Path(root)/'role-index';registry.mkdir(exist_ok=True);marker=registry/(r['game_date']+'-'+protocol['sha256']+'.json')
 if marker.exists():return {'status':'skipped_original_preserved'}
 path=Path(outdir)/'role-challenger.json';write_new(path,r);write_new(marker,{'record_path':str(path.resolve()),'sha256':r['sha256'],'protocol_sha256':protocol['sha256'],'date':r['game_date']})
 return {'status':'recorded','eligible':sum(x['eligible'] for x in r['rows']),'conflicts':sum(x.get('conflict',False) for x in r['rows']),'sha256':r['sha256']}

def grade(record,diagnostics,scoreboard=None):
 checked(record)
 if diagnostics['snapshot_sha256']!=record['source_snapshot_sha256'] or diagnostics['game_date']!=record['game_date']:raise ValueError('wrong source diagnostics')
 byid={(str(r['event_id']),r['player_key']):r for r in diagnostics['rows']};rows=[]
 for r in record['rows']:
  if not r['eligible']:continue
  d=byid.get((r['event_id'],r['player_key']));a=d.get('actual') if d else None
  resolved=bool(d and d['status']=='verified' and a and a.get('played'))
  rows.append({'id':r['event_id']+':'+r['player_key'],'player':r['player'],'player_key':r['player_key'],'conflict':r['conflict'],'status':'resolved' if resolved else (d['status'] if d else 'missing'),'actual':a if resolved else None,'forecasts':{'original':r['original'],'candidate':r['candidate'],'recent':r['recent_rate_baseline']},'market':r['market'],'source_v2_blend':r['source_v2_blend']})
 def metrics(rs):
  valid=[r for r in rs if r['actual'] is not None];n=len(valid)
  return {'n':n,'unresolved':len(rs)-n,'by_status':dict(Counter(r['status'] for r in rs)),'paired_references':{key:{'n':len(z:=[r for r in valid if finite(r[key])]),'reference_mae':sum(abs(r[key]-r['actual']['points']) for r in z)/len(z) if z else None,'candidate_mae':sum(abs(r['forecasts']['candidate']['points']-r['actual']['points']) for r in z)/len(z) if z else None} for key in ['market','source_v2_blend']},'opportunity_mae':{field:{key:sum(abs(r['forecasts'][key][field]-r['actual'][field]) for r in valid)/n if n else None for key in ['original','candidate','recent']} for field in ['minutes','fga','fg3a','fta']},'fga_per_minute':{'n':len(q:=[r for r in valid if r['actual']['minutes']>0]),'mae':{key:sum(abs(r['forecasts'][key]['fga']/r['forecasts'][key]['minutes']-r['actual']['fga']/r['actual']['minutes']) for r in q)/len(q) if q else None for key in ['original','candidate','recent']}},'points_mae':{k:sum(abs(r['forecasts'][k]['points']-r['actual']['points']) for r in valid)/n if n else None for k in ['original','candidate','recent']},'fga_mae':{k:sum(abs(r['forecasts'][k]['fga']-r['actual']['fga']) for r in valid)/n if n else None for k in ['original','candidate','recent']}}
 def trio(ids):
  if ids is None:return {'status':'no_trio'}
  lookup={r['id']:r for r in rows};selected=[lookup[i] for i in ids];resolved=[r for r in rows if r['actual'] is not None];actuals=sorted([r['actual']['points'] for r in resolved],reverse=True)
  if any(r['actual'] is None for r in selected):return {'status':'incomplete','ids':ids}
  best=sum(actuals[:3]) if len(actuals)>=3 else None;boundary=actuals[2] if len(actuals)>=3 else None;above=sum(p>boundary for p in actuals) if boundary is not None else 0;ties=sum(p==boundary for p in actuals) if boundary is not None else 0
  capture=sum(1 if r['actual']['points']>boundary else (3-above)/ties if r['actual']['points']==boundary else 0 for r in selected) if boundary is not None else None
  total=sum(r['actual']['points'] for r in selected);return {'status':'graded','ids':ids,'actual':total,'best_verified':best,'gap':best-total if best is not None else None,'top3_slots':capture,'provisional':len(resolved)!=len(rows)}
 out={'version':VERSION+'-grade','record_sha256':record['sha256'],'protocol_sha256':record['protocol_sha256'],'model_sha256':record['model_sha256'],'game_date':record['game_date'],'graded_at':datetime.now(timezone.utc).isoformat(),'rows':rows,'primary':metrics([r for r in rows if r['conflict']]),'all_eligible':metrics(rows),'market_at_least20':metrics([r for r in rows if r['market'] is not None and r['market']>=20]),'trios':{k:trio(v) for k,v in record['trios'].items()},'interpretation':'Point-only challenger. Low-minute played outcomes count; missing/DNP remain unresolved. No probabilities or automatic promotion.'}
 out['all_games_final']=bool(record.get('scheduled_events')) and all(next((e.get('status',{}).get('type',{}).get('completed',False) for e in (scoreboard or {}).get('events',[]) if str(e['id'])==g['id']),False) for g in record.get('scheduled_events',[]))
 out['evaluation_ready']=out['all_games_final'] and bool(rows) and sum(r['status'] in ['resolved','dnp'] for r in rows)/len(rows)>=.95
 out['sha256']=digest(out);return out

def bootstrap(rows,field,cluster,seed,all_groups=None):
 import random
 groups={k:[] for k in (all_groups or [])}
 for row in rows:groups.setdefault(row[cluster],[]).append(row)
 if not rows or len({r[cluster] for r in rows})<5:return None
 keys=list(groups);rng=random.Random(seed);values=[];undefined=0
 while len(values)<10000:
  sample=[r for _ in keys for r in groups[rng.choice(keys)]]
  if not sample:undefined+=1;continue
  values.append(sum(r[field] for r in sample)/len(sample))
 values.sort();return {'interval95':[values[249],values[9749]],'undefined_resamples':undefined}

def report(root,protocol):
 checked(protocol);records={};excluded=[];seen_dates=set()
 for p in (root/'role-index').glob('*.json') if (root/'role-index').exists() else []:
  idx=json.loads(p.read_text());r=checked(json.loads(Path(idx['record_path']).read_text(encoding='utf-8')))
  if r['protocol_sha256']!=protocol['sha256'] or r['model_sha256']!=protocol['model_sha256']:continue
  f=checked(json.loads(Path(idx['record_path']).with_name('forecast.json').read_text(encoding='utf-8')))
  if r['sha256']!=idx['sha256'] or r['forecast_sha256']!=f['sha256'] or r['mode']!='prospective':raise ValueError('invalid indexed original')
  if p.name!=r['game_date']+'-'+protocol['sha256']+'.json' or r['game_date'] in seen_dates:raise ValueError('invalid or duplicate per-date index')
  seen_dates.add(r['game_date']);records[r['sha256']]=r
 latest={}
 for p in (root/'role-grades').glob('*.json') if (root/'role-grades').exists() else []:
  g=checked(json.loads(p.read_text(encoding='utf-8')));k=g['record_sha256']
  if k not in records:excluded.append(str(p));continue
  if g['protocol_sha256']!=protocol['sha256'] or g['model_sha256']!=protocol['model_sha256']:raise ValueError('mixed grade identity')
  if k not in latest or g['graded_at']>latest[k]['graded_at']:latest[k]=g
 rows=[{**r,'date':g['game_date']} for g in latest.values() for r in g['rows']];resolved=[r for r in rows if r['actual'] is not None];primary=[r for r in resolved if r['conflict']];ready=sum(g['evaluation_ready'] for g in latest.values());checkpoint=ready>=30 and len(primary)>=100
 def errors(rs):
  return {'n':len(rs),'point_mae':{k:sum(abs(r['forecasts'][k]['points']-r['actual']['points']) for r in rs)/len(rs) if rs else None for k in ['original','candidate','recent']},'fga_mae':{k:sum(abs(r['forecasts'][k]['fga']-r['actual']['fga']) for r in rs)/len(rs) if rs else None for k in ['original','candidate','recent']}}
 intervals={}
 if checkpoint:
  for label,rs,field in [('primary_points',primary,'points'),('primary_fga',primary,'fga'),('all_points',resolved,'points')]:
   deltas=[{'date':r['date'],'player_key':r['player_key'],'delta':abs(r['forecasts']['candidate'][field]-r['actual'][field])-abs(r['forecasts']['original'][field]-r['actual'][field])} for r in rs]
   intervals[label]={c:bootstrap(deltas,'delta',c,seed,{r[c] for r in resolved}) for c,seed in [('date',20261031),('player_key',20261032)]}
 out={'version':VERSION+'-report','created_at':datetime.now(timezone.utc).isoformat(),'protocol_sha256':protocol['sha256'],'model_sha256':protocol['model_sha256'],'recordings':len(records),'graded_dates':len(latest),'evaluation_ready_dates':ready,'primary':errors(primary),'all_eligible':errors(resolved),'by_status':dict(Counter(r['status'] for r in rows)),'checkpoint_reached':checkpoint,'formal_intervals':intervals,'excluded_unindexed_grades':len(excluded),'advancement_gates_passed':checkpoint and all(intervals.get(endpoint,{}).get(cluster) is not None and intervals[endpoint][cluster]['interval95'][1]<limit for endpoint,limit in [('primary_points',0),('primary_fga',0),('all_points',.25)] for cluster in ['date','player_key']),'decision':'Review only; no automatic tuning or promotion. Probabilities unavailable.'};out['sha256']=digest(out);return out

def main():
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True)
 q=sub.add_parser('grade');q.add_argument('--record',type=Path,required=True);q.add_argument('--diagnostics',type=Path,required=True);q.add_argument('--output',type=Path,required=True);q.add_argument('--no-fetch',action='store_true')
 q=sub.add_parser('report');q.add_argument('--research-root',type=Path,required=True);q.add_argument('--output',type=Path,required=True)
 a=p.parse_args()
 if a.action=='grade':
  r=checked(json.loads(a.record.read_text(encoding='utf-8')));board=None
  if not a.no_fetch:
   from research.nba_ghost_runner import fetch_scoreboard
   from datetime import date
   board,captured=fetch_scoreboard(date.fromisoformat(r['game_date']))
  out=grade(r,json.loads(a.diagnostics.read_text(encoding='utf-8')),board)
 else:out=report(a.research_root,checked(json.loads(PROTOCOL.read_text(encoding='utf-8'))))
 write_new(a.output,out);print(json.dumps({k:v for k,v in out.items() if k not in ['rows','trios']}))
if __name__=='__main__':main()
