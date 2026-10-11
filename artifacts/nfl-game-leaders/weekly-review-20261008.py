from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import io,json,hashlib
from collections import defaultdict
from datetime import datetime,timezone,timedelta
import numpy as np
import pandas as pd
import requests
from model.nfl_game_leaders import prepare,forecast,grade,Settings,METRICS,timestamp,canonical
from research.nfl_game_leaders import roster
from research.nfl_longest_touchdown import read,write
from ingest.ff_independent import NFLVERSE_WEEKLY_TEAM_STATS_URL
root=Path('artifacts/nfl-game-leaders')
snapshot=read(root/'full-capture-20261008.json.gz')
old=read(root/'reviewed-evaluation-2026.json')
url=NFLVERSE_WEEKLY_TEAM_STATS_URL.format(season=2026)
response=requests.get(url,timeout=60);response.raise_for_status()
frame=pd.read_csv(io.BytesIO(response.content),low_memory=False)
team_records=json.loads(frame.to_json(orient='records'))
team_source={'url':url,'captured_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(response.content).hexdigest(),'rows':team_records}
write(root/'weekly-review-team-source.json',team_source)
index={(r['game_id'],canonical(r['team'])):r for r in team_records if r['season_type']=='REG'}
if len(index)!=len([r for r in team_records if r['season_type']=='REG']):raise ValueError('Duplicate team stat')
checks=[];failures=[]
games=sorted((g for g in snapshot['games'] if g['season']==2026 and 1<=g['week']<=4),key=lambda g:(timestamp(g['kickoff']),g['game_id']))
for g in games:
 issues=[]
 for t,o in ((g['away'],g['home']),(g['home'],g['away'])):
  team=index.get((g['game_id'],t))
  if not team or canonical(team['opponent_team'])!=o or int(team['week'])!=g['week']:issues.append('Missing or mismatched team record');continue
  rows=[r for r in snapshot['boxes'] if r['game_id']==g['game_id'] and r['team']==t]
  for field in ('carries','targets',*METRICS):
   total=sum(float(r[field]) for r in rows)
   if abs(total-team[field])>.01:issues.append({'team':t,'field':field,'player_total':total,'team_total':team[field]})
 if issues:failures.append({'game_id':g['game_id'],'issues':issues})
 else:checks.append(g['game_id'])
history,rejected=prepare(snapshot,datetime.now(timezone.utc).isoformat(),True)
predictions={p['game']['game_id']:p for p in old['forecasts']}
grades={g['game_id']:g for g in old['grades']}
added=[]
for g in games:
 gid=g['game_id']
 if gid not in checks:continue
 if gid in predictions:continue
 cutoff=timestamp(g['kickoff'])-timedelta(minutes=1)
 training=[h for h in history if timestamp(h['game']['kickoff'])<cutoff]
 request={'game':g,'decision_at':cutoff.isoformat(),'players':roster(training,g),'availability_verified':False,'roster_evidence':'reconstructed previous-three-game usage only'}
 p=forecast(training,request,Settings(**old['settings']))
 result=grade(p,snapshot)
 if result['status']!='graded':raise ValueError(result)
 predictions[gid]=p;grades[gid]=result;added.append(gid)
 print(json.dumps({'additional_game':gid}),flush=True)
weekly=[];details=[]
for week in range(1,5):
 selected=[g for g in games if g['week']==week]
 usable=[grades[g['game_id']] for g in selected if g['game_id'] in checks and g['game_id'] in grades]
 summary={}
 for metric in METRICS:
  values=[g['metrics'][metric] for g in usable]
  summary[metric]={'model_credit':sum(v['top_choice_credit'] for v in values),'baseline_credit':sum(v['mean_baseline_credit'] for v in values),
   'model_rate':float(np.mean([v['top_choice_credit'] for v in values])),'baseline_rate':float(np.mean([v['mean_baseline_credit'] for v in values])),
   'first_or_tied_hits':sum(v['top_choice_credit']>0 for v in values),'ties':sum(len(v['winner_ids'])>1 for v in values)}
 weekly.append({'week':week,'scheduled_games':len(selected),'graded_games':len(usable),'metrics':summary})
for g in games:
 gid=g['game_id']
 if gid not in checks or gid not in predictions:continue
 p=predictions[gid];gr=grades[gid]
 box=[b for b in snapshot['boxes'] if b['game_id']==gid];names={b['identity']:b['name'] for b in box}
 row={'game_id':gid,'week':g['week'],'matchup':g['away']+' at '+g['home'],'metrics':{}}
 for metric in METRICS:
  family=p['metrics'][metric]['players'];top=family[0];baseline=max((r for r in family if not r['residual']),key=lambda r:r['baseline_mean'])
  outcome=gr['metrics'][metric]
  row['metrics'][metric]={'model_pick':top['name'],'model_probability':top['win_share'],'baseline_pick':baseline['name'],
    'actual_winners':[names.get(i,i) for i in outcome['winner_ids']],'winning_stat':outcome['winning_stat'],
    'model_credit':outcome['top_choice_credit'],'baseline_credit':outcome['mean_baseline_credit']}
 details.append(row)
aggregate={}
for metric in METRICS:
 all_values=[grades[g['game_id']]['metrics'][metric] for g in games if g['game_id'] in checks and g['game_id'] in grades]
 aggregate[metric]={'games':len(all_values),'model_credit':sum(v['top_choice_credit'] for v in all_values),'baseline_credit':sum(v['mean_baseline_credit'] for v in all_values),
  'model_rate':float(np.mean([v['top_choice_credit'] for v in all_values])),'baseline_rate':float(np.mean([v['mean_baseline_credit'] for v in all_values]))}
result={'authority':'retrospective_not_frozen_pregame_validation','weekly':weekly,'overall':aggregate,'games':details,'additional_games':added,'team_box_coverage_failures':failures,
 'result_rule':'Full-source player totals reconcile to independent team feed for all five stats. Target PBP discrepancies remain excluded from training, but do not discard independently corroborated box outcomes.',
 'training_reconciliation_rejections':rejected,'source_sha256':old['source_sha256'],'team_source_sha256':team_source['sha256'],
 'additional_forecasts':[predictions[g] for g in added],'additional_grades':[grades[g] for g in added],
 'limits':['Historical rosters reconstructed from prior usage; corrected data, not archived pregame availability.','No model parameters were retuned. Original 56 forecasts and repaired grades retained.','First-choice rates split tied-winner credit; no profitability or calibration claim.']}
write(root/'weekly-review-all-games.json',result)
print(json.dumps({'weekly':weekly,'overall':aggregate,'failures':failures},indent=2))
