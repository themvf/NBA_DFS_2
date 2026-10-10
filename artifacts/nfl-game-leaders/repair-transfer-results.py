from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from datetime import timedelta
from collections import defaultdict
import numpy as np
from model.nfl_game_leaders import prepare,forecast,grade,Settings,timestamp
from research.nfl_game_leaders import roster
from research.nfl_longest_touchdown import read,write
s=read(Path('artifacts/nfl-game-leaders/full-capture-20261008.json.gz'))
h,_=prepare(s,'2026-10-08T12:00:00Z',True)
r=read(Path('artifacts/nfl-game-leaders/final-evaluation-2026.json'))
cases=read(Path('artifacts/nfl-game-leaders/transfer-audit.json'))['cases']
corrected=[]
for case in cases:
 if case['file']!='final-evaluation-2026':continue
 idx=next(j for j,p in enumerate(r['forecasts']) if p['game']['game_id']==case['game'])
 old=r['forecasts'][idx];g=old['game'];cutoff=timestamp(g['kickoff'])-timedelta(minutes=1)
 training=[x for x in h if timestamp(x['game']['kickoff'])<cutoff]
 req={'game':g,'decision_at':cutoff.isoformat(),'players':roster(training,g),'availability_verified':False,'roster_evidence':'reconstructed previous-three-game usage only'}
 prediction=forecast(training,req,Settings(**old['settings']))
 outcome=grade(prediction,s)
 r['forecasts'][idx]=prediction;r['grades'][idx]=outcome
 corrected.append({'game':g['game_id'],'previous_sha256':old['implementation_sha256'],'new_sha256':prediction['implementation_sha256']})
for metric,summary in r['summary'].items():
 vals=[g['metrics'][metric] for g in r['grades']]
 for key in ('brier','log_loss','top_choice_credit','mean_baseline_credit'):
  summary[key]=float(np.mean([v[key] for v in vals]))
 differences=np.array([v['top_choice_credit']-v['mean_baseline_credit'] for v in vals]);rng=np.random.default_rng(r['settings']['seed'])
 summary['top_choice_minus_baseline_95pct_game_bootstrap']=np.quantile(rng.choice(differences,(2000,len(vals))).mean(axis=1),[.025,.975]).tolist()
 bins=defaultdict(list)
 for v in vals:
  for row in v['calibration_rows']:
   if not row['residual']:bins[min(9,int(row['probability']*10))].append(row)
 summary['calibration']=[{'range':[i/10,(i+1)/10],'player_game_rows':len(rows),'mean_probability':float(np.mean([x['probability'] for x in rows])),'observed_credit':float(np.mean([x['observed_credit'] for x in rows]))} for i,rows in sorted(bins.items())]
r['transfer_corrections']=corrected
r['correction_policy']='Only audited identity-overwrite cases replayed; unchanged forecasts retain their original implementation digests.'
write(Path('artifacts/nfl-game-leaders/reviewed-evaluation-2026.json'),r)
print({m:(v['top_choice_credit'],v['mean_baseline_credit']) for m,v in r['summary'].items()})
