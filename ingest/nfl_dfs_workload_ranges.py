"""Freeze the workload-range diagnostic and audit available historical salaries."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
import psycopg2
from config import load_config
from ingest.nfl_dfs_target_share import read_sources
from ingest.nfl_dfs_volume_benchmark import STUDY, EXPECTED, pair
from model.nfl_dfs_target_share import replay as volume_replay
from model.nfl_dfs_workload_ranges import CONFIG, replay, calibration

ROOT = Path(__file__).resolve().parents[1]


def salary_audit():
    with psycopg2.connect(load_config().database_url) as c:
        c.set_session(readonly=True)
        with c.cursor() as q:
            q.execute('SELECT r.season,count(*),count(p.salary) FROM nfl_dfs_projection_runs r JOIN nfl_dfs_player_projections p USING(run_id) GROUP BY r.season')
            projections = [dict(zip(['season','rows','salary_rows'],r)) for r in q.fetchall()]
            q.execute('SELECT count(*),min(game_info),max(game_info),min(created_at),max(created_at) FROM nfl_dfs_slate_players')
            uploads = dict(zip(['rows','first_game_info','last_game_info','first_capture','last_capture'],q.fetchone()))
            q.execute('SELECT sport,count(*),min(slate_date),max(slate_date) FROM dk_slates GROUP BY sport')
            legacy = [dict(zip(['sport','slates','first_date','last_date'],r)) for r in q.fetchall()]
    return {'as_of':datetime.now(timezone.utc).isoformat(),'projection_rows':projections,'uploaded_salary_rows':uploads,'legacy_slates':legacy,
            'verified_replay_salary_rows':0,'status':'No verified 2024–2025 NFL salary mapping; historical salary-multiple validation unavailable.'}


def metrics(rows, source):
    return {'n':len(rows),'mae':float(np.mean([abs(r['actual']-r[source]['mean']) for r in rows])),
            'interval80':float(np.mean([r[source]['p90']-r[source]['p10']+10*(max(r[source]['p10']-r['actual'],0)+max(r['actual']-r[source]['p90'],0)) for r in rows])),
            'below_p10':float(np.mean([r['actual']<r[source]['p10'] for r in rows])), 'above_p90':float(np.mean([r['actual']>r[source]['p90'] for r in rows]))}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--source-root',type=Path,required=True);args=parser.parse_args()
    history,teams,sources=read_sources(args.source_root)
    raw=STUDY.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=EXPECTED:
        raise ValueError('Frozen study changed')
    saved=json.loads(gzip.decompress(raw))
    forecasts,_=volume_replay(history,teams)
    adjusted,errors=replay(forecasts)
    paired,unmatched=pair(adjusted,saved)
    baseline={(r['sample_key'],r['game_id']):r for r in saved if r['model']=='baseline' and r['position']=='WR'}
    years={}
    for year in [2024,2025]:
        rows=[r for r in paired if r['season']==year]
        years[str(year)]={'models':{m:metrics(rows,m) for m in ['production','candidate','workload']},
            'targets':{str(t):calibration(rows,t) for t in CONFIG['thresholds']},
            'production_25_brier':float(np.mean([(baseline[(f"{r['season']}:{r['week']}:{r['identity']}",r['game_id'])]['boom_probability']-(r['actual']>=25))**2 for r in rows])),
            'tiers':{str(t):metrics([r for r in rows if (0 if r['targets_baseline']<4 else 1 if r['targets_baseline']<7 else 2)==t],'workload') for t in [0,1,2]}}
    result={'version':'nfl-workload-ranges-v1','config':CONFIG,'seasons':years,'unmatched':unmatched,'salary_audit':salary_audit(),
        'sources':sources,'study_sha256':EXPECTED,'optimizer_enabled':False,
        'recipe_sha256':{name:hashlib.sha256((ROOT/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for name in ['model/nfl_dfs_workload_ranges.py','ingest/nfl_dfs_workload_ranges.py']},
        'calculator_residuals':{str(k):v[-2000:] for k,v in errors.items()},
        'limits':['Previously inspected 2024 and 2025; diagnostic, not untouched validation.','Recorded WR games exclude missing/DNP outcomes.','Uncentered workload residuals adjust the mean as well as ranges.','Production comparator disables market inputs; not archived live forecasts.','No historical salary calibration, injury redistribution, coverage matchup adjustment or lineup-payout validation.']}
    result['passes_screen']=all(y['models']['workload']['mae']<y['models']['production']['mae'] and y['models']['workload']['interval80']<y['models']['production']['interval80'] and y['targets']['25']['brier']<=y['production_25_brier'] for y in years.values())
    archive=ROOT/'artifacts/nfl_volume_share';archive.mkdir(exist_ok=True,parents=True)
    payload=json.dumps(paired,sort_keys=True,allow_nan=False).encode();result['paired_sha256']=hashlib.sha256(payload).hexdigest()
    (archive/f"workload-{result['paired_sha256']}.json.gz").write_bytes(gzip.compress(payload,mtime=0))
    (ROOT/'web/src/data/nfl-workload-ranges.json').write_text(json.dumps(result,default=str,allow_nan=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k in ['seasons','passes_screen','salary_audit']},default=str,indent=2))


if __name__=='__main__':main()
