"""Paired diagnostic for team-game roles and observed prior quarterback continuity."""
import argparse,gzip,hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd
from ingest.nfl_dfs_target_share import read_sources
from ingest.nfl_dfs_volume_benchmark import STUDY,EXPECTED,pair,measure
from model.nfl_dfs_role_context import replay,CONFIG
from model.nfl_dfs_target_share import normalize_team
ROOT=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-root',type=Path,required=True);a=p.parse_args()
    history,teams,sources=read_sources(a.source_root)
    manifest=json.loads((a.source_root/'artifacts/ff_v2_historical_context_2020_2025.json').read_text())
    primary={}
    for season in [2023,2024,2025]:
        # Already hash-verified by read_sources; recheck because this is a separate read.
        source=manifest['sources'][f'weekly-stats:{season}'];path=a.source_root/source['cachePath']
        if hashlib.sha256(path.read_bytes()).hexdigest()!=source['responseHash']:raise ValueError('Source changed')
        f=pd.read_parquet(path);f=f[(f.season_type=='REG')&(f.position=='QB')&(f.attempts>0)]
        for (game,team),rows in f.groupby(['game_id','team']):
            best=rows[rows.attempts==rows.attempts.max()]
            primary[(game,normalize_team(team))]=best.player_id.iloc[0] if len(best)==1 else None
    teams=[{**t,'primary_qb':primary.get((t['game_id'],t['team']))} for t in teams]
    raw=STUDY.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=EXPECTED:raise ValueError('Frozen study changed')
    saved=json.loads(gzip.decompress(raw));predictions,excluded=replay(history,teams);paired,unmatched=pair(predictions,saved)
    baseline={(r['sample_key'],r['game_id']):r for r in saved if r['model']=='baseline' and r['position']=='WR'}
    seasons={}
    for year in [2024,2025]:
        rows=[r for r in paired if r['season']==year];m=measure(rows)
        m['candidate_brier25']=float(np.mean([(r['boom_probability']-(r['actual']>=25))**2 for r in rows]))
        m['production_brier25']=float(np.mean([(baseline[(f"{r['season']}:{r['week']}:{r['identity']}",r['game_id'])]['boom_probability']-(r['actual']>=25))**2 for r in rows]))
        m['qb_states']={state:measure([r for r in rows if r['qb_state']==state]) for state in ['stable','changed','unknown'] if any(r['qb_state']==state for r in rows)}
        m['pooled_residual_ablation']=measure([{**r,'candidate':r['pooled_candidate']} for r in rows])['candidate']
        m['pooled_brier25']=float(np.mean([(r['pooled_boom_probability']-(r['actual']>=25))**2 for r in rows]))
        m['targets_mae']=float(np.mean([abs(r['targets_actual']-r['targets_candidate']) for r in rows]))
        seasons[str(year)]=m
    result={'version':'nfl-role-context-v1','config':CONFIG,'seasons':seasons,'excluded':excluded,'unmatched':unmatched,'sources':sources,'study_sha256':EXPECTED,'optimizer_enabled':False,
        'passes_screen':all(s['candidate']['mae']<s['production']['mae'] and s['candidate']['interval80']<s['production']['interval80'] and s['candidate_brier25']<=s['production_brier25'] for s in seasons.values()),
        'limits':['Previously inspected 2024/2025 diagnostic; no untouched holdout.','Evaluation includes recorded WR games only, not DNPs or rookies without four prior games.','Quarterback context uses only the prior four team games; it does not predict current injuries or starting QB.','2026 coaching records are never applied to historical games.','No historical salary-multiple, contest payout or joint-lineup calibration.']}
    payload=json.dumps(paired,sort_keys=True,allow_nan=False).encode();result['predictions_sha256']=hashlib.sha256(payload).hexdigest()
    result['recipe_sha256']={name:hashlib.sha256((ROOT/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for name in ['model/nfl_dfs_role_context.py','ingest/nfl_dfs_role_context.py','ingest/nfl_dfs_target_share.py','ingest/nfl_dfs_volume_benchmark.py','model/nfl_dfs_target_share.py']}
    archive=ROOT/'artifacts/nfl_volume_share';archive.mkdir(parents=True,exist_ok=True)
    (archive/f"role-context-{result['predictions_sha256']}.json.gz").write_bytes(gzip.compress(payload,mtime=0))
    (ROOT/'web/src/data/nfl-role-context.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'seasons':seasons,'passes_screen':result['passes_screen']},indent=2))


if __name__=='__main__':main()
