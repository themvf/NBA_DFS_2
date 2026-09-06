"""Read-only source audit and volume/share replay; writes a versioned local UI report."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from config import load_config
from model.nfl_dfs_historical import draftkings_points, OFFENSE_FIELDS
from model.nfl_dfs_target_share import VERSION, CONFIG, forecast, replay, metrics, pregame_availability, normalize_team

ROOT=Path(__file__).resolve().parents[1]


def read_sources(root):
    manifest=json.loads((root/'artifacts/ff_v2_historical_context_2020_2025.json').read_text())
    history,teams,sources=[],[],[]
    for season in [2023,2024,2025]:
        source=manifest['sources'][f'weekly-stats:{season}']
        path=root/source['cachePath']
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        if digest!=source['responseHash']:
            raise ValueError('Weekly source digest mismatch')
        frame=pd.read_parquet(path); frame=frame[frame.season_type=='REG'].copy(); frame['team']=frame.team.map(normalize_team)
        if frame.duplicated(['game_id','player_id']).any():
            raise ValueError('Duplicate player game')
        if frame[['targets','attempts']].isna().any().any():
            raise ValueError('Incomplete team target/pass totals')
        for (game,team),group in frame.groupby(['game_id','team']):
            targets,attempts=float(group.targets.sum()),float(group.attempts.sum())
            if not 0<=targets<=attempts:
                raise ValueError('Targets exceed attempts')
            teams.append({'game_id':game,'team':team,'season':season,'week':int(group.week.iloc[0]),'targets':targets,'attempts':attempts})
        for row in frame[frame.position.isin(['QB','RB','WR','TE','FB'])].to_dict('records'):
            if any(pd.isna(row.get(k)) for k in OFFENSE_FIELDS):
                continue
            history.append({'identity':row['player_id'],'name':row['player_display_name'],'position':row['position'],'team':row['team'],'season':season,'week':int(row['week']),'game_id':row['game_id'],'targets':float(row['targets']),'fpts':draftkings_points(row['position'],row)})
        sources.append({'season':season,'sha256':digest,'rows':len(frame),'url':source['url']})
    return history,teams,sources


def current_evidence(now):
    with psycopg2.connect(load_config().database_url) as c:
        c.set_session(readonly=True)
        with c.cursor(cursor_factory=RealDictCursor) as q:
            q.execute('SELECT season, count(*) n, min(observed_at) first_capture, max(observed_at) latest_capture FROM ff_player_injury_observations WHERE observed_at<=%s GROUP BY season ORDER BY season',(now,))
            audit=[dict(r) for r in q.fetchall()]
            q.execute("SELECT g.season,g.week,g.kickoff,h.abbreviation home_team,a.abbreviation away_team FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id WHERE g.season=2026 AND g.week=1 AND g.game_type='REG' AND g.kickoff>%s ORDER BY g.kickoff",(now,))
            games=[dict(r) for r in q.fetchall()]
            q.execute("SELECT gsis_id identity, canonical_name name, team_abbrev team, position, fetched_at, metadata->'sleeper' sleeper FROM ff_players WHERE season=2026 AND gsis_id IS NOT NULL")
            roster=[{**dict(r),'team':normalize_team(r['team'])} for r in q.fetchall()]
    return audit,games,roster


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-root',type=Path,required=True);args=p.parse_args()
    history,teams,sources=read_sources(args.source_root)
    predictions,excluded=replay(history,teams)
    now=datetime.now(timezone.utc)
    audit,games,roster=current_evidence(now)
    forward=[]
    for game in games:
        for team in [game['home_team'],game['away_team']]:
            members=[r for r in roster if r['team']==team]
            known={}
            for r in members:
                evidence=pregame_availability(r,team,now,game['kickoff'])
                if evidence:
                    known[r['identity']]=evidence
            out=[pid for pid,e in known.items() if e['out']]
            base=forecast(history,teams,team,(2026,1))
            adjusted=forecast(history,teams,team,(2026,1),out)
            if not base or not adjusted:
                continue
            rows=[]
            for member in members:
                pid=member['identity'];value=base['players'].get(pid)
                if member['position']!='WR' or not value or value['games']<CONFIG['min_player_games']:
                    continue
                a=adjusted['players'][pid]
                # Frozen before kickoff. Unadjusted candidate residuals do not validate injury scenarios.
                residual=np.array([r['actual']-r['candidate']['mean'] for r in predictions[-2000:]])
                residual-=residual.mean()
                quantiles=np.quantile(value['candidate_fpts']+residual,[.1,.5,.9])
                rows.append({'identity':pid,'name':member['name'],'history_games':value['games'],'targets_baseline':value['baseline_targets'],'targets_volume':value['targets'],'targets_if_out':a['targets'],'fpts_baseline':value['baseline_fpts'],'fpts_volume':value['candidate_fpts'],'fpts_if_out':a['candidate_fpts'],'p10':float(quantiles[0]),'p50':float(quantiles[1]),'p90':float(quantiles[2]),'availability':known.get(pid),'source':'research_volume_share'})
            forward.append({'team':team,'kickoff':game['kickoff'],'attempts':base['attempts'],'target_budget':base['target_budget'],'unallocated_targets':adjusted['unallocated_targets'],'unmatched_historical_targets':sum(v['targets'] for pid,v in adjusted['players'].items() if pid not in {m['identity'] for m in members}),'removed_share':adjusted['removed_share'],'redistributed_share':adjusted['redistributed_share'],'out_players':[{'identity':pid,'name':next((r['name'] for r in members if r['identity']==pid),pid),**known[pid]} for pid in out], 'players':sorted(rows,key=lambda r:-r['fpts_volume'])})
    result={'version':VERSION,'as_of':now,'config':CONFIG,'sources':sources,'roster_evidence_digest':hashlib.sha256(json.dumps(roster,default=str,sort_keys=True).encode()).hexdigest(),'recipe_digest':hashlib.sha256((ROOT/'model/nfl_dfs_target_share.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest(),'replay':{str(year):metrics([r for r in predictions if r['season']==year]) for year in [2024,2025]},'excluded':excluded,'availability_audit':audit,'historical_pregame_availability_rows':sum(r['n'] for r in audit if r['season'] in [2024,2025]),'forward':forward,'optimizer_enabled':False,'limits':['2024 and 2025 retrospective diagnostics; previously inspected seasons.','Historical replay uses no target-week roster or injury outcomes. Recorded stat rows exclude missing/DNP observations.','Current Sleeper roster evidence is pregame retrieval evidence, not official game-day confirmation.','Half redistribution is an unvalidated scenario, not an activated injury adjustment.','Player residual ranges are not joint-lineup percentiles. No Kelly sizing.']}
    result['snapshot_digest']=hashlib.sha256(json.dumps(result,default=str,sort_keys=True,allow_nan=False).encode()).hexdigest()
    archive=ROOT/'artifacts/nfl_volume_share'/f"{result['snapshot_digest']}.json"
    archive.parent.mkdir(parents=True,exist_ok=True)
    with archive.open('x',encoding='utf-8') as f:
        f.write(json.dumps(result,default=str,indent=2,allow_nan=False)+'\n')
    path=ROOT/'web/src/data/nfl-volume-share-report.json'
    path.write_text(json.dumps(result,default=str,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'replay':result['replay'],'availability_audit':audit,'forward_teams':len(forward),'forward_wr':sum(len(f['players']) for f in forward)},default=str,indent=2))

if __name__=='__main__': main()
