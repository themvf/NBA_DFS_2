"""Freeze research-only WR volume/share forecasts for one NFL week.

History comes from stored nflverse weekly stats (`ff_player_week_stats`, REG):
player rows supply each receiver's targets and DraftKings points; the
team-week rows supply team pass attempts and targets. Every row used is
strictly before the target (season, week). One immutable run is appended to
`nfl_dfs_volume_share_runs`; the DFS page's "Position workload" source reads
the newest run captured at or before a saved slate's projection cutoff.

Nothing here changes a production projection. `--dry-run` reads only
(a read-only session, no DDL) and prints the summary without writing.

`read_sources` (the digest-verified local parquet reader) is kept for the
research studies that import it; the weekly refresh no longer uses it.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from config import load_config
from model.nfl_dfs_historical import draftkings_points, OFFENSE_FIELDS
from model.nfl_dfs_target_share import VERSION, CONFIG, forecast, replay, metrics, pregame_availability, normalize_team

ROOT=Path(__file__).resolve().parents[1]
# The run container. v1 was the committed week-1 JSON built from local parquet
# with prior-season history only. v2 is database-backed, parameterised by week,
# and may use same-season weeks strictly before the target week -- the same
# walk-forward population the replay (and ingest/nfl_dfs_volume_benchmark.py)
# evaluated, not an extrapolation from it.
REPORT_VERSION='nfl-dfs-volume-share-run-v2'
HISTORY_SEASONS=3
HISTORY_SOURCE='ff_player_week_stats (nflverse, REG): player rows for targets and DK points; team-week rows for team attempts and targets'


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


def _number(value):
    if value is None:
        return None
    try:
        number=float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def history_from_rows(rows, cutoff):
    """Build forecast inputs from stored weekly rows, strictly before `cutoff`.

    `rows` carry position (the canonical roster position; 'DST' marks a team
    row), season, week, team, fetched_at and source_row. Missing stat values
    stay missing: a player row with an absent scoring field is skipped rather
    than scored as zero, and a team row without attempts/targets is an error.
    """
    history,teams,seen_players,seen_teams,per_season=[],[],set(),set(),{}
    for r in rows:
        key=(int(r['season']),int(r['week']))
        if key>=tuple(cutoff):
            continue
        raw=r['source_row'] if isinstance(r['source_row'],dict) else {}
        season_summary=per_season.setdefault(key[0],{'season':key[0],'player_rows':0,'team_rows':0,'latest_week':0,'latest_fetched_at':None})
        season_summary['latest_week']=max(season_summary['latest_week'],key[1])
        fetched=r.get('fetched_at')
        if fetched is not None and (season_summary['latest_fetched_at'] is None or fetched>season_summary['latest_fetched_at']):
            season_summary['latest_fetched_at']=fetched
        if r['position']=='DST':
            stats=raw.get('raw_team_stats') if isinstance(raw.get('raw_team_stats'),dict) else {}
            team=normalize_team(r['team']); game=stats.get('game_id')
            targets,attempts=_number(stats.get('targets')),_number(stats.get('attempts'))
            if not game or targets is None or attempts is None:
                raise ValueError(f'Incomplete team target/pass totals: {team} {key}')
            if (game,team) in seen_teams:
                raise ValueError(f'Duplicate team game: {team} {game}')
            if not 0<=targets<=attempts:
                raise ValueError(f'Targets exceed attempts: {team} {game}')
            seen_teams.add((game,team))
            teams.append({'game_id':game,'team':team,'season':key[0],'week':key[1],'targets':targets,'attempts':attempts})
            season_summary['team_rows']+=1
            continue
        position=raw.get('position')
        if position not in ('QB','RB','WR','TE','FB') or not raw.get('player_id') or not raw.get('game_id'):
            continue
        if (raw['game_id'],raw['player_id']) in seen_players:
            raise ValueError(f"Duplicate player game: {raw['player_id']} {raw['game_id']}")
        seen_players.add((raw['game_id'],raw['player_id']))
        if any(_number(raw.get(k)) is None for k in ('targets',*OFFENSE_FIELDS)):
            continue
        history.append({'identity':raw['player_id'],'name':raw.get('player_display_name') or raw.get('player_name') or raw['player_id'],
            'position':position,'team':normalize_team(raw.get('team') or r['team']),'season':key[0],'week':key[1],
            'game_id':raw['game_id'],'targets':float(raw['targets']),'fpts':draftkings_points(position,raw)})
        season_summary['player_rows']+=1
    sources=[{**s,'latest_fetched_at':s['latest_fetched_at'].isoformat() if hasattr(s['latest_fetched_at'],'isoformat') else s['latest_fetched_at']}
             for _,s in sorted(per_season.items())]
    return history,teams,sources


def read_db_history(cursor, season, week):
    cursor.execute("""SELECT p.position,w.season,w.week,w.team,w.fetched_at,w.source_row
      FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id
      WHERE w.source='nflverse' AND w.season_type='REG' AND w.season BETWEEN %s AND %s
      ORDER BY w.season,w.week,w.id""",(season-HISTORY_SEASONS,season))
    return history_from_rows([dict(r) for r in cursor.fetchall()],(season,week))


def current_evidence(cursor, season, week, now):
    cursor.execute('SELECT season, count(*) n, min(observed_at) first_capture, max(observed_at) latest_capture FROM ff_player_injury_observations WHERE observed_at<=%s GROUP BY season ORDER BY season',(now,))
    audit=[dict(r) for r in cursor.fetchall()]
    cursor.execute("""SELECT g.season,g.week,g.kickoff,h.abbreviation home_team,a.abbreviation away_team FROM nfl_season_games g
      JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
      WHERE g.season=%s AND g.week=%s AND g.game_type='REG' AND g.kickoff>%s ORDER BY g.kickoff""",(season,week,now))
    games=[dict(r) for r in cursor.fetchall()]
    cursor.execute("SELECT gsis_id identity, canonical_name name, team_abbrev team, position, fetched_at, metadata->'sleeper' sleeper FROM ff_players WHERE season=%s AND gsis_id IS NOT NULL",(season,))
    roster=[{**dict(r),'team':normalize_team(r['team'])} for r in cursor.fetchall()]
    return audit,games,roster


def next_week(cursor, season, now):
    cursor.execute("SELECT min(week) week FROM nfl_season_games WHERE season=%s AND game_type='REG' AND kickoff>%s",(season,now))
    row=cursor.fetchone()
    return row['week'] if row else None


def forward_forecasts(history, teams, predictions, games, roster, season, week, now):
    cutoff=(season,week)
    forward=[]
    residual=np.array([r['actual']-r['candidate']['mean'] for r in predictions[-2000:]])
    residual=residual-residual.mean() if len(residual) else residual
    for game in games:
        for team in [normalize_team(game['home_team']),normalize_team(game['away_team'])]:
            members=[r for r in roster if r['team']==team]
            known={}
            for r in members:
                evidence=pregame_availability(r,team,now,game['kickoff'])
                if evidence:
                    known[r['identity']]=evidence
            out=[pid for pid,e in known.items() if e['out']]
            base=forecast(history,teams,team,cutoff)
            adjusted=forecast(history,teams,team,cutoff,out)
            if not base or not adjusted:
                continue
            rows=[]
            for member in members:
                pid=member['identity'];value=base['players'].get(pid)
                if member['position']!='WR' or not value or value['games']<CONFIG['min_player_games'] or not len(residual):
                    continue
                a=adjusted['players'][pid]
                # Frozen before kickoff. Unadjusted candidate residuals do not validate injury scenarios.
                quantiles=np.quantile(value['candidate_fpts']+residual,[.1,.5,.9])
                rows.append({'identity':pid,'name':member['name'],'history_games':value['games'],'targets_baseline':value['baseline_targets'],'targets_volume':value['targets'],'targets_if_out':a['targets'],'fpts_baseline':value['baseline_fpts'],'fpts_volume':value['candidate_fpts'],'fpts_if_out':a['candidate_fpts'],'p10':float(quantiles[0]),'p50':float(quantiles[1]),'p90':float(quantiles[2]),'availability':known.get(pid),'source':'research_volume_share'})
            forward.append({'team':team,'kickoff':game['kickoff'].isoformat(),'attempts':base['attempts'],'target_budget':base['target_budget'],'unallocated_targets':adjusted['unallocated_targets'],'unmatched_historical_targets':sum(v['targets'] for pid,v in adjusted['players'].items() if pid not in {m['identity'] for m in members}),'removed_share':adjusted['removed_share'],'redistributed_share':adjusted['redistributed_share'],'out_players':[{'identity':pid,'name':next((r['name'] for r in members if r['identity']==pid),pid),**known[pid]} for pid in out], 'players':sorted(rows,key=lambda r:-r['fpts_volume'])})
    return forward


def build_report(history, teams, sources, audit, games, roster, season, week, now):
    predictions,excluded=replay(history,teams)
    forward=forward_forecasts(history,teams,predictions,games,roster,season,week,now)
    history_through=max(((t['season'],t['week']) for t in teams),default=None)
    if history_through is not None and history_through>=(season,week):
        raise ValueError('History reaches the target week')
    result={'version':VERSION,'report_version':REPORT_VERSION,'season':season,'week':week,'history_cutoff_exclusive':[season,week],
            'history_through':list(history_through) if history_through else None,'history_source':HISTORY_SOURCE,
            'as_of':now.isoformat(),'config':CONFIG,'sources':sources,
            'roster_evidence_digest':hashlib.sha256(json.dumps(roster,default=str,sort_keys=True).encode()).hexdigest(),
            'recipe_digest':hashlib.sha256((ROOT/'model/nfl_dfs_target_share.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest(),
            'implementation_digest':hashlib.sha256((ROOT/'ingest/nfl_dfs_target_share.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest(),
            'replay':{str(year):metrics([r for r in predictions if r['season']==year]) for year in [2024,2025]},'excluded':excluded,
            'availability_audit':audit,'historical_pregame_availability_rows':sum(r['n'] for r in audit if r['season'] in [2024,2025]),
            'forward':forward,'optimizer_enabled':False,
            'limits':['2024 and 2025 retrospective diagnostics; previously inspected seasons.',
                      'Walk-forward replay: every week is forecast from strictly earlier weeks, so in-season cutoffs are the evaluated population.',
                      'Stored weekly rows cover players on the current fantasy roster table; departed players can be missing from older seasons, which only affects share normalisation. Team totals come from the complete team-week feed.',
                      'Historical replay uses no target-week roster or injury outcomes. Recorded stat rows exclude missing/DNP observations.',
                      'Current Sleeper roster evidence is pregame retrieval evidence, not official game-day confirmation.',
                      'Half redistribution is an unvalidated scenario, not an activated injury adjustment.',
                      'Player residual ranges are not joint-lineup percentiles. No Kelly sizing.']}
    result['snapshot_digest']=hashlib.sha256(json.dumps(result,default=str,sort_keys=True,allow_nan=False).encode()).hexdigest()
    return result


def summary(result):
    return {'run_digest':result['snapshot_digest'],'season':result['season'],'week':result['week'],'history_through':result['history_through'],
            'replay':{year:{'n':r['n'],'candidate_mae':r['candidate']['mae'] if r['candidate'] else None} for year,r in result['replay'].items()},
            'forward_teams':len(result['forward']),'forward_wr':sum(len(f['players']) for f in result['forward'])}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--season',type=int); p.add_argument('--week',type=int)
    p.add_argument('--dry-run',action='store_true',help='read-only: compute and print, write nothing')
    p.add_argument('--output',type=Path,help='also write the full run payload to this local JSON file')
    args=p.parse_args()
    now=datetime.now(timezone.utc)
    season=args.season or (now.year-1 if now.month<=3 else now.year)
    url=load_config().database_url
    with psycopg2.connect(url) as connection:
        connection.set_session(readonly=True)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            week=args.week or next_week(cursor,season,now)
            if not week:
                print(json.dumps({'status':'no_upcoming_regular_season_games','season':season}))
                return
            history,teams,sources=read_db_history(cursor,season,week)
            audit,games,roster=current_evidence(cursor,season,week,now)
    if not games:
        print(json.dumps({'status':'no_unstarted_games','season':season,'week':week}))
        return
    result=build_report(history,teams,sources,audit,games,roster,season,week,now)
    if args.output:
        args.output.write_text(json.dumps(result,default=str,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    if args.dry_run:
        print(json.dumps({'dry_run':True,**summary(result)},default=str,indent=2))
        return
    # Schema creation is a write, so it happens only on a real run.
    from ingest.nfl_dfs_weekly import PipelineDatabase
    db=PipelineDatabase(url)
    dump=lambda value: json.dumps(value,default=str,allow_nan=False)
    with db.connect() as c:
        with c.cursor() as q:
            q.execute("""INSERT INTO nfl_dfs_volume_share_runs(run_digest,season,week,as_of_at,payload)
              VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",(result['snapshot_digest'],season,week,now,Json(result,dumps=dump)))
    print(json.dumps(summary(result),default=str,indent=2))

if __name__=='__main__': main()
