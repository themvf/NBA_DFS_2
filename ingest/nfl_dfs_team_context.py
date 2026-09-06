"""Verified 2025 play mix plus current roster; no production projection mutations."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from config import load_config
from model.nfl_dfs_target_share import normalize_team
from model.nfl_dfs_team_context import coaching_status,roster_role,caller_evidence_status

ROOT=Path(__file__).resolve().parents[1]


def play_profile(frame):
    # Designed pass plays include sacks and scrambles; exclude clock-management plays.
    x=frame[frame.play_type.isin(['pass','run']) & frame.qb_kneel.ne(1) & frame.qb_spike.ne(1) & frame.two_point_attempt.ne(1)].copy()
    if not len(x):return None
    drop=x.qb_dropback.eq(1)
    target=x.receiver_player_id.notna() & drop & x.sack.ne(1) & x.qb_scramble.ne(1)
    attempts=drop & x.sack.ne(1) & x.qb_scramble.ne(1)
    n=int(drop.sum());a=int(attempts.sum())
    return {'plays':len(x),'games':int(x.game_id.nunique()),'plays_per_game':len(x)/x.game_id.nunique(),
            'designed_run_rate':float((~drop).mean()),'scramble_rate':float((drop & x.qb_scramble.eq(1)).sum()/n) if n else 0.,
            'sack_rate':float((drop & x.sack.eq(1)).sum()/n) if n else 0.,'target_rate':float(target.sum()/a) if a else 0.}


def measured_tendencies(rows):
    """Observed binary rates retain their eligible and known denominators."""
    x=rows[rows.play_type.isin(['pass','run']) & rows.qb_kneel.ne(1) & rows.qb_spike.ne(1) & rows.two_point_attempt.ne(1)]
    def binary(frame, column):
        values=frame[column] if column in frame else pd.Series(dtype=float)
        known=values[values.isin([0,1])]
        return {'eligible':len(frame),'known':len(known),'rate':float(known.mean()) if len(known) else None}
    red=x[x.yardline_100.between(1,20)] if 'yardline_100' in x else x.iloc[:0]
    goal=x[x.yardline_100.between(1,5)] if 'yardline_100' in x else x.iloc[:0]
    return {'shotgun':binary(x,'shotgun'),'no_huddle':binary(x,'no_huddle'),
            'red_zone_dropback':binary(red,'qb_dropback'),'inside_five_dropback':binary(goal,'qb_dropback')}


def participation_audit(plays, participation):
    columns=['offense_formation','offense_personnel','defense_personnel','route','defense_coverage_type']
    available=[c for c in columns if c in participation]
    right=participation.rename(columns={'nflverse_game_id':'game_id'})
    result=plays.merge(right[['game_id','play_id',*available]],on=['game_id','play_id'],how='left',validate='one_to_one',suffixes=('_pbp',''))
    result=result[result.play_type.isin(['pass','run']) & result.qb_kneel.ne(1) & result.qb_spike.ne(1) & result.two_point_attempt.ne(1)]
    audits={}
    for team,rows in result.groupby('posteam'):
        audits[team]={}
        for column in columns:
            values=rows[column].astype('string').str.strip() if column in rows else pd.Series(dtype='string')
            known=values.notna() & ~values.str.lower().isin(['','unknown','na','n/a','none','null','nan'])
            counts=values[known].value_counts()
            audits[team][column]={'eligible':len(rows),'known':int(known.sum()),'categories':{str(k):int(v) for k,v in counts.items()}}
    return audits


def positional_opportunities(rows, stats):
    """Use identities and positions from the same historical game, never current rosters."""
    keys=['game_id','player_id']
    if stats.duplicated(keys).any():raise ValueError('Duplicate historical position identity')
    positions={(r.game_id,r.player_id):r.position for r in stats.itertuples()}
    x=rows[rows.play_type.isin(['pass','run']) & rows.qb_kneel.ne(1) & rows.qb_spike.ne(1) & rows.two_point_attempt.ne(1)]
    def split(frame,column):
        counts={p:0 for p in ['QB','RB','FB','WR','TE','Unknown']}
        for r in frame.itertuples():
            pos=positions.get((r.game_id,getattr(r,column)))
            counts[pos if pos in counts else 'Unknown']+=1
        return {'opportunities':len(frame),'positions':counts}
    targets=x[x.qb_dropback.eq(1)&x.sack.ne(1)&x.qb_scramble.ne(1)&x.receiver_player_id.notna()]
    runs=x[x.qb_dropback.ne(1)]
    return {'targets':split(targets,'receiver_player_id'),'designed_carries':split(runs,'rusher_player_id'),
            'inside_five_carries':split(runs[runs.yardline_100.between(1,5)],'rusher_player_id')}


def possession_clock_spacing(rows):
    """Clock spacing between adjacent offensive snaps in the same drive and quarter.

    Includes elapsed play time, unlike a snap-to-snap wall-clock pace measure.
    All rows are retained before shifting so intervening no-play rows break a pair.
    """
    x=rows.sort_values(['game_id','play_id']).copy()
    eligible=x.play_type.isin(['pass','run'])&x.qb_kneel.ne(1)&x.qb_spike.ne(1)&x.two_point_attempt.ne(1)
    previous=x.shift(1)
    delta=previous.game_seconds_remaining-x.game_seconds_remaining
    mask=eligible & eligible.shift(1,fill_value=False) & x.game_id.eq(previous.game_id) & x.drive.eq(previous.drive) & x.qtr.eq(previous.qtr) & x.drive.notna() & delta.between(0,60)
    values=delta[mask]
    return {'pairs':len(values),'mean_seconds':float(values.mean()) if len(values) else None,'median_seconds':float(values.median()) if len(values) else None}


def prior_role_shares(rows):
    """Last eight TEAM games, including zeros when a player did not participate."""
    weeks=sorted(rows.week.unique())[-8:]
    x=rows[rows.week.isin(weeks) & rows.play_type.isin(['pass','run']) & rows.qb_kneel.ne(1) & rows.qb_spike.ne(1) & rows.two_point_attempt.ne(1)]
    targets=x[x.qb_dropback.eq(1) & x.sack.ne(1) & x.qb_scramble.ne(1) & x.receiver_player_id.notna()]
    runs=x[x.qb_dropback.ne(1)]
    target_counts=targets.receiver_player_id.value_counts().to_dict()
    carry_counts=runs.rusher_player_id.value_counts().to_dict()
    return {'weeks': [int(w) for w in weeks], 'games':int(x.game_id.nunique()), 'targets':len(targets),'designed_runs':len(runs),
            'players':{pid:{'target_share':target_counts.get(pid,0)/len(targets) if len(targets) else 0.,
                            'carry_share':carry_counts.get(pid,0)/len(runs) if len(runs) else 0.} for pid in sorted(set(target_counts)|set(carry_counts))}}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--source-root',type=Path,required=True);parser.add_argument('--season',type=int,default=2026);args=parser.parse_args()
    now=datetime.now(timezone.utc);prior=args.season-1
    manifest=json.loads((args.source_root/'artifacts/ff_v2_historical_context_2020_2025.json').read_text())
    evidence={}
    def read(key):
        source=manifest['sources'][key];path=args.source_root/source['cachePath'];raw=path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=source['responseHash']:raise ValueError('Source digest mismatch')
        evidence[key]={'url':source['url'],'sha256':source['responseHash']}
        return pd.read_csv(path) if path.suffix=='.csv' else pd.read_parquet(path)
    plays=read(f'play-by-play:{prior}');plays=plays[plays.season_type=='REG'].copy();plays['posteam']=plays.posteam.map(normalize_team)
    audits=participation_audit(plays,read(f'participation:{prior}'))
    stats=read(f'weekly-stats:{prior}');stats=stats[stats.season_type=='REG'].copy();stats['team']=stats.team.map(normalize_team)
    schedule=read('schedule:all');schedule=schedule[(schedule.season==prior)&(schedule.game_type=='REG')]
    old=stats.groupby('player_id').team.agg(lambda s:sorted(set(s))).to_dict()
    with psycopg2.connect(load_config().database_url) as c:
        c.set_session(readonly=True)
        with c.cursor(cursor_factory=RealDictCursor) as q:
            q.execute("SELECT id,gsis_id identity,canonical_name name,team_abbrev team,position,fetched_at,metadata->'sleeper' sleeper FROM ff_players WHERE season=%s AND position IN ('QB','RB','WR','TE','FB')",(args.season,))
            roster=[{**dict(r),'team':normalize_team(r['team'])} for r in q.fetchall()]
    coaching=json.loads((ROOT/'web/src/data/nfl-coaching-evidence.json').read_text())
    teams=[]
    for team,rows in plays.groupby('posteam'):
        prior_shares=prior_role_shares(rows)
        profiles={'all':play_profile(rows),'neutral':play_profile(rows[rows.score_differential.between(-7,7)&rows.qtr.le(3)]),
                  'trailing':play_profile(rows[rows.score_differential.lt(-7)]),'leading':play_profile(rows[rows.score_differential.gt(7)])}
        members=[]
        for member in roster:
            if member['team']!=team:continue
            role=roster_role(member,old.get(member['identity'],[]),args.season,now)
            shares=prior_shares['players'].get(member['identity'])
            members.append({'id':str(member['id']),'identity':member['identity'],'name':member['name'],'position':member['position'],**role,
                            'prior_target_share':shares['target_share'] if shares else None,'prior_carry_share':shares['carry_share'] if shares else None})
        coach=dict(coaching.get(team) or {})
        history=[]
        for side in ['home','away']:
            for _,game in schedule[schedule[f'{side}_team'].map(normalize_team)==team].iterrows():
                name=game[f'{side}_coach']
                if pd.notna(name):history.append({'week':int(game.week),'date':str(game.gameday),'name':name})
        coach['previous_head_coach_history']=sorted(history,key=lambda r:r['week'])
        previous={r['name'] for r in history}
        if previous:
            coach['head_coach_same']=previous=={coach.get('head_coach')}
            coach['head_coach_changed']=coach.get('head_coach') not in previous
        teams.append({'team':team,'positional_opportunities':positional_opportunities(rows,stats[stats.team==team]),'clock_spacing':possession_clock_spacing(rows),'caller_status':caller_evidence_status(coach,now,args.season),'participation_audit':audits[team],'tendencies':measured_tendencies(rows),'profiles':profiles,'coaching':coach,'continuity':coaching_status(coach,now,args.season),'prior_role_window':{k:v for k,v in prior_shares.items() if k!='players'},
                      'players':sorted(members,key=lambda r:(r['position'],r['name']))})
    result={'season':args.season,'historical_season':prior,'as_of':now.isoformat(),'teams':teams,'sources':evidence,'optimizer_enabled':False,
            'roster_digest':hashlib.sha256(json.dumps(roster,default=str,sort_keys=True).encode()).hexdigest(),
            'recipe_digest':hashlib.sha256(b''.join((ROOT/path).read_bytes().replace(b'\r\n',b'\n') for path in ['model/nfl_dfs_team_context.py','ingest/nfl_dfs_team_context.py'])).hexdigest(),
            'coaching_digest':hashlib.sha256(json.dumps(coaching,sort_keys=True).encode()).hexdigest(),
            'limits':['Historical play mix is a scenario reference, not a current forecast.','Participation coverage is an availability audit only. Route labels do not establish routes run for every receiver; coverage labels are not player-level matchup advantages.','Neutral: within seven points through quarter three; leading/trailing: beyond seven points, all quarters.','Sacks and scrambles consume designed dropbacks; kneels, spikes, no-play and two-point plays excluded.','Role shares require explicit assumptions. Departures and rookies never receive automatic transferred shares.','Roster retrieval is not official game-day confirmation. Coaching evidence must be current and complete for continuity status.']}
    (ROOT/'web/src/data/nfl-team-context.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'teams':len(teams),'players':sum(len(t['players']) for t in teams),'continuity':{t['team']:t['continuity'] for t in teams},'examples':[p for t in teams for p in t['players'] if p['name'] in ['Mike Evans','Kirk Cousins']]},indent=2))


if __name__=='__main__':main()
