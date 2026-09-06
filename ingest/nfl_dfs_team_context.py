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
from model.nfl_dfs_team_context import coaching_status,roster_role

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
    stats=read(f'weekly-stats:{prior}');stats=stats[stats.season_type=='REG'].copy();stats['team']=stats.team.map(normalize_team)
    old=stats.groupby('player_id').team.agg(lambda s:sorted(set(s))).to_dict()
    with psycopg2.connect(load_config().database_url) as c:
        c.set_session(readonly=True)
        with c.cursor(cursor_factory=RealDictCursor) as q:
            q.execute("SELECT id,gsis_id identity,canonical_name name,team_abbrev team,position,fetched_at,metadata->'sleeper' sleeper FROM ff_players WHERE season=%s AND position IN ('QB','RB','WR','TE','FB')",(args.season,))
            roster=[{**dict(r),'team':normalize_team(r['team'])} for r in q.fetchall()]
    coaching=json.loads((ROOT/'web/src/data/nfl-coaching-evidence.json').read_text())
    teams=[]
    for team,rows in plays.groupby('posteam'):
        profiles={'all':play_profile(rows),'neutral':play_profile(rows[rows.score_differential.between(-7,7)&rows.qtr.le(3)]),
                  'trailing':play_profile(rows[rows.score_differential.lt(-7)]),'leading':play_profile(rows[rows.score_differential.gt(7)])}
        members=[]
        for member in roster:
            if member['team']!=team:continue
            role=roster_role(member,old.get(member['identity'],[]),args.season,now)
            members.append({'id':str(member['id']),'identity':member['identity'],'name':member['name'],'position':member['position'],**role})
        coach=coaching.get(team)
        teams.append({'team':team,'profiles':profiles,'coaching':coach,'continuity':coaching_status(coach,now,args.season),
                      'players':sorted(members,key=lambda r:(r['position'],r['name']))})
    result={'season':args.season,'historical_season':prior,'as_of':now.isoformat(),'teams':teams,'sources':evidence,'optimizer_enabled':False,
            'roster_digest':hashlib.sha256(json.dumps(roster,default=str,sort_keys=True).encode()).hexdigest(),
            'recipe_digest':hashlib.sha256((ROOT/'model/nfl_dfs_team_context.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest(),
            'limits':['Historical play mix is a scenario reference, not a current forecast.','Neutral: within seven points through quarter three; leading/trailing: beyond seven points, all quarters.','Sacks and scrambles consume designed dropbacks; kneels, spikes, no-play and two-point plays excluded.','Role shares require explicit assumptions. Departures and rookies never receive automatic transferred shares.','Roster retrieval is not official game-day confirmation. Coaching evidence must be current and complete for continuity status.']}
    (ROOT/'web/src/data/nfl-team-context.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'teams':len(teams),'players':sum(len(t['players']) for t in teams),'continuity':{t['team']:t['continuity'] for t in teams},'examples':[p for t in teams for p in t['players'] if p['name'] in ['Mike Evans','Kirk Cousins']]},indent=2))


if __name__=='__main__':main()
