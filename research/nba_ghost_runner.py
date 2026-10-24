"""Freeze, grade and report NBA ghost v2; isolated append-only research tables.

Without --apply this writes exclusive local artifacts only. No paid odds fetches.
"""
from __future__ import annotations
import argparse
from datetime import date,datetime,timedelta,timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from research.nba_points_forward import build_snapshot, aware, write_new
from research.nba_points_ghost_v2 import VERSION,merge_history,freeze,grade,report

TABLES=[
    '''CREATE TABLE IF NOT EXISTS nba_ghost_snapshots (
        snapshot_sha256 TEXT PRIMARY KEY,game_date DATE NOT NULL,version TEXT NOT NULL,
        mode TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL,snapshot JSONB NOT NULL)''',
    '''CREATE UNIQUE INDEX IF NOT EXISTS nba_ghost_one_prospective_day
       ON nba_ghost_snapshots(game_date,version) WHERE mode='prospective' ''',
    '''CREATE TABLE IF NOT EXISTS nba_ghost_grades (
        id BIGSERIAL PRIMARY KEY,snapshot_sha256 TEXT NOT NULL REFERENCES nba_ghost_snapshots(snapshot_sha256),
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),grade JSONB NOT NULL)''',
]


def classifications_from_scoreboard(payload,captured):
    from ingest.nba_box_scores import eastern_date
    result=[]
    for event in payload.get('events',[]):
        comp=event['competitions'][0];sides={p['homeAway']:p['team']['displayName'] for p in comp['competitors']}
        result.append({'espn_event_id':str(event['id']),'game_date':str(eastern_date(aware(event['date']))),
                       'season_type':(event.get('season') or {}).get('type'),'captured_at':aware(captured).isoformat(),
                       'home_team':sides['home'],'away_team':sides['away'],
                       'source_url':'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates='+str(eastern_date(aware(event['date']))).replace('-','')})
    return result


def fetch_scoreboard(day):
    import requests
    response=requests.get('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard',params={'dates':str(day).replace('-','')},timeout=30)
    response.raise_for_status()
    return response.json(),datetime.now(timezone.utc)


def load_inputs(conn,day,cutoff):
    with conn.cursor() as c:
        c.execute('''SELECT p.*,t.fga AS team_fga FROM nba_player_game_logs p
                     LEFT JOIN nba_team_game_logs t ON t.game_id=p.game_id AND t.team_id=p.team_id
                     AND t.season_type=p.season_type AND t.fetched_at < %s
                     WHERE p.game_date < %s AND p.season_type='Regular Season' AND p.fetched_at < %s''',(cutoff,day,cutoff))
        logs=[dict(r) for r in c.fetchall()]
        c.execute('SELECT team_id,name,abbreviation FROM teams');teams=[dict(r) for r in c.fetchall()]
        c.execute('SELECT * FROM nba_box_score_games WHERE game_date < %s AND game_date >= %s AND completed AND fetched_at < %s',(day,day-timedelta(days=45),cutoff));games=[dict(r) for r in c.fetchall()]
        c.execute('''SELECT p.*,g.game_date FROM nba_box_score_players p JOIN nba_box_score_games g USING(espn_event_id)
                     WHERE g.game_date < %s AND g.game_date >= %s AND g.completed AND p.fetched_at < %s AND g.fetched_at < %s''',(day,day-timedelta(days=45),cutoff,cutoff));boxes=[dict(r) for r in c.fetchall()]
    proofs=[]
    for prior_day in sorted({str(g['game_date']) for g in games}):
        payload,captured=fetch_scoreboard(prior_day);proofs.extend(classifications_from_scoreboard(payload,captured))
    return {'logs':logs,'boxes':boxes,'games':games,'teams':teams,'classifications':proofs}


def make_snapshot(market,bundle,model,created):
    cutoff=min(aware(created),aware(market['lock_at']))
    merged,blocked,audit=merge_history(bundle['logs'],bundle['boxes'],bundle['games'],bundle['teams'],bundle['classifications'],market['game_date'],cutoff)
    return freeze(market,merged,blocked,audit,created,model,bundle['classifications'],bundle.get('availability'))


def persist_snapshot(conn,snapshot):
    from psycopg2.extras import Json
    from research.nba_points_ghost import verify
    verify(snapshot,VERSION)
    with conn.cursor() as c:
        for statement in TABLES:c.execute(statement)
        c.execute('''INSERT INTO nba_ghost_snapshots(snapshot_sha256,game_date,version,mode,created_at,snapshot)
                     VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING snapshot_sha256''',
                  (snapshot['sha256'],snapshot['game_date'],VERSION,snapshot['mode'],snapshot['created_at'],Json(snapshot)))
        inserted=c.fetchone()
    conn.commit()
    return bool(inserted)


def render(snapshot):
    rows=['# NBA ghost v2 — '+snapshot['game_date'],'',
          'Mode: '+snapshot['mode']+'. Experimental research; watch scores are not probabilities.',
          'Availability is unverified unless explicit timestamped evidence is attached. Cross-season transfer and rookie estimates remain limited.','',
          '| Player | Market | Blended ghost | Opportunity points | Historical P90 | Experimental P90 | Ceiling watch | Availability | Recent games / total | Workload facts |',
          '|---|---:|---:|---:|---:|---:|---|---|---|---|']
    fmt=lambda v:'—' if v is None else f'{v:.1f}'
    for c in sorted(snapshot['candidates'],key=lambda c:(-c['line'],c['player'])):
        watch=c['ceiling_watch'];label='Watch' if watch['flag'] else ('No flag' if watch['flag'] is False else 'Insufficient history')
        if watch['status']=='reported_out':label='Reported out'
        if 'opening_season_transfer_unvalidated' in c['quality_flags'] and watch['flag']:label+=' (transfer unvalidated)'
        rows.append('| '+ ' | '.join([c['player'],fmt(c['line']),fmt(c['predictions']['blend90_10']),fmt(c['predictions']['opportunity_points']),fmt(c['historical_p90']),fmt(c['experimental_p90']),label,c['availability'].get('status','unverified'),
                                     f"{c['current_season_history_count']} / {c['history_count']}",', '.join(c['observed_workload_flags']) or '—'])+' |')
    return '\n'.join(rows)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    p=sub.add_parser('freeze');p.add_argument('--date',type=date.fromisoformat);p.add_argument('--market-snapshot',type=Path);p.add_argument('--input-bundle',type=Path)
    p.add_argument('--availability',type=Path,help='Optional canonical-name keyed, timestamped availability evidence JSON')
    p.add_argument('--model',type=Path,default=Path('research/artifacts/nba_ghost_opportunity_v1.json'));p.add_argument('--output',type=Path,required=True);p.add_argument('--allow-retrospective',action='store_true');p.add_argument('--apply',action='store_true')
    p=sub.add_parser('grade');p.add_argument('--snapshot',type=Path);p.add_argument('--output',type=Path);p.add_argument('--date',type=date.fromisoformat);p.add_argument('--days-back',type=int,default=3);p.add_argument('--apply',action='store_true')
    p=sub.add_parser('report');p.add_argument('--grades',type=Path,nargs='+');p.add_argument('--output',type=Path,required=True)
    p.add_argument('--since',type=date.fromisoformat);p.add_argument('--through',type=date.fromisoformat);p.add_argument('--model-sha')
    args=parser.parse_args()
    if args.action=='report':
        if args.grades:outcomes=[json.loads(p.read_text()) for p in args.grades]
        else:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            from config import load_config
            with psycopg2.connect(load_config().database_url,cursor_factory=RealDictCursor) as conn:
                conn.set_session(readonly=True)
                with conn.cursor() as c:
                    c.execute("SELECT to_regclass('nba_ghost_grades') AS table_name")
                    if c.fetchone()['table_name'] is None:outcomes=[]
                    else:
                        c.execute('''SELECT DISTINCT ON (g.snapshot_sha256) g.grade FROM nba_ghost_grades g
                                     JOIN nba_ghost_snapshots s USING(snapshot_sha256)
                                     WHERE s.version=%s AND s.mode='prospective'
                                     AND (%s::date IS NULL OR s.game_date >= %s::date)
                                     AND (%s::date IS NULL OR s.game_date <= %s::date)
                                     AND (%s::text IS NULL OR s.snapshot->>'model_sha256'=%s)
                                     ORDER BY g.snapshot_sha256,g.graded_at DESC,g.id DESC''',
                                  (VERSION,args.since,args.since,args.through,args.through,args.model_sha,args.model_sha))
                        outcomes=[r['grade'] for r in c.fetchall()]
        write_new(args.output,report(outcomes));return
    model=json.loads(args.model.read_text()) if args.action=='freeze' else None
    if args.action=='freeze' and args.input_bundle:
        if not args.market_snapshot:parser.error('--input-bundle requires --market-snapshot')
        market=json.loads(args.market_snapshot.read_text());bundle=json.loads(args.input_bundle.read_text())
        if args.availability:bundle['availability']=json.loads(args.availability.read_text())
        result=make_snapshot(market,bundle,model,datetime.now(timezone.utc))
    else:
        import psycopg2
        from psycopg2.extras import RealDictCursor,Json
        from config import load_config
        from db.database import DatabaseManager
        from model.nba_points_calibration import load_box_scores,BoxGame
        day=args.date or datetime.now(ZoneInfo('America/New_York')).date()
        if args.action=='grade' and not args.date:day-=timedelta(days=1)
        db=DatabaseManager(load_config().database_url,initialize_schema=False)
        with psycopg2.connect(db.database_url,cursor_factory=RealDictCursor) as conn:
            conn.set_session(readonly=True);db._shared_connection=conn
            try:
                if args.action=='freeze':
                    if args.market_snapshot:market=json.loads(args.market_snapshot.read_text())
                    else:
                        payload,captured=fetch_scoreboard(day)
                        starts=[aware(e['date']) for e in payload.get('events',[])]
                        if not starts:print(json.dumps({'skipped':'no scheduled games'}));return
                        lock=min(starts)
                        if datetime.now(timezone.utc)>=lock and not args.allow_retrospective:
                            print(json.dumps({'skipped':'common first-game cutoff has passed'}));return
                        with conn.cursor() as c:
                            c.execute("""SELECT * FROM prop_odds_history WHERE sport='nba' AND game_date=%s AND market IN ('player_points','player_points_alternate') AND captured_at < %s""",(day,min(datetime.now(timezone.utc),lock)))
                            raw=[dict(r) for r in c.fetchall()]
                        if not raw:print(json.dumps({'skipped':'no archived pregame market quotes'}));return
                        schedule=[BoxGame(e['id'],next(x['team']['displayName'] for x in e['competitions'][0]['competitors'] if x['homeAway']=='home'),next(x['team']['displayName'] for x in e['competitions'][0]['competitors'] if x['homeAway']=='away'),[],aware(e['date'])) for e in payload['events']]
                        market=build_snapshot(raw,day,lock,datetime.now(timezone.utc),schedule)
                    cutoff=min(datetime.now(timezone.utc),aware(market['lock_at']))
                    bundle=load_inputs(conn,date.fromisoformat(market['game_date']),cutoff)
                    if args.availability:bundle['availability']=json.loads(args.availability.read_text())
                    result=make_snapshot(market,bundle,model,datetime.now(timezone.utc))
                elif args.snapshot:
                    snap=json.loads(args.snapshot.read_text());result=grade(snap,load_box_scores(db,date.fromisoformat(snap['game_date'])))
                else:
                    with conn.cursor() as c:
                        c.execute("SELECT to_regclass('nba_ghost_snapshots') AS table_name")
                        if c.fetchone()['table_name'] is None:snapshots=[]
                        else:
                            c.execute("SELECT snapshot FROM nba_ghost_snapshots WHERE version=%s AND mode='prospective' AND game_date BETWEEN %s AND %s ORDER BY game_date",(VERSION,day-timedelta(days=max(1,args.days_back)-1),day))
                            snapshots=[r['snapshot'] for r in c.fetchall()]
                    outcomes=[grade(s,load_box_scores(db,date.fromisoformat(s['game_date']))) for s in snapshots]
                    if args.apply:
                        # Separate write connection; research grades are append-only.
                        with psycopg2.connect(db.database_url) as write_conn:
                            with write_conn.cursor() as c:
                                for g in outcomes:c.execute('INSERT INTO nba_ghost_grades(snapshot_sha256,grade) VALUES(%s,%s)',(g['snapshot_sha256'],Json(g)))
                    print(json.dumps({'graded_snapshots':len(outcomes),'applied':args.apply,'report':report(outcomes)},default=str));return
            finally:db._shared_connection=None
    if args.action=='freeze' and result['mode']!='prospective' and not args.allow_retrospective:
        raise ValueError('not a prospective snapshot; use --allow-retrospective for an explicit diagnostic')
    if args.action=='freeze' and args.apply:
        import psycopg2
        from config import load_config
        with psycopg2.connect(load_config().database_url) as conn:
            inserted=persist_snapshot(conn,result)
        if not inserted:print(json.dumps({'stored':False,'reason':'original snapshot already exists; it was preserved'}));return
    if args.output:
        write_new(args.output,result)
        if args.action=='freeze':
            with args.output.with_suffix('.md').open('x',encoding='utf-8') as f:f.write(render(result))
    print(json.dumps({'version':VERSION,'mode':result.get('mode',result.get('snapshot_mode')),'players':len(result.get('candidates',result.get('rows',[]))),'output':str(args.output)}))


if __name__=='__main__':main()
