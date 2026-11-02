"""Append-only publication of parsed official NBA injury observations for the UI."""
import json,hashlib
from pathlib import Path
from datetime import datetime,timezone
from research.nba_points_forward import digest,aware
from research.nba_points_injury_evidence import VERSION,TEAM_NAMES,STATUSES,team_key,url_time

def prepared(report):
    if report.get('version')!=VERSION or report.get('sha256')!=digest({k:v for k,v in report.items() if k!='sha256'}):raise ValueError('invalid official observation digest')
    if aware(report['published_at'])!=url_time(report['source_url']) or not aware(report['published_at'])<=aware(report['observed_at'])<=datetime.now(timezone.utc):raise ValueError('invalid observation timeline')
    states=[]
    for r in report['rows']:
        away,home=r['matchup'].split('@')
        if away not in TEAM_NAMES or home not in TEAM_NAMES:raise ValueError('unrecognized NBA matchup')
        if r['status'] not in STATUSES or team_key(r['team']) not in {team_key(TEAM_NAMES[away]),team_key(TEAM_NAMES[home])}:raise ValueError('invalid injury status/team')
        states.append({**r,'away_key':team_key(TEAM_NAMES[away]),'home_key':team_key(TEAM_NAMES[home])})
    return states

def publish(report,conn):
    from psycopg2.extras import Json
    states=prepared(report)
    with conn.cursor() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS nba_injury_observations (
          report_sha256 TEXT PRIMARY KEY,published_at TIMESTAMPTZ NOT NULL,observed_at TIMESTAMPTZ NOT NULL,
          report JSONB NOT NULL,states JSONB NOT NULL,CHECK(published_at<=observed_at))''')
        c.execute('''INSERT INTO nba_injury_observations(report_sha256,published_at,observed_at,report,states)
          VALUES(%s,%s,%s,%s,%s) ON CONFLICT(report_sha256) DO NOTHING''',
          (report['sha256'],report['published_at'],report['observed_at'],Json(report),Json(states)))
    return report['sha256']

def hydrate(day,root,conn):
    import requests
    from research.nba_points_injury_evidence import archive
    from psycopg2.extras import RealDictCursor
    existing=set()
    for path in root.rglob('observation.json') if root.exists() else []:
        r=json.loads(path.read_text(encoding='utf-8'));existing.add((r.get('source_sha256'),r.get('observed_at')))
    with conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute("SELECT to_regclass('nba_injury_observations') t")
        if c.fetchone()['t'] is None:return {'imported':0,'failures':[]}
        c.execute("SELECT report FROM nba_injury_observations WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date'=%s)",(day,));reports=[r['report'] for r in c.fetchall()]
    imported=0;failures=[]
    for r in reports:
        if (r['source_sha256'],r['observed_at']) in existing:continue
        try:
            prepared(r);response=requests.get(r['source_url'],timeout=25);response.raise_for_status();data=response.content
            if hashlib.sha256(data).hexdigest()!=r['source_sha256']:raise ValueError('download differs from originally observed PDF')
            receipt={'source_sha256':r['source_sha256'],'source_url':r['source_url'],'captured_at':r['observed_at'],'original_observation_sha256':r['sha256'],'provenance':'append-only original collector database observation'}
            archive(data,r['source_url'],aware(r['observed_at']),root,receipt=receipt);imported+=1
        except Exception as e:failures.append({'report':r['sha256'],'error':str(e)})
    return {'imported':imported,'failures':failures}

def main():
    import argparse,psycopg2
    from config import load_config
    p=argparse.ArgumentParser();g=p.add_mutually_exclusive_group(required=True);g.add_argument('--observation',type=Path);g.add_argument('--hydrate-date');p.add_argument('--archive-root',type=Path);a=p.parse_args()
    with psycopg2.connect(load_config().database_url) as conn:
        if a.observation:
            report=json.loads(a.observation.read_text(encoding='utf-8'))
            if hashlib.sha256(a.observation.with_name('report.pdf').read_bytes()).hexdigest()!=report['source_sha256']:raise ValueError('raw PDF digest mismatch')
            sha=publish(report,conn);out={'published_observation':sha,'observed_at':report['observed_at']}
        else:
            if not a.archive_root:p.error('--hydrate-date requires --archive-root')
            conn.set_session(readonly=True);out=hydrate(a.hydrate_date,a.archive_root,conn)
    print(json.dumps(out))
if __name__=='__main__':main()
