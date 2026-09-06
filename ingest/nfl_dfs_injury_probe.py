"""Read-only FantasyPros period/identity probe. Never changes player mappings."""
import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from config import load_config
from ingest.ff_fantasypros import FantasyProsClient, response_hash, normalize_name


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season',type=int,default=2026)
    parser.add_argument('--week',type=int,default=1)
    args=parser.parse_args()
    if not 1 <= args.week <= 18: parser.error('week must be 1 through 18')
    client=FantasyProsClient(os.environ.get('FANTASYPROS_API_KEY',''))
    payloads=[]
    for year,week in [(args.season,args.week),(args.season,0),(args.season-1,args.week)]:
        params={'year':year,'week':week,'include_probabilities':'true'}
        payload=client.get('nfl/injuries',params)
        payloads.append({'request':params,'captured_at':datetime.now(timezone.utc).isoformat(),
                         'hash':response_hash(payload),'payload':payload})
    conn=psycopg2.connect(load_config().database_url,cursor_factory=RealDictCursor)
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as q:
            q.execute("SELECT id,canonical_name,normalized_name,team_abbrev,position,fantasypros_player_id,gsis_id,COALESCE(yahoo_id,metadata->'sleeper'->>'yahoo_id') yahoo_id FROM ff_players WHERE season=%s", (args.season,))
            players=list(q.fetchall())
    finally:
        conn.close()
    unmatched=[]
    for row in payloads[0]['payload'].get('injuries',[]):
        if row.get('position_id') not in ['QB','RB','WR','TE','K']:
            continue
        fp=[p for p in players if str(p['fantasypros_player_id'])==str(row.get('player_id'))]
        names=[p for p in players if normalize_name(p['canonical_name'])==normalize_name(row.get('name',''))]
        old=[p for p in players if p['normalized_name']==normalize_name(row.get('name','')) and p['team_abbrev']==row.get('team_id') and p['position']==row.get('position_id')]
        if not fp and not old:
            yahoo=[p for p in players if row.get('yahoo_id') and str(p['yahoo_id'])==str(row['yahoo_id'])]
            unmatched.append({'source':row,'name_candidates':names,'yahoo_candidates':yahoo})
    report={'captured_at':datetime.now(timezone.utc).isoformat(),'responses':payloads,'unmatched_offense':unmatched,
            'positions':dict(Counter(r.get('position_id') for r in payloads[0]['payload'].get('injuries',[])))}
    out=Path('artifacts/nfl-injury-contract-probe.json');out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report,indent=2,default=str)+'\n',encoding='utf-8')
    print(json.dumps({'requests':[{'request':p['request'],'hash':p['hash']} for p in payloads],'unmatched_offense':len(unmatched)}))


if __name__=='__main__':
    main()
