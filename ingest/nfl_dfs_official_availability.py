"""Import explicitly reviewed NFL.com inactive-list evidence; never infer active from absence."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase, response_hash
from ingest.ff_injuries import persist_injury_observation
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot


def validate_report(report, now):
    if report.get('report_type') != 'inactive_list':
        raise ValueError('Official confirmation requires an explicitly reviewed inactive list')
    url = urlparse(report.get('url', ''))
    if url.scheme != 'https' or url.hostname not in {'www.nfl.com', 'nfl.com'}:
        raise ValueError('Use the supporting official NFL.com article URL')
    published = datetime.fromisoformat(report['published_at'].replace('Z', '+00:00'))
    kickoff = datetime.fromisoformat(report['kickoff'].replace('Z', '+00:00'))
    if not published.tzinfo or not kickoff.tzinfo or not published <= now < kickoff or (kickoff-published).total_seconds() > 3*3600:
        raise ValueError('Inactive-list evidence must be timestamped within three hours before kickoff')
    if not 1 <= int(report['week']) <= 18:
        raise ValueError('Invalid regular-season week')
    identities = set()
    if not isinstance(report.get('players'), list) or not report['players']:
        raise ValueError('Explicit player observations required')
    for row in report['players']:
        if row['status'] not in {'ACTIVE', 'INACTIVE'} or not row.get('team') or not row.get('position') or not row.get('gsis_id'):
            raise ValueError('Identity, team, position and explicit game-day status required')
        if row['gsis_id'] in identities:
            raise ValueError('Duplicate player')
        identities.add(row['gsis_id'])
    return published, kickoff


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args=parser.parse_args()
    report=json.loads(args.report.read_text(encoding='utf-8-sig'))
    published,kickoff=validate_report(report,datetime.now(timezone.utc))
    db=RefreshDatabase(load_config().database_url)
    try:
        source_id=persist_source_snapshot(db,SnapshotProvenance(source='nfl_official',dataset='inactive_list',season=int(report['season']),
            request_params={'week':int(report['week']),'url':report['url'],'manual_review':True},source_published_at=published,
            response_hash=response_hash(report),row_count=len(report['players']),model_eligible=True,
            eligibility_reason='Manually reviewed explicit official game-day availability; not workload confirmation'))
        for row in report['players']:
            players=db.execute("SELECT id FROM ff_players WHERE season=%s AND gsis_id=%s AND team_abbrev=%s AND position=%s",
                               (report['season'],row['gsis_id'],row['team'],row['position']))
            games=db.execute("""SELECT g.kickoff FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
              JOIN nfl_teams a ON a.team_id=g.away_team_id WHERE g.season=%s AND g.week=%s AND g.game_type='REG'
              AND (h.abbreviation=%s OR a.abbreviation=%s)""",(report['season'],report['week'],row['team'],row['team']))
            if len(players)!=1 or len(games)!=1 or games[0]['kickoff']!=kickoff:
                raise ValueError('Player or canonical game mismatch')
            raw={**row,'injury_status':'OUT' if row['status']=='INACTIVE' else 'ACTIVE','updated_at':published.isoformat(),'kickoff':kickoff.isoformat(),
                 'report_type':'inactive_list','url':report['url'],'week':report['week']}
            persist_injury_observation(db,player_id=int(players[0]['id']),season=int(report['season']),source='nfl_official',
                                      source_snapshot_id=source_id,row=raw,reconcile_current=False)
        db.close()
        print(json.dumps({'source_snapshot_id':source_id,'observations':len(report['players'])}))
    except Exception:
        db.close(error=True)
        raise


if __name__=='__main__':
    main()
