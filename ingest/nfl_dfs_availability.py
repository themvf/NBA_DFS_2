"""Capture explicitly week-scoped FantasyPros injuries without changing canonical roles."""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from ingest.ff_fantasypros import FantasyProsClient, RefreshDatabase, snapshot, response_hash
from ingest.ff_injuries import persist_fantasypros_injury_observations
from ingest.nfl_dfs_weekly import target_season


def validate_payload(payload, season, week):
    if not isinstance(payload, dict) or not isinstance(payload.get('injuries'), list):
        raise ValueError('Expected an injuries list; missing is not healthy')
    for record in [payload, *payload['injuries']]:
        if not isinstance(record, dict):
            raise ValueError('Malformed injury record')
        for field, expected in [('week', week), ('season', season), ('year', season)]:
            if record.get(field) is not None and int(record[field]) != expected:
                raise ValueError('Provider period does not match requested game week')
    return payload['injuries']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int)
    parser.add_argument('--week', type=int)
    parser.add_argument('--output', default='artifacts/nfl-availability-audit.json')
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    report = {'season': season, 'week': args.week, 'captured_at': now.isoformat(),
              'official_inactives_verified': False, 'status': 'unavailable'}
    db = None
    failed = False
    try:
        if not os.environ.get('FANTASYPROS_API_KEY'):
            raise ValueError('FANTASYPROS_API_KEY is not configured')
        db = RefreshDatabase(load_config().database_url)
        week = args.week or db.execute_one("SELECT min(week) week FROM nfl_season_games WHERE season=%s AND game_type='REG' AND kickoff>%s", (season, now))['week']
        if week is None or not 1 <= week <= 18:
            raise ValueError('No eligible regular-season week')
        report['week'] = week
        params = {'year': season, 'week': week, 'include_probabilities': 'true'}
        payload = FantasyProsClient(os.environ['FANTASYPROS_API_KEY']).get('nfl/injuries', params)
        rows = validate_payload(payload, season, week)
        source_id = snapshot(db, dataset='injuries', season=season, payload=payload, params=params)
        counts = persist_fantasypros_injury_observations(db, season=season, source_snapshot_id=source_id, rows=rows)
        report.update(status='captured' if rows else 'empty_unverified', source_snapshot_id=source_id,
                      response_hash=response_hash(payload), counts=counts,
                      period_confirmed=payload.get('week') is not None,
                      limits=['A requested week is not proof of provider coverage.', 'Missing players are not cleared.',
                              'Reported availability is not official game-day confirmation.'])
    except Exception as exc:
        failed = True
        # Do not serialize HTTP errors, URLs or credentials.
        report['error_type'] = type(exc).__name__
    finally:
        if db:
            db.close(error=failed)
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(report))
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
