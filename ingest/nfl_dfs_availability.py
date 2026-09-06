"""Capture explicitly week-scoped FantasyPros injuries without changing canonical roles."""
import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from ingest.ff_fantasypros import FantasyProsClient, RefreshDatabase, response_hash
from ingest.ff_injuries import persist_fantasypros_injury_observations
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot
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
        # The source store deduplicates by source/dataset/response hash, not request
        # parameters. Scope the dataset so identical week-zero/list responses cannot
        # silently reuse a different week's provenance.
        source_id = persist_source_snapshot(db, SnapshotProvenance(source='fantasypros',
            dataset=f'game-week-injuries-{season}-{week}', season=season, week=week,
            request_params=params, response_hash=response_hash(payload), row_count=len(rows),
            model_eligible=True, eligibility_reason='Week-requested injury observations; provider coverage unverified'))
        counts = persist_fantasypros_injury_observations(db, season=season, source_snapshot_id=source_id, rows=rows)
        report.update(status='captured' if rows else 'empty_unverified', source_snapshot_id=source_id,
                      response_hash=response_hash(payload), counts=counts,
                      positions=dict(Counter(str(row.get('position_id','unknown')) for row in rows)),
                      practice_rows=sum(any(row.get(f'practice_{i}') is not None for i in [1,2,3]) for row in rows),
                      undated_rows=sum(not any(row.get(k) for k in ['updated_at','last_updated','last_updated_ts','news_updated']) for row in rows),
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
