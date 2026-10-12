"""Backfill pregame NFL player props from The Odds API archive.

    python -m ingest.nfl_prop_backfill --season 2026 --weeks 1-4               # dry run: plan and cost
    python -m ingest.nfl_prop_backfill --season 2026 --weeks 1-4 --apply --max-credits 7200

For every regular-season game in the weeks it reads the archived snapshot
LEAD_MINUTES before kickoff (the archive returns the latest snapshot at or
before that time) for the same markets and books as the live capture, and
stores it through the live writer with the snapshot's own timestamp as
captured_at, so it is pregame data on the same footing as a live capture.
Historical calls cost 10x: 10 credits per market per game, plus 1 per
kickoff time for the event list. Each game requests only the markets it has no
pregame rows for, so a rerun never pays twice for data it already holds.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_prop_odds import CAPTURE_MARKETS, ODDS_BASE, SPORT, persist_event_payload, record_usage, require_credit_budget
from ingest.sportsbook_policy import BOOKMAKER_KEYS

LEAD_MINUTES = 90
HISTORICAL_MULTIPLIER = 10


def iso(dt):
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def plan(db, season, weeks):
    rows = db.execute("""
        SELECT g.nflverse_game_id game_id, g.week, g.kickoff, h.odds_api_name home, a.odds_api_name away
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week = ANY(%s) AND g.game_type='REG' ORDER BY g.kickoff""", (season, list(weeks)))
    have = defaultdict(set)
    for r in db.execute("""
        SELECT DISTINCT home_team_name home, away_team_name away, commence_time commence, market FROM prop_odds_history
        WHERE sport='nfl' AND captured_at < commence_time"""):
        have[r['home'], r['away'], r['commence']].add(r['market'])
    todo = []
    for r in rows:
        if r['kickoff'] >= datetime.now(timezone.utc):
            continue  # not in the archive yet; the live capture covers it
        missing = missing_markets(have.get((r['home'], r['away'], r['kickoff']), set()))
        if missing:
            todo.append({**r, 'markets': missing})
    return rows, todo


def missing_markets(present):
    """Markets the live capture takes that a game has no pregame rows for. Markets
    DraftKings did not post leave no rows and are requested again; that costs credits
    only when the archive returns them."""
    return [m for m in CAPTURE_MARKETS if m not in present]


def match_event(events, game):
    """The archived event with the same two teams and a commence time within 15 minutes of kickoff."""
    hits = [e for e in events if e['home_team'] == game['home'] and e['away_team'] == game['away']
            and abs(datetime.fromisoformat(e['commence_time'].replace('Z', '+00:00')) - game['kickoff']) <= timedelta(minutes=15)]
    if len(hits) != 1:
        raise ValueError(f"{game['game_id']}: {len(hits)} archived events match")
    return hits[0]


def backfill(db, api_key, season, weeks, apply, max_credits, log=print):
    rows, todo = plan(db, season, weeks)
    by_time = defaultdict(list)
    for g in todo:
        by_time[g['kickoff']].append(g)
    estimate = sum(len(g['markets']) for g in todo) * HISTORICAL_MULTIPLIER + len(by_time)
    summary = {'games': len(rows), 'to_fetch': len(todo), 'kickoff_times': len(by_time),
               'markets_requested': sum(len(g['markets']) for g in todo), 'estimated_credits': estimate}
    if not apply:
        return summary
    require_credit_budget(estimated_credits=estimate, max_credits=max_credits)
    base = f'{ODDS_BASE}/historical/sports/{SPORT}'
    written, spent, failures = 0, 0, []
    for kickoff, games in sorted(by_time.items()):
        at = iso(kickoff - timedelta(minutes=LEAD_MINUTES))
        listing = requests.get(f'{base}/events', params={'apiKey': api_key, 'date': at}, timeout=30)
        listing.raise_for_status()
        spent += int(listing.headers.get('x-requests-last') or 0)
        for g in games:
            try:
                event = match_event(listing.json()['data'], g)
                response = requests.get(f"{base}/events/{event['id']}/odds", params={
                    'apiKey': api_key, 'date': at, 'markets': ','.join(g['markets']),
                    'bookmakers': ','.join(BOOKMAKER_KEYS), 'oddsFormat': 'american', 'dateFormat': 'iso'}, timeout=60)
                response.raise_for_status()
                spent += int(response.headers.get('x-requests-last') or 0)
                record_usage(db, response, 'nfl_props_backfill', f'{base}/events/{{id}}/odds', 1, [event['id']], g['markets'])
                body = response.json()
                snapshot = datetime.fromisoformat(body['timestamp'].replace('Z', '+00:00'))
                if snapshot >= kickoff:
                    raise ValueError(f"{g['game_id']}: archived snapshot {snapshot} is not pregame")
                payload = {**body['data'], 'archive_snapshot_timestamp': body['timestamp'],
                           'archive_requested_at': at, 'source_endpoint': 'historical'}
                n = persist_event_payload(db, event=body['data'], payload=payload, captured_at=snapshot)
                written += n
                log(json.dumps({'game': g['game_id'], 'snapshot': body['timestamp'], 'rows': n,
                                'remaining': response.headers.get('x-requests-remaining')}), flush=True)
            except (ValueError, requests.RequestException) as exc:
                failures.append({'game': g['game_id'], 'error': str(exc)})
                log(json.dumps({'game': g['game_id'], 'error': str(exc)}), flush=True)
    summary.update(rows_written=written, credits_spent=spent, failures=failures)
    if failures:
        raise SystemExit(json.dumps(summary, default=str))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, required=True)
    parser.add_argument('--weeks', required=True, help='e.g. 1-4')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--max-credits', type=int, default=0)
    args = parser.parse_args()
    first, last = (int(x) for x in args.weeks.split('-'))
    config = load_config()
    db = DatabaseManager(config.database_url)
    result = backfill(db, config.odds_api.api_key, args.season, range(first, last + 1), args.apply, args.max_credits)
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
