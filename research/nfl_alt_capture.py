from __future__ import annotations

import argparse
from datetime import datetime, timezone
from math import ceil
from pathlib import Path

import requests

from model.nfl_joint_contracts import digest
from model.nfl_longest_touchdown import timestamp
from research.nfl_longest_touchdown import read, write

SPORT = 'americanfootball_nfl'
API_BASE = 'https://api.the-odds-api.com/v4'


def normalize(payload, observed_at, canonical_event=None, identity_map=None):
    rows, unresolved = [], []
    timestamp(observed_at)
    identity_map = identity_map or {}
    if canonical_event:
        if payload.get('id') != canonical_event['provider_event_id'] or timestamp(payload['commence_time']) != timestamp(canonical_event['kickoff']):
            raise ValueError('Provider/canonical event mismatch')
        for side in ('home', 'away'):
            if payload[f'{side}_team'] != canonical_event[f'provider_{side}']:
                raise ValueError('Provider/canonical team mismatch')
    for book in payload.get('bookmakers', []):
        for market in book.get('markets', []):
            for outcome in market.get('outcomes', []):
                name = outcome.get('description')
                if not name or outcome.get('name', '').lower() not in ('over', 'under') or outcome.get('point') is None:
                    unresolved.append({'reason': 'unsupported_shape', 'book': book['key'], 'market': market['key'], 'outcome': outcome})
                    continue
                identity = identity_map.get(name)
                row = {'provider_event_id': payload['id'], 'game_id': (canonical_event or {}).get('game_id'),
                       'identity': identity, 'provider_player_name': name, 'book': book['key'], 'market': market['key'],
                       'side': outcome['name'].lower(), 'line': outcome['point'], 'comparator': 'gt' if outcome['name'].lower() == 'over' else 'lt',
                       'price': outcome['price'], 'price_format': 'american', 'observed_at': observed_at,
                       'published_at': market.get('last_update') or book.get('last_update'), 'payload_digest': digest(payload),
                       'kickoff': payload['commence_time'], 'eligible_pregame': timestamp(observed_at) < timestamp(payload['commence_time'])}
                if not identity:
                    unresolved.append({'reason': 'unmapped_player', 'quote': row})
                rows.append(row)
    seen = set()
    for row in rows:
        key = (row['book'], row['market'], row['provider_player_name'], row['side'], row['line'])
        if key in seen:
            raise ValueError('Duplicate provider quote')
        seen.add(key)
    pairs = {}
    for row in rows:
        key = (row['book'], row['market'], row['provider_player_name'], row['line'])
        pairs.setdefault(key, set()).add(row['side'])
    return {'quotes': rows, 'unresolved': unresolved, 'paired_lines': sum(sides == {'over', 'under'} for sides in pairs.values()),
            'one_sided_lines': sum(len(sides) == 1 for sides in pairs.values())}


def plan(events, markets, books, decision_at, budget=0):
    if not markets or len(set(markets)) != len(markets) or not books or len(set(books)) != len(books):
        raise ValueError('Explicit unique markets and books required')
    if len(books) > 10 or budget < 0:
        raise ValueError('Pilot requires <=10 books and nonnegative credit budget')
    if len({e['id'] for e in events}) != len(events):
        raise ValueError('Duplicate provider events')
    upcoming = [e for e in events if timestamp(e['commence_time']) > timestamp(decision_at)]
    credits = len(upcoming) * len(markets) * ceil(len(books) / 10)
    return {'version': 'nfl-alt-capture-plan-v1', 'decision_at': decision_at, 'events': upcoming,
            'markets': markets, 'books': books, 'estimated_credits': credits, 'credit_budget': budget,
            'paid_capture_permitted': budget > 0 and credits <= budget,
            'limits': ['Explicit market keys require provider coverage verification.', 'Historical endpoints are not used.']}


def execute(capture_plan, api_key, output, reserve=1000, session=None, now=None):
    if not capture_plan['paid_capture_permitted'] or capture_plan['estimated_credits'] > capture_plan['credit_budget']:
        raise ValueError('Capture requires an explicit sufficient credit budget')
    if not api_key or reserve < 0:
        raise ValueError('Provider key and nonnegative reserve required')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    write(output / 'plan.json', capture_plan)
    session = session or requests.Session()
    now = now or (lambda: datetime.now(timezone.utc).isoformat())
    spent, captures = 0, []
    for event in capture_plan['events']:
        observed = now()
        if timestamp(observed) >= timestamp(event['commence_time']):
            captures.append({'event': event['id'], 'status': 'kickoff_passed'})
            continue
        estimated = len(capture_plan['markets']) * ceil(len(capture_plan['books']) / 10)
        if spent + estimated > capture_plan['credit_budget']:
            break
        try:
            response = session.get(f"{API_BASE}/sports/{SPORT}/events/{event['id']}/odds", params={
                'apiKey': api_key, 'markets': ','.join(capture_plan['markets']), 'bookmakers': ','.join(capture_plan['books']),
                'oddsFormat': 'american'}, timeout=30)
        except requests.RequestException as error:
            captures.append({'event': event['id'], 'status': 'network_error', 'error_type': type(error).__name__})
            break
        observed = now()
        try:
            payload = response.json()
        except ValueError:
            payload = {'error': 'non_json_response'}
        headers = {k: response.headers.get(k) for k in ('x-requests-last', 'x-requests-used', 'x-requests-remaining')}
        event_path = output / digest(event['id'])[:16]
        event_path.mkdir()
        write(event_path / 'raw.json', {'observed_at': observed, 'status': response.status_code, 'quota': headers, 'payload': payload})
        last = headers['x-requests-last']
        if last is None or not str(last).isdigit():
            captures.append({'event': event['id'], 'status': 'unknown_quota_stop'})
            break
        spent += int(last)
        if response.status_code != 200:
            captures.append({'event': event['id'], 'status': 'provider_error', 'http_status': response.status_code})
            break
        try:
            normalized = normalize(payload, observed, event.get('canonical'), event.get('identity_map'))
        except (ValueError, KeyError, TypeError) as error:
            captures.append({'event': event['id'], 'status': 'normalization_error', 'error_type': type(error).__name__})
            break
        write(event_path / 'quotes.json', normalized)
        captures.append({'event': event['id'], 'status': 'captured', 'quotes': len(normalized['quotes']),
                         'paired_lines': normalized['paired_lines'], 'one_sided_lines': normalized['one_sided_lines']})
        remaining = headers['x-requests-remaining']
        if remaining is None or not str(remaining).isdigit() or int(remaining) <= reserve or spent >= capture_plan['credit_budget']:
            break
    report = {'spent_credits': spent, 'captures': captures, 'credit_budget': capture_plan['credit_budget'],
              'budget_exceeded': spent > capture_plan['credit_budget'],
              'unknown_charge_requests': sum(c['status'] in ('network_error', 'unknown_quota_stop') for c in captures)}
    write(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description='Local immutable NFL ladder capture; dry-run by default')
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--markets', required=True)
    parser.add_argument('--books', required=True)
    parser.add_argument('--decision-at', required=True)
    parser.add_argument('--credit-budget', type=int, default=0)
    parser.add_argument('--reserve', type=int, default=1000)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    capture_plan = plan(read(args.events), args.markets.split(','), args.books.split(','), args.decision_at, args.credit_budget)
    if args.apply:
        from config import load_config
        execute(capture_plan, load_config().odds_api.api_key, args.output, args.reserve)
    else:
        write(args.output, capture_plan)


if __name__ == '__main__':
    main()
