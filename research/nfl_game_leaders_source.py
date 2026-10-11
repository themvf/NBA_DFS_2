"""Preserve nflverse player stat credits omitted from the archetype capture."""
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json

from model.nfl_game_leaders import canonical
from model.nfl_drive_archetypes import load_pbp, PBP_URL


def enrich(snapshot, frames, sources):
    result = deepcopy(snapshot)
    indexed = {}
    stat_fields = ('play_type', 'receiver_player_id', 'receiver_player_name', 'receiving_yards',
        'complete_pass', 'pass_attempt', 'rush_attempt', 'no_play', 'qb_spike', 'two_point_attempt',
        'rusher_player_id', 'rusher_player_name', 'rushing_yards',
        'lateral_receiver_player_id', 'lateral_receiver_player_name', 'lateral_receiving_yards',
        'lateral_rusher_player_id', 'lateral_rusher_player_name', 'lateral_rushing_yards')
    positions = {(b['game_id'], b['identity']): b['position'] for b in snapshot['boxes']}
    for season, frame in frames.items():
        columns = [c for c in ('game_id', 'play_id', 'season', 'week', 'home_team', 'away_team', 'desc', *stat_fields) if c in frame]
        for r in json.loads(frame[columns].to_json(orient='records')):
            if not r.get('game_id') or r.get('play_id') is None:
                continue
            key = r['game_id'], int(r['play_id'])
            if key in indexed:
                raise ValueError('Duplicate primary source play')
            if int(r['season']) != season:
                raise ValueError('Primary source season mismatch')
            indexed[key] = r
    missing = []
    for p in result['plays']:
        r = indexed.get((p['game_id'], int(p['play_id'])))
        if not r:
            missing.append([p['game_id'], p['play_id']])
            continue
        if any(canonical(r[k]) != canonical(p[k]) for k in ('home_team', 'away_team')) or int(r['week']) != int(p['week']):
            raise ValueError('Primary source canonical game mismatch')
        if (r.get('desc') or '').strip() != (p.get('description') or '').strip():
            missing.append([p['game_id'], p['play_id'], 'description_changed'])
            continue  # A later correction must not silently change the frozen event.
        credit = {k: r.get(k) for k in stat_fields}
        if credit.get('no_play') is None:
            credit['no_play'] = int(r.get('play_type') == 'no_play')
        p['stat_credit'] = credit
        p['archetype_actors'] = deepcopy(p['actors'])
        for role in ('receiver', 'rusher'):
            identity = credit.get(f'{role}_player_id')
            if identity and not any(a['role'] == role for a in p['actors']):
                p['actors'].append({'role': role, 'player_id': identity,
                    'name': credit.get(f'{role}_player_name'), 'position': positions.get((p['game_id'], identity), 'UNKNOWN'),
                    'basis': 'primary stat-credit fields'})
        source = next(s for s in sources if s['season'] == p['season'])
        p['stat_credit_captured_at'] = source['captured_at']
    result['player_stat_sources'] = sources
    result['player_stat_credit_missing'] = missing
    return result


def capture_stat_credits(snapshot, cache_root):
    frames, sources = {}, []
    cache_root = Path(cache_root); cache_root.mkdir(parents=True, exist_ok=True)
    for season in sorted({p['season'] for p in snapshot['plays']}):
        path = cache_root / f'play_by_play_{season}.parquet'
        frames[season] = load_pbp(season, path)
        sources.append({'season': season, 'url': PBP_URL.format(season=season),
            'captured_at': datetime.now(timezone.utc).isoformat(),
            'cache_sha256': sha256(path.read_bytes()).hexdigest(), 'rows': len(frames[season])})
    return enrich(snapshot, frames, sources)


def attach_box_verification(snapshot, sources):
    from research.nfl_game_leaders_challenger import verify_boxes
    good, rejected = verify_boxes(snapshot, sources)
    snapshot['box_verified_game_ids'] = [h['game']['game_id'] for h in good]
    snapshot['box_verification'] = {'rejected': rejected,
        'sources': [{k: v for k, v in s.items() if k != 'rows'} for s in sources],
        'basis': 'Full player boxes reconcile against separate same-provider team aggregates.'}
    return snapshot


def capture_box_verification(snapshot):
    import io
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_TEAM_STATS_URL
    sources = []
    for season in sorted({g['season'] for g in snapshot['games']}):
        url = NFLVERSE_WEEKLY_TEAM_STATS_URL.format(season=season)
        response = requests.get(url, timeout=90); response.raise_for_status()
        sources.append({'url': url, 'captured_at': datetime.now(timezone.utc).isoformat(),
            'sha256': sha256(response.content).hexdigest(),
            'rows': json.loads(pd.read_csv(io.BytesIO(response.content)).to_json(orient='records'))})
    return attach_box_verification(snapshot, sources)
