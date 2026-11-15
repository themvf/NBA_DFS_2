"""Independently frozen percentile and joint-P75 card evaluation."""
from collections import Counter
from datetime import datetime, timezone
from itertools import combinations
import json
import math
from pathlib import Path
from statistics import mean

from research.nba_points_forward import aware, digest, write_new

PROTOCOL = Path(__file__).with_name('artifacts') / 'nba_percentile_protocol_v1.json'
PROTOCOL_SHA = '90b0127636631282356b52b398213fcf82d6e87b8ecb8fd58ab24876cc2ea7e8'
FIRST_SLATE = '2026-11-16'
INDEX = 'percentile-index'
FILENAME = 'percentile-trial.json'
GRADE = 'percentile-grade.json'


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def signed(value):
    return {**value, 'sha256': digest(value)}


def checked(value):
    if signed({k: v for k, v in value.items() if k != 'sha256'})['sha256'] != value.get('sha256'):
        raise ValueError('percentile evidence digest mismatch')
    return value


def protocol():
    rules = checked(read(PROTOCOL))
    if rules['sha256'] != PROTOCOL_SHA or rules['first_slate'] != FIRST_SLATE:
        raise ValueError('percentile protocol pin mismatch')
    return rules


def identity(row):
    return (str(row['event_id']), row['player_key'])


def cutoff(source):
    starts = [aware(g['start_time']) for g in source['slate_schedule']]
    if not starts:
        raise ValueError('no verified slate schedule')
    return min(aware(source['lock_at']), min(starts))


def verify_record(record, forecast, source):
    rules = protocol()
    for value in (record, forecast, source):
        checked(value)
    start, complete = aware(record['created_at']), aware(record['completed_at'])
    if (record['version'] != 'nba-percentile-record-v1' or forecast.get('version') != 'nba-points-ghost-v3'
            or record['mode'] != forecast['mode'] or record['mode'] != source['mode']
            or record['mode'] != 'prospective'
            or record['protocol_sha256'] != PROTOCOL_SHA
            or record['model_sha256'] != forecast['model_sha256'] or record['model_sha256'] != rules['model_sha256']
            or record['forecast_sha256'] != forecast['sha256']
            or record['source_snapshot_sha256'] != source['sha256'] or forecast['source_snapshot_sha256'] != source['sha256']
            or record['game_date'] != forecast['game_date'] or record['game_date'] != source['game_date']
            or record['game_date'] < FIRST_SLATE
            or aware(record['lock_at']) != cutoff(source) or aware(forecast['lock_at']) != aware(source['lock_at'])
            or not aware(rules['registered_at']) < aware(forecast['created_at']) <= start <= complete < cutoff(source)
            or not aware(source['as_of']) <= aware(source['created_at']) <= aware(forecast['created_at'])):
        raise ValueError('percentile record does not match a timely original')
    return record


def now():
    return datetime.now(timezone.utc)


def attach(forecast, source, directory, root):
    """Called once by the original recording path, before durable publication."""
    if forecast['game_date'] < FIRST_SLATE:
        return {'status': 'before_trial'}
    import numpy as np

    rules = protocol()
    created = now()
    if created >= cutoff(source):
        raise ValueError('percentile enrollment cutoff passed')
    marker = root / 'prospective-index' / (forecast['game_date'] + '-' + rules['model_sha256'] + '.json')
    original = read(marker)
    if (original['forecast_sha256'] != forecast['sha256'] or original['model_sha256'] != rules['model_sha256']
            or original['date'] != forecast['game_date']
            or Path(original['forecast_path']).resolve() != (directory / 'forecast.json').resolve()):
        raise ValueError('percentile trial requires the indexed original')
    rows = sorted(forecast['rows'], key=lambda r: (-r['distribution']['mean'], identity(r)))
    ids = [identity(r) for r in rows]
    if not rows or len(set(ids)) != len(ids):
        raise ValueError('empty or ambiguous percentile player pool')
    draws = np.asarray([r['samples'] for r in rows], dtype=float)
    if draws.ndim != 2 or draws.shape[1] == 0 or not np.isfinite(draws).all() or (draws < 0).any():
        raise ValueError('invalid aligned player draws')
    levels = rules['percentile_levels']
    quantiles = np.quantile(draws, np.array(levels) / 100, axis=1).T
    players = []
    for i, row in enumerate(rows):
        qs = dict(zip(map(str, levels), quantiles[i].tolist()))
        if not (math.isclose(qs['50'], row['distribution']['median'])
                and math.isclose(qs['90'], row['distribution']['p90'])
                and math.isclose(float(draws[i].mean()), row['distribution']['mean'])):
            raise ValueError('aligned draws cannot reproduce the original distribution')
        players.append({'event_id': ids[i][0], 'player_key': ids[i][1], 'player': row['player'],
                        'market_line': row['market_line'], 'v2_blend': row['v2_blend'],
                        'v3_mean': row['distribution']['mean'], 'quantiles': qs})

    pool = range(min(rules['trio_pool_size'], len(rows)))
    entries = []
    for combo in combinations(pool, 3):
        total = draws[list(combo)].sum(axis=0)
        entries.append({'players': sorted(ids[i] for i in combo), 'mean': float(total.mean()),
                        'p75': float(np.quantile(total, .75))})
    budget = rules['entries']
    cards = ({name: sorted(entries, key=lambda e: (-e[metric], e['players']))[:budget]
              for name, metric in [('mean_baseline', 'mean'), ('joint_p75', 'p75')]}
             if len(entries) >= budget else {})
    record = signed({'version': 'nba-percentile-record-v1', 'mode': 'prospective',
                     'game_date': forecast['game_date'], 'created_at': created.isoformat(),
                     'completed_at': now().isoformat(), 'lock_at': cutoff(source).isoformat(),
                     'protocol_sha256': PROTOCOL_SHA, 'model_sha256': forecast['model_sha256'],
                     'source_snapshot_sha256': source['sha256'], 'forecast_sha256': forecast['sha256'],
                     'players': players, 'cards': cards, 'trio_pool': [ids[i] for i in pool],
                     'draw_n': draws.shape[1]})
    verify_record(record, forecast, source)
    index = root / INDEX / (record['game_date'] + '-' + PROTOCOL_SHA + '.json')
    if index.exists():
        raise ValueError('percentile index already exists')
    path = directory / FILENAME
    write_new(path, record)
    indexed_at = now()
    if indexed_at >= cutoff(source):
        raise ValueError('percentile index cutoff passed')
    write_new(index, {'record_path': str(path.resolve()), 'record_sha256': record['sha256'],
                      'forecast_sha256': forecast['sha256'], 'indexed_at': indexed_at.isoformat()})
    return {'status': 'indexed', 'record_sha256': record['sha256'], 'player_n': len(players),
            'trio_status': 'paired_cards_frozen' if cards else 'insufficient_pool'}


def grade(record, forecast, source, diagnostics, scoreboard, graded_at):
    verify_record(record, forecast, source)
    if (diagnostics['snapshot_sha256'] != source['sha256'] or diagnostics['game_date'] != record['game_date']
            or aware(graded_at) <= aware(record['lock_at'])):
        raise ValueError('percentile diagnostics do not match the frozen source')
    expected = {str(g['event_id']) for g in source['slate_schedule']}
    events = scoreboard['payload'].get('events', [])
    final = bool(expected) and len(events) == len(expected) and {str(e['id']) for e in events} == expected and all(
        e['status']['type'].get('completed') for e in events)
    outcomes = {identity(row): row for row in diagnostics['rows']}
    if len(outcomes) != len(diagnostics['rows']):
        raise ValueError('ambiguous outcome identity')
    players = []
    actual_by_id = {}
    for row in record['players']:
        key = identity(row)
        outcome = outcomes.get(key, {})
        status = outcome.get('status', 'missing_diagnostic')
        actual = outcome['actual']['points'] if status == 'verified' else None
        if actual is not None and (not math.isfinite(actual) or actual < 0):
            raise ValueError('invalid verified points')
        if status == 'verified' and actual is None:
            raise ValueError('verified result missing points')
        players.append({**row, 'status': status, 'actual': actual})
        actual_by_id[key] = actual
    cards = {}
    for name, entries in record['cards'].items():
        totals = []
        for entry in entries:
            points = [actual_by_id[tuple(key)] for key in entry['players']]
            totals.append(sum(points) if all(value is not None for value in points) else None)
        complete = final and len(totals) == protocol()['entries'] and None not in totals
        cards[name] = {'entry_totals': totals, 'best': max(totals) if complete else None,
                       'combined': sum(totals) if complete else None}
    return signed({'version': 'nba-percentile-grade-v1', 'record_sha256': record['sha256'],
                   'model_sha256': record['model_sha256'], 'protocol_sha256': PROTOCOL_SHA,
                   'game_date': record['game_date'], 'forecast_sha256': record['forecast_sha256'],
                   'source_snapshot_sha256': source['sha256'], 'graded_at': graded_at.isoformat(),
                   'diagnostics_sha256': digest(diagnostics), 'scoreboard_sha256': digest(scoreboard),
                   'full_slate_final': final, 'status_counts': dict(Counter(r['status'] for r in players)),
                   'players': players, 'cards': cards})


def score(rows, value, level):
    errors = [r['actual'] - value(r) for r in rows]
    over = sum(e > 0 for e in errors)
    ties = sum(e == 0 for e in errors)
    return {'n': len(errors), 'strictly_above': over, 'ties': ties,
            'strict_exceedance_rate': over / len(errors) if errors else None,
            'at_or_above_rate': (over + ties) / len(errors) if errors else None,
            'loss': mean(max(level * e, (level - 1) * e) for e in errors) if errors else None,
            'mae': mean(map(abs, errors)) if errors else None,
            'bias_actual_minus_forecast': mean(errors) if errors else None}


def cohort(rows, rules):
    output = {}
    observed = [r for r in rows if r['actual'] is not None]
    for level in rules['percentile_levels']:
        key = str(level)
        references = {}
        for field in rules['reference_point_fields']:
            same = [r for r in observed if r[field] is not None]
            references[field] = {'point_reference': score(same, lambda r: r[field], level / 100),
                                 'quantile_on_same_rows': score(same, lambda r: r['quantiles'][key], level / 100)}
        output[key] = {'experimental': score(observed, lambda r: r['quantiles'][key], level / 100),
                       'references': references, 'nominal_exceedance': (100 - level) / 100}
    return output


def report(root):
    rules = protocol()
    enrolled = {}
    for path in sorted((root / INDEX).glob('*-' + PROTOCOL_SHA + '.json')):
        idx = read(path)
        record_path = Path(idx['record_path'])
        record = verify_record(read(record_path), read(record_path.parent / 'forecast.json'),
                               read(record_path.parent / 'source-snapshot.json'))
        if (path.name != record['game_date'] + '-' + PROTOCOL_SHA + '.json'
                or idx['record_sha256'] != record['sha256'] or idx['forecast_sha256'] != record['forecast_sha256']
                or not aware(record['completed_at']) <= aware(idx['indexed_at']) < aware(record['lock_at'])):
            raise ValueError('invalid percentile report index')
        enrolled[record['sha256']] = record
    latest = {}
    for path in sorted((root / 'operations').glob('*/*/' + GRADE)):
        result = checked(read(path))
        key = result['record_sha256']
        if key not in enrolled:
            continue
        record = enrolled[key]
        if (result['version'] != 'nba-percentile-grade-v1' or
                any(result[k] != record[k] for k in ('forecast_sha256', 'source_snapshot_sha256',
                                                      'protocol_sha256', 'model_sha256', 'game_date'))):
            raise ValueError('invalid percentile grade identity')
        old = latest.get(key)
        if old is None or aware(result['graded_at']) > aware(old['graded_at']):
            latest[key] = result

    grades = sorted(latest.values(), key=lambda r: r['game_date'])
    players = [p for g in grades for p in g['players']]
    paired = []
    for g in grades:
        baseline, challenger = (g['cards'].get(k, {}) for k in ('mean_baseline', 'joint_p75'))
        if baseline.get('best') is not None and challenger.get('best') is not None:
            paired.append({'date': g['game_date'], 'baseline_best': baseline['best'],
                           'challenger_best': challenger['best'], 'baseline_combined': baseline['combined'],
                           'challenger_combined': challenger['combined']})
    phase = rules['paired_checkpoint']
    # The first fixed-size cohort and subsequent confirmation never pool.
    def compare(values, key):
        differences = [v['challenger_' + key] - v['baseline_' + key] for v in values]
        return {'n': len(differences), 'delta': mean(differences) if differences else None,
                'helped': sum(v > 0 for v in differences), 'hurt': sum(v < 0 for v in differences),
                'tied': sum(v == 0 for v in differences)}
    return signed({'version': 'nba-percentile-report-v1', 'protocol_sha256': PROTOCOL_SHA,
                   'model_sha256': rules['model_sha256'], 'primary_player_targets': rules['primary_player_targets'],
                   'recordings': len(enrolled), 'graded_dates': len(grades), 'paired_dates': len(paired),
                   'status_counts': dict(Counter(p['status'] for p in players)),
                   'cohorts': {'all': cohort(players, rules),
                               'market_line_below_20': cohort([p for p in players if p['market_line'] is not None and p['market_line'] < rules['market_line_split']], rules),
                               'market_line_at_least_20': cohort([p for p in players if p['market_line'] is not None and p['market_line'] >= rules['market_line_split']], rules)},
                   'research': {key: compare(paired[:phase], key) for key in ('best', 'combined')},
                   'confirmation': {key: compare(paired[phase:phase + rules['confirmation_dates']], key) for key in ('best', 'combined')},
                   'review_checkpoint': phase, 'confirmation_dates': rules['confirmation_dates'],
                   'paired': paired, 'pending_dates': sorted(set(r['game_date'] for r in enrolled.values())
                                                             - {r['date'] for r in paired}),
                   'decision': 'research only; no automatic promotion'})
