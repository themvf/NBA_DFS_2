"""Publish a mean-ranked lead and four joint-P75 alternatives from a frozen trial record."""
from research.nba_percentile_trial import checked, identity, signed

GRADE_FILENAME = 'p75-card-grade.json'
VERSION = 'nba-anchored-p75-card-v1'


def project(record):
    """Only the lead and four strongest distinct alternatives are needed by the website."""
    checked(record)
    cards = record['cards']
    if not cards:
        return None
    lead = cards['mean_baseline'][0]
    alternatives = [entry for entry in cards['joint_p75'] if entry['players'] != lead['players']][:4]
    selected = [lead, *alternatives]
    if len(selected) != 5 or len({tuple(map(tuple, entry['players'])) for entry in selected}) != 5:
        raise ValueError('anchored P75 card needs five distinct trios')
    players = {identity(row): row['player'] for row in record['players']}
    entries = [{
        'players': [{'event_id': key[0], 'player_key': key[1], 'name': players[tuple(key)]} for key in entry['players']],
        'mean': entry['mean'], 'p75': entry['p75'],
    } for entry in selected]
    return signed({
        'version': VERSION, 'game_date': record['game_date'], 'frozen_at': record['completed_at'],
        'lock_at': record['lock_at'], 'forecast_sha256': record['forecast_sha256'],
        'record_sha256': record['sha256'], 'entries': entries,
    })


def grade(card, percentile_grade):
    checked(card)
    checked(percentile_grade)
    if any(card[k] != percentile_grade[k] for k in ('game_date', 'record_sha256', 'forecast_sha256')):
        raise ValueError('P75 card grade must match the original')
    outcomes = {identity(row): row['actual'] for row in percentile_grade['players']}
    totals = []
    for entry in card['entries']:
        points = [outcomes.get(identity(player)) for player in entry['players']]
        totals.append(sum(points) if all(value is not None for value in points) else None)
    complete = percentile_grade['full_slate_final'] and None not in totals
    return signed({
        'version': 'nba-anchored-p75-grade-v1', 'game_date': card['game_date'],
        'card_sha256': card['sha256'], 'percentile_grade_sha256': percentile_grade['sha256'],
        'graded_at': percentile_grade['graded_at'], 'totals': totals,
        'best': max(totals) if complete else None, 'combined': sum(totals) if complete else None,
    })
