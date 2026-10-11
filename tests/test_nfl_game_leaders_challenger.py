from copy import deepcopy

import numpy as np

from research.nfl_game_leaders_challenger import assess, examples, fit, verify_boxes


def histories():
    rows = []
    for week in range(1, 5):
        g = {'game_id': f'2025_{week:02}_A_B', 'season': 2025, 'week': week,
             'kickoff': f'2025-09-{week * 7:02}T17:00:00Z', 'away': 'A', 'home': 'B', 'completed': True}
        boxes = [{'identity': t + str(i), 'name': t + str(i), 'team': t, 'position': 'WR',
                  'carries': 0, 'targets': 5, 'rushing_yards': 0, 'receptions': 3 + i,
                  'receiving_yards': 20 + i * 10, 'game_id': g['game_id']} for t in ('A', 'B') for i in range(2)]
        rows.append({'game': g, 'boxes': boxes})
    return rows


def test_target_results_never_change_features_or_candidates():
    h = histories()
    original = examples(h, 'receiving_yards')[-1]
    changed = deepcopy(h)
    changed[-1]['boxes'][0]['receiving_yards'] = 1000
    altered = examples(changed, 'receiving_yards')[-1]
    assert original['rows'] == altered['rows']
    assert original['y'] != altered['y']
    # Still excludes data that kicks off at the same instant as the target.
    concurrent = deepcopy(h[2]); concurrent['game']['game_id'] = 'concurrent'
    concurrent['game']['kickoff'] = h[3]['game']['kickoff']
    concurrent['boxes'][0]['receiving_yards'] = 2000
    assert examples(h[:3] + [concurrent, h[3]], 'receiving_yards')[-1]['rows'] == original['rows']


def test_unknown_winner_is_category_not_summed_yard_competitor():
    h = histories()
    h[-1]['boxes'].append({**h[-1]['boxes'][0], 'identity': 'new', 'name': 'new', 'receiving_yards': 90})
    d = examples(h, 'receiving_yards')[-1]
    assert d['y'][next(i for i, r in enumerate(d['rows']) if r['identity'] == 'OTHER:A')] == 1
    assert 'new' not in [r['identity'] for r in d['rows']]
    assert sum(d['y']) == 1


def test_unobserved_transfer_does_not_credit_opposing_old_role():
    h = histories()
    h[-1]['boxes'][0].update(identity='B0', name='B0', receiving_yards=90)
    h[-1]['boxes'] = [b for b in h[-1]['boxes'] if not (b['team'] == 'B' and b['identity'] == 'B0')]
    d = examples(h, 'receiving_yards')[-1]
    assert d['y'][next(i for i, r in enumerate(d['rows']) if r['identity'] == 'B0')] == 0
    assert d['y'][next(i for i, r in enumerate(d['rows']) if r['identity'] == 'OTHER:A')] == 1


def test_team_total_mismatch_excludes_game():
    h = histories()
    snapshot = {'games': [r['game'] for r in h], 'boxes': [b for r in h for b in r['boxes']]}
    source = []
    for r in h:
        for t, o in (('A', 'B'), ('B', 'A')):
            source.append({**r['game'], 'team': t, 'opponent_team': o, 'season_type': 'REG',
                           **{f: sum(b[f] for b in r['boxes'] if b['team'] == t)
                              for f in ('carries', 'targets', 'rushing_yards', 'receptions', 'receiving_yards')}})
    valid, rejected = verify_boxes(snapshot, [{'rows': source}])
    assert len(valid) == 4 and not rejected
    source[0]['receiving_yards'] += 1
    valid, rejected = verify_boxes(snapshot, [{'rows': source}])
    assert len(valid) == 3 and len(rejected) == 1


def test_rank_fit_learns_winner_and_preserves_tie_credit():
    data = [{'game': {'game_id': str(i)}, 'rows': [
        {'identity': 'low', 'name': 'low', 'features': [0.], 'baselines': {6: 0}},
        {'identity': 'high', 'name': 'high', 'features': [1.], 'baselines': {6: 1}}], 'y': [0., 1.]} for i in range(30)]
    fitted = fit(data, 1.)
    scores = assess(data, fitted)
    assert all(r['pick'] == 'high' and r['credit'] == 1 for r in scores)
    assert np.mean([r['log_loss'] for r in scores]) < .2
    data[0]['y'] = [.5, .5]
    assert assess(data[:1], fitted)[0]['credit'] == .5
    data[0]['rows'][1]['identity'] = 'OTHER:A'
    assert assess(data[:1], fitted)[0]['credit'] == 0
