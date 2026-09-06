import pytest
from ingest.nfl_dfs_volume_benchmark import pair, measure


def fixture():
    candidate = {'season': 2024, 'week': 2, 'identity': 'gsis', 'game_id': 'game', 'actual': 12., 'targets_baseline': 8., 'candidate': {'mean': 10., 'p10': 5., 'p90': 15.}}
    baseline = {'season': 2024, 'week': 2, 'sample_key': '2024:2:gsis', 'game_id': 'game', 'actual': 12., 'model': 'baseline', 'position': 'WR', 'prediction': 9., 'p10': 6., 'p90': 14., 'history_cutoff': [2024, 1]}
    return candidate, baseline


def test_exact_pair_and_prior_tier():
    c, b = fixture()
    rows, audit = pair([c], [b])
    assert rows[0]['tier'] == '7+ targets'
    assert audit == {'candidate_unmatched': 0, 'production_unmatched': 0}
    assert measure(rows)['candidate']['mae'] == 2
    assert pair([c], [{**b, 'game_id': 'different'}])[1]['candidate_unmatched'] == 1


def test_reject_ambiguous_scoring_and_leakage():
    c, b = fixture()
    for candidates, saved in [([c, c], [b]), ([c], [b, b]), ([c], [{**b, 'actual': 13}]), ([c], [{**b, 'history_cutoff': [2024, 2]}])]:
        with pytest.raises(ValueError):
            pair(candidates, saved)
