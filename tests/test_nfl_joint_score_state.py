import zlib

import numpy as np
import pytest

from model.nfl_joint_score_state import (BANDS, FLIP, block_weights, conditioned_volumes_state, draw_profiles,
                                         fit_market_weighted, fit_state_profile, profile_rates, team_game_profiles)


def plays(game_id, kickoff, spread, total, margins, pass_share=.6, plays_per_game=120):
    """Synthetic game: `margins` is a list of (seconds_remaining_at_or_below, home_margin), ascending; default tied."""
    rows = []
    rng = np.random.default_rng(zlib.crc32(game_id.encode()))
    stamps = sorted(rng.integers(0, 3600, size=plays_per_game), reverse=True)
    for j, gsr in enumerate(stamps):
        margin = next((m for s, m in sorted(margins) if gsr <= s), 0)
        posteam = 'HOME' if j % 2 == 0 else 'AWAY'
        is_pass = rng.random() < pass_share
        rows.append({'game_id': game_id, 'kickoff': kickoff, 'home_team': 'HOME', 'away_team': 'AWAY', 'posteam': posteam,
                     'score_differential': margin if posteam == 'HOME' else -margin, 'game_seconds_remaining': int(gsr),
                     'play_type': 'pass' if is_pass else 'run', 'had_sack': False, 'scramble': False,
                     'spread_line': spread, 'total_line': total})
    return rows


def history(n=80, season_kick='2024-09-%02dT17:00:00Z'):
    rows = []
    rng = np.random.default_rng(7)
    for j in range(n):
        spread = float(rng.normal(0, 6))
        # Favourites spend more time leading; that is the structure Part A should recover.
        lead = 20 if spread > 3 else -20 if spread < -3 else 0
        kick = season_kick % (1 + j % 28)
        rows += plays(f'g{j}', kick, spread, 45 + rng.normal(0, 3), [(2400, lead)],
                      pass_share=.7 if lead < 0 else .5 if lead > 0 else .6)
    return rows


def test_profiles_sum_to_regulation_and_mirror_for_away():
    games = team_game_profiles(plays('x', '2024-09-01T17:00:00Z', -3, 44, [(1800, 10)]))
    assert len(games) == 1
    profile = games[0]['home_profile']
    assert abs(sum(profile) - 1) < 1e-9
    assert profile[BANDS.index('tied')] == pytest.approx(.5, abs=.05)
    assert profile[BANDS.index('lead8')] == pytest.approx(.5, abs=.05)
    mirrored = [profile[i] for i in FLIP]
    assert mirrored[BANDS.index('trail8')] == profile[BANDS.index('lead8')]
    assert games[0]['home_targets'] + games[0]['home_carries'] + games[0]['away_targets'] + games[0]['away_carries'] == 120


def test_games_missing_lines_are_dropped_not_invented():
    rows = plays('x', '2024-09-01T17:00:00Z', -3, 44, [])
    for r in rows:
        r['spread_line'] = None
    assert team_game_profiles(rows) == []


def test_fit_rejects_boundary_and_duplicates():
    games = team_game_profiles(history())
    with pytest.raises(ValueError, match='boundary'):
        fit_state_profile(games, '2024-09-10T00:00:00Z')
    with pytest.raises(ValueError, match='Duplicate'):
        fit_state_profile(games + games[:1], '2025-01-01T00:00:00Z')
    assert fit_state_profile(games, '2025-01-01T00:00:00Z')['rows'] == len(games)


def test_part_b_recovers_that_trailing_teams_pass_more():
    fitted = fit_state_profile(team_game_profiles(history()), '2025-01-01T00:00:00Z', k=20)
    trailing = np.zeros((1, 7)); trailing[0, BANDS.index('trail15')] = 1
    leading = np.zeros((1, 7)); leading[0, BANDS.index('lead15')] = 1
    assert profile_rates(fitted, trailing)['home']['targets'][0] > profile_rates(fitted, leading)['home']['targets'][0]
    assert profile_rates(fitted, trailing)['home']['carries'][0] < profile_rates(fitted, leading)['home']['carries'][0]
    # The away team reads the same profile mirrored, so a home lead is an away trail.
    assert profile_rates(fitted, leading)['away']['targets'][0] == pytest.approx(profile_rates(fitted, trailing)['home']['targets'][0])


def test_part_a_neighbourhood_follows_the_line_and_is_pregame_only():
    fitted = fit_state_profile(team_game_profiles(history()), '2025-01-01T00:00:00Z', k=10)
    evidence = {'source_ref': 'market', 'captured_at': '2025-09-07T12:00:00Z'}
    rng = np.random.default_rng(1)
    favoured, _ = draw_profiles(rng, fitted, {'home_spread': 9, 'total': 45, 'evidence': evidence}, '2025-09-07T13:00:00Z', 500)
    dogs, _ = draw_profiles(rng, fitted, {'home_spread': -9, 'total': 45, 'evidence': evidence}, '2025-09-07T13:00:00Z', 500)
    assert favoured[:, BANDS.index('lead15')].mean() > dogs[:, BANDS.index('lead15')].mean()
    with pytest.raises(ValueError, match='after decision'):
        draw_profiles(rng, fitted, {'home_spread': 0, 'total': 45, 'evidence': evidence}, '2024-06-01T00:00:00Z', 5)
    late = {'home_spread': 0, 'total': 45, 'evidence': {'source_ref': 'market', 'captured_at': '2025-09-07T14:00:00Z'}}
    with pytest.raises(ValueError, match='boundary'):
        draw_profiles(rng, fitted, late, '2025-09-07T13:00:00Z', 5)


def bank(draws=200):
    rng = np.random.default_rng(3)
    players = []
    for team in ('AWAY', 'HOME'):
        for j in range(3):
            players.append({'identity': f'{team}{j}', 'name': f'{team}{j}', 'team': team, 'residual': False,
                            'draws': {'targets': rng.poisson(10, draws).tolist(), 'carries': rng.poisson(8, draws).tolist(),
                                      'rushYds': [0] * draws, 'recYds': [0] * draws, 'receptions': [0] * draws}})
    return {'scenario_ids': [f's{i}' for i in range(draws)], 'decision_at': '2025-09-07T13:00:00Z',
            'game': {'game_id': 'g', 'home': 'HOME', 'away': 'AWAY', 'kickoff': '2025-09-07T17:00:00Z'}, 'players': players}


def test_conditioned_volumes_are_coherent_and_deterministic():
    fitted = fit_state_profile(team_game_profiles(history()), '2025-01-01T00:00:00Z', k=10)
    market = {'home_spread': 9, 'total': 45, 'evidence': {'source_ref': 'market', 'captured_at': '2025-09-07T12:00:00Z'}}
    first, diag = conditioned_volumes_state(bank(), fitted, market, seed=5)
    second, _ = conditioned_volumes_state(bank(), fitted, market, seed=5)
    assert set(first) == {('AWAY', 'targets'), ('AWAY', 'carries'), ('HOME', 'targets'), ('HOME', 'carries')}
    for key in first:
        assert np.array_equal(first[key], second[key])
        assert (first[key] >= 0).all()
    # A heavy home favourite leads more, so it runs more and the away side throws more.
    assert diag['home_carries_mean_ratio'] > diag['away_carries_mean_ratio']
    assert diag['away_targets_mean_ratio'] > diag['home_targets_mean_ratio']
    with pytest.raises(ValueError, match='state profile fit'):
        conditioned_volumes_state(bank(), {'method': 'ridge_log_volume'}, market, seed=5)


def test_market_weighted_blocks_weight_only_fitted_neighbours():
    games = team_game_profiles(history())
    fitted = fit_market_weighted(games, '2025-01-01T00:00:00Z', k=5)
    hist = [{'game': {'game_id': g['game_id'], 'kickoff': g['kickoff']}} for g in games[:40]]
    market = {'home_spread': 0, 'total': 45, 'evidence': {'source_ref': 'market', 'captured_at': '2025-09-07T12:00:00Z'}}
    weights, report = block_weights(fitted, hist, market, '2025-09-07T13:00:00Z')
    assert weights.sum() == pytest.approx(1)
    assert np.count_nonzero(weights) == report['in_history'] == 5 - len(report['missing_from_history'])
    with pytest.raises(ValueError, match='market-weighted'):
        block_weights(fit_state_profile(games, '2025-01-01T00:00:00Z'), hist, market, '2025-09-07T13:00:00Z')
