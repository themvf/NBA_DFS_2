import pytest

from research.nfl_game_leaders_volume_prior import (
    RESERVE, blended_rate, project, roles_rate, shares, team_budget)


def test_roles_rate_matches_roles_v1_formula():
    # 3 of 4 games played, 21 targets: on=7, overall=5.25 -> (3*7 + 1*5.25)/4
    assert roles_rate(3, 21., 4) == pytest.approx((21 + 5.25) / 4)
    assert roles_rate(0, 0., 4) == 0.


def test_blend_uses_prior_as_pseudo_games_and_fades():
    early = blended_rate(4., 1, 10., 4, .7)
    late = blended_rate(4., 6, 10., 4, .7)
    assert early == pytest.approx((4 + 40) / 5)
    assert late == pytest.approx((24 + 40) / 10)
    assert abs(late - 4.) < abs(early - 4.)


def test_no_prior_keeps_current_rate_and_returner_gets_rho():
    assert blended_rate(5., 3, None, 4, .7) == 5.
    assert blended_rate(0., 0, 8., 4, .7) == pytest.approx(5.6)


def test_shares_keep_the_newcomer_reserve():
    s = shares({'a': 3., 'b': 1.}, RESERVE['targets'])
    assert sum(s.values()) == pytest.approx(1 - RESERVE['targets'])
    assert s['a'] == pytest.approx(3 * s['b'])


def test_team_budget_shrinks_toward_league():
    assert team_budget([40., 40., 40.], 30.) < 40.
    assert team_budget([30., 30., 30.], 30.) == pytest.approx(30.)


def test_control_ignores_prior_and_v1_moves_toward_it():
    row = {'game': 'g', 'week': 3, 'action': 'targets', 'budget': 30., 'players': [
        {'ident': 'star', 'actual': 10, 'n': 3, 'r_cur': 4., 'r_prev': 11.},
        {'ident': 'other', 'actual': 4, 'n': 3, 'r_cur': 6., 'r_prev': 4.}]}
    c0 = {r[0:3] + (r[3],) for r in project([row], 'targets')}
    v1 = project([row], 'targets', 4, .7)
    star_c0 = next(r[3] for r in project([row], 'targets') if r[2] == 10)
    star_v1 = next(r[3] for r in v1 if r[2] == 10)
    assert star_v1 > star_c0
    assert len(c0) == 2
