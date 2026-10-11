from datetime import datetime, timezone

import numpy as np
import pytest

from research.nfl_game_leaders_market import (check_matches_published, fair_over, leader_shares, match_event,
                                              pick_quote, price_game, shift_to_market)


def test_fair_over_removes_the_margin_and_needs_both_sides():
    assert fair_over({'line': 50.5, 'over': -110, 'under': -110}) == pytest.approx(.5)
    assert fair_over({'line': 50.5, 'over': -150, 'under': 120}) == pytest.approx(.6 / (.6 + 100 / 220))
    assert fair_over({'line': 50.5, 'over': -110, 'under': None}) is None


def test_draftkings_is_preferred_and_other_books_are_a_labelled_fallback():
    books = {'fanduel': {'line': 60.5, 'over': -110, 'under': -110}, 'draftkings': {'line': 62.5, 'over': -115, 'under': -105}}
    assert pick_quote(books)['book'] == 'draftkings'
    assert pick_quote({'fanduel': books['fanduel']})['book'] == 'fanduel'
    assert pick_quote({'draftkings': {'line': 1, 'over': -110, 'under': None}}) is None


def test_shift_matches_the_market_probability_over_the_line():
    draws = np.random.default_rng(1).normal(40, 20, 20000)
    shifted = shift_to_market(draws, 55.5, .6)
    assert (shifted > 55.5).mean() == pytest.approx(.6, abs=.01)


def test_leader_shares_split_ties_equally():
    shares = leader_shares([{}, {}], [np.array([5., 3.]), np.array([5., 1.])])
    assert shares.tolist() == pytest.approx([.75, .25])


def _players():
    rng = np.random.default_rng(2)
    star = {'identity': 's', 'name': "Ja'Marr Chase", 'team': 'CIN', 'residual': False,
            'draws': {'rushYds': [0.] * 4000, 'recYds': rng.normal(50, 25, 4000).tolist(), 'receptions': rng.poisson(4, 4000).tolist()}}
    other = {'identity': 'o', 'name': 'Tee Higgins', 'team': 'CIN', 'residual': False,
             'draws': {'rushYds': [0.] * 4000, 'recYds': rng.normal(55, 25, 4000).tolist(), 'receptions': rng.poisson(4, 4000).tolist()}}
    gone = {'identity': 'g', 'name': 'Former', 'team': 'CIN', 'residual': True,
            'draws': {'rushYds': [0.] * 4000, 'recYds': [0.] * 4000, 'receptions': [0.] * 4000}}
    return [star, other, gone]


def _prop(player, market='player_reception_yds', line=80.5, over=-110, under=-110, event='e1'):
    return {'event_id': event, 'commence_time': None, 'market': market, 'player': player,
            'books': {'draftkings': {'line': line, 'over': over, 'under': under}},
            'captured_at': datetime(2026, 10, 11, 15, tzinfo=timezone.utc)}


def test_a_high_market_line_raises_the_market_implied_leader_share():
    priced = price_game(_players(), [_prop('Ja’Marr Chase'.replace('’', "'"))])['receiving_yards']
    rows = {r['identity']: r for r in priced['rows']}
    assert rows['s']['quote']['line'] == 80.5
    assert rows['s']['market_leader_share'] > rows['s']['model_leader_share']
    assert sum(r['market_leader_share'] for r in priced['rows']) == pytest.approx(1)
    assert rows['OTHER:CIN']['quote'] is None and rows['o']['quote'] is None


def test_event_match_needs_real_overlap():
    players = _players()
    assert match_event(players, [_prop("Ja'Marr Chase")])[0] is None
    many = [_prop(n, event='e2') for n in ("Ja'Marr Chase", 'Tee Higgins', "Ja'Marr Chase", 'Tee Higgins', 'Tee Higgins')]
    assert match_event(players, many + [_prop('Somebody Else', event='e3')])[0] == 'e2'


def test_pricing_refuses_a_forecast_that_differs_from_the_page():
    published = {'metrics': {m: {'players': [{'identity': 'a', 'win_share': .6}, {'identity': 'b', 'win_share': .4}]}
                             for m in ('receptions', 'receiving_yards', 'rushing_yards')}}
    same = {m: {'a': .6, 'b': .4} for m in ('receptions', 'receiving_yards', 'rushing_yards')}
    check_matches_published('g', same, published)
    with pytest.raises(ValueError, match='differs from the published'):
        check_matches_published('g', {**same, 'receptions': {'a': .5, 'b': .5}}, published)
