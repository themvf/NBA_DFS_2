"""The two spread columns use opposite signs; mixing them swaps favourites.

Real 2026 week-1 game: Detroit (home) -325 over New Orleans, total 49.5.
`nfl_matchups.home_spread` stores -7.0; `nfl_season_games.quoted_spread_line`
stores +7.0. Both must give Detroit the bigger share.
"""

from ingest.nfl_dfs_projections import implied_from_lines


def test_book_spread_home_favourite():
    home, away = implied_from_lines(49.5, -7.0, None)
    assert (home, away) == (28.25, 21.25)


def test_nflverse_spread_home_favourite_is_not_swapped():
    # The bug: this value used to go through the book-style formula and made
    # Detroit the 21.25 team.
    home, away = implied_from_lines(49.5, None, 7.0)
    assert (home, away) == (28.25, 21.25)


def test_both_conventions_agree_on_the_same_game():
    assert implied_from_lines(49.5, -7.0, None) == implied_from_lines(49.5, None, 7.0)
    assert implied_from_lines(47.0, 3.0, None) == implied_from_lines(47.0, None, -3.0)


def test_book_value_wins_when_both_exist():
    # Odds API is the fresher capture; the nflverse value is only a fallback.
    assert implied_from_lines(49.5, -7.0, 3.0) == (28.25, 21.25)


def test_missing_inputs_return_none():
    assert implied_from_lines(None, -7.0, 7.0) is None
    assert implied_from_lines(49.5, None, None) is None
