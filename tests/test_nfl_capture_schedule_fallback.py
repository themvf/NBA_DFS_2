"""Pre-2023 game identity from the nflverse schedule file (research.nfl_longest_touchdown)."""
import pytest

import research.nfl_longest_touchdown as capture_module
from research.nfl_longest_touchdown import capture, schedule_games, schedule_kickoff, schedule_team


def row(**overrides):
    base = {'game_id': '2022_01_BUF_LA', 'season': 2022, 'game_type': 'REG', 'week': 1,
            'gameday': '2022-09-08', 'gametime': '20:20', 'away_team': 'BUF', 'away_score': 31.0,
            'home_team': 'LA', 'home_score': 10.0}
    return {**base, **overrides}


def test_team_codes_normalise_to_the_2023_capture_abbreviations():
    assert schedule_team('LA') == 'LAR'
    assert schedule_team('WAS') == 'WSH'
    for code in ('LV', 'LAC', 'JAX', 'GB', 'NO'):
        assert schedule_team(code) == code


@pytest.mark.parametrize('code', ['OAK', 'SD', 'STL', 'XXX', ''])
def test_relocated_or_unknown_codes_are_refused_not_guessed(code):
    with pytest.raises(ValueError):
        schedule_team(code)


def test_kickoff_is_eastern_converted_to_utc_in_capture_format():
    # September: EDT (UTC-4). December: EST (UTC-5). London 9:30 ET stays ET-based.
    assert schedule_kickoff('2022-09-08', '20:20') == '2022-09-09 00:20:00+00:00'
    assert schedule_kickoff('2022-12-11', '13:00') == '2022-12-11 18:00:00+00:00'
    assert schedule_kickoff('2022-10-02', '09:30') == '2022-10-02 13:30:00+00:00'


@pytest.mark.parametrize('gametime', [None, '', float('nan')])
def test_missing_kickoff_time_raises(gametime):
    with pytest.raises(ValueError):
        schedule_kickoff('2022-09-08', gametime)


def test_schedule_games_filters_regular_season_and_marks_unplayed():
    records = [row(), row(game_id='2022_19_X', game_type='WC'), row(game_id='2021_01_X', season=2021),
               row(game_id='2022_17_BUF_CIN', home_team='CIN', home_score=None, away_score=float('nan'))]
    games = schedule_games(records, {2022}, '2026-10-11 00:00:00+00:00')
    assert [g['game_id'] for g in games] == ['2022_01_BUF_LA', '2022_17_BUF_CIN']
    first = games[0]
    assert first == {'game_id': '2022_01_BUF_LA', 'season': 2022, 'week': 1,
                     'kickoff': '2022-09-09 00:20:00+00:00', 'away': 'BUF', 'home': 'LAR', 'completed': True,
                     'home_score': 10, 'away_score': 31, 'source_captured_at': '2026-10-11 00:00:00+00:00'}
    assert games[1]['completed'] is False and games[1]['home_score'] is None


def test_duplicate_schedule_game_raises():
    with pytest.raises(ValueError):
        schedule_games([row(), row()], {2022}, 'x')


class FakeDb:
    def __init__(self, fallback_plays):
        self.fallback_plays = fallback_plays

    def execute(self, sql, params):
        if 'nfl_season_games' in sql:
            return []  # no canonical schedule rows before 2023
        return self.fallback_plays


def play(game_id='2022_01_BUF_LA', play_id=1, **overrides):
    base = {'game_id': game_id, 'play_id': play_id, 'season': 2022, 'week': 1, 'home_team': 'LA',
            'away_team': 'BUF', 'quarter': 4, 'game_seconds_remaining': 0, 'description': 'END GAME',
            'actors': []}
    return {**base, **overrides}


def test_capture_falls_back_to_schedule_identity(monkeypatch):
    schedule = schedule_games([row()], {2022}, '2026-10-11 00:00:00+00:00')
    monkeypatch.setattr(capture_module, 'fetch_schedule_games',
                        lambda seasons: (schedule, {'seasons': sorted(seasons)}))
    result = capture(FakeDb([play(play_id=2), play(play_id=1)]), 2022, 2022)
    assert [p['play_id'] for p in result['plays']] == [1, 2]
    p = result['plays'][0]
    assert (p['kickoff'], p['canonical_home'], p['canonical_away'], p['game_type']) == (
        '2022-09-09 00:20:00+00:00', 'LAR', 'BUF', 'REG')
    assert list(p)[-1] == 'actors'
    assert result['games'] == schedule
    assert result['game_coverage']['2022_01_BUF_LA'] == {'play_count': 2, 'regulation_end_observed': True}
    assert result['schedule_fallback']['pbp_games'] == 1


def test_capture_raises_when_a_season_has_no_identity_source(monkeypatch):
    monkeypatch.setattr(capture_module, 'fetch_schedule_games', lambda seasons: ([], {'seasons': sorted(seasons)}))
    with pytest.raises(ValueError, match='No game identity'):
        capture(FakeDb([play()]), 2022, 2022)


def test_capture_raises_when_pbp_disagrees_with_schedule(monkeypatch):
    schedule = schedule_games([row()], {2022}, 'x')
    monkeypatch.setattr(capture_module, 'fetch_schedule_games', lambda seasons: (schedule, {}))
    with pytest.raises(ValueError, match='disagrees'):
        capture(FakeDb([play(week=2)]), 2022, 2022)
    with pytest.raises(ValueError, match='absent from the schedule'):
        capture(FakeDb([play(game_id='2022_01_OTHER')]), 2022, 2022)
