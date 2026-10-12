import pytest

from model.nfl_dfs_historical import MODEL_CONFIG, HistoricalWeek, ProjectionContext, project_player
from model.nfl_qb_starter import (CONFIG, project_expected_starter, rescale_to_team_volume, spot_start_prior_rows,
                                  team_pass_attempts, thin_starting_history)


def line(pid, season, week, team, attempts, pass_yds, pass_tds=1, rush=10, position='QB'):
    return HistoricalWeek(player_id=pid, player_gsis_id=f'g{pid}', player_name=f'P{pid}', position=position, season=season, week=week,
                          team=team, opponent='OPP', stats={'attempts': attempts, 'passing_yards': pass_yds, 'passing_tds': pass_tds,
                                                            'passing_interceptions': 0, 'carries': 3, 'rushing_yards': rush,
                                                            'fumbles_lost_total': 0})


def history():
    rows = []
    for w in range(1, 11):
        rows.append(line(1, 2025, w, 'AAA', 36, 260))            # established starter, high-volume team
        rows.append(line(2, 2025, w, 'AAA', 2 if w < 9 else 0, 12 if w < 9 else 0))  # backup: mop-up only
        rows.append(line(3, 2025, w, 'BBB', 26, 190))            # another team
        rows.append(line(4, 2025, w, 'CCC', 30, 220))
    return rows


BASE = {**MODEL_CONFIG, 'draws': 300}


def test_team_volume_is_recency_weighted_and_shrunk_toward_the_league():
    rows = history()
    aaa, bbb = team_pass_attempts(rows, 'AAA'), team_pass_attempts(rows, 'BBB')
    # Team totals per game: AAA 38 (weeks 1-8) and 36 (weeks 9-10, backup idle), BBB 26, CCC 30.
    league = (38 * 8 + 36 * 2 + 26 * 10 + 30 * 10) / 30
    assert bbb < league < aaa
    assert 26 < bbb < 32 and 32 < aaa < 38
    assert team_pass_attempts(rows, 'ZZZ') == pytest.approx(league)   # unknown team: league mean
    assert team_pass_attempts([], 'AAA') is None


def test_spot_start_prior_pools_each_quarterbacks_first_starts():
    pool = spot_start_prior_rows(history(), {**CONFIG, 'spot_start_prior_games': 3})
    by_player = {}
    for r in pool:
        by_player.setdefault(r.player_id, []).append(r.week)
    assert by_player == {1: [1, 2, 3], 3: [1, 2, 3], 4: [1, 2, 3]}   # the mop-up backup never qualifies


def test_rescale_moves_passing_with_attempts_and_leaves_rushing_alone():
    stats = {'attempts': 20, 'passing_yards': 150, 'passing_tds': 1, 'passing_interceptions': 1, 'carries': 4, 'rushing_yards': 30}
    out = rescale_to_team_volume(stats, 30)
    assert out['attempts'] == 30 and out['passing_yards'] == 225 and out['passing_tds'] == 1.5
    assert out['rushing_yards'] == 30 and out['carries'] == 4
    assert rescale_to_team_volume(stats, 100)['attempts'] == 40   # clipped at 2x


def test_thin_history_is_the_studys_spot_start_definition():
    rows = history()
    assert thin_starting_history([r for r in rows if r.player_id == 2])
    assert not thin_starting_history([r for r in rows if r.player_id == 1])
    assert thin_starting_history([line(9, 2025, 1, 'AAA', 40, 300)])   # one game is too few to call established


def test_named_backup_is_projected_at_team_volume_and_the_base_model_is_untouched():
    rows = history()
    volume = team_pass_attempts(rows, 'AAA')
    common = dict(player_id=2, player_gsis_id='g2', player_name='P2', historical_rows=rows, cutoff_season=2025, cutoff_week=11, seed=1)
    base = project_player(position='QB', context=ProjectionContext(), config=BASE, **common)
    named = project_expected_starter(context=ProjectionContext(), team_pass_attempts=volume, config=BASE, **common)
    assert base.stat_means['attempts'] < volume / 2
    assert named.stat_means['attempts'] == pytest.approx(volume, rel=.02)
    assert named.stat_means['passing_yards'] > base.stat_means['passing_yards']
    assert named.projection_status == 'position_prior'     # no starter-quality line of his own
    assert named.feature_snapshot['qb_starter_construction']['own_starter_lines'] == 0
    assert named.model_version == base.model_version


def test_an_established_starter_is_not_touched():
    rows = history()
    assert project_expected_starter(player_id=1, player_gsis_id='g1', player_name='P1', historical_rows=rows, cutoff_season=2025,
                                    cutoff_week=11, context=ProjectionContext(), team_pass_attempts=team_pass_attempts(rows, 'AAA'),
                                    seed=1, config=BASE) is None


def test_a_returning_starter_with_a_thin_recent_window_uses_his_own_starter_lines():
    # Ten starts, then six quiet weeks (hurt): trailing attempts below the limit, so the
    # construction applies, drawing mostly from his own starter-quality lines.
    rows = history() + [line(1, 2025, w, 'AAA', 0, 0) for w in range(11, 17)]
    volume = team_pass_attempts(rows, 'AAA')
    named = project_expected_starter(player_id=1, player_gsis_id='g1', player_name='P1', historical_rows=rows, cutoff_season=2025,
                                     cutoff_week=17, context=ProjectionContext(), team_pass_attempts=volume, seed=1, config=BASE)
    assert named.projection_status == 'historical'
    assert named.feature_snapshot['qb_starter_construction']['own_starter_lines'] == 10
    assert named.stat_means['attempts'] == pytest.approx(volume, rel=.02)


def test_expected_starter_naming_prefers_a_fresh_depth_chart_then_the_last_game_leader():
    from model.nfl_dfs_availability import expected_qb_starters
    qb = lambda pid, team, depth=None, led=False: {'player_id': pid, 'player_name': f'P{pid}', 'position': 'QB', 'team': team,
                                                   'depth_order': depth, 'led_last_team_game': led}
    players = [qb(1, 'AAA', 1, led=True), qb(2, 'AAA', 2), qb(3, 'BBB', None, led=True), qb(4, 'BBB', None),
               qb(5, 'CCC', 1), qb(6, 'CCC', 2, led=True), {'player_id': 7, 'position': 'RB', 'team': 'AAA'}]
    named = expected_qb_starters(players, out_ids=set(), passing_leaders={})
    assert named[1]['reason'] == 'depth_chart' and 2 not in named
    assert named[3]['reason'] == 'last_game_starter' and 4 not in named
    assert named[5]['reason'] == 'depth_chart'            # the chart's QB1 even though the other QB led last game
    named = expected_qb_starters(players, out_ids={1, 3}, passing_leaders={})
    assert named[2]['depth_order'] == 2 and 1 not in named and 3 not in named and 4 not in named
