import pytest

from model.nfl_dfs_historical import (MODEL_CONFIG, HistoricalWeek, ProjectionContext, project_player,
                                      rescale_to_team_volume, spot_start_prior_rows, team_pass_attempts)


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


def test_team_volume_is_recency_weighted_and_shrunk_toward_the_league():
    rows = history()
    aaa = team_pass_attempts(rows, 'AAA')
    bbb = team_pass_attempts(rows, 'BBB')
    # Team totals per game: AAA 38 (weeks 1-8) and 36 (weeks 9-10, backup idle), BBB 26, CCC 30.
    league = (38 * 8 + 36 * 2 + 26 * 10 + 30 * 10) / 30
    assert bbb < league < aaa
    assert 26 < bbb < 32 and 32 < aaa < 38
    assert team_pass_attempts(rows, 'ZZZ') == pytest.approx(league)   # unknown team: league mean
    assert team_pass_attempts([], 'AAA') is None


def test_spot_start_prior_pools_each_quarterbacks_first_starts():
    rows = history()
    pool = spot_start_prior_rows(rows, {**MODEL_CONFIG, 'qb_spot_start_prior_games': 3})
    by_player = {}
    for r in pool:
        by_player.setdefault(r.player_id, []).append(r.week)
    assert by_player == {1: [1, 2, 3], 3: [1, 2, 3], 4: [1, 2, 3]}   # the mop-up backup never qualifies


def test_rescale_moves_passing_with_attempts_and_leaves_rushing_alone():
    stats = {'attempts': 20, 'passing_yards': 150, 'passing_tds': 1, 'passing_interceptions': 1, 'carries': 4, 'rushing_yards': 30}
    out = rescale_to_team_volume(stats, 30, MODEL_CONFIG)
    assert out['attempts'] == 30 and out['passing_yards'] == 225 and out['passing_tds'] == 1.5
    assert out['rushing_yards'] == 30 and out['carries'] == 4
    capped = rescale_to_team_volume(stats, 100, MODEL_CONFIG)
    assert capped['attempts'] == 40   # clipped at 2x


def test_expected_starter_backup_is_projected_at_team_volume_only_when_named():
    rows = history()
    volume = team_pass_attempts(rows, 'AAA')
    common = dict(player_id=2, player_gsis_id='g2', player_name='P2', position='QB', historical_rows=rows,
                  cutoff_season=2025, cutoff_week=11, seed=1, config={**MODEL_CONFIG, 'qb_starter_mode': 'on', 'draws': 300})
    as_backup = project_player(context=ProjectionContext(), **common)
    named = project_player(context=ProjectionContext(expected_qb_starter=True, team_pass_attempts=volume), **common)
    # Unnamed, he keeps the v5 projection: his own mop-up lines blended with similar low scorers.
    assert as_backup.stat_means['attempts'] < volume / 2
    assert named.stat_means['attempts'] == pytest.approx(volume, rel=.02)
    assert named.stat_means['passing_yards'] > 150
    assert named.projection_status == 'position_prior'     # no starter-quality line of his own
    assert named.feature_snapshot['qb_starter_construction']['own_starter_lines'] == 0
    # The switch off reproduces v5 exactly, named or not.
    off = project_player(context=ProjectionContext(expected_qb_starter=True, team_pass_attempts=volume),
                         **{**common, 'config': {**MODEL_CONFIG, 'draws': 300}})
    assert off.stat_means == as_backup.stat_means


def test_established_starter_keeps_his_own_lines_under_the_construction():
    rows = history()
    volume = team_pass_attempts(rows, 'AAA')
    common = dict(player_id=1, player_gsis_id='g1', player_name='P1', position='QB', historical_rows=rows,
                  cutoff_season=2025, cutoff_week=11, seed=1, config={**MODEL_CONFIG, 'qb_starter_mode': 'on', 'draws': 300})
    named = project_player(context=ProjectionContext(expected_qb_starter=True, team_pass_attempts=volume), **common)
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
    # A ruled-out starter is skipped: the chart's next man is named; without a chart, nobody is.
    named = expected_qb_starters(players, out_ids={1, 3}, passing_leaders={})
    assert named[2]['depth_order'] == 2 and 1 not in named and 3 not in named and 4 not in named
