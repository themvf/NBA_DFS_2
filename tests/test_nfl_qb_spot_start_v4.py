"""nfl-qb-spot-start-v4 runner logic (docs/nfl-qb-spot-start-study.md, "v4")."""
import json

import pytest

from research.nfl_qb_spot_start_study import (GAP_MIN_TEST_GAMES, R_GRID, STUDY_V4, fit_v4, gap_v4, grade_v4, projection,
                                              projection_v4, v4_eligible)


def record(key, *, v2, current, construction, actual, volume=34.0, cur_att=20.0, con_att=32.0, act_att=30.0,
           act_yds=None, con_yds=None, week=1, season=2014, study=STUDY_V4, trailing=12.0, window=6, starters=2):
    return {'study': study, 'key': key, 'season': season, 'week': week, 'v2_applies': v2, 'team_pass_attempts': volume,
            'window': window, 'starter_games': starters, 'trailing_attempts': trailing,
            'actual': {'points': actual, 'attempts': act_att, 'pass_yds': act_yds if act_yds is not None else act_att * 7},
            'current': {'proj_points': current, 'proj_attempts': cur_att, 'proj_pass_yds': cur_att * 7},
            'construction': None if construction is None else
            {'proj_points': construction, 'proj_attempts': con_att, 'proj_pass_yds': con_yds if con_yds is not None else con_att * 7}}


def test_v4_is_a_superset_of_v2_and_compares_projected_attempts_with_team_volume():
    v2_game = record('a', v2=True, current=10, construction=14, actual=15, cur_att=33)
    gap_game = record('b', v2=False, current=10, construction=14, actual=15, cur_att=20, volume=34)   # 20 < 0.70 x 34
    edge = record('c', v2=False, current=10, construction=14, actual=15, cur_att=27, volume=34)       # 0.794 of volume
    no_construction = record('d', v2=False, current=10, construction=None, actual=15, cur_att=10)
    no_volume = record('e', v2=False, current=10, construction=14, actual=15, cur_att=10, volume=None)
    for r in R_GRID:
        assert v4_eligible(v2_game, r) and projection_v4(v2_game, 'v4', r) == projection(v2_game, 'v2')
        assert v4_eligible(gap_game, r)
        assert not v4_eligible(no_construction, r) and projection_v4(no_construction, 'v4', r) == no_construction['current']
        assert not v4_eligible(no_volume, r)
    assert not v4_eligible(edge, 0.75) and v4_eligible(edge, 0.80)
    assert [x['key'] for x in gap_v4([v2_game, gap_game, edge, no_construction], 0.80)] == ['b', 'c']


def test_fit_chooses_r_on_whole_population_mae_with_ties_to_the_smallest(tmp_path):
    rows = [
        record('g1', v2=False, current=8, construction=14, actual=15, cur_att=20, study='nfl-qb-spot-start-v3', season=2021),
        # a starter at 0.82 of volume the construction over-projects: r=0.85 pulls him in and loses
        record('g2', v2=False, current=15, construction=24, actual=15, cur_att=28, week=2, study='nfl-qb-spot-start-v3', season=2021),
        record('old', v2=False, current=0, construction=40, actual=15, cur_att=10, season=2016),   # outside development, ignored
    ]
    path = tmp_path / 'dev.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in rows))
    fit = fit_v4(path, 'digest')
    assert fit['records'] == 2 and fit['chosen_r'] == 0.70
    assert fit['population_mae']['v4_r0.7'] < fit['population_mae']['v4_r0.85']
    assert fit['gap_cohort_by_r']['r0.85']['n'] == 2 and fit['gap_cohort_by_r']['r0.8']['n'] == 1


def test_grade_checks_every_gate_including_the_yards_bias_guard():
    def games(construction, con_yds):
        return [record(f'b{i}', v2=False, current=8, construction=construction + (i % 3) * 0.1, actual=15,
                       cur_att=20, con_att=31, con_yds=con_yds + (i % 2), act_att=30, act_yds=210,
                       week=i % 17 + 1, season=2012 + i % 4) for i in range(GAP_MIN_TEST_GAMES)]
    passing = grade_v4(games(14, 215), 0.80)
    assert passing['verdict'] == 'pass' and all(passing['gates'].values())
    # same points gain, but a 30-yard overshoot fails the calibration guard (v3's failure mode)
    overshoot = grade_v4(games(14, 240), 0.80)
    assert overshoot['verdict'] == 'fail' and not overshoot['gates']['calibration_attempts_within_5_and_yards_within_15']
    worse = grade_v4(games(2, 215), 0.80)
    assert worse['verdict'] == 'fail' and not worse['gates']['points_ci_upper_below_zero']
    assert grade_v4(games(14, 215)[:10], 0.80)['verdict'] == 'insufficient'
    # records from another study are never graded
    assert grade_v4([{**g, 'study': 'nfl-qb-spot-start-v3'} for g in games(14, 215)], 0.80)['gap']['n'] == 0


@pytest.mark.parametrize('r', R_GRID)
def test_grid_is_the_registered_one(r):
    assert R_GRID == (0.70, 0.75, 0.80, 0.85) and 0.7 <= r <= 0.85
