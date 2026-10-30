"""nfl-qb-spot-start-v3 runner logic (docs/nfl-qb-spot-start-study.md, "v3")."""
import json

from model.nfl_dfs_historical import HistoricalWeek
from research.nfl_qb_spot_start_study import (GAP_MIN_TEST_GAMES, K_GRID, events_v3, fit_v3, gap, grade_v3,
                                              projection, v3_eligible)


def line(pid, season, week, team, attempts):
    return HistoricalWeek(player_id=pid, player_gsis_id=f'g{pid}', player_name=f'P{pid}', position='QB', season=season,
                          week=week, team=team, opponent='OPP', stats={'attempts': attempts, 'passing_yards': attempts * 7})


def record(key, *, v2, window, starters, current, construction, actual, week=1, season=2024, attempts=(30, 30, 30)):
    cur_att, con_att, act_att = attempts
    return {'study': 'nfl-qb-spot-start-v3', 'key': key, 'season': season, 'week': week, 'v2_applies': v2,
            'window': window, 'starter_games': starters,
            'actual': {'points': actual, 'attempts': act_att, 'pass_yds': act_att * 7},
            'current': {'proj_points': current, 'proj_attempts': cur_att, 'proj_pass_yds': cur_att * 7},
            'construction': None if construction is None else
            {'proj_points': construction, 'proj_attempts': con_att, 'proj_pass_yds': con_att * 7}}


def test_starter_games_count_the_previous_six_with_15_plus_attempts_and_nothing_is_sampled():
    # Huntley's real week-6 window: 22, 0, 10, 20, 9, 32 -> mean 15.5 (v2 says established), 3 starter games.
    rows = [line(1, 2026, w, 'BAL', a) for w, a in zip(range(1, 7), (22, 0, 10, 20, 9, 32))]
    rows.append(line(1, 2026, 7, 'BAL', 31))
    rows += [line(2, 2026, w, 'CCC', 35) for w in range(1, 8)]
    events = {e['target'].player_id: e for e in events_v3(rows, {2026}) if e['target'].week == 7}
    assert events[1]['starter_games'] == 3 and events[1]['window'] == 6
    assert abs(events[1]['trailing_attempts'] - 15.5) < 1e-9
    assert events[2]['starter_games'] == 6
    # every qualifying team-game is kept (no 1-in-4 established sampling)
    assert len([e for e in events_v3(rows, {2026}) if e['target'].player_id == 2]) == 7


def test_v3_is_a_superset_of_v2_and_needs_an_available_construction():
    v2_game = record('a', v2=True, window=6, starters=6, current=10, construction=14, actual=15)
    gap_game = record('b', v2=False, window=6, starters=3, current=10, construction=14, actual=15)
    established = record('c', v2=False, window=6, starters=5, current=18, construction=17, actual=15)
    no_construction = record('d', v2=False, window=6, starters=2, current=10, construction=None, actual=15)
    for k in K_GRID:
        assert v3_eligible(v2_game, k)
        assert projection(v2_game, 'v3', k) == projection(v2_game, 'v2')      # v3 never changes a v2 game
        assert not v3_eligible(no_construction, k) and projection(no_construction, 'v3', k) == no_construction['current']
    assert v3_eligible(gap_game, 4) and not v3_eligible(gap_game, 3)
    assert not v3_eligible(established, 5)                                   # 5 of 6 is outside every K in the grid
    assert [r['key'] for r in gap([v2_game, gap_game, established, no_construction], 4)] == ['b']


def test_fit_chooses_k_on_whole_population_mae_with_ties_to_the_smallest(tmp_path):
    rows = [
        # helps at K>=4: construction is much closer than the current model
        record('g3', v2=False, window=6, starters=3, current=8, construction=14, actual=15),
        # hurts at K=5: a 4-of-6 starter the construction over-projects
        record('g4', v2=False, window=6, starters=4, current=15, construction=24, actual=15, week=2),
        record('e6', v2=False, window=6, starters=6, current=20, construction=19, actual=20, week=3),
    ]
    path = tmp_path / 'dev.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in rows))
    fit = fit_v3(path)
    assert fit['chosen_k'] == 4
    assert fit['population_mae']['v3_k4'] < fit['population_mae']['v3_k5']
    assert fit['gap_cohort_by_k']['k3']['n'] == 0 and fit['gap_cohort_by_k']['k4']['n'] == 1

    tie = tmp_path / 'tie.jsonl'
    tie.write_text(json.dumps(record('e', v2=False, window=6, starters=6, current=20, construction=19, actual=20)))
    assert fit_v3(tie)['chosen_k'] == 3


def test_grade_reports_insufficient_below_the_floor_and_checks_every_gate():
    small = [record(f's{i}', v2=False, window=6, starters=2, current=8, construction=14, actual=15, week=i % 17 + 1)
             for i in range(10)]
    assert grade_v3(small, 4)['verdict'] == 'insufficient'

    def big(construction, con_attempts):
        return [record(f'b{i}', v2=False, window=6, starters=2, current=8, construction=construction + (i % 3) * 0.1,
                       actual=15, week=i % 17 + 1, season=2016 + i % 4, attempts=(20, con_attempts, 30))
                for i in range(GAP_MIN_TEST_GAMES)]
    passing = grade_v3(big(14, 31), 4)
    assert passing['verdict'] == 'pass' and all(passing['gates'].values())
    overshoot = grade_v3(big(14, 40), 4)                                      # +10 attempts bias breaks the guard
    assert overshoot['verdict'] == 'fail' and not overshoot['gates']['attempts_bias_within_5']
    worse = grade_v3(big(2, 31), 4)                                           # construction further from actual
    assert worse['verdict'] == 'fail' and not worse['gates']['gap_ci_upper_below_zero']
