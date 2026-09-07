import pytest

from model.nfl_archive_case_study import checked_points, normalized_name, solve_lineup
from model.nfl_dfs_historical import OFFENSE_FIELDS, HistoricalWeek, project_player


def test_scoring_bonus_thresholds_and_missing_are_not_zero():
    stats = dict.fromkeys(OFFENSE_FIELDS, 0)
    stats.update(passing_yards=300, rushing_yards=100, receiving_yards=100,
                 receptions=5, passing_interceptions=2, fumbles_lost_total=1)
    assert checked_points('QB', stats) == 43
    del stats['fumbles_lost_total']
    with pytest.raises(ValueError, match='fumbles_lost_total'):
        checked_points('QB', stats)


def entries():
    return [dict(player_id=i, player_name=str(i), position='WR', team_nflverse='A' if i<5 else 'B',
                 game_id='game', roster_slot=slot, salary=salary,
                 scoring_multiplier=1.5 if slot=='CPT' else 1,
                 actual=20-i, projection_mean=20-i)
            for i in range(7) for slot,salary in [('CPT',12000),('FLEX',7000)]]


def test_showdown_captain_salary_multiplier_distinct_and_team_constraint():
    result = solve_lineup(entries(), 'showdown', 'actual')
    assert result['salary'] == 47000
    assert result['actual_points'] == 115
    assert len({p['player_id'] for p in result['players']}) == 6
    assert next(p for p in result['players'] if p['roster_slot']=='CPT')['player_id'] == 0
    assert {p['team_nflverse'] for p in result['players']} == {'A','B'}


def test_unknown_actual_stays_unknown_in_projection_replay():
    pool = entries()
    for e in pool:
        if e['player_id']==0:
            e['actual'] = None
    result = solve_lineup(pool, 'showdown', 'projection_mean')
    assert result['actual_points'] is None
    assert result['unresolved_selected'] == ['0']
    actual = solve_lineup(pool, 'showdown', 'actual')
    assert 0 not in {p['player_id'] for p in actual['players']}


def test_classic_assigns_flex_once_and_needs_two_games():
    pool=[]
    for i,pos in enumerate(['QB','RB','RB','WR','WR','WR','TE','RB','DST','RB']):
        for slot in ([pos,'FLEX'] if pos in ('RB','WR','TE') else [pos]):
            pool.append(dict(player_id=i,player_name=str(i),position=pos,roster_slot=slot,
                salary=5000,scoring_multiplier=1,actual=10+i,
                team_nflverse='A',game_id='two' if i==9 else 'one'))
    result=solve_lineup(pool,'classic','actual')
    assert result['salary']==45000
    assert len({p['game_id'] for p in result['players']})==2
    assert len({p['player_id'] for p in result['players']})==9


def test_target_week_outcomes_cannot_change_projection():
    stats=dict.fromkeys(OFFENSE_FIELDS,0)
    stats['receiving_yards']=50
    before=[HistoricalWeek(1,'gsis','Player','WR',2025,w,'A','B',stats) for w in range(1,5)]
    future=HistoricalWeek(1,'gsis','Player','WR',2025,5,'A','B',dict(stats,receiving_yards=999))
    args=dict(player_id=1,player_gsis_id='gsis',player_name='Player',position='WR',
              cutoff_season=2025,cutoff_week=5,seed=202505)
    assert project_player(historical_rows=before,**args)==project_player(historical_rows=before+[future],**args)


def test_suffix_and_accents_normalize_without_fuzzy_matching():
    assert normalized_name('Brian Robinson Jr.')==normalized_name('Brian Robinson')
    assert normalized_name('Eddy Piñeiro')==normalized_name('Eddy Pineiro')
