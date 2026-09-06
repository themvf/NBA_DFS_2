from copy import deepcopy
import pytest
from model.nfl_dfs_role_context import forecast,replay


def fixture():
    teams=[{'season':2023,'week':w,'game_id':str(w),'team':'SF','targets':10,'primary_qb':'qb'} for w in range(1,7)]
    history=[{'season':2023,'week':w,'game_id':str(w),'team':'SF','identity':'wr','name':'WR','position':'WR','targets':5,'fpts':10} for w in [1,2,3,4,6]]
    return history,teams


def test_roles_include_zero_team_games_and_ignore_target_week():
    history,teams=fixture();r=forecast(history,teams,'SF',(2023,6))
    assert r['players']['wr']['share']==pytest.approx(.5*sum(.5**(i/4) for i in [1,2,3,4])/sum(.5**(i/4) for i in [0,1,2,3,4]))
    changed=deepcopy(history);changed[-1]['targets']=100;changed[-1]['fpts']=200
    assert forecast(changed,teams,'SF',(2023,6))==r
    teams[-1]['primary_qb']='future'
    assert forecast(history,teams,'SF',(2023,6))==r


def test_recent_qb_changes_are_observed_history_not_injury_inference():
    history,teams=fixture();teams[3]['primary_qb']='backup'
    assert forecast(history,teams,'SF',(2023,6))['qb_state']=='changed'
    teams[3]['primary_qb']=None
    assert forecast(history,teams,'SF',(2023,6))['qb_state']=='unknown'


def test_residuals_never_borrow_same_week_outcomes():
    history=[];teams=[]
    for w in range(1,18):
        teams.append({'season':2023,'week':w,'game_id':str(w),'team':'SF','targets':20,'primary_qb':'qb'})
        for n in range(12):history.append({'season':2023,'week':w,'game_id':str(w),'team':'SF','identity':str(n),'position':'WR','targets':1,'fpts':n+w%3})
    first,_=replay(history,teams)
    changed=deepcopy(history)
    for r in changed:
        if r['week']==17:r['fpts']+=1000
    second,_=replay(changed,teams)
    assert first and len(first)==len(second)
    assert [r['candidate'] for r in first]==[r['candidate'] for r in second]
    assert [r['boom_probability'] for r in first]==[r['boom_probability'] for r in second]

    assert [r['pooled_candidate'] for r in first]==[r['pooled_candidate'] for r in second]
