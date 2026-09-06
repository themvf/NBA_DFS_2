from datetime import datetime,timezone,timedelta
import pytest
from model.nfl_dfs_team_context import coaching_status,roster_role,opportunity

NOW=datetime(2026,9,6,tzinfo=timezone.utc)


def test_coaching_requires_all_roles_and_current_evidence():
    r={'season':2026,'checked_at':NOW.isoformat(),'head_coach_same':True,'coordinator_same':True}
    assert coaching_status(r,NOW,2026)=='partial_continuity'
    assert coaching_status({**r,'play_caller_same':True},NOW,2026)=='continuity_verified'
    assert coaching_status({**r,'head_coach_changed':True},NOW,2026)=='changed'
    assert coaching_status(r,NOW+timedelta(days=31),2026)=='unresolved'
    assert coaching_status(r,NOW,2027)=='unresolved'


def test_rookie_and_transfer_do_not_copy_shares():
    member={'team':'WSH','position':'WR','fetched_at':NOW,'sleeper':{'team':'WAS','position':'WR','status':'Active','depth_chart_order':1,'metadata':{'rookie_year':'2026'}}}
    r=roster_role(member,[],2026,NOW)
    assert r['rookie'] and r['evidence_current'] and not r['share_transferred']
    r=roster_role(member,['TB'],2026,NOW)
    assert r['new_team'] and r['opportunity_basis']=='Current-team role required'
    assert not roster_role(member,[],2026,NOW+timedelta(days=4))['evidence_current']


def test_budget_conserves_plays_and_excludes_sacks_scrambles_from_targets():
    r=opportunity(65,.4,.1,.1,.9,.5,.2)
    assert r['designed_runs']==26 and r['player_carries']==13
    assert r['pass_attempts']==pytest.approx(31.2)
    assert r['team_targets']==pytest.approx(28.08)
    assert r['designed_runs']+r['qb_scrambles']+r['sacks']+r['pass_attempts']==pytest.approx(65)
    assert r['player_targets']+r['other_targets']==r['team_targets']
    with pytest.raises(ValueError):opportunity(65,.4,.9,.2,1,.5,.2)


def test_play_profile_excludes_clock_and_penalty_plays():
    import pandas as pd
    from ingest.nfl_dfs_team_context import play_profile
    base={'game_id':'g','play_type':'pass','qb_kneel':0,'qb_spike':0,'two_point_attempt':0,'qb_dropback':1,'sack':0,'qb_scramble':0,'receiver_player_id':'wr'}
    frame=pd.DataFrame([base,{**base,'sack':1,'receiver_player_id':None},{**base,'qb_scramble':1,'play_type':'run','receiver_player_id':None},{**base,'play_type':'run','qb_dropback':0,'receiver_player_id':None},{**base,'qb_spike':1},{**base,'qb_kneel':1},{**base,'play_type':'no_play'}])
    p=play_profile(frame)
    assert p['plays']==4 and p['designed_run_rate']==.25
    assert p['scramble_rate']==pytest.approx(1/3) and p['target_rate']==1


def test_prior_shares_use_team_games_not_only_player_appearances():
    import pandas as pd
    from ingest.nfl_dfs_team_context import prior_role_shares
    base={'play_type':'pass','qb_kneel':0,'qb_spike':0,'two_point_attempt':0,'qb_dropback':1,'sack':0,'qb_scramble':0,'rusher_player_id':None}
    frame=pd.DataFrame([{**base,'game_id':str(w),'week':w,'receiver_player_id':'a' if w==9 else 'b'} for w in range(1,10)])
    r=prior_role_shares(frame)
    assert r['weeks']==list(range(2,10))
    assert r['players']['a']['target_share']==1/8
    assert sum(p['target_share'] for p in r['players'].values())==1
