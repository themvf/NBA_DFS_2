from copy import deepcopy

import pytest

from research.nfl_longest_touchdown_publish import publish


def inputs():
    row={'identity':'known','name':'Known','longest_td_win_share':.4,
         'any_td_probability':.6,'td_20_plus_probability':.3,'td_40_plus_probability':.2}
    f={'version':'v2','authority':'unvalidated_exploratory','outcome_scope':'regulation_scrimmage',
       'decision_at':'2026-10-06T13:49:36+00:00','players':[row],
       'no_scrimmage_td_probability':.6,'unresolved_players':[{'identity':'new','name':'New'}],
       'game':{'game_id':'G'},'settings':{'draws':2000},'training_games':873,
       'market_inputs_used':False,'source_sha256':'s','request_sha256':'r','implementation_sha256':'i'}
    s={'decision_at':f['decision_at'],'support':[{'identity':'known','current_opportunities':5}]}
    return f,s


def test_publication_preserves_unknowns_and_original_forecast():
    f,s=inputs();original=deepcopy(f)
    result=publish(f,s)
    assert result['experimental'] and result['unresolved']==['New']
    assert result['players'][0]['longestShare']==.4 and f==original


@pytest.mark.parametrize('defect',['scope','authority','accounting','nan','market','support_time'])
def test_publication_rejects_invalid_or_mismatched_evidence(defect):
    f,s=inputs()
    if defect=='scope':f['outcome_scope']='full_game'
    if defect=='authority':f['authority']='validated'
    if defect=='accounting':f['no_scrimmage_td_probability']=.5
    if defect=='nan':f['no_scrimmage_td_probability']=float('nan')
    if defect=='market':f['market_inputs_used']=True
    if defect=='support_time':s['decision_at']='2026-10-07T13:49:36+00:00'
    with pytest.raises(ValueError):publish(f,s)


def test_zero_observed_role_is_not_published_as_a_named_zero_chance():
    f,s=inputs();s['support'][0]['current_opportunities']=0
    result=publish(f,s)
    assert not result['players'] and 'Known' in result['unresolved']
    assert result['residual'][0]['longestShare']==.4
