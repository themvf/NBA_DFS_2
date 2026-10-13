from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from model.nhl_signal_audit import grade_prices, timing_evidence, GRADING_VERSION

NOW=datetime(2026,10,12,20,0,tzinfo=timezone.utc)

def fixture():
    alert={'sport':'nhl','id':1,'matchup_id':7,'side':'home','alert_type':'walking','trigger_history_id':12,
           'created_at':NOW,'commence_time':NOW+timedelta(hours=1),'alert_prob':.55,
           'details_json':{'exec_book':'fanduel','exec_decimal':2.0}}
    trigger={'id':12,'sport':'nhl','matchup_id':7,'captured_at':NOW-timedelta(minutes=3),
             'books':{'fanduel':{'ml_home':100,'ml_away':-120,'last_update':(NOW-timedelta(minutes=4)).isoformat()}}}
    close={'sport':'nhl','matchup_id':7,'history_matchup_id':7,'history_sport':'nhl','history_id':18,'history_row_id':18,
           'captured_at':NOW+timedelta(minutes=57),'history_captured_at':NOW+timedelta(minutes=57),
           'boundary_at':alert['commence_time'],'scheduled_start_at':alert['commence_time'],
           'boundary_source':'scheduled_nhl','quality':'A','primary_clv_eligible':True,'clv_cohort':'verified_clv_v1',
           'books':{'fanduel':{'ml_home':-150,'ml_away':130,'last_update':(NOW+timedelta(minutes=56)).isoformat()}}}
    return alert,trigger,close

def test_exact_book_close_and_separate_probability_clv():
    a,t,c=fixture(); g=grade_prices(a,t,c)
    assert g['grading_version']==GRADING_VERSION
    assert g['dk_close_decimal']==pytest.approx(1+100/150)
    assert g['dk_clv_pct']==pytest.approx(20)
    assert g['grading_json']['clv_pp']!=pytest.approx(20)
    assert g['grading_json']['timing']['quote_status']=='fresh'
    assert g['grading_json']['close_history_id']==18

def test_missing_selected_book_never_falls_back_to_history_or_other_book():
    a,t,c=fixture(); c['books']={'draftkings':deepcopy(c['books']['fanduel'])}
    older={**t,'id':15,'captured_at':NOW+timedelta(minutes=50)}
    g=grade_prices(a,t,c,[older])
    assert g['dk_clv_pct'] is None and g['dk_close_decimal'] is None
    assert g['grading_json']['close_status']=='CLOSE_UNAVAILABLE'
    assert g['grading_json']['close_source']=='event_closing_lines'

@pytest.mark.parametrize('change',[{'history_matchup_id':8},{'history_row_id':99},{'history_sport':'mlb'},
                                  {'primary_clv_eligible':False},{'quality':'stale'},
                                  {'captured_at':NOW+timedelta(hours=1)},
                                  {'scheduled_start_at':NOW+timedelta(hours=2)}])
def test_invalid_close_is_not_graded(change):
    a,t,c=fixture();c.update(change);g=grade_prices(a,t,c)
    assert g['dk_clv_pct'] is None and g['grading_json']['clv_pp'] is None
    assert g['grading_json']['close_status']=='INVALID_CLOSE'

def test_incomplete_pair_or_future_update_cannot_grade_price():
    for update in [{'ml_away':None},{'last_update':(NOW+timedelta(hours=2)).isoformat()}]:
        a,t,c=fixture();c['books']['fanduel'].update(update)
        assert grade_prices(a,t,c)['dk_clv_pct'] is None

def test_trigger_not_inferred_from_alert_creation_when_newer_capture_exists():
    a,t,c=fixture();t['captured_at']=NOW-timedelta(minutes=10)
    t['books']['fanduel']['last_update']=(NOW-timedelta(minutes=11)).isoformat()
    changed={**t,'id':13,'captured_at':NOW-timedelta(minutes=2),
             'books':{'fanduel':{'ml_home':-120,'ml_away':100}}}
    g=grade_prices(a,t,c,[changed])
    assert g['dk_survival_min']==8
    assert g['grading_json']['quote_changed_before_alert'] is True
    assert g['grading_json']['survival_time_basis']=='trigger_capture'

def test_unverified_entry_is_not_price_graded():
    a,t,c=fixture();a['details_json']['exec_decimal']=3
    g=grade_prices(a,t,c)
    assert not g['grading_json']['entry_verified'] and g['dk_clv_pct'] is None

def test_missing_close_preserves_timing_and_null_grade():
    a,t,c=fixture();g=grade_prices(a,t,None)
    assert g['dk_clv_pct'] is None and g['grading_json']['timing']['quote_fresh']

def test_future_trigger_cannot_supply_roi_or_clv_evidence():
    a,t,c=fixture();t['captured_at']=NOW+timedelta(minutes=1)
    g=grade_prices(a,t,c)
    assert not g['grading_json']['entry_verified']
    assert g['grading_json']['timing']['quote_status']=='invalid_time'
    assert g['dk_clv_pct'] is None and g['grading_json']['clv_pp'] is None

@pytest.mark.parametrize('capture,updated,decision,start,status',[
    (-3,-4,0,60,'fresh'),(-6,-6,0,60,'stale'),(-3,-9,0,60,'stale'),
    (1,0,0,60,'invalid_time'),(-3,-2,0,60,'invalid_time'),(-3,-4,60,60,'invalid_time'),
    (None,-4,0,60,'unknown'),(-5,-5,0,60,'fresh')])
def test_decision_time_freshness(capture,updated,decision,start,status):
    values=[NOW+timedelta(minutes=v) if v is not None else None for v in (capture,updated,decision,start)]
    e=timing_evidence(*values);assert e['quote_status']==status
    assert e['quote_fresh']==(status=='fresh') and e['execution_verified'] is False
