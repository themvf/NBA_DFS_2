from copy import deepcopy
from datetime import datetime, timezone, timedelta
import pytest

from research.nhl_market_audit import verify_preservation, forward_report
from model import line_alerts

def test_repair_preserves_entries_and_prior_grade_fields():
    before={'alerts':[{'id':1,'details_json':{'exec_decimal':2},'side':'home','grading_version':'old'}],
            'grades':[{'id':3,'alert_id':1,'is_current':True,'grading_version':'old','outcome':'won'}]}
    after=deepcopy(before);after['alerts'][0]['grading_version']='new'
    after['grades'][0]['is_current']=False
    after['grades'].append({'id':4,'alert_id':1,'is_current':True,'grading_version':'new','outcome':'won'})
    assert verify_preservation(before,after)['new_grades']==1
    after['alerts'][0]['details_json']['exec_decimal']=3
    with pytest.raises(RuntimeError,match='entry'):verify_preservation(before,after)

def test_repair_rejects_old_grade_changes_and_duplicate_current_grades():
    before={'alerts':[{'id':1}],'grades':[{'id':3,'alert_id':1,'is_current':True,'outcome':'won'}]}
    after=deepcopy(before);after['grades'][0]['outcome']='lost'
    with pytest.raises(RuntimeError,match='Prior'):verify_preservation(before,after)
    after=deepcopy(before);after['grades'].append({'id':4,'alert_id':1,'is_current':True})
    with pytest.raises(RuntimeError,match='Multiple'):verify_preservation(before,after)

def test_forward_protocol_detects_threshold_drift(monkeypatch):
    monkeypatch.setattr(line_alerts,'_WALK_MIN_PP',1.5)
    with pytest.raises(RuntimeError,match='thresholds drifted'):
        forward_report(None,{'games':[],'alerts':[],'closes':[]})

def test_forward_cohort_keeps_no_signal_games_and_excludes_development():
    snap={'alerts':[],'closes':[],'games':[
        {'id':1,'nhl_game_id':100,'commence_time':datetime(2026,10,12,tzinfo=timezone.utc),'game_type':2,'schedule_state':'OK','completed':True},
        {'id':2,'nhl_game_id':101,'commence_time':datetime(2026,10,14,tzinfo=timezone.utc),'game_type':2,'schedule_state':'OK','completed':False}]}
    result=forward_report(None,snap)
    assert result['eligible_games']==1 and result['all_forward_alerts']==0
    assert result['game_coverage'][0]['close_history_id'] is None
    assert all(g['status']=='collecting' for g in result['families'].values())

def test_nhl_insert_freezes_timing_and_never_alters_other_sport_defaults():
    now=datetime.now(timezone.utc)
    class Db:
        def execute(self,sql,params):
            self.sql,self.params=sql,params
            return [{'id':1}]
    db=Db()
    r={'matchup_id':1,'game_date':'2026-10-12','commence_time':now+timedelta(hours=1),
       'capture_key':'k','history_id':9,'captured_at':now-timedelta(minutes=10),
       'books':{'fanduel':{'ml_home':100,'ml_away':-120,'last_update':now.isoformat()}}}
    alerts=line_alerts._insert(db,sport='nhl',r=r,label='A @ B',alert_type='walking',side='home',
                              alert_prob=.5,sharp_prob=None,details={'exec_decimal':2,'exec_book':'fanduel'})
    assert alerts[0]['details']['quote_status']=='invalid_time'
    assert alerts[0]['details']['execution_verified'] is False
    assert alerts[0]['details']['signal_version']=='nhl-moneyline-v1'
    assert db.params[-1] is not None
    assert alerts[0]['details']['alert_created_at']==db.params[-1].isoformat()
    line_alerts._insert(db,sport='soccer',r=r,label='A @ B',alert_type='walking',side='home',
                       alert_prob=.5,sharp_prob=None,details={})
    assert db.params[-1] is None

def test_nhl_live_messages_exclude_stale_quotes_and_do_not_claim_execution(monkeypatch):
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('DISCORD_WEBHOOK_URL','https://example.invalid')
    monkeypatch.delenv('TELEGRAM_BOT_TOKEN',raising=False)
    posted=[]
    monkeypatch.setattr(line_alerts.requests,'post',lambda *a,**kw:posted.append(kw['json']['content']))
    base={'sport':'nhl','matchup':'A @ B','alert_type':'walking','side':'home',
          'details':{'exec_book':'fanduel','exec_odds':100,'scheduled_start_at':(now+timedelta(hours=1)).isoformat(),
                     'trigger_capture_at':(now-timedelta(minutes=1)).isoformat(),'book_updated_at':(now-timedelta(minutes=2)).isoformat()}}
    stale=deepcopy(base);stale['details']['trigger_capture_at']=(now-timedelta(minutes=10)).isoformat()
    line_alerts._notify([base,stale])
    assert len(posted)==1 and 'execution not verified' in posted[0]
