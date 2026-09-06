import pytest
from ingest.nfl_dfs_availability import validate_payload

def test_week_contract():
    assert validate_payload({'week':1,'injuries':[]},2026,1)==[]
    assert validate_payload({'injuries':[{'week':1,'team':'NO'}]},2026,1)
    for bad in [{}, {'injuries':None}, {'week':0,'injuries':[]}, {'injuries':[{'year':2025}]}, {'injuries':[None]}]:
        with pytest.raises((ValueError,TypeError)):
            validate_payload(bad,2026,1)

from datetime import datetime, timezone
from ingest.nfl_dfs_official_availability import validate_report

def test_official_report_contract():
    now=datetime(2026,9,13,16,tzinfo=timezone.utc)
    valid={'report_type':'inactive_list','url':'https://www.nfl.com/news/week-1-inactives','published_at':'2026-09-13T15:30:00Z','kickoff':'2026-09-13T17:00:00Z','week':1,'players':[{'gsis_id':'id','team':'NO','position':'WR','status':'INACTIVE'}]}
    validate_report(valid,now)
    for patch in [{'url':'https://nfl.com.example.org/article'}, {'published_at':'2026-09-13T18:00:00Z'},{'kickoff':'2026-09-13T14:00:00Z'},{'published_at':'2026-09-12T12:00:00Z'},{'players':[]},{'players':valid['players']*2}]:
        with pytest.raises(ValueError): validate_report({**valid,**patch},now)

def test_capture_contract_retains_limits():
    from ingest.nfl_dfs_availability import capture_contract
    contract=capture_contract(2026,1,{'week':1},{'injuries':[{'player_id':1}]},{'counts':{'matched':1},'decisions':[]},{'timestamp_timezone':'unverified'})
    assert contract.model_eligible is False
    assert contract.fallback_tier=='C' and contract.confidence_multiplier==0
    assert contract.matched_count==1 and contract.unmatched_count==0
    other=capture_contract(2026,2,{'week':2},{'injuries':[{'player_id':1}]},{'counts':{'matched':1},'decisions':[]},{})
    assert other.dataset!=contract.dataset # identical payloads cannot borrow another week's metadata

def test_capture_saves_only_resolved_observations(monkeypatch,tmp_path):
    import json
    from types import SimpleNamespace
    from ingest import nfl_dfs_availability as capture
    class DB:
        def execute(self,*args):
            return [{'id':1,'canonical_name':'Mike Woods','team_abbrev':'DEN','position':'WR','yahoo_id':'34158','fantasypros_player_id':None}]
        def close(self,error=False): assert not error
    class Client:
        def __init__(self,*args): pass
        def get(self,*args):
            return {'injuries':[{'player_id':2,'yahoo_id':'34158','name':'Michael Woods II','team_id':'DEN','position_id':'WR'}, {'player_id':3,'name':'Unmatched','team_id':'FA','position_id':'WR'}]}
    saved=[]
    monkeypatch.setenv('FANTASYPROS_API_KEY','unit-test-placeholder')
    monkeypatch.setattr(capture,'load_config',lambda:SimpleNamespace(database_url='unused'))
    monkeypatch.setattr(capture,'RefreshDatabase',lambda _:DB())
    monkeypatch.setattr(capture,'FantasyProsClient',Client)
    monkeypatch.setattr(capture,'persist_source_snapshot',lambda db,contract:7)
    monkeypatch.setattr(capture,'persist_injury_observation',lambda db,**kwargs:saved.append(kwargs))
    out=tmp_path/'report.json'
    monkeypatch.setattr('sys.argv',['capture','--season','2026','--week','1','--output',str(out)])
    capture.main()
    assert len(saved)==1 and saved[0]['player_id']==1 and saved[0]['reconcile_current'] is False
    report=json.loads(out.read_text());assert report['identity_audit']['counts']=={'matched':1,'provider_nonteam':1}
