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
