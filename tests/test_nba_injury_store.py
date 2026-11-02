from datetime import datetime,timezone,timedelta
import copy
import pytest
from research.nba_points_forward import digest
from research.nba_injury_store import prepared

def report():
 r={'version':'nba-injury-evidence-v1','source_url':'https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-11-02_04_30PM.pdf','published_at':'2026-11-02T21:30:00+00:00','observed_at':'2026-11-02T21:37:30+00:00','rows':[{'game_date':'2026-11-02','player':'Scottie Barnes','player_key':'scottiebarnes','matchup':'TOR@LAC','team':'Toronto Raptors','status':'Questionable','reason':'injury'}]};r['sha256']=digest(r);return r

def test_official_observation_and_canonical_matchup():
 r=report();before=copy.deepcopy(r);s=prepared(r);assert s[0]['away_key']=='torontoraptors';assert s[0]['home_key']=='losangelesclippers';assert r==before

def test_poisoned_digest_future_and_status():
 for field,value in [('source_url','https://example.com/report.pdf'),('observed_at',(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()),('observed_at','2026-11-02T21:00:00Z')]:
  r=report();r[field]=value;r['sha256']=digest({k:v for k,v in r.items() if k!='sha256'})
  with pytest.raises(ValueError):prepared(r)
 r=report();r['rows'][0]['status']='Active';r['sha256']=digest({k:v for k,v in r.items() if k!='sha256'})
 with pytest.raises(ValueError):prepared(r)
 r=report();r['rows'][0]['reason']='changed'
 with pytest.raises(ValueError):prepared(r)
