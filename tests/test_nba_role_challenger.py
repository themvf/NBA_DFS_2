import copy,hashlib,unittest
from pathlib import Path
from research.nba_points_forward import digest
from research.nba_points_role_challenger import adapt,record,grade,ROSTER_VERSION

def signed(d):return {**d,'sha256':digest(d)}
class RoleTests(unittest.TestCase):
 def setUp(self):
  self.p={'minutes':30.,'fga':10.,'fg3a':2.,'fta':4.,'points':13.,'conversion_rates':{'two':.5,'three':1/3,'free':.75}}
  self.c={'event_id':'e','player_key':'a','player':'A','home_team':'Boston Celtics','away_team':'Toronto Raptors','history_status':'ready','predictions':{'market_line':14.,'blend90_10':14.},'history':[{'game_date':f'2026-10-{n}','source_game_id':str(n),'source_player_id':'1','espn_athlete_id':'1','available_at':'2026-10-27T22:00:00Z','played':True,'minutes':30.,'fga':20.,'fg3a':4.,'fta':6.,'team':'Boston Celtics'} for n in [25,26,27]]}
  self.roster={'provider':'ESPN','version':ROSTER_VERSION,'observed_at':'2026-10-28T14:00:00Z','team':'Boston Celtics','espn_athlete_id':'1'}
 def a(self,**changes):return adapt(self.c,self.p,'2026-10-28','2026-10-28T15:00:00Z',{**self.roster,**changes})
 def test_arithmetic(self):
  before=copy.deepcopy(self.p);r=self.a();self.assertTrue(r['eligible']);self.assertTrue(r['conflict']);self.assertEqual(r['candidate']['minutes'],30);self.assertAlmostEqual(r['candidate']['points'],18.75);self.assertEqual(self.p,before)
 def test_roster_and_time(self):
  for change in [{'team':'Toronto Raptors'},{'espn_athlete_id':'2'},{'observed_at':'2026-10-28T16:00:00Z'},{'provider':'unknown'}]:self.assertFalse(self.a(**change)['eligible'])
 def test_bad_arithmetic(self):self.p['points']=20;self.assertEqual(self.a()['reason'],'base_arithmetic_mismatch')
 def test_zero_and_invalid_games(self):
  self.c['history'][0]['fga']=0;self.c['history'][0]['fg3a']=0;self.c['history'][0]['fta']=0;self.assertTrue(self.a()['eligible']);self.c['history'][0]['minutes']=0;self.c['history'][0]['fga']=1;self.assertFalse(self.a()['eligible'])
 def test_duplicate_and_missing(self):
  self.c['history'].append(self.c['history'][0]);self.assertEqual(self.a()['reason'],'ambiguous_history');self.c['history']=self.c['history'][:2];self.assertFalse(self.a()['eligible'])
 def fixtures(self):
  s=signed({'mode':'prospective','game_date':'2026-10-28','candidates':[self.c]});f=signed({'mode':'prospective','source_snapshot_sha256':s['sha256'],'model_sha256':'m','game_date':'2026-10-28','created_at':'2026-10-28T15:00:00Z','lock_at':'2026-10-28T23:00:00Z','rows':[{'event_id':'e','player_key':'a','prediction':self.p}]});p=signed({'model_sha256':'m','registered_at':'2026-10-27T00:00:00Z','code_sha256':hashlib.sha256(Path('research/nba_points_role_challenger.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest()});return s,f,p
 def test_registration_and_identity(self):
  s,f,p=self.fixtures();r=record(f,s,p,{'a':self.roster},'2026-10-28T15:01:00Z');self.assertIsNone(r['probabilities']);self.assertEqual(r['trios']['candidate'],None)
  with self.assertRaises(ValueError):record(f,s,p,{'a':self.roster},'2026-10-28T23:00:00Z')
  p=signed({**{k:v for k,v in p.items() if k!='sha256'},'registered_at':'2026-10-28T15:01:00Z'})
  with self.assertRaises(ValueError):record(f,s,p,{},'2026-10-28T15:02:00Z')
 def test_grading_and_dnp(self):
  s,f,p=self.fixtures();r=record(f,s,p,{'a':self.roster},'2026-10-28T15:01:00Z');d={'game_date':r['game_date'],'snapshot_sha256':s['sha256'],'rows':[{'event_id':'e','player_key':'a','status':'verified','actual':{'played':True,'points':20,'fga':18,'fg3a':4,'fta':4,'minutes':35}}]};g=grade(r,d);self.assertEqual(g['primary']['n'],1);self.assertAlmostEqual(g['primary']['points_mae']['candidate'],1.25)
  d['rows'][0]['status']='dnp';self.assertEqual(grade(r,d)['primary']['n'],0);self.assertIsNone(grade(r,d)['primary']['points_mae']['candidate'])
  d['snapshot_sha256']='wrong'
  with self.assertRaises(ValueError):grade(r,d)
if __name__=='__main__':unittest.main()
