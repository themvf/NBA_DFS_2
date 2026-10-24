from copy import deepcopy
from datetime import date, datetime, timedelta, timezone

import pytest

from model.nba_points_calibration import BoxGame, BoxPlayer
from research import nba_points_forward as forward
from research import nba_points_ghost as ghost


LOCK=datetime(2026,10,24,22,tzinfo=timezone.utc)
DAY=date(2026,10,24)


def inputs():
    source=[dict(event_id='e',game_date=DAY,commence_time=LOCK,captured_at=LOCK-timedelta(hours=3),
                 home_team_name='A',away_team_name='B',player=n,market='player_points',
                 books={'dk':{'line':line,'over':-110,'under':-110}}) for n,line in [('One',25.5),('Two',20.5),('Three',10.5)]]
    market=forward.build_snapshot(source,DAY,LOCK,LOCK-timedelta(hours=2))
    history=[dict(player_id=1,name='One',game_id=str(i),game_date='2026-04-%02d'%(i+1),
                  season='2025-26',season_type='Regular Season',team_id=1,minutes=30,points=35,
                  fetched_at=LOCK-timedelta(days=1)) for i in range(20)]
    return market,history


def test_future_or_late_history_never_changes_frozen_projection():
    market,history=inputs()
    created=LOCK-timedelta(hours=1)
    snap=ghost.freeze(market,history,created)
    poison=[{**history[0],'game_id':'future','game_date':'2026-10-24','points':500},
            {**history[0],'game_id':'late','points':500,'fetched_at':created},
            {**history[0],'game_id':'playoff','points':500,'season_type':'Playoffs'}]
    changed=ghost.freeze(market,history+poison,created)
    assert changed['sha256']==snap['sha256']
    one=next(c for c in snap['candidates'] if c['player']=='One')
    assert one['predictions']=={'market_line':25.5,'history20':35.,'blend90_10':26.45}
    assert next(c for c in snap['candidates'] if c['player']=='Two')['predictions']['history20'] is None
    history[0]['points']=1000
    assert next(c for c in ghost.freeze(market,history,created)['candidates'] if c['player']=='One')['predictions']['blend90_10']==27.5


def test_identity_ambiguity_and_duplicate_history_fail_closed():
    market,history=inputs()
    snap=ghost.freeze(market,history+[{**history[0],'player_id':2}],LOCK-timedelta(hours=1))
    one=next(c for c in snap['candidates'] if c['player']=='One')
    assert one['history_status']=='ambiguous_history' and one['predictions']['history20'] is None
    with pytest.raises(ValueError,match='duplicate'):
        ghost.freeze(market,history+[history[0]],LOCK-timedelta(hours=1))


def test_grading_uses_frozen_predictions_pushes_count_and_dnp_does_not():
    market,history=inputs()
    snap=ghost.freeze(market,history,LOCK-timedelta(hours=1))
    players=[BoxPlayer('One','one','A',True,True,25.5,30),BoxPlayer('Two','two','A',True,False,None,None),BoxPlayer('Three','three','B',False,True,50,30)]
    graded=ghost.grade(snap,[BoxGame('espn','A','B',players,LOCK)])
    assert graded['by_status']=={'push':1,'dnp':1,'graded':1}
    # Only One has history; the market comparator must use that exact same player.
    cell=graded['all_players']['history20']
    assert cell['ghost']['n']==cell['paired_market']['n']==1
    assert cell['ghost']['mae']==9.5 and cell['paired_market']['mae']==0
    tampered=deepcopy(snap);tampered['candidates'][0]['predictions']['history20']=999
    with pytest.raises(ValueError,match='digest'):ghost.grade(tampered,[])


def test_report_excludes_reconstruction_and_duplicate_dates():
    market,history=inputs()
    snap=ghost.freeze(market,history,LOCK+timedelta(minutes=1))
    grade=ghost.grade(snap,[])
    assert ghost.report([grade])['dates']==[]
    assert ghost.report([grade])['retrospective_dates_excluded']==['2026-10-24']
    grade=ghost.grade(ghost.freeze(market,history,LOCK-timedelta(minutes=1)),[])
    with pytest.raises(ValueError,match='multiple snapshots'):ghost.report([grade,grade])


def test_earlier_known_schedule_invalidates_common_lock():
    market,history=inputs()
    market['espn_scheduled_starts']=[{'start_time':(LOCK-timedelta(minutes=10)).isoformat()}]
    market['sha256']=forward.digest({k:v for k,v in market.items() if k!='sha256'})
    with pytest.raises(ValueError,match='earliest known'):
        ghost.freeze(market,history,LOCK-timedelta(hours=1))
    market,history=inputs()
    snap=ghost.freeze(market,history,LOCK-timedelta(hours=1))
    with pytest.raises(ValueError,match='verified scheduled'):
        ghost.grade(snap,[BoxGame('e','A','B',[],LOCK-timedelta(minutes=10))])
