from datetime import datetime, timedelta, timezone
import pytest
from model.nfl_dfs_target_share import allocate, forecast, pregame_availability, replay


def fixture():
    teams=[];history=[]
    for week in range(1,9):
        teams.append(dict(season=2025,week=week,team='A',attempts=40.,targets=36.))
        for pid,targets in [('one',8.),('two',6.),('three',4.)]:
            history.append(dict(season=2025,week=week,team='A',identity=pid,name=pid,position='WR',targets=targets,fpts=targets*2,game_id=str(week)))
    return history,teams


def test_budget_conservation_and_reserve():
    r=allocate({'a':.3,'b':.2},30,['a'])
    assert r['players']['a']['targets']==0
    assert r['players']['b']['targets']==pytest.approx(10.5)
    assert r['unallocated_targets']==pytest.approx(19.5)
    assert sum(p['targets'] for p in r['players'].values())+r['unallocated_targets']==pytest.approx(30)
    assert allocate({'a':.8,'b':.8},40)['unallocated_targets']==pytest.approx(0)
    assert allocate({'a':1},30,['a'])['unallocated_targets']==30
    with pytest.raises(ValueError): allocate({'a':float('nan')},30)


def test_forecast_cutoff_and_team_isolation():
    history,teams=fixture()
    expected=forecast(history,teams,'A',(2025,8))
    history[-1]['fpts']=9999;teams[-1]['attempts']=900
    history.append({**history[0],'team':'B','fpts':9999})
    assert forecast(history,teams,'A',(2025,8))==expected
    assert expected['target_budget']<=expected['attempts']
    assert forecast(history,teams,'A',(2025,3)) is None
    changed=forecast(history,teams,'A',(2025,8),['one'])
    assert changed['players']['one']['targets']==0
    assert changed['players']['two']['targets']>expected['players']['two']['targets']


def test_pregame_source_checks():
    now=datetime(2026,9,6,tzinfo=timezone.utc); kickoff=now+timedelta(days=1)
    member=dict(team='A',position='WR',fetched_at=now,sleeper=dict(team='A',position='WR',injury_status='Out'))
    assert pregame_availability(member,'A',now,kickoff)['out']
    for captured in [now+timedelta(seconds=1),now-timedelta(days=4),now.replace(tzinfo=None)]:
        assert pregame_availability({**member,'fetched_at':captured},'A',now,kickoff) is None
    assert pregame_availability(member,'B',now,kickoff) is None
    assert pregame_availability(member,'A',kickoff,kickoff) is None
    assert not pregame_availability({**member,'sleeper':dict(team='A',position='WR',injury_status='Questionable')},'A',now,kickoff)['out']


def test_replay_ranges_do_not_see_same_week_actuals():
    history,teams=fixture()
    # Enough independent player errors to warm up the positional distribution.
    many=[]
    for i in range(40):
        many.extend({**r,'identity':str(i)+r['identity']} for r in history)
    before,_=replay(many,teams)
    for r in many:
        if r['week']==8: r['fpts']+=10000
    after,_=replay(many,teams)
    assert before
    assert [r['candidate'] for r in before]==[r['candidate'] for r in after]
    assert all(r['candidate']['p10']<=r['candidate']['p50']<=r['candidate']['p90'] for r in before)


def test_team_aliases_preserve_pregame_identity():
    now=datetime(2026,9,6,tzinfo=timezone.utc)
    m=dict(team='WSH',position='WR',fetched_at=now,sleeper=dict(team='WAS',position='WR',status='Out'))
    assert pregame_availability(m,'WSH',now,now+timedelta(days=1))['out']
